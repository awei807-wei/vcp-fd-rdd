use std::cmp::Reverse;
use std::collections::{BTreeSet, HashMap};
use std::path::PathBuf;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use crate::core::FileKey;
use crate::stats::DirtyQueueStats;

const ROTATING_REPAIR_SLICE_DELAY: Duration = Duration::from_millis(10);

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct DirectoryFingerprint {
    pub(crate) file_key: FileKey,
    pub(crate) mtime_ns: i128,
    pub(crate) ctime_ns: i128,
    pub(crate) nlink: u64,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum DirtyScope {
    /// 无法定位具体目录（例如严重风暴/采样上限触发），按全局 dirty 处理。
    All {
        /// 上一次 fast-sync 完成时间（ns since epoch，best-effort）。
        cutoff_ns: u64,
    },
    /// 可定位到"可能丢事件"的目录集合（去重、有上限）。
    Dirs {
        /// 上一次 fast-sync 完成时间（ns since epoch，best-effort）。
        cutoff_ns: u64,
        dirs: Vec<PathBuf>,
    },
}

impl DirtyScope {
    pub fn dirs(cutoff_ns: u64, dirs: Vec<PathBuf>) -> Self {
        let dirs = normalize_dirs(dirs);
        if dirs.is_empty() {
            Self::All { cutoff_ns }
        } else {
            Self::Dirs { cutoff_ns, dirs }
        }
    }

    pub fn cutoff_ns(&self) -> u64 {
        match self {
            Self::All { cutoff_ns } | Self::Dirs { cutoff_ns, .. } => *cutoff_ns,
        }
    }

    pub fn dir_paths(&self) -> &[PathBuf] {
        match self {
            Self::All { .. } => &[],
            Self::Dirs { dirs, .. } => dirs.as_slice(),
        }
    }

    fn normalized(self) -> Self {
        match self {
            Self::All { cutoff_ns } => Self::All { cutoff_ns },
            Self::Dirs { cutoff_ns, dirs } => Self::dirs(cutoff_ns, dirs),
        }
    }

    fn expanded_for_retry(&self) -> Self {
        match self {
            Self::All { cutoff_ns } => Self::All {
                cutoff_ns: *cutoff_ns,
            },
            Self::Dirs { cutoff_ns, dirs } => {
                let expanded = dirs
                    .iter()
                    .map(|dir| dir.parent().unwrap_or(dir.as_path()).to_path_buf())
                    .collect::<Vec<_>>();
                Self::dirs(*cutoff_ns, expanded)
            }
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum DirtyReason {
    InotifyEvent,
    RecursiveSubtreeRepair,
    QueryHitStale,
    QueryMiss,
    PeriodicColdScan,
    RotatingColdWindow { cycle_id: u64 },
    FastScanBootstrapDir,
    FastScanChangedDir,
    StartupRepair,
    StartupRepairDeferred,
    OverflowRecovery,
}

impl DirtyReason {
    pub fn default_priority(self) -> DirtyPriority {
        match self {
            Self::OverflowRecovery => DirtyPriority::Critical,
            Self::QueryHitStale | Self::StartupRepair => DirtyPriority::High,
            Self::InotifyEvent
            | Self::RecursiveSubtreeRepair
            | Self::QueryMiss
            | Self::FastScanChangedDir => DirtyPriority::Normal,
            Self::PeriodicColdScan
            | Self::RotatingColdWindow { .. }
            | Self::FastScanBootstrapDir
            | Self::StartupRepairDeferred => DirtyPriority::Low,
        }
    }

    pub fn is_cold_scan(self) -> bool {
        matches!(
            self,
            Self::PeriodicColdScan | Self::RotatingColdWindow { .. }
        )
    }

    pub fn rotating_cold_window_cycle_id(self) -> Option<u64> {
        match self {
            Self::RotatingColdWindow { cycle_id } => Some(cycle_id),
            _ => None,
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum DirtyPriority {
    Low,
    Normal,
    High,
    Critical,
}

impl DirtyPriority {
    fn rank(self) -> u8 {
        match self {
            Self::Low => 0,
            Self::Normal => 1,
            Self::High => 2,
            Self::Critical => 3,
        }
    }

    fn max(self, other: Self) -> Self {
        if self.rank() >= other.rank() {
            self
        } else {
            other
        }
    }
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub(crate) struct DirtyRepairProgress {
    scanned: usize,
    changed: usize,
    elapsed_ms: u64,
    project_roots: BTreeSet<PathBuf>,
}

impl DirtyRepairProgress {
    pub(crate) fn accumulate(
        &mut self,
        scanned: usize,
        changed: usize,
        elapsed_ms: u64,
        project_roots: &[PathBuf],
    ) {
        self.scanned = self.scanned.saturating_add(scanned);
        self.changed = self.changed.saturating_add(changed);
        self.elapsed_ms = self.elapsed_ms.saturating_add(elapsed_ms);
        self.project_roots.extend(project_roots.iter().cloned());
    }

    pub(crate) fn into_parts(self) -> (usize, usize, u64, Vec<PathBuf>) {
        (
            self.scanned,
            self.changed,
            self.elapsed_ms,
            self.project_roots.into_iter().collect(),
        )
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DirtyRepairCursor {
    pub dir: PathBuf,
    pub offset: i64,
    pending_dirs: BTreeSet<PathBuf>,
    completed_dir_stamps: Vec<(PathBuf, DirectoryFingerprint)>,
    current_dir_start_stamp: Option<DirectoryFingerprint>,
    scan_invalidation_epoch: Option<u64>,
    progress: DirtyRepairProgress,
}

impl DirtyRepairCursor {
    pub fn new(dir: PathBuf, offset: i64) -> Self {
        Self {
            dir,
            offset,
            pending_dirs: BTreeSet::new(),
            completed_dir_stamps: Vec::new(),
            current_dir_start_stamp: None,
            scan_invalidation_epoch: None,
            progress: DirtyRepairProgress::default(),
        }
    }

    pub fn with_pending_dirs(dir: PathBuf, offset: i64, pending_dirs: Vec<PathBuf>) -> Self {
        Self::with_recursive_state(dir, offset, pending_dirs, Vec::new(), None)
    }

    pub(crate) fn with_recursive_state(
        dir: PathBuf,
        offset: i64,
        pending_dirs: Vec<PathBuf>,
        completed_dir_stamps: Vec<(PathBuf, DirectoryFingerprint)>,
        current_dir_start_stamp: Option<DirectoryFingerprint>,
    ) -> Self {
        Self {
            dir,
            offset,
            pending_dirs: pending_dirs.into_iter().collect(),
            completed_dir_stamps,
            current_dir_start_stamp,
            scan_invalidation_epoch: None,
            progress: DirtyRepairProgress::default(),
        }
    }

    pub fn pending_dirs(&self) -> Vec<PathBuf> {
        self.pending_dirs.iter().cloned().collect()
    }

    pub(crate) fn current_dir_start_stamp(&self) -> Option<DirectoryFingerprint> {
        self.current_dir_start_stamp
    }

    pub(crate) fn scan_invalidation_epoch(&self) -> Option<u64> {
        self.scan_invalidation_epoch
    }

    pub(crate) fn with_scan_invalidation_epoch(mut self, epoch: u64) -> Self {
        self.scan_invalidation_epoch = Some(epoch);
        self
    }

    pub(crate) fn into_recursive_collections(
        self,
    ) -> (
        PathBuf,
        BTreeSet<PathBuf>,
        Vec<(PathBuf, DirectoryFingerprint)>,
        DirtyRepairProgress,
    ) {
        (
            self.dir,
            self.pending_dirs,
            self.completed_dir_stamps,
            self.progress,
        )
    }

    pub(crate) fn from_recursive_collections(
        dir: PathBuf,
        offset: i64,
        pending_dirs: BTreeSet<PathBuf>,
        completed_dir_stamps: Vec<(PathBuf, DirectoryFingerprint)>,
        current_dir_start_stamp: Option<DirectoryFingerprint>,
        progress: DirtyRepairProgress,
    ) -> Self {
        Self {
            dir,
            offset,
            pending_dirs,
            completed_dir_stamps,
            current_dir_start_stamp,
            scan_invalidation_epoch: None,
            progress,
        }
    }

    fn pending_dir_count(&self) -> usize {
        self.pending_dirs.len().saturating_add(1)
    }

    fn tracked_path_count(&self) -> usize {
        self.pending_dir_count()
            .saturating_add(self.completed_dir_stamps.len())
            .saturating_add(self.progress.project_roots.len())
    }

    fn tracked_path_bytes(&self) -> u64 {
        let mut bytes = self.dir.as_os_str().as_encoded_bytes().len() as u64;
        bytes = bytes.saturating_add(
            self.pending_dirs
                .iter()
                .chain(self.completed_dir_stamps.iter().map(|(path, _)| path))
                .map(|path| path.as_os_str().as_encoded_bytes().len() as u64)
                .sum(),
        );
        bytes = bytes.saturating_add(
            self.progress
                .project_roots
                .iter()
                .map(|path| path.as_os_str().as_encoded_bytes().len() as u64)
                .sum(),
        );
        bytes
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DirtyQueueEntry {
    pub scope: DirtyScope,
    pub reason: DirtyReason,
    recursive_subtree_repair: bool,
    rotating_cold_window_cycle_id: Option<u64>,
    pace_repair_slices: bool,
    pub priority: DirtyPriority,
    pub first_enqueue_ns: u64,
    pub last_enqueue_ns: u64,
    pub not_before_ns: u64,
    pub attempts: u32,
    pub repair_cursor: Option<DirtyRepairCursor>,
}

impl DirtyQueueEntry {
    pub fn requires_recursive_subtree_repair(&self) -> bool {
        self.recursive_subtree_repair
    }

    pub fn rotating_cold_window_cycle_id(&self) -> Option<u64> {
        self.rotating_cold_window_cycle_id
    }

    pub(crate) fn clone_without_repair_cursor(&self) -> Self {
        Self {
            scope: self.scope.clone(),
            reason: self.reason,
            recursive_subtree_repair: self.recursive_subtree_repair,
            rotating_cold_window_cycle_id: self.rotating_cold_window_cycle_id,
            pace_repair_slices: self.pace_repair_slices,
            priority: self.priority,
            first_enqueue_ns: self.first_enqueue_ns,
            last_enqueue_ns: self.last_enqueue_ns,
            not_before_ns: self.not_before_ns,
            attempts: self.attempts,
            repair_cursor: None,
        }
    }
}

#[derive(Clone, Debug)]
pub struct DirtyQueue {
    debounce_ns: u64,
    rotating_repair_slice_delay_ns: u64,
    retry_base_delay_ns: u64,
    max_attempts: u32,
    entries: HashMap<DirtyScopeKey, DirtyQueueEntry>,
}

struct DirtyQueueRequest {
    scope: DirtyScope,
    reason: DirtyReason,
    recursive_subtree_repair: bool,
    rotating_cold_window_cycle_id: Option<u64>,
    pace_repair_slices: bool,
    priority: DirtyPriority,
    repair_cursor: Option<DirtyRepairCursor>,
    attempts: u32,
}

impl Default for DirtyQueue {
    fn default() -> Self {
        Self::new(Duration::from_millis(250))
    }
}

impl DirtyQueue {
    pub fn new(debounce: Duration) -> Self {
        Self {
            debounce_ns: duration_ns(debounce),
            rotating_repair_slice_delay_ns: duration_ns(ROTATING_REPAIR_SLICE_DELAY),
            retry_base_delay_ns: duration_ns(Duration::from_secs(1)),
            max_attempts: 3,
            entries: HashMap::new(),
        }
    }

    pub fn with_retry_policy(mut self, retry_base_delay: Duration, max_attempts: u32) -> Self {
        self.retry_base_delay_ns = duration_ns(retry_base_delay);
        self.max_attempts = max_attempts.max(1);
        self
    }

    pub fn len(&self) -> usize {
        self.entries.len()
    }

    pub fn count_by_reason(&self, reason: DirtyReason) -> usize {
        self.entries
            .values()
            .filter(|entry| entry.reason == reason)
            .count()
    }

    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    /// Returns how long the worker should wait until the earliest queued entry is ready.
    pub fn next_ready_delay(&self, now_ns: u64) -> Option<Duration> {
        self.entries
            .values()
            .map(|entry| entry.not_before_ns.saturating_sub(now_ns))
            .min()
            .map(Duration::from_nanos)
    }

    pub fn memory_stats(&self) -> DirtyQueueStats {
        use std::mem::size_of;

        let pending_scopes = self.entries.len();
        let map_capacity = self.entries.capacity();
        let mut pending_dirs = 0usize;
        let mut pending_path_bytes = 0u64;
        let mut cursor_tracked_paths = 0usize;
        let mut all_scope_pending = false;

        for entry in self.entries.values() {
            match &entry.scope {
                DirtyScope::All { .. } => {
                    all_scope_pending = true;
                }
                DirtyScope::Dirs { dirs, .. } => {
                    pending_dirs = pending_dirs.saturating_add(dirs.len());
                    pending_path_bytes = pending_path_bytes.saturating_add(path_bytes(dirs));
                }
            }
            if let Some(cursor) = &entry.repair_cursor {
                pending_dirs = pending_dirs.saturating_add(cursor.pending_dir_count());
                cursor_tracked_paths =
                    cursor_tracked_paths.saturating_add(cursor.tracked_path_count());
                pending_path_bytes = pending_path_bytes.saturating_add(cursor.tracked_path_bytes());
            }
        }

        let map_bytes = map_capacity as u64
            * (size_of::<(DirtyScopeKey, DirtyQueueEntry)>() as u64 + 1)
            + size_of::<HashMap<DirtyScopeKey, DirtyQueueEntry>>() as u64;
        let cursor_node_bytes = cursor_tracked_paths as u64
            * (size_of::<PathBuf>()
                + size_of::<FileKey>()
                + size_of::<i64>()
                + size_of::<usize>() * 3) as u64;
        let estimated_bytes = map_bytes
            .saturating_add(pending_path_bytes.saturating_mul(2))
            .saturating_add(cursor_node_bytes);

        DirtyQueueStats {
            pending_scopes,
            pending_dirs,
            all_scope_pending,
            pending_path_bytes,
            map_capacity,
            estimated_bytes,
        }
    }

    pub fn enqueue(
        &mut self,
        scope: DirtyScope,
        reason: DirtyReason,
        priority: DirtyPriority,
        now_ns: u64,
    ) {
        self.enqueue_request(
            DirtyQueueRequest {
                scope,
                reason,
                recursive_subtree_repair: reason == DirtyReason::RecursiveSubtreeRepair,
                rotating_cold_window_cycle_id: reason.rotating_cold_window_cycle_id(),
                pace_repair_slices: false,
                priority,
                repair_cursor: None,
                attempts: 0,
            },
            now_ns,
            true,
        );
    }

    pub fn enqueue_recursive(
        &mut self,
        scope: DirtyScope,
        reason: DirtyReason,
        priority: DirtyPriority,
        now_ns: u64,
    ) {
        self.enqueue_recursive_with_pacing(scope, reason, priority, now_ns, false);
    }

    pub(crate) fn enqueue_paced_recursive(
        &mut self,
        scope: DirtyScope,
        reason: DirtyReason,
        priority: DirtyPriority,
        now_ns: u64,
    ) {
        self.enqueue_recursive_with_pacing(scope, reason, priority, now_ns, true);
    }

    fn enqueue_recursive_with_pacing(
        &mut self,
        scope: DirtyScope,
        reason: DirtyReason,
        priority: DirtyPriority,
        now_ns: u64,
        pace_repair_slices: bool,
    ) {
        let cycle_id = reason.rotating_cold_window_cycle_id();
        let mut request = DirtyQueueRequest {
            scope: scope.normalized(),
            reason,
            recursive_subtree_repair: true,
            rotating_cold_window_cycle_id: cycle_id,
            pace_repair_slices,
            priority,
            repair_cursor: None,
            attempts: 0,
        };
        let request_dir = request.scope.dir_paths().first().cloned();
        if request.scope.dir_paths().len() == 1 {
            if let Some(ancestor_scope) = request_dir.as_ref().and_then(|dir| {
                self.entries.values().find_map(|entry| {
                    let existing_dirs = entry.scope.dir_paths();
                    (entry.recursive_subtree_repair
                        && entry.repair_cursor.is_none()
                        && entry.rotating_cold_window_cycle_id == cycle_id
                        && existing_dirs.len() == 1
                        && dir.starts_with(&existing_dirs[0]))
                    .then(|| entry.scope.clone())
                })
            }) {
                request.scope = ancestor_scope;
            } else if let Some(dir) = request_dir.as_ref() {
                let descendants = self
                    .entries
                    .iter()
                    .filter_map(|(key, entry)| {
                        let existing_dirs = entry.scope.dir_paths();
                        (entry.recursive_subtree_repair
                            && entry.repair_cursor.is_none()
                            && entry.rotating_cold_window_cycle_id == cycle_id
                            && existing_dirs.len() == 1
                            && existing_dirs[0].starts_with(dir))
                        .then(|| key.clone())
                    })
                    .collect::<Vec<_>>();
                for key in descendants {
                    if let Some(entry) = self.entries.remove(&key) {
                        request.reason = merge_reason(request.reason, entry.reason);
                        request.priority = request.priority.max(entry.priority);
                        request.attempts = request.attempts.max(entry.attempts);
                        request.pace_repair_slices &= entry.pace_repair_slices;
                    }
                }
            }
        }
        self.enqueue_request(request, now_ns, true);
    }

    pub fn enqueue_repair_slice(
        &mut self,
        scope: DirtyScope,
        source: &DirtyQueueEntry,
        now_ns: u64,
        cursor: DirtyRepairCursor,
    ) {
        let pace_rotating_scan = source.pace_repair_slices && source.priority == DirtyPriority::Low;
        self.enqueue_request_with_delay(
            DirtyQueueRequest {
                scope,
                reason: source.reason,
                recursive_subtree_repair: source.recursive_subtree_repair,
                rotating_cold_window_cycle_id: source.rotating_cold_window_cycle_id,
                pace_repair_slices: source.pace_repair_slices,
                priority: source.priority,
                repair_cursor: Some(cursor),
                attempts: source.attempts,
            },
            now_ns,
            pace_rotating_scan.then_some(self.rotating_repair_slice_delay_ns),
            pace_rotating_scan,
        );
    }

    fn enqueue_request(&mut self, request: DirtyQueueRequest, now_ns: u64, debounce: bool) {
        let delay_ns = debounce.then_some(self.debounce_ns);
        self.enqueue_request_with_delay(request, now_ns, delay_ns, false);
    }

    fn enqueue_request_with_delay(
        &mut self,
        request: DirtyQueueRequest,
        now_ns: u64,
        delay_ns: Option<u64>,
        pacing_delay: bool,
    ) {
        let scope = request.scope.normalized();
        let key = DirtyScopeKey::from_scope(&scope);
        let not_before_ns = now_ns.saturating_add(delay_ns.unwrap_or(0));
        self.entries
            .entry(key)
            .and_modify(|entry| {
                entry.reason = merge_reason(entry.reason, request.reason);
                entry.recursive_subtree_repair |= request.recursive_subtree_repair;
                entry.rotating_cold_window_cycle_id = merge_rotating_cycle_id(
                    entry.rotating_cold_window_cycle_id,
                    request.rotating_cold_window_cycle_id,
                );
                entry.pace_repair_slices &= request.pace_repair_slices;
                entry.priority = entry.priority.max(request.priority);
                entry.last_enqueue_ns = now_ns;
                let pacing_survives_merge =
                    entry.pace_repair_slices && entry.priority == DirtyPriority::Low;
                entry.not_before_ns = if pacing_delay && !pacing_survives_merge {
                    entry.not_before_ns.min(not_before_ns)
                } else if delay_ns.is_some() {
                    not_before_ns
                } else {
                    entry.not_before_ns.min(not_before_ns)
                };
                entry.attempts = entry.attempts.max(request.attempts);
                entry.repair_cursor =
                    merge_repair_cursor(entry.repair_cursor.take(), request.repair_cursor.clone());
            })
            .or_insert_with(|| DirtyQueueEntry {
                scope,
                reason: request.reason,
                recursive_subtree_repair: request.recursive_subtree_repair,
                rotating_cold_window_cycle_id: request.rotating_cold_window_cycle_id,
                pace_repair_slices: request.pace_repair_slices,
                priority: request.priority,
                first_enqueue_ns: now_ns,
                last_enqueue_ns: now_ns,
                not_before_ns,
                attempts: request.attempts,
                repair_cursor: request.repair_cursor,
            });
    }

    pub fn pop_ready(&mut self, now_ns: u64, limit: usize) -> Vec<DirtyQueueEntry> {
        if limit == 0 || self.entries.is_empty() {
            return Vec::new();
        }

        let mut ready = self
            .entries
            .iter()
            .filter_map(|(key, entry)| {
                if entry.not_before_ns <= now_ns {
                    Some((
                        Reverse(entry.priority.rank()),
                        entry.not_before_ns,
                        entry.last_enqueue_ns,
                        key.clone(),
                    ))
                } else {
                    None
                }
            })
            .collect::<Vec<_>>();
        ready.sort_by_key(|(priority, not_before, last_enqueue, key)| {
            (*priority, *not_before, *last_enqueue, key.sort_key())
        });

        let mut out = Vec::with_capacity(ready.len().min(limit));
        for (_, _, _, key) in ready.into_iter().take(limit) {
            if let Some(entry) = self.entries.remove(&key) {
                out.push(entry);
            }
        }
        out
    }

    pub fn retry(&mut self, mut entry: DirtyQueueEntry, now_ns: u64) -> bool {
        entry.attempts = entry.attempts.saturating_add(1);
        if entry.attempts > self.max_attempts {
            return false;
        }
        if !entry.requires_recursive_subtree_repair() {
            entry.scope = entry.scope.expanded_for_retry();
        }
        entry.repair_cursor = None;
        let delay = self
            .retry_base_delay_ns
            .saturating_mul(1u64 << entry.attempts.saturating_sub(1).min(16));
        entry.not_before_ns = now_ns.saturating_add(delay);
        entry.last_enqueue_ns = now_ns;
        let key = DirtyScopeKey::from_scope(&entry.scope);
        self.entries
            .entry(key)
            .and_modify(|existing| {
                existing.reason = merge_reason(existing.reason, entry.reason);
                existing.recursive_subtree_repair |= entry.recursive_subtree_repair;
                existing.rotating_cold_window_cycle_id = merge_rotating_cycle_id(
                    existing.rotating_cold_window_cycle_id,
                    entry.rotating_cold_window_cycle_id,
                );
                existing.pace_repair_slices &= entry.pace_repair_slices;
                existing.priority = existing.priority.max(entry.priority);
                existing.last_enqueue_ns = existing.last_enqueue_ns.max(entry.last_enqueue_ns);
                existing.not_before_ns = existing.not_before_ns.min(entry.not_before_ns);
                existing.attempts = existing.attempts.min(entry.attempts);
                existing.repair_cursor =
                    merge_repair_cursor(existing.repair_cursor.take(), entry.repair_cursor.clone());
            })
            .or_insert(entry);
        true
    }
}

pub fn now_ns() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos()
        .min(u128::from(u64::MAX)) as u64
}

fn normalize_dirs(mut dirs: Vec<PathBuf>) -> Vec<PathBuf> {
    dirs.retain(|dir| !dir.as_os_str().is_empty());
    dirs.sort();
    dirs.dedup();
    dirs
}

fn path_bytes(paths: &[PathBuf]) -> u64 {
    paths
        .iter()
        .map(|path| path.as_os_str().as_encoded_bytes().len() as u64)
        .sum()
}

fn duration_ns(duration: Duration) -> u64 {
    duration.as_nanos().min(u128::from(u64::MAX)) as u64
}

fn merge_reason(existing: DirtyReason, incoming: DirtyReason) -> DirtyReason {
    let existing_rank = existing.default_priority().rank();
    let incoming_rank = incoming.default_priority().rank();
    match incoming_rank.cmp(&existing_rank) {
        std::cmp::Ordering::Greater => incoming,
        std::cmp::Ordering::Less => existing,
        std::cmp::Ordering::Equal => match (existing, incoming) {
            (
                DirtyReason::RotatingColdWindow {
                    cycle_id: existing_cycle,
                },
                DirtyReason::RotatingColdWindow {
                    cycle_id: incoming_cycle,
                },
            ) => DirtyReason::RotatingColdWindow {
                cycle_id: existing_cycle.max(incoming_cycle),
            },
            (DirtyReason::RotatingColdWindow { .. }, _) => existing,
            (_, DirtyReason::RotatingColdWindow { .. }) => incoming,
            _ => incoming,
        },
    }
}

fn merge_rotating_cycle_id(existing: Option<u64>, incoming: Option<u64>) -> Option<u64> {
    match (existing, incoming) {
        (Some(existing), Some(incoming)) => Some(existing.max(incoming)),
        (Some(existing), None) => Some(existing),
        (None, Some(incoming)) => Some(incoming),
        (None, None) => None,
    }
}

fn merge_repair_cursor(
    existing: Option<DirtyRepairCursor>,
    incoming: Option<DirtyRepairCursor>,
) -> Option<DirtyRepairCursor> {
    match (existing, incoming) {
        // A full rescan request is safer than any partial cursor.
        (_, None) | (None, Some(_)) => None,
        (Some(existing), Some(incoming))
            if existing.dir == incoming.dir
                && existing.completed_dir_stamps == incoming.completed_dir_stamps
                && existing.current_dir_start_stamp == incoming.current_dir_start_stamp
                && existing.scan_invalidation_epoch == incoming.scan_invalidation_epoch
                && existing.progress == incoming.progress =>
        {
            let dir = existing.dir.clone();
            let offset = existing.offset.min(incoming.offset);
            let mut pending_dirs = existing.pending_dirs;
            pending_dirs.extend(incoming.pending_dirs);
            let mut merged = DirtyRepairCursor::from_recursive_collections(
                dir,
                offset,
                pending_dirs,
                existing.completed_dir_stamps,
                existing.current_dir_start_stamp,
                existing.progress,
            );
            merged.scan_invalidation_epoch = existing.scan_invalidation_epoch;
            Some(merged)
        }
        (Some(_), Some(_)) => None,
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
enum DirtyScopeKey {
    All,
    Dirs(Vec<PathBuf>),
}

impl DirtyScopeKey {
    fn from_scope(scope: &DirtyScope) -> Self {
        match scope {
            DirtyScope::All { .. } => Self::All,
            DirtyScope::Dirs { dirs, .. } => Self::Dirs(dirs.clone()),
        }
    }

    fn sort_key(&self) -> String {
        match self {
            Self::All => String::new(),
            Self::Dirs(dirs) => dirs
                .first()
                .map(|dir| dir.to_string_lossy().into_owned())
                .unwrap_or_default(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn dirty_queue_debounces_duplicate_scope() {
        let mut q = DirtyQueue::new(Duration::from_millis(10));
        let dir = PathBuf::from("/tmp/a");
        q.enqueue(
            DirtyScope::dirs(0, vec![dir.clone()]),
            DirtyReason::InotifyEvent,
            DirtyPriority::Normal,
            100_000_000,
        );
        q.enqueue(
            DirtyScope::dirs(0, vec![dir]),
            DirtyReason::QueryHitStale,
            DirtyPriority::High,
            105_000_000,
        );

        assert_eq!(q.len(), 1);
        assert!(q.pop_ready(114_999_999, 10).is_empty());
        let ready = q.pop_ready(115_000_000, 10);
        assert_eq!(ready.len(), 1);
        assert_eq!(ready[0].reason, DirtyReason::QueryHitStale);
        assert_eq!(ready[0].priority, DirtyPriority::High);
        assert!(ready[0].repair_cursor.is_none());
    }

    #[test]
    fn dirty_queue_schedules_higher_priority_first() {
        let mut q = DirtyQueue::new(Duration::ZERO);
        q.enqueue(
            DirtyScope::dirs(0, vec![PathBuf::from("/tmp/low")]),
            DirtyReason::PeriodicColdScan,
            DirtyPriority::Low,
            1,
        );
        q.enqueue(
            DirtyScope::dirs(0, vec![PathBuf::from("/tmp/high")]),
            DirtyReason::OverflowRecovery,
            DirtyPriority::Critical,
            2,
        );

        let ready = q.pop_ready(2, 10);
        assert_eq!(ready.len(), 2);
        assert_eq!(ready[0].priority, DirtyPriority::Critical);
        assert_eq!(ready[0].scope.dir_paths(), &[PathBuf::from("/tmp/high")]);
    }

    #[test]
    fn dirty_queue_preserves_latest_rotating_cycle_over_periodic_scan() {
        let mut q = DirtyQueue::new(Duration::ZERO);
        let scope = || DirtyScope::dirs(0, vec![PathBuf::from("/tmp/cold")]);
        q.enqueue(
            scope(),
            DirtyReason::PeriodicColdScan,
            DirtyPriority::Low,
            1,
        );
        q.enqueue(
            scope(),
            DirtyReason::RotatingColdWindow { cycle_id: 7 },
            DirtyPriority::Low,
            2,
        );
        q.enqueue(
            scope(),
            DirtyReason::RotatingColdWindow { cycle_id: 8 },
            DirtyPriority::Low,
            3,
        );
        q.enqueue(
            scope(),
            DirtyReason::PeriodicColdScan,
            DirtyPriority::Low,
            4,
        );

        let ready = q.pop_ready(4, 1);
        assert_eq!(
            ready[0].reason,
            DirtyReason::RotatingColdWindow { cycle_id: 8 }
        );
        assert_eq!(ready[0].rotating_cold_window_cycle_id(), Some(8));
    }

    #[test]
    fn dirty_queue_keeps_rotating_provenance_when_higher_priority_reason_wins() {
        let mut q = DirtyQueue::new(Duration::ZERO).with_retry_policy(Duration::ZERO, 2);
        let scope = || DirtyScope::dirs(0, vec![PathBuf::from("/tmp/cold")]);
        q.enqueue(
            scope(),
            DirtyReason::RotatingColdWindow { cycle_id: 9 },
            DirtyPriority::Low,
            1,
        );
        q.enqueue(scope(), DirtyReason::InotifyEvent, DirtyPriority::Normal, 2);

        let entry = q.pop_ready(2, 1).pop().unwrap();
        assert_eq!(entry.reason, DirtyReason::InotifyEvent);
        assert_eq!(entry.priority, DirtyPriority::Normal);
        assert_eq!(entry.rotating_cold_window_cycle_id(), Some(9));

        assert!(q.retry(entry, 3));
        let retry = q.pop_ready(3, 1).pop().unwrap();
        assert_eq!(retry.rotating_cold_window_cycle_id(), Some(9));
    }

    #[test]
    fn dirty_queue_preserves_recursive_repair_across_reason_merges_and_retry() {
        for recursive_first in [true, false] {
            let mut q = DirtyQueue::new(Duration::ZERO).with_retry_policy(Duration::ZERO, 2);
            let scope = || DirtyScope::dirs(0, vec![PathBuf::from("/tmp/cold-rename")]);
            let reasons = if recursive_first {
                [
                    DirtyReason::RecursiveSubtreeRepair,
                    DirtyReason::InotifyEvent,
                ]
            } else {
                [
                    DirtyReason::InotifyEvent,
                    DirtyReason::RecursiveSubtreeRepair,
                ]
            };
            for (now, reason) in reasons.into_iter().enumerate() {
                q.enqueue(scope(), reason, reason.default_priority(), now as u64 + 1);
            }

            let entry = q.pop_ready(3, 1).pop().unwrap();
            assert!(entry.requires_recursive_subtree_repair());
            assert!(q.retry(entry, 4));
            let retry = q.pop_ready(4, 1).pop().unwrap();
            assert!(retry.requires_recursive_subtree_repair());
        }
    }

    #[test]
    fn recursive_rotating_entry_keeps_cycle_provenance() {
        let mut q = DirtyQueue::new(Duration::ZERO);
        q.enqueue_recursive(
            DirtyScope::dirs(0, vec![PathBuf::from("/tmp/rotating-recursive")]),
            DirtyReason::RotatingColdWindow { cycle_id: 42 },
            DirtyPriority::Low,
            1,
        );

        let entry = q.pop_ready(1, 1).pop().unwrap();
        assert!(entry.requires_recursive_subtree_repair());
        assert_eq!(entry.rotating_cold_window_cycle_id(), Some(42));
    }

    #[test]
    fn recursive_queue_coalesces_same_cycle_ancestor_and_descendants() {
        let mut q = DirtyQueue::new(Duration::ZERO);
        let reason = DirtyReason::RotatingColdWindow { cycle_id: 42 };
        q.enqueue_recursive(
            DirtyScope::dirs(0, vec![PathBuf::from("/tmp/repo")]),
            reason,
            reason.default_priority(),
            1,
        );
        q.enqueue_recursive(
            DirtyScope::dirs(0, vec![PathBuf::from("/tmp/repo/src/deep")]),
            reason,
            reason.default_priority(),
            2,
        );

        assert_eq!(q.len(), 1);
        let entry = q.pop_ready(2, 1).pop().unwrap();
        assert_eq!(entry.scope.dir_paths(), [PathBuf::from("/tmp/repo")]);
        assert_eq!(entry.rotating_cold_window_cycle_id(), Some(42));
    }

    #[test]
    fn recursive_queue_replaces_same_cycle_descendants_with_later_ancestor() {
        let mut q = DirtyQueue::new(Duration::ZERO);
        let reason = DirtyReason::RecursiveSubtreeRepair;
        q.enqueue_recursive(
            DirtyScope::dirs(0, vec![PathBuf::from("/tmp/repo/src")]),
            reason,
            reason.default_priority(),
            1,
        );
        q.enqueue_recursive(
            DirtyScope::dirs(0, vec![PathBuf::from("/tmp/repo/tests")]),
            reason,
            reason.default_priority(),
            2,
        );
        q.enqueue_recursive(
            DirtyScope::dirs(0, vec![PathBuf::from("/tmp/repo")]),
            reason,
            reason.default_priority(),
            3,
        );

        assert_eq!(q.len(), 1);
        let entry = q.pop_ready(3, 1).pop().unwrap();
        assert_eq!(entry.scope.dir_paths(), [PathBuf::from("/tmp/repo")]);
    }

    #[test]
    fn recursive_queue_keeps_different_cycles_separate() {
        let mut q = DirtyQueue::new(Duration::ZERO);
        q.enqueue_recursive(
            DirtyScope::dirs(0, vec![PathBuf::from("/tmp/repo")]),
            DirtyReason::RotatingColdWindow { cycle_id: 41 },
            DirtyPriority::Low,
            1,
        );
        q.enqueue_recursive(
            DirtyScope::dirs(0, vec![PathBuf::from("/tmp/repo/src")]),
            DirtyReason::RotatingColdWindow { cycle_id: 42 },
            DirtyPriority::Low,
            2,
        );

        assert_eq!(q.len(), 2);
        let entries = q.pop_ready(2, 2);
        assert!(entries
            .iter()
            .any(|entry| entry.rotating_cold_window_cycle_id() == Some(41)));
        assert!(entries
            .iter()
            .any(|entry| entry.rotating_cold_window_cycle_id() == Some(42)));
    }

    #[test]
    fn recursive_repair_continuation_keeps_pending_dirs_and_cycle_after_merge_and_retry() {
        let mut q = DirtyQueue::new(Duration::ZERO).with_retry_policy(Duration::ZERO, 2);
        let root = PathBuf::from("/tmp/rotating-recursive");
        let scope = || DirtyScope::dirs(0, vec![root.clone()]);
        q.enqueue_recursive(
            scope(),
            DirtyReason::RotatingColdWindow { cycle_id: 42 },
            DirtyPriority::Low,
            1,
        );
        q.enqueue(scope(), DirtyReason::InotifyEvent, DirtyPriority::Normal, 2);

        let entry = q.pop_ready(2, 1).pop().unwrap();
        assert_eq!(entry.reason, DirtyReason::InotifyEvent);
        assert!(entry.requires_recursive_subtree_repair());
        assert_eq!(entry.rotating_cold_window_cycle_id(), Some(42));

        q.enqueue_repair_slice(
            scope(),
            &entry,
            3,
            DirtyRepairCursor::with_pending_dirs(root.join("child"), 0, vec![root.join("sibling")]),
        );

        let continuation = q.pop_ready(3, 1).pop().unwrap();
        assert!(continuation.requires_recursive_subtree_repair());
        assert_eq!(continuation.rotating_cold_window_cycle_id(), Some(42));
        let cursor = continuation.repair_cursor.as_ref().unwrap();
        assert_eq!(cursor.dir, root.join("child"));
        assert_eq!(cursor.pending_dirs(), vec![root.join("sibling")]);

        assert!(q.retry(continuation, 4));
        let retry = q.pop_ready(4, 1).pop().unwrap();
        assert!(retry.requires_recursive_subtree_repair());
        assert_eq!(retry.rotating_cold_window_cycle_id(), Some(42));
        assert_eq!(retry.scope.dir_paths(), std::slice::from_ref(&root));
        assert!(retry.repair_cursor.is_none());

        q.enqueue_repair_slice(
            retry.scope.clone(),
            &retry,
            5,
            DirtyRepairCursor::new(root, 512),
        );
        let retried_continuation = q.pop_ready(5, 1).pop().unwrap();
        assert_eq!(retried_continuation.attempts, 1);
    }

    #[test]
    fn rotating_cold_window_continuation_is_paced_below_normal_debounce() {
        let mut q = DirtyQueue::new(Duration::from_millis(250));
        let root = PathBuf::from("/tmp/paced-rotating-continuation");
        let scope = DirtyScope::dirs(0, vec![root.clone()]);
        let start_ns = 1_000_000_000;
        q.enqueue_paced_recursive(
            scope.clone(),
            DirtyReason::RotatingColdWindow { cycle_id: 7 },
            DirtyPriority::Low,
            start_ns,
        );

        assert!(q.pop_ready(start_ns + 249_999_999, 1).is_empty());
        let source = q.pop_ready(start_ns + 250_000_000, 1).pop().unwrap();
        let continuation_ns = start_ns + 250_000_001;
        q.enqueue_repair_slice(
            scope,
            &source,
            continuation_ns,
            DirtyRepairCursor::new(root, 512),
        );

        assert!(q.pop_ready(continuation_ns + 9_999_999, 1).is_empty());
        assert_eq!(
            q.next_ready_delay(continuation_ns),
            Some(Duration::from_millis(10))
        );
        let continuation = q.pop_ready(continuation_ns + 10_000_000, 1).pop().unwrap();
        assert_eq!(continuation.repair_cursor.unwrap().offset, 512);
    }

    #[test]
    fn event_merge_disables_rotating_continuation_pacing() {
        let mut q = DirtyQueue::new(Duration::ZERO);
        let root = PathBuf::from("/tmp/event-promoted-rotating-continuation");
        let scope = DirtyScope::dirs(0, vec![root.clone()]);
        q.enqueue_paced_recursive(
            scope.clone(),
            DirtyReason::RotatingColdWindow { cycle_id: 7 },
            DirtyPriority::Low,
            1,
        );
        q.enqueue(
            scope.clone(),
            DirtyReason::InotifyEvent,
            DirtyPriority::Normal,
            2,
        );

        let source = q.pop_ready(2, 1).pop().unwrap();
        assert_eq!(source.reason, DirtyReason::InotifyEvent);
        q.enqueue_repair_slice(scope, &source, 3, DirtyRepairCursor::new(root, 512));

        let continuation = q.pop_ready(3, 1).pop().unwrap();
        assert_eq!(continuation.repair_cursor.unwrap().offset, 512);
    }

    #[test]
    fn paced_continuation_does_not_delay_an_already_ready_event() {
        let mut q = DirtyQueue::new(Duration::ZERO);
        let root = PathBuf::from("/tmp/ready-event-before-paced-continuation");
        let scope = DirtyScope::dirs(0, vec![root.clone()]);
        q.enqueue_paced_recursive(
            scope.clone(),
            DirtyReason::RotatingColdWindow { cycle_id: 7 },
            DirtyPriority::Low,
            1,
        );
        let paced_source = q.pop_ready(1, 1).pop().unwrap();
        q.enqueue(
            scope.clone(),
            DirtyReason::InotifyEvent,
            DirtyPriority::Normal,
            2,
        );

        q.enqueue_repair_slice(scope, &paced_source, 3, DirtyRepairCursor::new(root, 512));

        let merged = q.pop_ready(3, 1).pop().unwrap();
        assert_eq!(merged.reason, DirtyReason::InotifyEvent);
        assert_eq!(merged.priority, DirtyPriority::Normal);
    }

    #[test]
    fn dirty_queue_memory_stats_counts_pending_scopes() {
        let mut q = DirtyQueue::new(Duration::ZERO);
        q.enqueue(
            DirtyScope::dirs(0, vec![PathBuf::from("/tmp/a"), PathBuf::from("/tmp/b")]),
            DirtyReason::InotifyEvent,
            DirtyPriority::Normal,
            1,
        );
        q.enqueue(
            DirtyScope::All { cutoff_ns: 0 },
            DirtyReason::OverflowRecovery,
            DirtyPriority::Critical,
            2,
        );

        let stats = q.memory_stats();
        assert_eq!(stats.pending_scopes, 2);
        assert_eq!(stats.pending_dirs, 2);
        assert!(stats.all_scope_pending);
        let expected_path_bytes = "/tmp/a".len() + "/tmp/b".len();
        assert!(stats.pending_path_bytes >= expected_path_bytes as u64);
        assert!(stats.map_capacity >= stats.pending_scopes);
        assert!(stats.estimated_bytes >= stats.pending_path_bytes);
    }

    #[test]
    fn dirty_queue_memory_stats_counts_recursive_cursor_paths() {
        let mut q = DirtyQueue::new(Duration::ZERO);
        let root = PathBuf::from("/tmp/recursive-root");
        let scope = DirtyScope::dirs(0, vec![root.clone()]);
        q.enqueue_recursive(
            scope.clone(),
            DirtyReason::RecursiveSubtreeRepair,
            DirtyPriority::Normal,
            1,
        );
        let entry = q.pop_ready(1, 1).pop().unwrap();
        q.enqueue_repair_slice(
            scope,
            &entry,
            2,
            DirtyRepairCursor::with_pending_dirs(
                root.join("current"),
                0,
                vec![root.join("pending")],
            ),
        );

        let stats = q.memory_stats();
        assert_eq!(stats.pending_dirs, 3);
        assert!(stats.pending_path_bytes >= 3 * root.as_os_str().as_encoded_bytes().len() as u64);
    }

    #[test]
    fn dirty_queue_retries_with_parent_scope() {
        let mut q = DirtyQueue::new(Duration::ZERO).with_retry_policy(Duration::from_millis(5), 2);
        let leaf = PathBuf::from("/tmp/a/b");
        q.enqueue(
            DirtyScope::dirs(0, vec![leaf]),
            DirtyReason::QueryHitStale,
            DirtyPriority::High,
            10_000_000,
        );

        let entry = q.pop_ready(10_000_000, 1).pop().unwrap();
        assert!(q.retry(entry, 20_000_000));
        assert_eq!(q.len(), 1);
        assert!(q.pop_ready(24_999_999, 1).is_empty());
        let retry = q.pop_ready(25_000_000, 1).pop().unwrap();
        assert_eq!(retry.attempts, 1);
        assert_eq!(retry.scope.dir_paths(), &[PathBuf::from("/tmp/a")]);
        assert!(retry.repair_cursor.is_none());

        assert!(q.retry(retry, 30_000_000));
        let second = q.pop_ready(40_000_000, 1).pop().unwrap();
        assert_eq!(second.scope.dir_paths(), &[PathBuf::from("/tmp")]);
        assert!(!q.retry(second, 50_000_000));
    }

    #[test]
    fn dirty_queue_keeps_partial_repair_cursor_until_full_scan_arrives() {
        let mut q = DirtyQueue::new(Duration::ZERO);
        let dir = PathBuf::from("/tmp/sliced");
        q.enqueue(
            DirtyScope::dirs(0, vec![dir.clone()]),
            DirtyReason::PeriodicColdScan,
            DirtyPriority::Low,
            0,
        );
        let source = q.pop_ready(0, 1).pop().unwrap();
        let mut source_12 = source.clone();
        source_12.rotating_cold_window_cycle_id = Some(12);

        q.enqueue_repair_slice(
            DirtyScope::dirs(0, vec![dir.clone()]),
            &source_12,
            1,
            DirtyRepairCursor::new(dir.clone(), 200),
        );
        let mut source_13 = source.clone();
        source_13.rotating_cold_window_cycle_id = Some(13);
        q.enqueue_repair_slice(
            DirtyScope::dirs(0, vec![dir.clone()]),
            &source_13,
            2,
            DirtyRepairCursor::new(dir.clone(), 100),
        );

        let entry = q.pop_ready(2, 1).pop().unwrap();
        assert_eq!(
            entry.repair_cursor,
            Some(DirtyRepairCursor::new(dir.clone(), 100))
        );
        assert_eq!(entry.rotating_cold_window_cycle_id(), Some(13));

        let mut source_14 = source;
        source_14.rotating_cold_window_cycle_id = Some(14);
        q.enqueue_repair_slice(
            DirtyScope::dirs(0, vec![dir.clone()]),
            &source_14,
            3,
            DirtyRepairCursor::new(dir.clone(), 300),
        );
        q.enqueue(
            DirtyScope::dirs(0, vec![dir]),
            DirtyReason::InotifyEvent,
            DirtyPriority::Normal,
            4,
        );

        let full = q.pop_ready(4, 1).pop().unwrap();
        assert!(full.repair_cursor.is_none());
        assert_eq!(full.reason, DirtyReason::InotifyEvent);
        assert_eq!(full.rotating_cold_window_cycle_id(), Some(14));
    }
}
