use std::cmp::Reverse;
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use crate::stats::DirtyQueueStats;

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
    QueryHitStale,
    QueryMiss,
    PeriodicColdScan,
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
            Self::InotifyEvent | Self::QueryMiss | Self::FastScanChangedDir => {
                DirtyPriority::Normal
            }
            Self::PeriodicColdScan | Self::StartupRepairDeferred => DirtyPriority::Low,
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

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DirtyQueueEntry {
    pub scope: DirtyScope,
    pub reason: DirtyReason,
    pub priority: DirtyPriority,
    pub first_enqueue_ns: u64,
    pub last_enqueue_ns: u64,
    pub not_before_ns: u64,
    pub attempts: u32,
}

#[derive(Clone, Debug)]
pub struct DirtyQueue {
    debounce_ns: u64,
    retry_base_delay_ns: u64,
    max_attempts: u32,
    entries: HashMap<DirtyScopeKey, DirtyQueueEntry>,
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

    pub fn memory_stats(&self) -> DirtyQueueStats {
        use std::mem::size_of;

        let pending_scopes = self.entries.len();
        let map_capacity = self.entries.capacity();
        let mut pending_dirs = 0usize;
        let mut pending_path_bytes = 0u64;
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
        }

        let map_bytes = map_capacity as u64
            * (size_of::<(DirtyScopeKey, DirtyQueueEntry)>() as u64 + 1)
            + size_of::<HashMap<DirtyScopeKey, DirtyQueueEntry>>() as u64;
        let estimated_bytes = map_bytes.saturating_add(pending_path_bytes.saturating_mul(2));

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
        let scope = scope.normalized();
        let key = DirtyScopeKey::from_scope(&scope);
        let not_before_ns = now_ns.saturating_add(self.debounce_ns);
        self.entries
            .entry(key)
            .and_modify(|entry| {
                entry.reason = merge_reason(entry.reason, reason);
                entry.priority = entry.priority.max(priority);
                entry.last_enqueue_ns = now_ns;
                entry.not_before_ns = not_before_ns;
            })
            .or_insert_with(|| DirtyQueueEntry {
                scope,
                reason,
                priority,
                first_enqueue_ns: now_ns,
                last_enqueue_ns: now_ns,
                not_before_ns,
                attempts: 0,
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
        entry.scope = entry.scope.expanded_for_retry();
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
                existing.priority = existing.priority.max(entry.priority);
                existing.last_enqueue_ns = existing.last_enqueue_ns.max(entry.last_enqueue_ns);
                existing.not_before_ns = existing.not_before_ns.min(entry.not_before_ns);
                existing.attempts = existing.attempts.min(entry.attempts);
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
    if incoming.default_priority().rank() >= existing.default_priority().rank() {
        incoming
    } else {
        existing
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

#[allow(dead_code)]
fn _path_parent(path: &Path) -> Option<PathBuf> {
    path.parent().map(Path::to_path_buf)
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

        assert!(q.retry(retry, 30_000_000));
        let second = q.pop_ready(40_000_000, 1).pop().unwrap();
        assert_eq!(second.scope.dir_paths(), &[PathBuf::from("/tmp")]);
        assert!(!q.retry(second, 50_000_000));
    }
}
