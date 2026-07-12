use std::collections::{HashMap, HashSet};
use std::ffi::OsString;
use std::path::{Path, PathBuf};
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::{Duration, Instant, UNIX_EPOCH};

use crate::config::L3ScanPolicy;
use crate::core::{EventRecord, EventType, FileIdentifier, FileKey, FileKind, FileMeta, Task};
use crate::event::sync::{
    now_ns, DirtyPriority, DirtyQueueEntry, DirtyReason, DirtyRepairCursor, DirtyScope,
};
use crate::fs_policy::FsPolicy;
use crate::index::l2_partition::{mtime_to_ns, PersistentIndex};
use crate::index::PathFreshness;
use crate::io_governor::IoGovernor;
use crate::util::{maybe_trim_rss, path_has_excluded_component};

use super::{
    directory_manifest::{DirectoryManifestBuilder, DirectoryManifestSummary},
    pathbuf_from_bytes, DirtyProcessReport, DirtyScanOutcome, ScanOutcome, StartupRepairStats,
    TieredIndex,
};

const REPAIR_SLICE_MAX_ENTRIES: usize = 512;
const REPAIR_SLICE_MAX_MS: u64 = 20;

/// readdir 批量删除对齐（v2）。
///
/// 将候选 `(doc_id, path)` 条目按 parent dir 分组，每个 dirty 目录做一次
/// `std::fs::read_dir` 构建 `HashSet<OsString>`，与索引文件名做差集：文件名不在
/// readdir 结果中即视为已删除。`read_dir` 失败（目录不存在/不可读）时该目录下所有
/// 索引条目都标记删除。
///
/// 内存权衡：`HashSet<OsString>` 在每个 parent dir 处理完后立即 drop，不跨目录累积。
/// 30 万文件目录的 HashSet 峰值约 20-30MB，远小于逐文件 stat 的 15-60s 开销。
pub(super) fn readdir_delete_alignment(
    to_delete: Vec<(u64, PathBuf)>,
    io_governor: Option<&IoGovernor>,
) -> Vec<PathBuf> {
    // 按 parent dir 分组，每个 dirty 目录构建一次 name set，循环外不累积。
    let mut by_parent: HashMap<PathBuf, Vec<(u64, PathBuf)>> = HashMap::new();
    for (doc_id, path) in to_delete {
        if let Some(parent) = path.parent() {
            by_parent
                .entry(parent.to_path_buf())
                .or_default()
                .push((doc_id, path));
        }
    }

    let mut deleted: Vec<PathBuf> = Vec::new();
    for (parent_dir, files) in by_parent {
        // 每个 dirty 目录构建一次 name set，用完立即 drop（作用域在本循环迭代内）。
        if let Some(gov) = io_governor {
            gov.before_io();
        }
        let current_names: Option<HashSet<OsString>> = match std::fs::read_dir(&parent_dir) {
            Ok(entries) => Some(
                entries
                    .filter_map(|e| e.ok().map(|e| e.file_name()))
                    .collect(),
            ),
            Err(_) => None, // 目录不存在或不可读 → 全部标记删除
        };

        for (_doc_id, path) in files {
            let should_delete = match &current_names {
                None => true, // 目录不可读，索引中所有文件视为已删除
                Some(names) => {
                    // 文件名不在 readdir 结果中 = 被删除；OsString 直接比对，支持非 UTF-8。
                    path.file_name()
                        .map(|name| !names.contains(name))
                        .unwrap_or(false)
                }
            };
            if should_delete {
                deleted.push(path);
            }
        }
        // current_names 在此处 drop，避免跨目录累积内存峰值。
    }
    deleted
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum RebuildAdmission {
    StartNow,
    Scheduled(Duration),
    Coalesced,
}

#[derive(Debug)]
struct BudgetedScanOutcome {
    outcome: ScanOutcome,
    budget_exhausted: bool,
    dropped_stale_batch: bool,
}

#[derive(Debug)]
struct SlicedScanOutcome {
    outcome: ScanOutcome,
    manifest_skipped: bool,
    completed: bool,
    next_cursor: Option<DirtyRepairCursor>,
    dropped_stale_batch: bool,
}

#[derive(Clone, Debug)]
struct DirChildEntry {
    path: PathBuf,
}

fn visit_dirs_since(
    roots: &[PathBuf],
    ignore_prefixes: &[PathBuf],
    exclude_dirs: &[String],
    cutoff_ns: u64,
    log_prefix: &str,
    io_governor: &IoGovernor,
    mut on_dir: impl FnMut(&std::path::Path, bool) -> bool,
) -> bool {
    use std::time::Duration;

    let cutoff = UNIX_EPOCH
        + Duration::new(
            cutoff_ns / 1_000_000_000,
            (cutoff_ns % 1_000_000_000) as u32,
        );

    let should_skip = |p: &std::path::Path| -> bool {
        ignore_prefixes
            .iter()
            .any(|ig| !ig.as_os_str().is_empty() && p.starts_with(ig))
    };

    let mut stack: Vec<PathBuf> = roots.to_vec();
    while let Some(dir) = stack.pop() {
        if should_skip(&dir) {
            continue;
        }
        if path_has_excluded_component(&dir, exclude_dirs) {
            continue;
        }

        io_governor.before_io();
        let md = match std::fs::symlink_metadata(&dir) {
            Ok(m) => m,
            Err(_) => continue,
        };
        if !md.is_dir() {
            continue;
        }

        let changed = if let Ok(modified) = md.modified() {
            cutoff_ns == 0 || modified > cutoff
        } else {
            true // 保守地认为已变化（部分文件系统不支持 mtime）
        };
        if on_dir(&dir, changed) {
            return true;
        }

        io_governor.before_io();
        let rd = match std::fs::read_dir(&dir) {
            Ok(rd) => rd,
            Err(e) => {
                // 权限/竞态等错误不应导致"永远判 stale"；保守地跳过不可读子树。
                tracing::debug!(
                    "{} mtime crawl: skip unreadable dir {:?}: {}",
                    log_prefix,
                    dir,
                    e
                );
                continue;
            }
        };
        for ent in rd {
            let ent = match ent {
                Ok(e) => e,
                Err(_) => continue,
            };
            let ft = match ent.file_type() {
                Ok(ft) => ft,
                Err(_) => continue,
            };
            if ft.is_dir() {
                stack.push(ent.path());
            }
        }
    }
    false
}

fn collect_dirs_changed_since(
    roots: &[PathBuf],
    ignore_prefixes: &[PathBuf],
    exclude_dirs: &[String],
    cutoff_ns: u64,
    io_governor: &IoGovernor,
) -> Vec<PathBuf> {
    let mut out: Vec<PathBuf> = Vec::new();
    visit_dirs_since(
        roots,
        ignore_prefixes,
        exclude_dirs,
        cutoff_ns,
        "fast-sync",
        io_governor,
        |dir, changed| {
            if changed {
                out.push(dir.to_path_buf());
            }
            false
        },
    );

    out.sort();
    out.dedup();
    out
}

struct ReadDirSlice {
    entries: Vec<DirChildEntry>,
    completed: bool,
    next_offset: Option<i64>,
}

// telldir/seekdir behavior across closedir/opendir cycles is filesystem-dependent:
// some filesystems (e.g. NFS, FUSE) may invalidate or reorder cookies after the
// directory handle is closed and reopened. This is acceptable for cold repair because
// the repair loop only requires eventual consistency -- missed or duplicated entries
// are corrected by subsequent repair passes, and the hot watcher provides a fallback
// for actively changing directories.
#[cfg(unix)]
fn read_dir_slice(
    dir: &Path,
    start_offset: i64,
    max_entries: usize,
    max_elapsed: Duration,
) -> std::io::Result<ReadDirSlice> {
    use std::ffi::{CStr, CString};
    use std::os::unix::ffi::{OsStrExt, OsStringExt};

    struct DirHandle(*mut libc::DIR);

    impl Drop for DirHandle {
        fn drop(&mut self) {
            unsafe {
                libc::closedir(self.0);
            }
        }
    }

    let start = Instant::now();
    let mut entries = Vec::new();
    let mut completed = true;
    let c_path = CString::new(dir.as_os_str().as_bytes())
        .map_err(|_| std::io::Error::new(std::io::ErrorKind::InvalidInput, "path has NUL byte"))?;
    let raw = unsafe { libc::opendir(c_path.as_ptr()) };
    if raw.is_null() {
        return Err(std::io::Error::last_os_error());
    }
    let handle = DirHandle(raw);
    if start_offset > 0 {
        unsafe {
            libc::seekdir(handle.0, start_offset as libc::c_long);
        }
    }
    let mut next_offset = start_offset;

    loop {
        if entries.len() >= max_entries || start.elapsed() >= max_elapsed {
            completed = false;
            break;
        }
        let entry = unsafe { libc::readdir(handle.0) };
        if entry.is_null() {
            break;
        }
        let name = unsafe { CStr::from_ptr((*entry).d_name.as_ptr()) }.to_bytes();
        if name == b"." || name == b".." {
            next_offset = unsafe { libc::telldir(handle.0) as i64 };
            continue;
        }
        let name = OsString::from_vec(name.to_vec());
        entries.push(DirChildEntry {
            path: dir.join(&name),
        });
        next_offset = unsafe { libc::telldir(handle.0) as i64 };
    }

    Ok(ReadDirSlice {
        entries,
        completed,
        next_offset: (!completed).then_some(next_offset),
    })
}

#[cfg(not(unix))]
fn read_dir_slice(
    dir: &Path,
    start_offset: i64,
    max_entries: usize,
    max_elapsed: Duration,
) -> std::io::Result<ReadDirSlice> {
    let start = Instant::now();
    let mut entries = Vec::new();
    let mut skipped = 0i64;
    let mut completed = true;

    for child in std::fs::read_dir(dir)? {
        if start.elapsed() >= max_elapsed {
            completed = false;
            break;
        }
        if skipped < start_offset {
            skipped = skipped.saturating_add(1);
            continue;
        }
        if entries.len() >= max_entries || start.elapsed() >= max_elapsed {
            completed = false;
            break;
        }
        let child = child?;
        entries.push(DirChildEntry { path: child.path() });
    }

    let consumed = start_offset.saturating_add(entries.len() as i64);
    Ok(ReadDirSlice {
        entries,
        completed,
        next_offset: (!completed).then_some(consumed),
    })
}

fn should_skip_dirty_dir(
    dir: &std::path::Path,
    ignore_prefixes: &[PathBuf],
    exclude_dirs: &[String],
) -> bool {
    ignore_prefixes
        .iter()
        .any(|ig| !ig.as_os_str().is_empty() && dir.starts_with(ig))
        || path_has_excluded_component(dir, exclude_dirs)
}

fn project_root_for_marker(path: &Path, markers: &[String]) -> Option<PathBuf> {
    if markers.is_empty() {
        return None;
    }
    let file_name = path.file_name()?.to_string_lossy();
    if markers.iter().any(|marker| marker == file_name.as_ref()) {
        return path.parent().map(Path::to_path_buf);
    }
    None
}

fn path_has_hidden_component_after_root(path: &Path, root: &Path) -> bool {
    path.strip_prefix(root)
        .unwrap_or(path)
        .components()
        .any(|component| match component {
            std::path::Component::Normal(name) => name.to_string_lossy().starts_with('.'),
            _ => false,
        })
}

#[derive(Debug, Default)]
pub(crate) struct FastSyncReport {
    pub(crate) dirs_scanned: usize,
    pub(crate) upsert_events: usize,
    pub(crate) delete_events: usize,
}

impl TieredIndex {
    #[cfg(test)]
    pub(super) fn try_start_rebuild_force(&self) -> bool {
        let mut st = self.rebuild_state.lock();
        if st.in_progress {
            return false;
        }
        st.in_progress = true;
        st.requested = false;
        st.scheduled = false;
        st.last_started_at = Some(Instant::now());
        true
    }

    pub(super) fn reserve_rebuild_with_cooldown(&self, reason: &'static str) -> RebuildAdmission {
        let mut st = self.rebuild_state.lock();
        if self.is_shutting_down() {
            st.requested = false;
            st.scheduled = false;
            tracing::debug!("Rebuild request rejected during shutdown ({})", reason);
            return RebuildAdmission::Coalesced;
        }
        if self.rebuild_snapshot_pending.load(Ordering::Acquire) {
            tracing::debug!(
                "Rebuild request coalesced into owned generation awaiting snapshot ({})",
                reason
            );
            return RebuildAdmission::Coalesced;
        }
        st.requested = true;

        if st.in_progress {
            tracing::debug!(
                "Rebuild merge: already in progress, coalescing ({})",
                reason
            );
            return RebuildAdmission::Coalesced;
        }

        let now = Instant::now();
        if let Some(last) = st.last_started_at {
            let elapsed = now.saturating_duration_since(last);
            let cooldown =
                Duration::from_secs(self.rebuild_cooldown_secs.load(Ordering::Relaxed).max(1));
            if elapsed < cooldown {
                if st.scheduled {
                    tracing::debug!(
                        "Rebuild merge: cooldown already scheduled, coalescing ({})",
                        reason
                    );
                    return RebuildAdmission::Coalesced;
                }

                let wait = cooldown - elapsed;
                st.scheduled = true;
                return RebuildAdmission::Scheduled(wait);
            }
        }

        // 立即开始：复位合并标记。
        st.in_progress = true;
        st.requested = false;
        st.scheduled = false;
        st.last_started_at = Some(now);
        RebuildAdmission::StartNow
    }

    fn try_start_rebuild_with_cooldown(self: &Arc<Self>, reason: &'static str) -> bool {
        match self.reserve_rebuild_with_cooldown(reason) {
            RebuildAdmission::StartNow => {
                self.run_rebuild_background(reason);
                true
            }
            RebuildAdmission::Scheduled(wait) => {
                let idx = self.clone();
                std::thread::spawn(move || {
                    std::thread::sleep(wait);
                    let _ = idx.try_start_rebuild_with_cooldown("cooldown elapsed (merged)");
                });
                false
            }
            RebuildAdmission::Coalesced => false,
        }
    }

    pub(super) fn finish_rebuild(self: &Arc<Self>, new_l2: Arc<PersistentIndex>) -> bool {
        enum FinishStep {
            Complete,
            Apply(Vec<EventRecord>),
            RetryIncomplete,
            WaitForReaders,
            RetrySharedGeneration,
        }

        let _event_boundary = self.snapshot_event_gate.lock();
        let mut new_l2 = Some(new_l2);
        let mut shared_generation_waits = 0usize;
        loop {
            let step = {
                // Global lock order is snapshot_event_gate →
                // rebuild_state → delta_buffer. The dual lock makes the replay
                // completeness check and base/L2 switch indivisible.
                let mut st = self.rebuild_state.lock();
                let mut db = self.delta_buffer.lock();
                let structural_replay_unproven = db.structural_replay_unproven();
                let subtree_requires_retry = if db.has_subtree_invalidations() {
                    let prefixes = db.snapshot_subtree_invalidations();
                    new_l2
                        .as_ref()
                        .expect("rebuild generation is present")
                        .snapshot_prefixes_match_descendants(&prefixes)
                } else {
                    false
                };
                if !db.is_complete() || structural_replay_unproven || subtree_requires_retry {
                    // A path was rejected after this rebuild's start boundary.
                    // A directory rename/recreate or a subtree invalidated
                    // after it was scanned is likewise unproven. Retain
                    // delta+WAL and retry instead of publishing stale or
                    // missing descendants. Exact file deletes have no
                    // descendants and are replayed below without restarting.
                    self.l2.store(Arc::new(PersistentIndex::new_with_roots(
                        self.roots.clone(),
                    )));
                    self.invalidate_memory_report_cache();
                    st.in_progress = false;
                    st.requested = false;
                    st.scheduled = false;
                    FinishStep::RetryIncomplete
                } else {
                    db.clear_subtree_invalidations();
                    if !db.is_empty() {
                        let mut events: Vec<EventRecord> = db.live_records().cloned().collect();
                        for path_bytes in db.deleted_paths() {
                            let path = pathbuf_from_bytes(path_bytes);
                            events.push(EventRecord {
                                seq: 0,
                                timestamp: std::time::SystemTime::UNIX_EPOCH,
                                event_type: EventType::Delete,
                                id: FileIdentifier::Path(path.clone()),
                                path_hint: Some(path),
                            });
                        }
                        events.sort_by_key(|e| e.seq);
                        db.clear();
                        FinishStep::Apply(events)
                    } else {
                        // 切换点：持锁判空 -> 原子切换，避免丢事件窗口。
                        self.l1.clear();
                        let current_l2 = self.l2.load_full();
                        if Arc::ptr_eq(
                            &current_l2,
                            new_l2.as_ref().expect("rebuild generation is present"),
                        ) {
                            self.l2.store(Arc::new(PersistentIndex::new_with_roots(
                                self.roots.clone(),
                            )));
                        }
                        drop(current_l2);
                        match Arc::try_unwrap(new_l2.take().expect("rebuild generation is present"))
                        {
                            Ok(generation) => {
                                self.note_pending_flush_rebuild(&generation);
                                let generation_stats = generation.memory_stats();
                                *self.pending_snapshot_generation.lock() = Some(generation);
                                {
                                    let mut telemetry = self.owned_snapshot_telemetry.lock();
                                    telemetry.lifecycle = super::OwnedSnapshotLifecycle::Pending;
                                    telemetry.l2 = generation_stats;
                                }
                                db.finish_full_rebuild_generation();
                                self.rebuild_snapshot_pending.store(true, Ordering::Release);
                                self.invalidate_memory_report_cache();
                                if !self.flush_requested.swap(true, Ordering::AcqRel) {
                                    self.flush_notify.notify_one();
                                }
                                st.in_progress = false;
                                // A complete scan plus complete boundary replay subsumes
                                // requests coalesced while this generation was building.
                                st.requested = false;
                                st.scheduled = false;
                                FinishStep::Complete
                            }
                            Err(shared) => {
                                new_l2 = Some(shared);
                                if shared_generation_waits < 500 {
                                    FinishStep::WaitForReaders
                                } else {
                                    st.in_progress = false;
                                    st.requested = false;
                                    st.scheduled = false;
                                    FinishStep::RetrySharedGeneration
                                }
                            }
                        }
                    }
                }
            };

            match step {
                FinishStep::Complete => {
                    self.mark_rebuild_recovery_complete();
                    return false;
                }
                FinishStep::Apply(batch) => new_l2
                    .as_ref()
                    .expect("rebuild generation is present")
                    .apply_events(&batch),
                FinishStep::RetryIncomplete => {
                    tracing::warn!(
                        "rebuild replay generation incomplete or subtree-stale; retaining delta/WAL and retrying"
                    );
                    return true;
                }
                FinishStep::WaitForReaders => {
                    shared_generation_waits += 1;
                    std::thread::sleep(Duration::from_millis(10));
                }
                FinishStep::RetrySharedGeneration => {
                    tracing::warn!(
                        "rebuild generation remained shared at publication; retaining WAL and retrying"
                    );
                    return true;
                }
            }
        }
    }

    fn run_rebuild_background(self: &Arc<Self>, reason: &'static str) {
        let idx = self.clone();
        std::thread::spawn(move || {
            idx.set_current_thread_idle_io_priority_for_scan();
            let strategy = {
                let mut sched = idx.scheduler.lock();
                sched.adjust_parallelism();
                sched.select_strategy(&Task::ColdBuild {
                    total_dirs: idx.roots.len(),
                })
            };

            tracing::warn!(
                "Starting background rebuild: {} (strategy={:?})",
                reason,
                strategy
            );
            let new_l2 = Arc::new(PersistentIndex::new_with_roots(idx.roots.clone()));
            {
                let _snapshot_boundary = idx.snapshot_event_gate.lock();
                idx.delta_buffer.lock().begin_full_rebuild_generation();
                idx.l2.store(new_l2.clone());
                idx.invalidate_memory_report_cache();
            }
            idx.l3.full_build_with_strategy(&new_l2, strategy);
            let again = idx.finish_rebuild(new_l2);
            tracing::warn!("Rebuild complete, triggering manual RSS trim...");
            maybe_trim_rss();
            tracing::warn!(
                "Background rebuild complete: {} files",
                idx.base.load_full().file_count()
            );
            if again {
                let _ = idx.try_start_rebuild_with_cooldown("merged rebuild request after rebuild");
            }
        });
    }

    /// 后台全量构建
    pub fn spawn_full_build(self: &Arc<Self>) {
        if !self.try_start_rebuild_with_cooldown("full build requested") {
            tracing::debug!("Background full build request coalesced or scheduled");
        }
    }

    /// overflow 兜底：dirty region + cooldown/max-staleness 触发后执行一次 fast-sync（best-effort）。
    ///
    /// 设计目标：
    /// - 避免 "overflow → 立刻全盘 rebuild" 在风暴中触发大分配/高水位；
    /// - 允许查询短暂陈旧，但不阻塞查询、不 OOM；
    /// - fast-sync 以"目录为单位"做对齐：只需要 read_dir + 必要的 metadata，不假设 mtime 冒泡。
    pub fn spawn_fast_sync(self: &Arc<Self>, scope: DirtyScope, ignore_prefixes: Vec<PathBuf>) {
        let permit = match self.fast_sync_semaphore.clone().try_acquire_owned() {
            Ok(p) => p,
            Err(_) => {
                tracing::debug!("Fast-sync already in progress, skipping duplicate spawn");
                return;
            }
        };

        let idx = self.clone();
        std::thread::spawn(move || {
            idx.set_current_thread_idle_io_priority_for_scan();
            let _permit = permit;
            let report = idx.fast_sync(scope, &ignore_prefixes);
            tracing::warn!(
                "Fast-sync complete: dirs={} upserts={} deletes={}",
                report.dirs_scanned,
                report.upsert_events,
                report.delete_events
            );
            tracing::warn!("Fast-sync complete, triggering manual RSS trim...");
            maybe_trim_rss();
        });
    }

    pub fn enqueue_dirty(&self, scope: DirtyScope, reason: DirtyReason) {
        self.enqueue_dirty_with_priority(scope, reason, reason.default_priority());
    }

    pub fn enqueue_dirty_dirs(&self, dirs: Vec<PathBuf>, reason: DirtyReason) {
        if dirs.is_empty() {
            return;
        }
        self.enqueue_dirty(DirtyScope::dirs(now_ns(), dirs), reason);
    }

    pub fn set_cold_sweep_period_estimate_from_tiered_policy(
        &self,
        l2_scan_interval_secs: u64,
        l3_scan_policy: L3ScanPolicy,
        l3_scan_interval_secs: u64,
    ) {
        let mut estimate = l2_scan_interval_secs.max(1);
        if l3_scan_policy.schedules_periodic_scan() {
            estimate = estimate.max(l3_scan_interval_secs.max(1));
        }
        self.set_cold_sweep_period_estimate(estimate);
    }

    pub fn enqueue_dirty_with_priority(
        &self,
        scope: DirtyScope,
        reason: DirtyReason,
        priority: DirtyPriority,
    ) {
        {
            let mut queue = self.dirty_queue.lock();
            queue.enqueue(scope, reason, priority, now_ns());
        }
        self.dirty_notify.notify_one();
    }

    pub fn dirty_queue_len(&self) -> usize {
        self.dirty_queue.lock().len()
    }

    pub fn deferred_repair_queue_len(&self) -> usize {
        self.dirty_queue
            .lock()
            .count_by_reason(DirtyReason::StartupRepairDeferred)
    }

    pub fn enqueue_startup_deferred_repair(&self) {
        let report = self.recovery_status().report;
        if !report.deferred_repair {
            return;
        }
        if report.deferred_unknown_scope {
            self.enqueue_dirty_dirs(self.roots.clone(), DirtyReason::StartupRepairDeferred);
        }
        if !report.deferred_dirty_dirs.is_empty() {
            self.enqueue_dirty_dirs(
                report.deferred_dirty_dirs,
                DirtyReason::StartupRepairDeferred,
            );
        }
    }

    pub fn dirty_queue_ready_batch(&self, limit: usize) -> Vec<DirtyQueueEntry> {
        self.dirty_queue.lock().pop_ready(now_ns(), limit)
    }

    pub fn retry_dirty_entry(&self, entry: DirtyQueueEntry) -> bool {
        let retry = {
            let mut queue = self.dirty_queue.lock();
            queue.retry(entry, now_ns())
        };
        if retry {
            self.dirty_notify.notify_one();
        }
        retry
    }

    pub async fn wait_for_dirty_queue(&self) {
        self.dirty_notify.notified().await;
    }

    pub fn process_dirty_entry(
        &self,
        entry: DirtyQueueEntry,
        ignore_prefixes: &[PathBuf],
    ) -> DirtyProcessReport {
        self.process_dirty_entry_with_project_markers(entry, ignore_prefixes, &[])
    }

    pub fn process_dirty_entry_with_project_markers(
        &self,
        entry: DirtyQueueEntry,
        ignore_prefixes: &[PathBuf],
        project_markers: &[String],
    ) -> DirtyProcessReport {
        self.process_dirty_entry_with_project_markers_and_manifest_skip_dirs(
            entry,
            ignore_prefixes,
            project_markers,
            &HashSet::new(),
        )
    }

    pub fn process_dirty_entry_with_project_markers_and_manifest_skip_dirs(
        &self,
        entry: DirtyQueueEntry,
        ignore_prefixes: &[PathBuf],
        project_markers: &[String],
        manifest_skip_dirs: &HashSet<PathBuf>,
    ) -> DirtyProcessReport {
        let mut report = DirtyProcessReport {
            entries_processed: 1,
            ..DirtyProcessReport::default()
        };

        match &entry.scope {
            DirtyScope::All { .. } => {
                let sync = self.fast_sync(entry.scope.clone(), ignore_prefixes);
                report.dirs_scanned = sync.dirs_scanned;
                report.fast_sync_upserts = sync.upsert_events;
                report.fast_sync_deletes = sync.delete_events;
                report.changed = sync.upsert_events.saturating_add(sync.delete_events);
                return report;
            }
            DirtyScope::Dirs { dirs, .. } => {
                if entry.reason == DirtyReason::StartupRepairDeferred {
                    let sync = self.fast_sync(entry.scope.clone(), ignore_prefixes);
                    report.dirs_scanned = sync.dirs_scanned;
                    report.fast_sync_upserts = sync.upsert_events;
                    report.fast_sync_deletes = sync.delete_events;
                    report.changed = sync.upsert_events.saturating_add(sync.delete_events);
                    return report;
                }
                if entry.reason == DirtyReason::FastScanChangedDir {
                    let sync = self.fast_sync(entry.scope.clone(), ignore_prefixes);
                    report.dirs_scanned = sync.dirs_scanned;
                    report.fast_sync_upserts = sync.upsert_events;
                    report.fast_sync_deletes = sync.delete_events;
                    report.changed = sync.upsert_events.saturating_add(sync.delete_events);
                    for dir in entry.scope.dir_paths() {
                        report.outcomes.push(DirtyScanOutcome {
                            dir: dir.clone(),
                            outcome: ScanOutcome {
                                scanned: sync.dirs_scanned,
                                changed: report.changed,
                                elapsed_ms: 0,
                                project_roots: Vec::new(),
                            },
                            reason: entry.reason,
                            manifest_skipped: false,
                        });
                    }
                    return report;
                }
                let mut had_failed_dir = false;
                for dir in dirs {
                    if should_skip_dirty_dir(dir, ignore_prefixes, &self.exclude_dirs) {
                        continue;
                    }
                    match std::fs::symlink_metadata(dir) {
                        Ok(meta) if meta.is_dir() => {
                            let allow_manifest_skip = entry.reason == DirtyReason::PeriodicColdScan
                                && manifest_skip_dirs.contains(dir);
                            let discard_if_event_seq_advances = matches!(
                                entry.reason,
                                DirtyReason::PeriodicColdScan
                                    | DirtyReason::StartupRepairDeferred
                                    | DirtyReason::FastScanBootstrapDir
                                    | DirtyReason::FastScanChangedDir
                            );
                            let (outcome, manifest_skipped, dropped_stale_batch) =
                                if entry.reason == DirtyReason::PeriodicColdScan {
                                    let sliced = self.scan_dir_repair_slice_with_project_markers(
                                        dir,
                                        entry.repair_cursor.as_ref(),
                                        project_markers,
                                        allow_manifest_skip,
                                        discard_if_event_seq_advances,
                                    );
                                    if let Some(cursor) = sliced.next_cursor {
                                        self.enqueue_dirty_repair_slice(
                                            dir.clone(),
                                            entry.reason,
                                            entry.priority,
                                            cursor,
                                        );
                                    }
                                    if sliced.completed {
                                        self.mark_cold_sweep_completed();
                                    }
                                    (
                                        sliced.outcome,
                                        sliced.manifest_skipped,
                                        sliced.dropped_stale_batch,
                                    )
                                } else {
                                    let scanned = self
                                        .scan_dirs_with_depth_and_project_markers_budgeted(
                                            &[dir],
                                            Some(1),
                                            10_000,
                                            project_markers,
                                            None,
                                            discard_if_event_seq_advances,
                                        );
                                    (scanned.outcome, false, scanned.dropped_stale_batch)
                                };
                            if dropped_stale_batch {
                                report.dropped_stale_batches =
                                    report.dropped_stale_batches.saturating_add(1);
                            }
                            report.dirs_scanned = report.dirs_scanned.saturating_add(1);
                            report.changed = report.changed.saturating_add(outcome.changed);
                            report.elapsed_ms =
                                report.elapsed_ms.saturating_add(outcome.elapsed_ms);
                            report.outcomes.push(DirtyScanOutcome {
                                dir: dir.clone(),
                                outcome,
                                reason: entry.reason,
                                manifest_skipped,
                            });
                        }
                        Ok(_) => {
                            had_failed_dir = true;
                        }
                        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                            had_failed_dir = true;
                        }
                        Err(e) => {
                            tracing::debug!(
                                "dirty queue skipped unreadable dir {}: {}",
                                dir.display(),
                                e
                            );
                            had_failed_dir = true;
                        }
                    }
                }
                report.failed = had_failed_dir && report.dirs_scanned == 0;
            }
        }

        report
    }

    fn enqueue_dirty_repair_slice(
        &self,
        dir: PathBuf,
        reason: DirtyReason,
        priority: DirtyPriority,
        cursor: DirtyRepairCursor,
    ) {
        {
            let mut queue = self.dirty_queue.lock();
            queue.enqueue_repair_slice(
                DirtyScope::dirs(now_ns(), vec![dir]),
                reason,
                priority,
                now_ns(),
                cursor,
            );
        }
        self.dirty_notify.notify_one();
    }

    pub(crate) fn fast_sync(
        &self,
        scope: DirtyScope,
        ignore_prefixes: &[PathBuf],
    ) -> FastSyncReport {
        use std::collections::HashSet;

        self.observe_clock_boundary();

        let mut report = FastSyncReport::default();
        let io_governor = self.io_governor.as_ref();

        // 1) 计算需要对齐的目录集合
        let mut dirs: Vec<PathBuf> = match scope {
            DirtyScope::All { cutoff_ns } => {
                let cutoff_ns = self.clock_cutoff_for_dirty(cutoff_ns);
                collect_dirs_changed_since(
                    &self.roots,
                    ignore_prefixes,
                    &self.exclude_dirs,
                    cutoff_ns,
                    io_governor,
                )
            }
            DirtyScope::Dirs { dirs, cutoff_ns } => {
                let root_set: HashSet<_> = self.roots.iter().cloned().collect();
                let (root_dirs, leaf_dirs): (Vec<_>, Vec<_>) =
                    dirs.into_iter().partition(|d| root_set.contains(d));

                let effective_cutoff_ns = self
                    .clock_cutoff_for_dirty(cutoff_ns)
                    .saturating_sub(10_000_000_000);
                let mut out = if !root_dirs.is_empty() {
                    collect_dirs_changed_since(
                        &root_dirs,
                        ignore_prefixes,
                        &self.exclude_dirs,
                        effective_cutoff_ns,
                        io_governor,
                    )
                } else {
                    Vec::new()
                };
                out.extend(leaf_dirs);
                out.sort();
                out.dedup();
                out
            }
        };

        // 过滤：忽略 self-write 目录/不存在目录
        dirs.retain(|d| {
            if ignore_prefixes
                .iter()
                .any(|ig| !ig.as_os_str().is_empty() && d.starts_with(ig))
                || path_has_excluded_component(d, &self.exclude_dirs)
            {
                return false;
            }
            io_governor.before_io();
            std::fs::symlink_metadata(d)
                .map(|m| m.is_dir())
                .unwrap_or(false)
        });
        dirs.sort();
        dirs.dedup();

        if dirs.is_empty() {
            self.stats.record_fast_sync();
            return report;
        }

        // 2) 扫描目录：生成 upsert events。
        //
        // 量化权衡（2026-06-20 实测）：
        // - 旧实现：30 万 stat × 50-200μs = 15-60s
        // - 新实现：1 readdir + 30 万 HashSet 查找（纳秒级）= 1-100ms
        // 30 万文件目录的 HashSet<OsString> 峰值约 20-30MB，远小于 760MB 的索引 RSS。
        let mut upsert_events: Vec<EventRecord> = Vec::with_capacity(2048);
        let mut upsert_metas: Vec<FileMeta> = Vec::with_capacity(2048);
        let mut seq: u64 = 0;

        for dir in dirs.iter() {
            report.dirs_scanned += 1;
            let mut builder = ignore::WalkBuilder::new(dir);
            builder
                .max_depth(Some(1))
                .hidden(!self.include_hidden)
                .follow_links(false)
                .ignore(self.ignore_enabled)
                .git_ignore(self.ignore_enabled)
                .git_global(self.ignore_enabled)
                .git_exclude(self.ignore_enabled);
            let fs_policy =
                crate::fs_policy::FsPolicy::current_with_config(self.fs_policy_config());
            let root = dir.clone();
            let exclude_dirs = self.exclude_dirs.clone();
            let mount_policy_counters = self.mount_policy_counters();
            builder.filter_entry(move |entry| {
                (exclude_dirs.is_empty()
                    || !path_has_excluded_component(entry.path(), &exclude_dirs))
                    && fs_policy
                        .as_ref()
                        .map(|policy| {
                            policy
                                .check_path_counted(
                                    entry.path(),
                                    Some(root.as_path()),
                                    mount_policy_counters.as_ref(),
                                )
                                .is_allowed()
                        })
                        .unwrap_or(true)
            });

            for ent in builder.build() {
                let ent = match ent {
                    Ok(e) => e,
                    Err(err) => {
                        tracing::warn!(
                            "fast-sync walker skipped entry under {}: {}",
                            dir.display(),
                            err
                        );
                        continue;
                    }
                };
                let Some(ft) = ent.file_type() else {
                    continue;
                };
                if !ft.is_file() && !ft.is_dir() {
                    continue;
                }
                if ft.is_dir() && ent.path() == dir.as_path() {
                    continue;
                }

                let path = super::normalize_path(ent.path());
                io_governor.before_io();
                let meta = match ent.metadata() {
                    Ok(meta) => meta,
                    Err(err) => {
                        tracing::warn!("fast-sync metadata failed for {}: {}", path.display(), err);
                        continue;
                    }
                };
                let Some(file_key) = FileKey::from_path_and_metadata(&path, &meta) else {
                    continue;
                };
                let mtime = meta.modified().ok();
                let mtime_ns = mtime_to_ns(mtime);
                if self.path_freshness(&path, file_key, mtime_ns) == PathFreshness::Unchanged {
                    continue;
                }
                seq = seq.wrapping_add(1);
                upsert_metas.push(FileMeta {
                    file_key,
                    path: path.clone(),
                    size: meta.len(),
                    mtime,
                    ctime: meta.created().ok(),
                    atime: meta.accessed().ok(),
                    kind: FileKind::from_metadata(&meta),
                });
                upsert_events.push(EventRecord {
                    seq,
                    timestamp: std::time::SystemTime::now(),
                    event_type: EventType::Modify,
                    id: FileIdentifier::Path(path),
                    path_hint: None,
                });
                report.upsert_events += 1;
            }

            if upsert_events.len() >= 2048 {
                self.apply_upserted_metas_inner(upsert_events.as_slice(), &mut upsert_metas, true);
                upsert_events.clear();
            }
        }
        if !upsert_events.is_empty() {
            self.apply_upserted_metas_inner(upsert_events.as_slice(), &mut upsert_metas, true);
            upsert_events.clear();
        }

        let dirty_dirs: HashSet<PathBuf> = dirs.into_iter().collect();

        // 3) 删除对齐：对齐"被标记 dirty 的目录"下的条目。按 parent dir 分组，
        //    每个 dirty 目录做一次 readdir 构建 HashSet<OsString>，与索引文件名做差集，
        //    避免逐文件 stat() 风暴。HashSet 在每个目录处理完后立即 drop，不跨目录累积。
        let mut delete_events: Vec<EventRecord> = Vec::new();

        let base = self.base.load_full();
        let to_delete = if base.file_count() > 0 {
            base.delete_alignment_with_parent_index(&dirty_dirs)
        } else if !self.rebuild_in_progress() {
            let mut l2_doc_id = 0u64;
            let mut candidates = Vec::new();
            self.l2.load_full().for_each_live_meta(|meta| {
                if meta
                    .path
                    .parent()
                    .is_some_and(|parent| dirty_dirs.contains(parent))
                {
                    candidates.push((l2_doc_id, meta.path));
                    l2_doc_id = l2_doc_id.saturating_add(1);
                }
            });
            candidates
        } else {
            Vec::new()
        };

        // v2: 按 parent dir 分组，readdir + HashSet 差集检测删除（见 `readdir_delete_alignment`）。
        let deleted_paths = readdir_delete_alignment(to_delete, Some(io_governor));
        for path in deleted_paths {
            seq = seq.wrapping_add(1);
            delete_events.push(EventRecord {
                seq,
                timestamp: std::time::SystemTime::now(),
                event_type: EventType::Delete,
                id: FileIdentifier::Path(path),
                path_hint: None,
            });
        }

        report.delete_events = delete_events.len();
        for chunk in delete_events.chunks(2048) {
            self.apply_events(chunk);
        }

        self.mark_clock_reconciled();
        self.stats.record_fast_sync();
        report
    }

    fn scan_dirs_with_depth(
        &self,
        dirs: &[&PathBuf],
        max_depth: Option<usize>,
        max_entries_per_dir: usize,
    ) -> ScanOutcome {
        self.scan_dirs_with_depth_and_project_markers(dirs, max_depth, max_entries_per_dir, &[])
    }

    fn scan_dirs_with_depth_and_project_markers(
        &self,
        dirs: &[&PathBuf],
        max_depth: Option<usize>,
        max_entries_per_dir: usize,
        project_markers: &[String],
    ) -> ScanOutcome {
        self.scan_dirs_with_depth_and_project_markers_budgeted(
            dirs,
            max_depth,
            max_entries_per_dir,
            project_markers,
            None,
            false,
        )
        .outcome
    }

    fn scan_dirs_with_depth_and_project_markers_budgeted(
        &self,
        dirs: &[&PathBuf],
        max_depth: Option<usize>,
        max_entries_per_dir: usize,
        project_markers: &[String],
        budget_ms: Option<u64>,
        discard_if_event_seq_advances: bool,
    ) -> BudgetedScanOutcome {
        let start = Instant::now();
        let scan_started_seq = self.event_seq.load(Ordering::Relaxed);

        let mut upsert_events: Vec<EventRecord> = Vec::new();
        let mut upsert_metas: Vec<FileMeta> = Vec::new();
        let mut scanned: usize = 0;
        let mut changed: usize = 0;
        let mut budget_exhausted = false;
        let mut seq: u64 = 0;
        let io_governor = self.io_governor.as_ref();

        let mut project_roots = Vec::new();
        let hidden_markers_enabled = project_markers.iter().any(|marker| marker.starts_with('.'));

        for dir in dirs {
            let mut dir_count = 0;
            let mut builder = ignore::WalkBuilder::new(dir);
            if let Some(d) = max_depth {
                builder.max_depth(Some(d));
            }
            builder
                .hidden(!self.include_hidden && !hidden_markers_enabled)
                .follow_links(false)
                .ignore(self.ignore_enabled)
                .git_ignore(self.ignore_enabled)
                .git_global(self.ignore_enabled)
                .git_exclude(self.ignore_enabled);
            let fs_policy =
                crate::fs_policy::FsPolicy::current_with_config(self.fs_policy_config());
            let root = (*dir).clone();
            let exclude_dirs = self.exclude_dirs.clone();
            let mount_policy_counters = self.mount_policy_counters();
            let include_hidden = self.include_hidden;
            let project_markers_filter = project_markers.to_vec();
            builder.filter_entry(move |entry| {
                (exclude_dirs.is_empty()
                    || !path_has_excluded_component(entry.path(), &exclude_dirs))
                    && (include_hidden
                        || !hidden_markers_enabled
                        || !path_has_hidden_component_after_root(entry.path(), root.as_path())
                        || project_root_for_marker(entry.path(), &project_markers_filter).is_some())
                    && fs_policy
                        .as_ref()
                        .map(|policy| {
                            policy
                                .check_path_counted(
                                    entry.path(),
                                    Some(root.as_path()),
                                    mount_policy_counters.as_ref(),
                                )
                                .is_allowed()
                        })
                        .unwrap_or(true)
            });
            for ent in builder.build() {
                if budget_ms
                    .filter(|budget| *budget > 0)
                    .is_some_and(|budget| start.elapsed().as_millis() as u64 >= budget)
                {
                    budget_exhausted = true;
                    break;
                }
                let ent = match ent {
                    Ok(e) => e,
                    Err(err) => {
                        tracing::warn!(
                            "scan_dirs_immediate walker skipped entry under {}: {}",
                            dir.display(),
                            err
                        );
                        continue;
                    }
                };
                let Some(ft) = ent.file_type() else {
                    continue;
                };
                if !ft.is_file() && !ft.is_dir() {
                    continue;
                }
                if ft.is_dir() && ent.path() == dir.as_path() {
                    continue;
                }
                if dir_count >= max_entries_per_dir {
                    break;
                }
                dir_count += 1;

                let path = super::normalize_path(ent.path());
                if let Some(project_root) = project_root_for_marker(path.as_path(), project_markers)
                {
                    project_roots.push(project_root);
                }
                if !self.include_hidden && path_has_hidden_component_after_root(ent.path(), dir) {
                    continue;
                }
                io_governor.before_io();
                let meta = match ent.metadata() {
                    Ok(m) => m,
                    Err(err) => {
                        tracing::warn!(
                            "scan_dirs_immediate metadata failed for {}: {}",
                            path.display(),
                            err
                        );
                        continue;
                    }
                };
                let Some(file_key) = FileKey::from_path_and_metadata(&path, &meta) else {
                    continue;
                };
                let mtime = meta.modified().ok();
                let mtime_ns = mtime_to_ns(mtime);
                if self.path_freshness(&path, file_key, mtime_ns) != PathFreshness::Unchanged {
                    changed += 1;
                }
                seq = seq.wrapping_add(1);
                upsert_metas.push(FileMeta {
                    file_key,
                    path: path.clone(),
                    size: meta.len(),
                    mtime,
                    ctime: meta.created().ok(),
                    atime: meta.accessed().ok(),
                    kind: FileKind::from_metadata(&meta),
                });
                upsert_events.push(EventRecord {
                    seq,
                    timestamp: std::time::SystemTime::now(),
                    event_type: EventType::Modify,
                    id: FileIdentifier::Path(path),
                    path_hint: None,
                });
                scanned += 1;
            }
            if budget_exhausted {
                break;
            }
        }

        let stale_low_priority_scan = discard_if_event_seq_advances
            && self.event_seq.load(Ordering::Relaxed) > scan_started_seq;
        if stale_low_priority_scan {
            tracing::debug!(
                "discarded stale low-priority scan result after newer apply seq advanced"
            );
            changed = 0;
        } else {
            if !upsert_events.is_empty() {
                self.apply_upserted_metas_inner(upsert_events.as_slice(), &mut upsert_metas, true);
            }
            self.update_directory_manifests_for_dirs(dirs, project_markers);
        }

        let elapsed_ms = start.elapsed().as_millis() as u64;
        project_roots.sort();
        project_roots.dedup();

        BudgetedScanOutcome {
            outcome: ScanOutcome {
                scanned,
                changed,
                elapsed_ms,
                project_roots,
            },
            budget_exhausted,
            dropped_stale_batch: stale_low_priority_scan,
        }
    }

    fn scan_dir_repair_slice_with_project_markers(
        &self,
        dir: &PathBuf,
        cursor: Option<&DirtyRepairCursor>,
        project_markers: &[String],
        allow_manifest_skip: bool,
        discard_if_event_seq_advances: bool,
    ) -> SlicedScanOutcome {
        // Phase 3：标记 mtime 预检是否判定为"变了或首次扫描"（Some(false) 或 None）。
        // 供段 1.5（unbounded summary 二次确认）使用。
        let mut mtime_precheck_changed = false;

        // ===== 段 1：目录 mtime 预检（Phase 1 新增）=====
        // 对所有 PeriodicColdScan 目录生效，独立于 allow_manifest_skip，
        // 在 manifest skip（段 2）之前执行——廉价优先（1 stat vs ≤512 stat）。
        if cursor.is_none() {
            if self.clock_cutoff_trusted() {
                match std::fs::symlink_metadata(dir) {
                    Ok(dir_meta) => match dir_meta.modified() {
                        Ok(dir_modified) => {
                            let current_mtime_ns = mtime_to_ns(Some(dir_modified));
                            match self
                                .directory_manifests
                                .try_mtime_precheck(dir.as_path(), current_mtime_ns)
                            {
                                None => {
                                    // 首次扫描，无 manifest 记录
                                    self.directory_manifests
                                        .mtime_precheck_no_record
                                        .fetch_add(1, Ordering::Relaxed);
                                    mtime_precheck_changed = true;
                                }
                                Some(true) => {
                                    // 目录 mtime 未变，跳过整个扫描
                                    self.directory_manifests
                                        .mtime_precheck_hits
                                        .fetch_add(1, Ordering::Relaxed);
                                    return SlicedScanOutcome {
                                        outcome: ScanOutcome {
                                            elapsed_ms: 0,
                                            ..ScanOutcome::default()
                                        },
                                        manifest_skipped: true,
                                        completed: true,
                                        next_cursor: None,
                                        dropped_stale_batch: false,
                                    };
                                }
                                Some(false) => {
                                    // 目录 mtime 变了，继续走后续逻辑
                                    self.directory_manifests
                                        .mtime_precheck_misses
                                        .fetch_add(1, Ordering::Relaxed);
                                    mtime_precheck_changed = true;
                                }
                            }
                        }
                        Err(_) => {
                            self.directory_manifests
                                .mtime_precheck_stat_errors
                                .fetch_add(1, Ordering::Relaxed);
                        }
                    },
                    Err(_) => {
                        self.directory_manifests
                            .mtime_precheck_stat_errors
                            .fetch_add(1, Ordering::Relaxed);
                    }
                }
            } else {
                // clock 不可信时不走预检，与 should_skip 行为一致
                self.directory_manifests
                    .mtime_precheck_untrusted_clock
                    .fetch_add(1, Ordering::Relaxed);
            }
        }

        // ===== 段 1.5：unbounded summary 二次确认（Phase 3 新增）=====
        // 仅在 mtime 预检判定"变了或首次扫描"（Some(false)/None）且 allow_manifest_skip 时执行。
        // 计算不受 512 条限制的 summary（WalkBuilder 过滤），与已存储的 manifest 比对。
        // 匹配则跳过（可能是 touch/atime 导致 mtime 变了但内容没变），不匹配才继续。
        // 对大目录（>512 条）尤其重要：段 2 的 bounded summary 对大目录永远 complete=false，
        // 而此处用 unbounded summary 可以覆盖大目录的 manifest 跳过。
        if allow_manifest_skip && cursor.is_none() && mtime_precheck_changed {
            if let Some(current_summary) =
                self.directory_manifest_summary_with_limit(dir, project_markers, None)
            {
                if self
                    .directory_manifests
                    .stored_matches_summary(dir.as_path(), &current_summary)
                {
                    self.directory_manifests
                        .unbounded_summary_hits
                        .fetch_add(1, Ordering::Relaxed);
                    return SlicedScanOutcome {
                        outcome: ScanOutcome {
                            elapsed_ms: 0,
                            ..ScanOutcome::default()
                        },
                        manifest_skipped: true,
                        completed: true,
                        next_cursor: None,
                        dropped_stale_batch: false,
                    };
                }
                self.directory_manifests
                    .unbounded_summary_misses
                    .fetch_add(1, Ordering::Relaxed);
            }
        }

        // ===== 段 2：现有 manifest skip（仅 L2/L3 PeriodicColdScan 生效）=====
        if allow_manifest_skip && cursor.is_none() {
            if let Some((summary, complete)) = self.directory_manifest_summary_bounded(
                dir,
                project_markers,
                REPAIR_SLICE_MAX_ENTRIES,
            ) {
                if complete {
                    let trusted = self.clock_cutoff_trusted();
                    if self
                        .directory_manifests
                        .should_skip(dir.as_path(), &summary, trusted)
                    {
                        self.directory_manifests.update(
                            dir.clone(),
                            summary,
                            self.event_seq.load(Ordering::Relaxed),
                        );
                        return SlicedScanOutcome {
                            outcome: ScanOutcome {
                                elapsed_ms: 0,
                                ..ScanOutcome::default()
                            },
                            manifest_skipped: true,
                            completed: true,
                            next_cursor: None,
                            dropped_stale_batch: false,
                        };
                    }
                }
            }
        }

        let start = Instant::now();
        let scan_started_seq = self.event_seq.load(Ordering::Relaxed);
        let start_offset = cursor
            .filter(|cursor| cursor.dir == *dir)
            .map(|cursor| cursor.offset.max(0))
            .unwrap_or(0);
        let slice = match read_dir_slice(
            dir,
            start_offset,
            REPAIR_SLICE_MAX_ENTRIES,
            Duration::from_millis(REPAIR_SLICE_MAX_MS),
        ) {
            Ok(slice) => slice,
            Err(err) => {
                tracing::debug!(
                    "repair slice skipped unreadable dir {}: {}",
                    dir.display(),
                    err
                );
                return SlicedScanOutcome {
                    outcome: ScanOutcome::default(),
                    manifest_skipped: false,
                    completed: true,
                    next_cursor: None,
                    dropped_stale_batch: false,
                };
            }
        };

        let mut upsert_events: Vec<EventRecord> = Vec::with_capacity(slice.entries.len());
        let mut upsert_metas: Vec<FileMeta> = Vec::with_capacity(slice.entries.len());
        let mut project_roots = Vec::new();
        let mut scanned = 0usize;
        let mut changed = 0usize;
        let mut seq = 0u64;
        let hidden_markers_enabled = project_markers.iter().any(|marker| marker.starts_with('.'));

        for child in &slice.entries {
            let path = super::normalize_path(child.path.as_path());
            self.io_governor.before_io();
            let meta = match std::fs::symlink_metadata(&path) {
                Ok(meta) => meta,
                Err(err) => {
                    tracing::debug!(
                        "repair slice metadata failed for {}: {}",
                        path.display(),
                        err
                    );
                    continue;
                }
            };
            if !meta.is_file() && !meta.is_dir() {
                continue;
            }
            if !self.repair_slice_path_allowed(
                dir.as_path(),
                path.as_path(),
                &meta,
                project_markers,
                hidden_markers_enabled,
            ) {
                continue;
            }
            if let Some(project_root) = project_root_for_marker(path.as_path(), project_markers) {
                project_roots.push(project_root);
            }

            let Some(file_key) = FileKey::from_path_and_metadata(&path, &meta) else {
                continue;
            };
            let mtime = meta.modified().ok();
            let mtime_ns = mtime_to_ns(mtime);
            if self.path_freshness(&path, file_key, mtime_ns) != PathFreshness::Unchanged {
                changed = changed.saturating_add(1);
            }
            seq = seq.wrapping_add(1);
            upsert_metas.push(FileMeta {
                file_key,
                path: path.clone(),
                size: meta.len(),
                mtime,
                ctime: meta.created().ok(),
                atime: meta.accessed().ok(),
                kind: FileKind::from_metadata(&meta),
            });
            upsert_events.push(EventRecord {
                seq,
                timestamp: std::time::SystemTime::now(),
                event_type: EventType::Modify,
                id: FileIdentifier::Path(path),
                path_hint: None,
            });
            scanned = scanned.saturating_add(1);
        }

        let stale_low_priority_scan = discard_if_event_seq_advances
            && self.event_seq.load(Ordering::Relaxed) > scan_started_seq;
        if stale_low_priority_scan {
            tracing::debug!(
                "discarded stale low-priority repair slice after newer apply seq advanced"
            );
            changed = 0;
        } else if !upsert_events.is_empty() {
            self.apply_upserted_metas_inner(upsert_events.as_slice(), &mut upsert_metas, true);
        }

        let completed = slice.completed;
        if completed && !stale_low_priority_scan {
            if let Some((summary, true)) = self.directory_manifest_summary_bounded(
                dir,
                project_markers,
                REPAIR_SLICE_MAX_ENTRIES,
            ) {
                self.directory_manifests.update(
                    dir.clone(),
                    summary,
                    self.event_seq.load(Ordering::Relaxed),
                );
            }
            // Phase 1 新增：记录目录 mtime（仅非 stale 路径，仅 last slice）
            // stale scan 的中间状态不应被记录，否则会延长陈旧窗口。
            if let Ok(dir_meta) = std::fs::symlink_metadata(dir) {
                if let Ok(dir_modified) = dir_meta.modified() {
                    self.directory_manifests
                        .record_dir_mtime(dir.as_path(), mtime_to_ns(Some(dir_modified)));
                }
            }
        }

        project_roots.sort();
        project_roots.dedup();
        SlicedScanOutcome {
            outcome: ScanOutcome {
                scanned,
                changed,
                elapsed_ms: start.elapsed().as_millis() as u64,
                project_roots,
            },
            manifest_skipped: false,
            completed,
            next_cursor: if completed {
                None
            } else {
                let offset = slice.next_offset.unwrap_or_else(|| {
                    start_offset.saturating_add(REPAIR_SLICE_MAX_ENTRIES as i64)
                });
                Some(DirtyRepairCursor::new(dir.clone(), offset))
            },
            dropped_stale_batch: stale_low_priority_scan,
        }
    }

    fn repair_slice_path_allowed(
        &self,
        root: &Path,
        path: &Path,
        meta: &std::fs::Metadata,
        project_markers: &[String],
        hidden_markers_enabled: bool,
    ) -> bool {
        if path_has_excluded_component(path, &self.exclude_dirs) {
            return false;
        }
        if !self.follow_symlinks && meta.file_type().is_symlink() {
            return false;
        }
        if !self.include_hidden
            && path_has_hidden_component_after_root(path, root)
            && !(hidden_markers_enabled && project_root_for_marker(path, project_markers).is_some())
        {
            return false;
        }

        FsPolicy::current_with_config(self.fs_policy_config())
            .as_ref()
            .map(|policy| {
                policy
                    .check_path_counted(path, Some(root), self.mount_policy_counters().as_ref())
                    .is_allowed()
            })
            .unwrap_or(true)
            && (meta.is_file() || meta.is_dir())
    }

    pub fn scan_dirs_immediate_outcome_with_project_markers(
        &self,
        dirs: &[PathBuf],
        project_markers: &[String],
    ) -> ScanOutcome {
        let dirs: Vec<&PathBuf> = dirs.iter().take(10).collect();
        self.scan_dirs_with_depth_and_project_markers(&dirs, Some(1), 10_000, project_markers)
    }

    fn directory_manifest_summary(
        &self,
        dir: &Path,
        project_markers: &[String],
    ) -> Option<DirectoryManifestSummary> {
        self.directory_manifest_summary_with_limit(dir, project_markers, None)
    }

    /// 计算目录 manifest summary，可选择限制条目数（Phase 3）。
    ///
    /// - `max_entries=None`：使用 WalkBuilder（ignore-filtered）路径，不限制条目数。
    ///   用于 mtime 预检失败后的二次确认（目录 mtime 变了但内容可能没变）。
    /// - `max_entries=Some(n)`：使用 read_dir + repair_slice_path_allowed 路径，
    ///   限制 n 条。返回 `Some` 仅当条目数 ≤ n（complete），否则 `None`（incomplete）。
    fn directory_manifest_summary_with_limit(
        &self,
        dir: &Path,
        project_markers: &[String],
        max_entries: Option<usize>,
    ) -> Option<DirectoryManifestSummary> {
        match max_entries {
            None => {
                let hidden_markers_enabled =
                    project_markers.iter().any(|marker| marker.starts_with('.'));
                let mut builder = ignore::WalkBuilder::new(dir);
                builder
                    .max_depth(Some(1))
                    .hidden(!self.include_hidden && !hidden_markers_enabled)
                    .follow_links(false)
                    .ignore(self.ignore_enabled)
                    .git_ignore(self.ignore_enabled)
                    .git_global(self.ignore_enabled)
                    .git_exclude(self.ignore_enabled);
                let fs_policy = FsPolicy::current_with_config(self.fs_policy_config());
                let root = dir.to_path_buf();
                let exclude_dirs = self.exclude_dirs.clone();
                let mount_policy_counters = self.mount_policy_counters();
                let include_hidden = self.include_hidden;
                let project_markers_filter = project_markers.to_vec();
                builder.filter_entry(move |entry| {
                    (exclude_dirs.is_empty()
                        || !path_has_excluded_component(entry.path(), &exclude_dirs))
                        && (include_hidden
                            || !hidden_markers_enabled
                            || !path_has_hidden_component_after_root(entry.path(), root.as_path())
                            || project_root_for_marker(entry.path(), &project_markers_filter)
                                .is_some())
                        && fs_policy
                            .as_ref()
                            .map(|policy| {
                                policy
                                    .check_path_counted(
                                        entry.path(),
                                        Some(root.as_path()),
                                        mount_policy_counters.as_ref(),
                                    )
                                    .is_allowed()
                            })
                            .unwrap_or(true)
                });

                let mut manifest = DirectoryManifestBuilder::default();
                for ent in builder.build() {
                    let ent = match ent {
                        Ok(e) => e,
                        Err(err) => {
                            tracing::debug!(
                                "directory manifest skipped entry under {}: {}",
                                dir.display(),
                                err
                            );
                            continue;
                        }
                    };
                    let path = ent.path();
                    if path == dir {
                        continue;
                    }
                    let Some(ft) = ent.file_type() else {
                        continue;
                    };
                    if !ft.is_file() && !ft.is_dir() {
                        continue;
                    }
                    self.io_governor.before_io();
                    let meta = match ent.metadata() {
                        Ok(meta) => meta,
                        Err(err) => {
                            tracing::debug!(
                                "directory manifest metadata failed for {}: {}",
                                path.display(),
                                err
                            );
                            continue;
                        }
                    };
                    manifest.push_child(
                        path,
                        FileKind::from_metadata(&meta),
                        mtime_to_ns(meta.modified().ok()),
                    );
                }

                Some(manifest.finish())
            }
            Some(n) => {
                // read_dir + repair_slice_path_allowed 路径（bounded）。
                // 复用 directory_manifest_summary_bounded，仅在 complete 时返回 Some。
                self.directory_manifest_summary_bounded(dir, project_markers, n)
                    .and_then(|(summary, complete)| if complete { Some(summary) } else { None })
            }
        }
    }

    fn directory_manifest_summary_bounded(
        &self,
        dir: &Path,
        project_markers: &[String],
        max_entries: usize,
    ) -> Option<(DirectoryManifestSummary, bool)> {
        let hidden_markers_enabled = project_markers.iter().any(|marker| marker.starts_with('.'));
        let mut manifest = DirectoryManifestBuilder::default();
        let mut seen = 0usize;
        let rd = match std::fs::read_dir(dir) {
            Ok(rd) => rd,
            Err(err) => {
                tracing::debug!(
                    "directory manifest bounded skipped unreadable dir {}: {}",
                    dir.display(),
                    err
                );
                return None;
            }
        };

        for child in rd {
            let child = match child {
                Ok(child) => child,
                Err(err) => {
                    tracing::debug!(
                        "directory manifest bounded skipped entry under {}: {}",
                        dir.display(),
                        err
                    );
                    continue;
                }
            };
            if seen >= max_entries {
                return Some((manifest.finish(), false));
            }
            let path = child.path();
            self.io_governor.before_io();
            let meta = match child.metadata() {
                Ok(meta) => meta,
                Err(err) => {
                    tracing::debug!(
                        "directory manifest bounded metadata failed for {}: {}",
                        path.display(),
                        err
                    );
                    continue;
                }
            };
            if !self.repair_slice_path_allowed(
                dir,
                path.as_path(),
                &meta,
                project_markers,
                hidden_markers_enabled,
            ) {
                continue;
            }
            manifest.push_child(
                path.as_path(),
                FileKind::from_metadata(&meta),
                mtime_to_ns(meta.modified().ok()),
            );
            seen = seen.saturating_add(1);
        }

        Some((manifest.finish(), true))
    }

    fn update_directory_manifests_for_dirs(&self, dirs: &[&PathBuf], project_markers: &[String]) {
        let generation = self.event_seq.load(Ordering::Relaxed);
        for dir in dirs {
            if let Some(summary) = self.directory_manifest_summary(dir, project_markers) {
                self.directory_manifests
                    .update((*dir).clone(), summary, generation);
            }
        }
    }

    pub fn path_freshness(
        &self,
        path: &std::path::Path,
        file_key: FileKey,
        mtime_ns: i64,
    ) -> PathFreshness {
        match self.l2.load_full().path_freshness(path, mtime_ns) {
            PathFreshness::Missing => self
                .base
                .load_full()
                .path_freshness(path, file_key, mtime_ns),
            known => known,
        }
    }

    /// 即时扫描指定目录并更新索引（同步执行，不走 debounce/channel）。
    ///
    /// 限制：最多 10 个目录，每目录最多 10000 条目。
    /// 返回 (scanned_files, elapsed_ms)。
    pub fn scan_dirs_immediate(&self, dirs: &[PathBuf]) -> (usize, u64) {
        let dirs: Vec<&PathBuf> = dirs.iter().take(10).collect();
        let outcome = self.scan_dirs_with_depth(&dirs, Some(1), 10_000);
        (outcome.scanned, outcome.elapsed_ms)
    }

    pub fn scan_dirs_immediate_outcome(&self, dirs: &[PathBuf]) -> ScanOutcome {
        let dirs: Vec<&PathBuf> = dirs.iter().take(10).collect();
        self.scan_dirs_with_depth(&dirs, Some(1), 10_000)
    }

    /// 深度即时扫描指定目录并更新索引（递归，不走 debounce/channel）。
    ///
    /// 限制：最多 10 个目录，每目录最多 50000 条目。
    /// 返回 (scanned_files, elapsed_ms)。
    pub fn scan_dirs_immediate_deep(&self, dirs: &[PathBuf]) -> (usize, u64) {
        let dirs: Vec<&PathBuf> = dirs.iter().take(10).collect();
        let outcome = self.scan_dirs_with_depth(&dirs, None, 50_000);
        (outcome.scanned, outcome.elapsed_ms)
    }

    pub fn startup_repair_if_needed(
        &self,
        enabled: bool,
        mode: &str,
        max_dirs: usize,
        budget_ms: u64,
        force_rebuild_ratio: f32,
    ) -> StartupRepairStats {
        let report = self.recovery_status().report;
        let should_run = enabled
            && match mode {
                "never" => false,
                "always" => true,
                "dirty-only" => report.startup_scan_required,
                other => {
                    tracing::warn!("unknown startup_repair_mode={}, using dirty-only", other);
                    report.startup_scan_required
                }
            };

        if !should_run {
            let stats = StartupRepairStats::default();
            self.set_startup_repair_stats(stats.clone());
            return stats;
        }

        let roots = self
            .roots
            .iter()
            .take(max_dirs.max(1))
            .cloned()
            .collect::<Vec<_>>();
        let dirs = roots.iter().collect::<Vec<_>>();
        let budget = (budget_ms > 0).then_some(budget_ms);
        let budgeted = self.scan_dirs_with_depth_and_project_markers_budgeted(
            &dirs,
            None,
            50_000,
            &[],
            budget,
            false,
        );
        let outcome = budgeted.outcome;
        let delete_count = self.align_missing_base_paths_for_roots(&roots);
        let changed_ratio = if outcome.scanned == 0 {
            0.0
        } else {
            outcome.changed as f32 / outcome.scanned as f32
        };
        let changed = outcome.changed.saturating_add(delete_count);
        let empty_index = self.file_count() == 0 && !report.soft_repair_needed;
        let force_ratio_exceeded = changed_ratio > force_rebuild_ratio;
        let (escalated, escalation_reason) = if report.hard_rebuild_needed {
            (true, "hard_rebuild_evidence")
        } else if empty_index {
            (true, "empty_index")
        } else if force_ratio_exceeded {
            (true, "force_rebuild_ratio")
        } else {
            (false, "")
        };
        let stats = StartupRepairStats {
            ran: true,
            escalated,
            scanned: outcome.scanned,
            changed,
            elapsed_ms: outcome.elapsed_ms,
            budget_ms,
            budget_exhausted: budgeted.budget_exhausted,
            escalation_reason: escalation_reason.to_string(),
        };
        self.set_startup_repair_stats(stats.clone());
        stats
    }

    fn align_missing_base_paths_for_roots(&self, roots: &[PathBuf]) -> usize {
        let base = self.base.load_full();
        let mut delete_events = Vec::new();
        let mut seq = 0u64;
        base.for_each_live_meta(|meta| {
            if !roots.iter().any(|root| meta.path.starts_with(root)) {
                return;
            }
            self.io_governor.before_io();
            match std::fs::symlink_metadata(&meta.path) {
                Ok(_) => return,
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
                Err(_) => return,
            }
            seq = seq.wrapping_add(1);
            delete_events.push(EventRecord {
                seq,
                timestamp: std::time::SystemTime::now(),
                event_type: EventType::Delete,
                id: FileIdentifier::Path(meta.path),
                path_hint: None,
            });
        });

        let count = delete_events.len();
        for chunk in delete_events.chunks(2048) {
            self.apply_events(chunk);
        }
        count
    }
}
