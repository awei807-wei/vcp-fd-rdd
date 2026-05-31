use std::collections::HashSet;
use std::path::{Path, PathBuf};
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::{Duration, Instant, UNIX_EPOCH};

use crate::core::{EventRecord, EventType, FileIdentifier, FileKey, FileKind, FileMeta, Task};
use crate::event::sync::{now_ns, DirtyPriority, DirtyQueueEntry, DirtyReason, DirtyScope};
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
        loop {
            let batch = {
                let mut st = self.rebuild_state.lock();
                let mut db = self.delta_buffer.lock();
                if db.is_empty() {
                    // 切换点：持锁判空 -> 原子切换，避免丢事件窗口。
                    self.l1.clear();
                    let new_base = Arc::new(new_l2.to_base_index_data());
                    self.base.store(new_base);
                    self.note_pending_flush_rebuild(new_l2.as_ref());
                    self.l2.store(Arc::new(PersistentIndex::new_with_roots(
                        self.roots.clone(),
                    )));
                    self.invalidate_memory_report_cache();
                    if !self.flush_requested.swap(true, Ordering::AcqRel) {
                        self.flush_notify.notify_one();
                    }
                    st.in_progress = false;
                    // 若 rebuild 期间又被请求（例如 overflow 风暴），合并为下一轮 rebuild。
                    let again = st.requested;
                    st.requested = false;
                    st.scheduled = false;
                    return again;
                }

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
                events
            };

            new_l2.apply_events(&batch);
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
            idx.l3.full_build_with_strategy(&new_l2, strategy);
            let again = idx.finish_rebuild(new_l2.clone());
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
                                    | DirtyReason::FastScanChangedDir
                            );
                            let (outcome, manifest_skipped) = self
                                .scan_dirs_periodic_cold_outcome_with_project_markers(
                                    std::slice::from_ref(dir),
                                    project_markers,
                                    allow_manifest_skip,
                                    discard_if_event_seq_advances,
                                );
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
        // 说明：这里不再构建"文件名集合（HashSet<OsString>）"用于删除对齐，
        // 因为它会在大目录下产生大量短命分配，容易把非索引 PD 顶到高水位。
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

        // 3) 删除对齐：只对齐"被标记 dirty 的目录"下的条目（但对文件做轻量存在性检查，避免构建巨大的 names set）。
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
        for (_doc_id, path) in to_delete {
            io_governor.before_io();
            match std::fs::symlink_metadata(&path) {
                Ok(_) => continue,
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
                Err(_) => continue,
            };
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
            self.update_directory_manifests_for_dirs(&dirs, project_markers);
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
        }
    }

    fn scan_dirs_periodic_cold_outcome_with_project_markers(
        &self,
        dirs: &[PathBuf],
        project_markers: &[String],
        allow_manifest_skip: bool,
        discard_if_event_seq_advances: bool,
    ) -> (ScanOutcome, bool) {
        let dirs: Vec<&PathBuf> = dirs.iter().take(10).collect();
        if allow_manifest_skip && dirs.len() == 1 {
            let dir = dirs[0];
            if let Some(summary) = self.directory_manifest_summary(dir, project_markers) {
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
                    return (
                        ScanOutcome {
                            elapsed_ms: 0,
                            ..ScanOutcome::default()
                        },
                        true,
                    );
                }
            }
        }

        let outcome = self
            .scan_dirs_with_depth_and_project_markers_budgeted(
                &dirs,
                Some(1),
                10_000,
                project_markers,
                None,
                discard_if_event_seq_advances,
            )
            .outcome;
        (outcome, false)
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
        let hidden_markers_enabled = project_markers.iter().any(|marker| marker.starts_with('.'));
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
            (exclude_dirs.is_empty() || !path_has_excluded_component(entry.path(), &exclude_dirs))
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
