use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Instant, UNIX_EPOCH};

use crate::core::{EventRecord, EventType, FileIdentifier, FileKey, FileMeta, Task};
use crate::event::recovery::{DirtyScope, DirtyTracker};
use crate::index::l2_partition::PersistentIndex;
use crate::util::maybe_trim_rss;

use super::{TieredIndex, REBUILD_COOLDOWN};

fn visit_dirs_since(
    roots: &[PathBuf],
    ignore_prefixes: &[PathBuf],
    cutoff_ns: u64,
    log_prefix: &str,
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

pub(super) fn dir_tree_changed_since(
    roots: &[PathBuf],
    ignore_prefixes: &[PathBuf],
    cutoff_ns: u64,
) -> bool {
    visit_dirs_since(
        roots,
        ignore_prefixes,
        cutoff_ns,
        "offline",
        |_dir, changed| changed,
    )
}

fn collect_dirs_changed_since(
    roots: &[PathBuf],
    ignore_prefixes: &[PathBuf],
    cutoff_ns: u64,
) -> Vec<PathBuf> {
    let mut out: Vec<PathBuf> = Vec::new();
    visit_dirs_since(
        roots,
        ignore_prefixes,
        cutoff_ns,
        "fast-sync",
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

#[derive(Debug, Default)]
pub(crate) struct FastSyncReport {
    pub(crate) dirs_scanned: usize,
    pub(crate) upsert_events: usize,
    pub(crate) delete_events: usize,
}

impl TieredIndex {
    pub(super) fn try_start_rebuild_force(&self) -> bool {
        let mut st = self.rebuild_state.lock();
        if st.in_progress {
            return false;
        }
        st.in_progress = true;
        st.pending_events.clear();
        st.requested = false;
        st.scheduled = false;
        st.last_started_at = Some(Instant::now());
        true
    }

    fn try_start_rebuild_with_cooldown(self: &Arc<Self>, reason: &'static str) -> bool {
        let mut schedule_after: Option<std::time::Duration> = None;
        {
            let mut st = self.rebuild_state.lock();
            st.requested = true;

            if st.in_progress {
                tracing::debug!(
                    "Rebuild merge: already in progress, coalescing ({})",
                    reason
                );
                return false;
            }

            let now = Instant::now();
            if let Some(last) = st.last_started_at {
                let elapsed = now.saturating_duration_since(last);
                if elapsed < REBUILD_COOLDOWN {
                    let wait = REBUILD_COOLDOWN - elapsed;
                    if !st.scheduled {
                        st.scheduled = true;
                        schedule_after = Some(wait);
                    }
                }
            }

            if schedule_after.is_none() {
                // 立即开始：清空 pending（新一轮 rebuild）并复位合并标记。
                st.in_progress = true;
                st.pending_events.clear();
                st.requested = false;
                st.scheduled = false;
                st.last_started_at = Some(now);
            }
        }

        if let Some(wait) = schedule_after {
            let idx = self.clone();
            std::thread::spawn(move || {
                std::thread::sleep(wait);
                let _ = idx.try_start_rebuild_with_cooldown("cooldown elapsed (merged)");
            });
            false
        } else {
            self.run_rebuild_background(reason);
            true
        }
    }

    pub(super) fn finish_rebuild(self: &Arc<Self>, new_l2: Arc<PersistentIndex>) -> bool {
        loop {
            let batch = {
                let mut st = self.rebuild_state.lock();
                if st.pending_events.is_empty() {
                    // 切换点：持锁判空 -> 原子切换，避免丢事件窗口。
                    self.l1.clear();
                    self.l2.store(new_l2.clone());
                    // rebuild 语义：新索引为权威数据源，旧 mmap segments 可能已过期，清空以避免双基座。
                    self.disk_layers.write().clear();
                    new_l2.rebuild_parent_index();
                    self.note_pending_flush_rebuild(new_l2.as_ref());
                    st.in_progress = false;
                    // 若 rebuild 期间又被请求（例如 overflow 风暴），合并为下一轮 rebuild。
                    let again = st.requested;
                    st.requested = false;
                    st.scheduled = false;
                    return again;
                }
                let mut v = st
                    .pending_events
                    .drain()
                    .map(|(id, ev)| EventRecord {
                        seq: ev.seq,
                        timestamp: ev.timestamp,
                        event_type: ev.event_type,
                        id,
                        path_hint: ev.path_hint,
                    })
                    .collect::<Vec<_>>();
                v.sort_by_key(|e| e.seq);
                v
            };

            new_l2.apply_events(&batch);
        }
    }

    fn run_rebuild_background(self: &Arc<Self>, reason: &'static str) {
        let idx = self.clone();
        std::thread::spawn(move || {
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
                idx.l2.load_full().file_count()
            );
            if again {
                let _ = idx.try_start_rebuild_with_cooldown("merged rebuild request after rebuild");
            }
        });
    }

    /// 后台全量构建
    pub fn spawn_full_build(self: &Arc<Self>) {
        if !self.try_start_rebuild_force() {
            tracing::debug!("Background build already in progress, skipping");
            return;
        }

        let idx = self.clone();
        std::thread::spawn(move || {
            let strategy = {
                let mut sched = idx.scheduler.lock();
                sched.adjust_parallelism();
                sched.select_strategy(&Task::ColdBuild {
                    total_dirs: idx.roots.len(),
                })
            };

            tracing::info!(
                "Starting background full build (strategy={:?})...",
                strategy
            );
            let new_l2 = Arc::new(PersistentIndex::new_with_roots(idx.roots.clone()));
            idx.l3.full_build_with_strategy(&new_l2, strategy);
            let again = idx.finish_rebuild(new_l2.clone());
            tracing::warn!("Full build complete, triggering manual RSS trim...");
            maybe_trim_rss();
            tracing::info!(
                "Background full build complete: {} files",
                idx.l2.load_full().file_count()
            );
            if again {
                let _ = idx.try_start_rebuild_with_cooldown("merged rebuild request after full build");
            }
        });
    }



    /// overflow 兜底：dirty region + cooldown/max-staleness 触发后执行一次 fast-sync（best-effort）。
    ///
    /// 设计目标：
    /// - 避免 "overflow → 立刻全盘 rebuild" 在风暴中触发大分配/高水位；
    /// - 允许查询短暂陈旧，但不阻塞查询、不 OOM；
    /// - fast-sync 以"目录为单位"做对齐：只需要 read_dir + 必要的 metadata，不假设 mtime 冒泡。
    pub fn spawn_fast_sync(
        self: &Arc<Self>,
        scope: DirtyScope,
        ignore_prefixes: Vec<PathBuf>,
        tracker: Arc<DirtyTracker>,
    ) {
        let permit = match self.fast_sync_semaphore.clone().try_acquire_owned() {
            Ok(p) => p,
            Err(_) => {
                tracing::debug!("Fast-sync already in progress, skipping duplicate spawn");
                tracker.rollback_sync(scope);
                return;
            }
        };

        let idx = self.clone();
        std::thread::spawn(move || {
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
            tracker.finish_sync();
        });
    }

    pub(crate) fn fast_sync(
        &self,
        scope: DirtyScope,
        ignore_prefixes: &[PathBuf],
    ) -> FastSyncReport {
        use std::collections::HashSet;

        let mut report = FastSyncReport::default();

        // 1) 计算需要对齐的目录集合
        let mut dirs: Vec<PathBuf> = match scope {
            DirtyScope::All { cutoff_ns } => {
                collect_dirs_changed_since(&self.roots, ignore_prefixes, cutoff_ns)
            }
            DirtyScope::Dirs { dirs, cutoff_ns } => {
                let root_set: HashSet<_> = self.roots.iter().cloned().collect();
                let (root_dirs, leaf_dirs): (Vec<_>, Vec<_>) =
                    dirs.into_iter().partition(|d| root_set.contains(d));

                let effective_cutoff_ns = cutoff_ns.saturating_sub(10_000_000_000);
                let mut out = if !root_dirs.is_empty() {
                    collect_dirs_changed_since(&root_dirs, ignore_prefixes, effective_cutoff_ns)
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
            {
                return false;
            }
            std::fs::symlink_metadata(d)
                .map(|m| m.is_dir())
                .unwrap_or(false)
        });
        dirs.sort();
        dirs.dedup();

        if dirs.is_empty() {
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
                if ft.is_dir() {
                    continue;
                }

                let path = super::normalize_path(ent.path());
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
                seq = seq.wrapping_add(1);
                upsert_metas.push(FileMeta {
                    file_key,
                    path: path.clone(),
                    size: meta.len(),
                    mtime: meta.modified().ok(),
                    ctime: meta.created().ok(),
                    atime: meta.accessed().ok(),
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

        let l2 = self.l2.load_full();
        // ParentIndex 可能是空的，需要在 fast_sync 时重建
        l2.rebuild_parent_index();
        let to_delete = l2.delete_alignment_with_parent_index(&dirty_dirs);
        for (_doc_id, path) in to_delete {
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

        report
    }

    fn scan_dirs_with_depth(
        &self,
        dirs: &[&PathBuf],
        max_depth: Option<usize>,
        max_entries_per_dir: usize,
    ) -> (usize, u64) {
        let start = Instant::now();

        let mut upsert_events: Vec<EventRecord> = Vec::new();
        let mut upsert_metas: Vec<FileMeta> = Vec::new();
        let mut scanned: usize = 0;
        let mut seq: u64 = 0;

        for dir in dirs {
            let mut dir_count = 0;
            let mut builder = ignore::WalkBuilder::new(dir);
            if let Some(d) = max_depth {
                builder.max_depth(Some(d));
            }
            builder
                .hidden(!self.include_hidden)
                .follow_links(false)
                .ignore(self.ignore_enabled)
                .git_ignore(self.ignore_enabled)
                .git_global(self.ignore_enabled)
                .git_exclude(self.ignore_enabled);
            for ent in builder.build() {
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
                if ft.is_dir() {
                    continue;
                }
                if dir_count >= max_entries_per_dir {
                    break;
                }
                dir_count += 1;

                let path = super::normalize_path(ent.path());
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
                seq = seq.wrapping_add(1);
                upsert_metas.push(FileMeta {
                    file_key,
                    path: path.clone(),
                    size: meta.len(),
                    mtime: meta.modified().ok(),
                    ctime: meta.created().ok(),
                    atime: meta.accessed().ok(),
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
        }

        if !upsert_events.is_empty() {
            self.apply_upserted_metas_inner(upsert_events.as_slice(), &mut upsert_metas, true);
        }

        let elapsed_ms = start.elapsed().as_millis() as u64;
        (scanned, elapsed_ms)
    }

    /// 即时扫描指定目录并更新索引（同步执行，不走 debounce/channel）。
    ///
    /// 限制：最多 10 个目录，每目录最多 10000 条目。
    /// 返回 (scanned_files, elapsed_ms)。
    pub fn scan_dirs_immediate(&self, dirs: &[PathBuf]) -> (usize, u64) {
        let dirs: Vec<&PathBuf> = dirs.iter().take(10).collect();
        self.scan_dirs_with_depth(&dirs, Some(1), 10_000)
    }

    /// 深度即时扫描指定目录并更新索引（递归，不走 debounce/channel）。
    ///
    /// 限制：最多 10 个目录，每目录最多 50000 条目。
    /// 返回 (scanned_files, elapsed_ms)。
    pub fn scan_dirs_immediate_deep(&self, dirs: &[PathBuf]) -> (usize, u64) {
        let dirs: Vec<&PathBuf> = dirs.iter().take(10).collect();
        self.scan_dirs_with_depth(&dirs, None, 50_000)
    }
}
