use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Instant, UNIX_EPOCH};

use crate::core::{EventRecord, EventType, FileIdentifier, FileKey, FileMeta, Task};
use crate::index::l2_partition::PersistentIndex;
use crate::util::maybe_trim_rss;

use super::TieredIndex;

/// 增量补偿的脏区域范围。
///
/// 当前由外部触发（例如 `socket.rs` 的人工 fast-sync、tests）使用；
/// 内部不再有自动 dirty 跟踪（DirtyTracker 已移除）。
#[derive(Clone, Debug)]
pub enum DirtyScope {
    /// 退化场景：按全局 dirty 处理（实际等价于扫描所有 roots）。
    All {
        /// 上一次 fast-sync 完成时间（ns since epoch，best-effort，0 表示无 cutoff）。
        cutoff_ns: u64,
    },
    /// 可定位到“可能丢事件”的目录集合。
    Dirs {
        /// 上一次 fast-sync 完成时间（ns since epoch，best-effort，0 表示无 cutoff）。
        cutoff_ns: u64,
        dirs: Vec<PathBuf>,
    },
}

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
            true
        };
        if on_dir(&dir, changed) {
            return true;
        }

        let rd = match std::fs::read_dir(&dir) {
            Ok(rd) => rd,
            Err(e) => {
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

/// 启动加载阶段：判断 LSM 快照是否在停机期间发生过目录树 mtime 变更。
///
/// 一次性使用，仅在 `load.rs` 启动路径调用，不参与运行期周期任务。
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

#[derive(Debug, Default)]
#[allow(dead_code)]
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

    pub(super) fn finish_rebuild(self: &Arc<Self>, new_l2: Arc<PersistentIndex>) -> bool {
        loop {
            let batch = {
                let mut st = self.rebuild_state.lock();
                if st.pending_events.is_empty() {
                    self.l1.clear();
                    self.l2.store(new_l2.clone());
                    self.disk_layers.write().clear();
                    {
                        let mut ov = self.overlay_state.lock();
                        Arc::make_mut(&mut ov.deleted_paths).clear();
                        Arc::make_mut(&mut ov.upserted_paths).clear();
                        Arc::make_mut(&mut ov.deleted_paths).maybe_shrink_after_clear();
                        Arc::make_mut(&mut ov.upserted_paths).maybe_shrink_after_clear();
                    }
                    self.note_pending_flush_rebuild(new_l2.as_ref());
                    st.in_progress = false;
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

    /// 后台全量构建（仅在冷启动 / 索引为空时调用）。
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
            let _ = idx.finish_rebuild(new_l2.clone());
            tracing::warn!("Full build complete, triggering manual RSS trim...");
            maybe_trim_rss();
            tracing::info!(
                "Background full build complete: {} files",
                idx.l2.load_full().file_count()
            );
        });
    }

    /// 增量补偿扫描：按目录粒度对齐索引与文件系统状态。
    ///
    /// 现在仅由外部 API（例如 `socket.rs` 测试、tests/）显式触发；过去由 overflow 兜底循环 /
    /// Hybrid Crawler 自动调度的入口已删除（参考 `重构方案包/causal-chain-report.md` 第 8.4 节）。
    #[allow(dead_code)]
    pub(crate) fn fast_sync(
        &self,
        scope: DirtyScope,
        ignore_prefixes: &[PathBuf],
    ) -> FastSyncReport {
        use std::collections::HashSet;

        let mut report = FastSyncReport::default();

        let mut dirs: Vec<PathBuf> = match scope {
            DirtyScope::All { .. } => self.roots.clone(),
            DirtyScope::Dirs { dirs, .. } => dirs,
        };

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

        let l2 = self.l2.load_full();
        let mut delete_events: Vec<EventRecord> = Vec::new();
        l2.for_each_live_meta_in_dirs(&dirty_dirs, |m| {
            match std::fs::symlink_metadata(&m.path) {
                Ok(_) => return,
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
                Err(_) => return,
            };
            seq = seq.wrapping_add(1);
            delete_events.push(EventRecord {
                seq,
                timestamp: std::time::SystemTime::now(),
                event_type: EventType::Delete,
                id: FileIdentifier::Path(m.path),
                path_hint: None,
            });
        });

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
