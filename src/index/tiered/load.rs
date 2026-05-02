use std::path::PathBuf;
use std::sync::Arc;

use crate::index::base_index::BaseIndexData;
use crate::index::l1_cache::L1Cache;
use crate::index::l2_partition::PersistentIndex;
use crate::index::l3_cold::IndexBuilder;
use crate::storage::traits::StorageBackend;

use super::TieredIndex;

impl TieredIndex {
    #[allow(clippy::too_many_arguments)]
    pub(super) fn new(
        l1: L1Cache,
        l2: Arc<PersistentIndex>,
        base: Arc<BaseIndexData>,
        l3: IndexBuilder,
        roots: Vec<PathBuf>,
        include_hidden: bool,
        ignore_enabled: bool,
        follow_symlinks: bool,
        disk_layers: Vec<super::DiskLayer>,
    ) -> Self {
        use arc_swap::ArcSwap;
        use parking_lot::{Mutex, RwLock};
        use std::sync::atomic::{AtomicBool, AtomicU64};
        use tokio::sync::Notify;

        use super::rebuild::RebuildState;
        use crate::core::AdaptiveScheduler;

        let base = ArcSwap::from(Arc::new(l2.to_base_index_data()));

        Self {
            l1,
            l2: ArcSwap::from(l2),
            base: ArcSwap::from(base),
            disk_layers: RwLock::new(disk_layers),
            l3,
            scheduler: Mutex::new(AdaptiveScheduler::new()),
            wal: Mutex::new(None),
            event_seq: AtomicU64::new(0),
            rebuild_state: Mutex::new(RebuildState::default()),
            delta_buffer: Mutex::new(crate::index::delta_buffer::DeltaBuffer::with_capacity(262_144)),
            flush_requested: AtomicBool::new(false),
            flush_notify: Notify::new(),
            auto_flush_overlay_paths: AtomicU64::new(250_000),
            auto_flush_overlay_bytes: AtomicU64::new(64 * 1024 * 1024),
            periodic_flush_min_events: AtomicU64::new(0),
            periodic_flush_min_bytes: AtomicU64::new(0),
            pending_flush_events: AtomicU64::new(0),
            pending_flush_bytes: AtomicU64::new(0),
            last_snapshot_time: AtomicU64::new(0),
            roots,
            include_hidden,
            ignore_enabled,
            follow_symlinks,
            fast_sync_semaphore: Arc::new(tokio::sync::Semaphore::new(1)),
        }
    }

    /// 直接以空索引启动（显式忽略快照加载）
    pub fn empty(roots: Vec<PathBuf>) -> Self {
        Self::empty_with_hidden(roots, false)
    }

    /// 直接以空索引启动，并指定是否包含隐藏项。
    pub fn empty_with_hidden(roots: Vec<PathBuf>, include_hidden: bool) -> Self {
        Self::empty_with_options(roots, include_hidden, true)
    }

    pub fn empty_with_options(
        roots: Vec<PathBuf>,
        include_hidden: bool,
        ignore_enabled: bool,
    ) -> Self {
        let l1 = L1Cache::with_capacity(1000);
        let l2 = Arc::new(PersistentIndex::new_with_roots(roots.clone()));
        let base = Arc::new(l2.to_base_index_data());
        let l3 = IndexBuilder::new_with_options(roots.clone(), include_hidden, ignore_enabled);
        Self::new(
            l1,
            l2,
            base,
            l3,
            roots,
            include_hidden,
            ignore_enabled,
            false,
            Vec::new(),
        )
    }

    /// 从快照加载（或回退为空），并在返回前执行启动清扫：
    /// 1) 物理清理 manifest 未引用的孤儿段文件（best-effort）
    /// 2) 若现有 delta 段达到阈值则触发后台 compaction（best-effort）
    pub async fn load<S: StorageBackend + ?Sized>(
        store: &S,
        roots: Vec<PathBuf>,
    ) -> anyhow::Result<Arc<Self>> {
        Self::load_with_options(store, roots, false, true).await
    }

    pub async fn load_with_hidden<S: StorageBackend + ?Sized>(
        store: &S,
        roots: Vec<PathBuf>,
        include_hidden: bool,
    ) -> anyhow::Result<Arc<Self>> {
        Self::load_with_options(store, roots, include_hidden, true).await
    }

    pub async fn load_with_options<S: StorageBackend + ?Sized>(
        store: &S,
        roots: Vec<PathBuf>,
        include_hidden: bool,
        ignore_enabled: bool,
    ) -> anyhow::Result<Arc<Self>> {
        let index = Arc::new(
            Self::load_or_empty_with_options(store, roots, include_hidden, ignore_enabled).await?,
        );

        // 1) 物理清理不在 MANIFEST 里的孤儿文件（best-effort）
        if let Err(e) = store.gc_stale_segments() {
            tracing::warn!("LSM gc stale segments failed on startup: {e}");
        }

        // 2) 启动阶段不持有可克隆的存储后端句柄时，跳过预热 compaction。
        // 后续 flush/snapshot 仍会按阈值触发后台 compaction。

        Ok(index)
    }

    /// 从快照加载或空索引启动
    pub async fn load_or_empty<S: StorageBackend + ?Sized>(
        store: &S,
        roots: Vec<PathBuf>,
    ) -> anyhow::Result<Self> {
        Self::load_or_empty_with_options(store, roots, false, true).await
    }

    /// 从快照加载或空索引启动，并指定是否包含隐藏项。
    pub async fn load_or_empty_with_hidden<S: StorageBackend + ?Sized>(
        store: &S,
        roots: Vec<PathBuf>,
        include_hidden: bool,
    ) -> anyhow::Result<Self> {
        Self::load_or_empty_with_options(store, roots, include_hidden, true).await
    }

    pub async fn load_or_empty_with_options<S: StorageBackend + ?Sized>(
        store: &S,
        roots: Vec<PathBuf>,
        include_hidden: bool,
        ignore_enabled: bool,
    ) -> anyhow::Result<Self> {
        let l1 = L1Cache::with_capacity(1000);
        let l3 = IndexBuilder::new_with_options(roots.clone(), include_hidden, ignore_enabled);

        // 阶段 A：优先加载 v7 单文件快照（最快路径，<1s mmap + 反序列化）。
        let v7_path = store.path().with_extension("v7");
        match crate::storage::snapshot_v7::try_load_v7(&v7_path) {
            Ok(Some(v7_data)) => {
                tracing::info!(
                    "v7 snapshot loaded: {} entries, {} trigrams",
                    v7_data.entries_by_key.len(),
                    v7_data.trigram_index.len()
                );
                let l2 = Arc::new(PersistentIndex::new_with_roots(roots.clone()));
                // 将 v7 数据灌入 L2（当前为简化实现：逐个 upsert；
                // 后续可优化为直接构造内部结构，避免 trigram 重建开销）。
                for i in 0..v7_data.entries_by_key.len() {
                    if let Some(entry) = v7_data.entries_by_key.get(i) {
                        if let Some(path_bytes) = v7_data.path_table.resolve(entry.path_idx) {
                            let path = {
                                #[cfg(unix)]
                                {
                                    use std::ffi::OsStr;
                                    use std::os::unix::ffi::OsStrExt;
                                    PathBuf::from(OsStr::from_bytes(&path_bytes))
                                }
                                #[cfg(not(unix))]
                                {
                                    PathBuf::from(std::str::from_utf8(&path_bytes).unwrap_or_default())
                                }
                            };
                            let mtime = if entry.mtime_ns < 0 {
                                None
                            } else {
                                Some(std::time::UNIX_EPOCH + std::time::Duration::from_nanos(entry.mtime_ns as u64))
                            };
                            l2.upsert(crate::core::FileMeta {
                                file_key: entry.file_key(),
                                path,
                                size: entry.size,
                                mtime,
                                ctime: None,
                                atime: None,
                            });
                        }
                    }
                }
                let base = Arc::new(l2.to_base_index_data());
                let idx = Self::new(
                    l1,
                    l2,
                    base,
                    l3,
                    roots,
                    include_hidden,
                    ignore_enabled,
                    false,
                    Vec::new(),
                );
                idx.attach_wal(store)?;
                return Ok(idx);
            }
            Ok(None) => {}
            Err(e) => tracing::warn!("v7 load failed: {}", e),
        }

        // 冷启动离线变更检测（仅 LSM 目录布局）：
        // - LSM 段可能包含停机期间的"幽灵记录"（已删除文件但索引仍在）。
        // - 查询会触发 mmap 触页把历史段读入 RSS（即使 L2 很小），造成突发内存暴涨与脏结果。
        // - 这里在加载任何 disk segments 之前做一次"目录 mtime crawl"（仅 stat 目录，O(目录数)）；
        //   若发现离线变更，则判定快照不可信：不挂载旧段进查询链路，从空索引启动（由上层触发 rebuild）。
        if let Ok(Some(last_build_ns)) = store.lsm_last_build_ns() {
            let ignores = vec![store.derived_lsm_dir_path()];
            if last_build_ns == 0 || dir_tree_changed_since(&roots, &ignores, last_build_ns) {
                tracing::warn!(
                    "LSM snapshot considered stale (offline dir mtime changed since last_build_ns={}), starting empty (will rebuild)",
                    last_build_ns
                );
                let l2 = Arc::new(PersistentIndex::new_with_roots(roots.clone()));
                let base = Arc::new(l2.to_base_index_data());
                return Ok(Self::new(
                    l1,
                    l2,
                    base,
                    l3,
                    roots,
                    include_hidden,
                    ignore_enabled,
                    false,
                    Vec::new(),
                ));
            }
        }

        // 阶段 C：优先加载 LSM 目录布局（base + delta segments），启动后不做全量 hydration。
        if let Ok(Some(lsm)) = store.load_lsm_if_valid(&roots) {
            let mut layers: Vec<DiskLayer> = Vec::new();
            if let Some(b) = lsm.base {
                let deleted_paths = Arc::new(path_arena_set_from_paths(b.deleted_paths));
                let (cnt, bytes, est) = deleted_paths_stats(deleted_paths.as_ref());
                layers.push(DiskLayer {
                    idx: Arc::new(MmapIndex::new(b.snap)),
                    deleted_paths,
                    deleted_paths_count: cnt,
                    deleted_paths_bytes: bytes,
                    deleted_paths_estimated_bytes: est,
                });
            }
            for d in lsm.deltas {
                let deleted_paths = Arc::new(path_arena_set_from_paths(d.deleted_paths));
                let (cnt, bytes, est) = deleted_paths_stats(deleted_paths.as_ref());
                layers.push(DiskLayer {
                    idx: Arc::new(MmapIndex::new(d.snap)),
                    deleted_paths,
                    deleted_paths_count: cnt,
                    deleted_paths_bytes: bytes,
                    deleted_paths_estimated_bytes: est,
                });
            }

            let l2 = Arc::new(PersistentIndex::new_with_roots(roots.clone()));
            let base = Arc::new(l2.to_base_index_data());
            let idx = Self::new(
                l1,
                l2,
                base,
                l3,
                roots,
                include_hidden,
                ignore_enabled,
                false,
                layers,
            );
            idx.attach_wal(store)?;
            idx.replay_wal_if_any(lsm.wal_seal_id);
            {
                let l2 = idx.l2.load_full();
                l2.rebuild_parent_index();
            }
            let new_base = idx.l2.load_full().to_base_index_data();
            idx.base.store(Arc::new(new_base));
            return Ok(idx);
        }

        // 兼容：legacy v6 单文件（mmap + lazy decode），作为长期 base 使用（不再 hydration）。
        if let Ok(Some(snap)) = store.load_v6_mmap_if_valid(&roots) {
            let base_layer = DiskLayer {
                idx: Arc::new(MmapIndex::new(snap)),
                deleted_paths: Arc::new(PathArenaSet::default()),
                deleted_paths_count: 0,
                deleted_paths_bytes: 0,
                deleted_paths_estimated_bytes: 0,
            };
            let l2 = Arc::new(PersistentIndex::new_with_roots(roots.clone()));
            let base = Arc::new(l2.to_base_index_data());
            let idx = Self::new(
                l1,
                l2,
                base,
                l3,
                roots,
                include_hidden,
                ignore_enabled,
                false,
                vec![base_layer],
            );
            idx.attach_wal(store)?;
            // legacy v6 没有 LSM manifest checkpoint：保守回放全部 WAL（如果存在）。
            idx.replay_wal_if_any(0);
            {
                let l2 = idx.l2.load_full();
                l2.rebuild_parent_index();
            }
            let new_base = idx.l2.load_full().to_base_index_data();
            idx.base.store(Arc::new(new_base));
            return Ok(idx);
        }

        let l2 = match store.load_if_valid().await {
            Ok(Some(snap)) => snap.into_persistent_index(roots.clone()),
            Ok(None) => {
                tracing::info!("No valid snapshot, starting with empty index");
                PersistentIndex::new_with_roots(roots.clone())
            }
            Err(e) => {
                tracing::warn!("Failed to load snapshot: {}, starting empty", e);
                PersistentIndex::new_with_roots(roots.clone())
            }
        };
        let base = Arc::new(l2.to_base_index_data());
        let l2 = Arc::new(l2);

        let base = Arc::new(l2.to_base_index_data());
        let idx = Self::new(
            l1,
            l2,
            base,
            l3,
            roots,
            include_hidden,
            ignore_enabled,
            false,
            Vec::new(),
        );
        idx.attach_wal(store)?;
        idx.replay_wal_if_any(0);
        {
            let l2 = idx.l2.load_full();
            l2.rebuild_parent_index();
        }
        let new_base = idx.l2.load_full().to_base_index_data();
        idx.base.store(Arc::new(new_base));
        Ok(idx)
    }

    pub fn attach_wal<S: StorageBackend + ?Sized>(&self, store: &S) -> anyhow::Result<()> {
        let mut g = self.wal.lock();
        if g.is_some() {
            return Ok(());
        }
        *g = Some(store.open_wal()?);
        Ok(())
    }

    fn replay_wal_if_any(&self, checkpoint_seal_id: u64) {
        let wal = { self.wal.lock().clone() };
        let Some(wal) = wal else { return };
        match wal.replay_since_seal(checkpoint_seal_id) {
            Ok(r) => {
                if !r.events.is_empty() {
                    tracing::info!(
                        "WAL replay: events={} sealed_used={} truncated_tail={}",
                        r.events.len(),
                        r.sealed_used,
                        r.truncated_tail_records
                    );
                    self.apply_events_inner(&r.events, false);
                }
            }
            Err(e) => {
                tracing::warn!("WAL replay failed, ignoring: {}", e);
            }
        }
    }
}