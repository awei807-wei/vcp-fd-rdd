use std::path::PathBuf;
use std::sync::Arc;

use crate::index::base_index::BaseIndexData;
use crate::index::l1_cache::L1Cache;
use crate::index::l2_partition::PersistentIndex;
use crate::index::l3_cold::IndexBuilder;
use crate::storage::snapshot::{
    read_recovery_runtime_state, stable_prev_v7_path_for, stable_v7_path_for,
};
use crate::storage::traits::StorageBackend;
use crate::util::maybe_trim_rss;

use super::{StartupRecoveryReport, TieredIndex};

impl TieredIndex {
    #[allow(dead_code, clippy::too_many_arguments)]
    pub(super) fn new(
        l1: L1Cache,
        l2: Arc<PersistentIndex>,
        l3: IndexBuilder,
        roots: Vec<PathBuf>,
        include_hidden: bool,
        ignore_enabled: bool,
        follow_symlinks: bool,
    ) -> Self {
        Self::new_with_excludes(
            l1,
            l2,
            l3,
            roots,
            include_hidden,
            ignore_enabled,
            follow_symlinks,
            Vec::new(),
        )
    }

    #[allow(clippy::too_many_arguments)]
    pub(super) fn new_with_excludes(
        l1: L1Cache,
        l2: Arc<PersistentIndex>,
        l3: IndexBuilder,
        roots: Vec<PathBuf>,
        include_hidden: bool,
        ignore_enabled: bool,
        follow_symlinks: bool,
        exclude_dirs: Vec<String>,
    ) -> Self {
        Self::new_with_base(
            l1,
            l2,
            l3,
            roots,
            include_hidden,
            ignore_enabled,
            follow_symlinks,
            exclude_dirs,
            None,
        )
    }

    #[allow(clippy::too_many_arguments)]
    pub(super) fn new_with_base(
        l1: L1Cache,
        l2: Arc<PersistentIndex>,
        l3: IndexBuilder,
        roots: Vec<PathBuf>,
        include_hidden: bool,
        ignore_enabled: bool,
        follow_symlinks: bool,
        exclude_dirs: Vec<String>,
        base_data: Option<BaseIndexData>,
    ) -> Self {
        use arc_swap::ArcSwap;
        use parking_lot::Mutex;
        use std::sync::atomic::{AtomicBool, AtomicU64};
        use tokio::sync::Notify;

        use super::rebuild::RebuildState;
        use crate::core::AdaptiveScheduler;

        let base_data = base_data.unwrap_or_else(|| l2.to_base_index_data());
        let base = ArcSwap::from(Arc::new(base_data));

        Self {
            l1,
            l2: ArcSwap::from(l2),
            l3,
            scheduler: Mutex::new(AdaptiveScheduler::new()),
            wal: Mutex::new(None),
            event_seq: AtomicU64::new(0),
            rebuild_state: Mutex::new(RebuildState::default()),
            delta_buffer: Mutex::new(crate::index::delta_buffer::DeltaBuffer::with_capacity(
                262_144,
            )),
            base,
            flush_requested: AtomicBool::new(false),
            flush_notify: Notify::new(),
            auto_flush_overlay_paths: AtomicU64::new(250_000),
            auto_flush_overlay_bytes: AtomicU64::new(64 * 1024 * 1024),
            // Periodic snapshot materializes the full visible base. Keep tiny
            // event trickles in WAL/DeltaBuffer so a few filesystem events do
            // not rebuild a 400K+ file base every interval.
            periodic_flush_min_events: AtomicU64::new(4_096),
            periodic_flush_min_bytes: AtomicU64::new(4 * 1024 * 1024),
            pending_flush_events: AtomicU64::new(0),
            pending_flush_bytes: AtomicU64::new(0),
            last_snapshot_time: AtomicU64::new(0),
            roots,
            include_hidden,
            ignore_enabled,
            follow_symlinks,
            exclude_dirs,
            fast_sync_semaphore: Arc::new(tokio::sync::Semaphore::new(1)),
            recovery_status: Mutex::new(super::RecoveryStatus::default()),
            stable_snapshot_enabled: AtomicBool::new(true),
            stats: Arc::new(crate::stats::StatsCollector::new()),
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
        Self::empty_with_options_and_follow(roots, include_hidden, ignore_enabled, false)
    }

    pub fn empty_with_options_and_follow(
        roots: Vec<PathBuf>,
        include_hidden: bool,
        ignore_enabled: bool,
        follow_symlinks: bool,
    ) -> Self {
        Self::empty_with_options_follow_and_excludes(
            roots,
            include_hidden,
            ignore_enabled,
            follow_symlinks,
            Vec::new(),
        )
    }

    pub fn empty_with_options_follow_and_excludes(
        roots: Vec<PathBuf>,
        include_hidden: bool,
        ignore_enabled: bool,
        follow_symlinks: bool,
        exclude_dirs: Vec<String>,
    ) -> Self {
        let l1 = L1Cache::with_capacity(1000);
        let l2 = Arc::new(PersistentIndex::new_with_roots(roots.clone()));
        let l3 = IndexBuilder::new_with_options_follow_and_excludes(
            roots.clone(),
            include_hidden,
            ignore_enabled,
            follow_symlinks,
            exclude_dirs.clone(),
        );
        Self::new_with_excludes(
            l1,
            l2,
            l3,
            roots,
            include_hidden,
            ignore_enabled,
            follow_symlinks,
            exclude_dirs,
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
        Self::load_with_options_and_follow(store, roots, include_hidden, ignore_enabled, false)
            .await
    }

    pub async fn load_with_options_and_follow<S: StorageBackend + ?Sized>(
        store: &S,
        roots: Vec<PathBuf>,
        include_hidden: bool,
        ignore_enabled: bool,
        follow_symlinks: bool,
    ) -> anyhow::Result<Arc<Self>> {
        Self::load_with_options_follow_and_excludes(
            store,
            roots,
            include_hidden,
            ignore_enabled,
            follow_symlinks,
            Vec::new(),
        )
        .await
    }

    pub async fn load_with_options_follow_and_excludes<S: StorageBackend + ?Sized>(
        store: &S,
        roots: Vec<PathBuf>,
        include_hidden: bool,
        ignore_enabled: bool,
        follow_symlinks: bool,
        exclude_dirs: Vec<String>,
    ) -> anyhow::Result<Arc<Self>> {
        let index = Arc::new(
            Self::load_or_empty_with_options_follow_and_excludes(
                store,
                roots,
                include_hidden,
                ignore_enabled,
                follow_symlinks,
                exclude_dirs,
            )
            .await?,
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
        Self::load_or_empty_with_options_and_follow(
            store,
            roots,
            include_hidden,
            ignore_enabled,
            false,
        )
        .await
    }

    pub async fn load_or_empty_with_options_and_follow<S: StorageBackend + ?Sized>(
        store: &S,
        roots: Vec<PathBuf>,
        include_hidden: bool,
        ignore_enabled: bool,
        follow_symlinks: bool,
    ) -> anyhow::Result<Self> {
        Self::load_or_empty_with_options_follow_and_excludes(
            store,
            roots,
            include_hidden,
            ignore_enabled,
            follow_symlinks,
            Vec::new(),
        )
        .await
    }

    pub async fn load_or_empty_with_options_follow_and_excludes<S: StorageBackend + ?Sized>(
        store: &S,
        roots: Vec<PathBuf>,
        include_hidden: bool,
        ignore_enabled: bool,
        follow_symlinks: bool,
        exclude_dirs: Vec<String>,
    ) -> anyhow::Result<Self> {
        let l1 = L1Cache::with_capacity(1000);
        let l3 = IndexBuilder::new_with_options_follow_and_excludes(
            roots.clone(),
            include_hidden,
            ignore_enabled,
            follow_symlinks,
            exclude_dirs.clone(),
        );

        let runtime_state = read_recovery_runtime_state(store.path()).unwrap_or_else(|e| {
            tracing::warn!("recovery runtime state read failed: {}", e);
            Default::default()
        });

        // 优先加载 stable.v7 / stable.prev.v7，再回退到 legacy v7 单文件快照。
        let stable_path = stable_v7_path_for(store.path());
        let stable_prev_path = stable_prev_v7_path_for(store.path());
        let legacy_v7_path = store.path().with_extension("v7");
        let snapshot_candidates = [
            ("stable", stable_path.as_path()),
            ("stable-prev", stable_prev_path.as_path()),
            ("legacy-v7", legacy_v7_path.as_path()),
        ];

        for (source, path) in snapshot_candidates {
            match crate::storage::snapshot_v7::try_load_v7(path) {
                Ok(Some(v7_data)) => {
                    tracing::info!(
                        "{} snapshot loaded directly into base: {} entries, {} trigrams",
                        source,
                        v7_data.entries_by_key.len(),
                        v7_data.trigram_index.len()
                    );
                    let l2 = Arc::new(PersistentIndex::new_with_roots(roots.clone()));
                    let idx = Self::new_with_base(
                        l1,
                        l2,
                        l3,
                        roots,
                        include_hidden,
                        ignore_enabled,
                        follow_symlinks,
                        exclude_dirs,
                        Some(v7_data),
                    );
                    idx.attach_wal(store)?;
                    let replay = idx.replay_wal_if_any(0);
                    idx.set_startup_recovery_report(StartupRecoveryReport {
                        snapshot_source: source.to_string(),
                        wal_events_replayed: replay.events_replayed,
                        wal_truncated_tail_records: replay.truncated_tail_records,
                        requires_repair: !runtime_state.last_clean_shutdown
                            || replay.truncated_tail_records > 0,
                        previous_clean_shutdown: runtime_state.last_clean_shutdown,
                    });
                    maybe_trim_rss();
                    return Ok(idx);
                }
                Ok(None) => {}
                Err(e) => tracing::warn!("{} load failed: {}", source, e),
            }
        }

        // 无可用快照：回退到空索引启动（由上层触发 rebuild）。
        let l2 = Arc::new(PersistentIndex::new_with_roots(roots.clone()));
        let idx = Self::new_with_excludes(
            l1,
            l2,
            l3,
            roots,
            include_hidden,
            ignore_enabled,
            follow_symlinks,
            exclude_dirs,
        );
        idx.attach_wal(store)?;
        let replay = idx.replay_wal_if_any(0);
        idx.set_startup_recovery_report(StartupRecoveryReport {
            snapshot_source: "empty".to_string(),
            wal_events_replayed: replay.events_replayed,
            wal_truncated_tail_records: replay.truncated_tail_records,
            requires_repair: true,
            previous_clean_shutdown: runtime_state.last_clean_shutdown,
        });
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

    fn replay_wal_if_any(&self, checkpoint_seal_id: u64) -> WalReplaySummary {
        let wal = { self.wal.lock().clone() };
        let Some(wal) = wal else {
            return WalReplaySummary::default();
        };
        match wal.replay_since_seal(checkpoint_seal_id) {
            Ok(r) => {
                let summary = WalReplaySummary {
                    events_replayed: r.events.len(),
                    truncated_tail_records: r.truncated_tail_records,
                };
                if !r.events.is_empty() {
                    tracing::info!(
                        "WAL replay: events={} sealed_used={} truncated_tail={}",
                        r.events.len(),
                        r.sealed_used,
                        r.truncated_tail_records
                    );
                    self.apply_events_inner(&r.events, false);
                }
                summary
            }
            Err(e) => {
                tracing::warn!("WAL replay failed, ignoring: {}", e);
                WalReplaySummary {
                    events_replayed: 0,
                    truncated_tail_records: 1,
                }
            }
        }
    }
}

#[derive(Clone, Copy, Debug, Default)]
struct WalReplaySummary {
    events_replayed: usize,
    truncated_tail_records: usize,
}
