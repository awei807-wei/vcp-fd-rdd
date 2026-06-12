use std::path::{Path, PathBuf};
use std::sync::Arc;

use crate::core::FileKey;
use crate::fs_policy::{FsPolicy, FsPolicyConfig};
use crate::index::base_index::BaseIndexData;
use crate::index::l1_cache::L1Cache;
use crate::index::l2_partition::PersistentIndex;
use crate::index::l3_cold::IndexBuilder;
use crate::io_governor::{IoGovernor, IoGovernorConfig};
use crate::storage::recovery_audit::{
    audit_recovery_ledger, choose_checkpoint, RecoveryAuditReport, SnapshotCheckpointSource,
};
use crate::storage::snapshot::{
    quarantine_sidecar_path_for, read_recovery_runtime_state, stable_prev_v7_path_for,
    stable_v7_path_for,
};
use crate::storage::traits::StorageBackend;
use crate::storage::wal::{WalReplayDamage, WalReplayRecord};
use crate::util::{maybe_trim_rss, path_has_excluded_component};

use super::{StartupRecoveryReport, TieredIndex, REBUILD_COOLDOWN};

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
        Self::new_with_base_and_io_governor(
            l1,
            l2,
            l3,
            roots,
            include_hidden,
            ignore_enabled,
            follow_symlinks,
            exclude_dirs,
            base_data,
            Arc::new(IoGovernor::disabled()),
        )
    }

    #[allow(clippy::too_many_arguments)]
    pub(super) fn new_with_base_and_io_governor(
        l1: L1Cache,
        l2: Arc<PersistentIndex>,
        l3: IndexBuilder,
        roots: Vec<PathBuf>,
        include_hidden: bool,
        ignore_enabled: bool,
        follow_symlinks: bool,
        exclude_dirs: Vec<String>,
        base_data: Option<BaseIndexData>,
        io_governor: Arc<IoGovernor>,
    ) -> Self {
        use arc_swap::ArcSwap;
        use parking_lot::Mutex;
        use std::sync::atomic::{AtomicBool, AtomicU64};
        use tokio::sync::Notify;

        use super::rebuild::RebuildState;
        use crate::core::AdaptiveScheduler;
        use crate::fs_policy::SharedMountPolicyCounters;

        let base_data = base_data.unwrap_or_else(|| l2.to_base_index_data());
        let base = ArcSwap::from(Arc::new(base_data));
        let fs_policy_config = l3.fs_policy_config.clone();
        let mount_policy_counters = Arc::new(SharedMountPolicyCounters::default());
        let l3 = l3
            .with_mount_policy_counters(mount_policy_counters.clone())
            .with_io_governor(io_governor.clone());

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
            periodic_flush_max_staleness_secs: AtomicU64::new(0),
            pending_flush_since_unix_secs: AtomicU64::new(0),
            rebuild_cooldown_secs: AtomicU64::new(REBUILD_COOLDOWN.as_secs()),
            wal_seal_bytes: AtomicU64::new(0),
            pending_flush_events: AtomicU64::new(0),
            pending_flush_bytes: AtomicU64::new(0),
            last_snapshot_time: AtomicU64::new(0),
            roots,
            include_hidden,
            ignore_enabled,
            follow_symlinks,
            exclude_dirs,
            fs_policy_config,
            fast_sync_semaphore: Arc::new(tokio::sync::Semaphore::new(1)),
            dirty_queue: Mutex::new(crate::event::sync::DirtyQueue::default()),
            dirty_notify: Notify::new(),
            recovery_status: Mutex::new(super::RecoveryStatus::default()),
            quarantine_state: Mutex::new(crate::storage::quarantine::QuarantineState::default()),
            freeze_gate: Mutex::new(crate::storage::quarantine::FreezeGate::default()),
            quarantine_verify_pending: AtomicU64::new(0),
            quarantine_verified_roots: AtomicU64::new(0),
            clock_skew: Mutex::new(crate::clock::ClockSkewDetector::new(
                std::time::Duration::from_secs(1),
            )),
            clock_reconciliation_count: AtomicU64::new(0),
            root_case_policies: Mutex::new(Vec::new()),
            ioprio_idle_set: AtomicBool::new(false),
            ioprio_set_failed: AtomicBool::new(false),
            mmap_warmup_enabled: AtomicBool::new(false),
            mmap_warmup_pages: AtomicU64::new(0),
            mmap_warmup_elapsed_ms: AtomicU64::new(0),
            mmap_warmup_cancel_reason: Mutex::new("disabled".to_string()),
            stable_snapshot_enabled: AtomicBool::new(true),
            mount_policy_counters,
            io_governor,
            stats: Arc::new(crate::stats::StatsCollector::new()),
            content_index_enabled: AtomicBool::new(false),
            content_index_config: Mutex::new(crate::config::ContentIndexConfig::default()),
            content_index_docs: Mutex::new(std::collections::HashMap::new()),
            content_indexed_paths: AtomicU64::new(0),
            content_indexed_bytes: AtomicU64::new(0),
            content_index_last_elapsed_ms: AtomicU64::new(0),
            content_hash_queue_pending: AtomicU64::new(0),
            content_hash_candidate_count: AtomicU64::new(0),
            content_hash_confirmed_groups: AtomicU64::new(0),
            content_hash_skipped_count: AtomicU64::new(0),
            content_hash_last_elapsed_ms: AtomicU64::new(0),
            content_hash_last_skip_reason: Mutex::new(String::new()),
            directory_manifests: super::directory_manifest::DirectoryManifestStore::default(),
            lazy_validation_enabled: AtomicBool::new(false),
            lazy_validation_cache_entries: AtomicU64::new(4096),
            lazy_validation_ttl_ns: AtomicU64::new(10_000_000_000),
            lazy_validation_stat_per_sec: AtomicU64::new(50),
            lazy_validation_state: Mutex::new(
                super::lazy_validation::LazyValidationState::default(),
            ),
            lazy_validation_notify: Notify::new(),
            lazy_validation_enqueued: AtomicU64::new(0),
            lazy_validation_completed: AtomicU64::new(0),
            lazy_validation_stale_hits: AtomicU64::new(0),
            lazy_validation_cache_hits: AtomicU64::new(0),
            lazy_validation_rate_limited: AtomicU64::new(0),
            lazy_validation_queue_full: AtomicU64::new(0),
            query_max_verify_per_query: AtomicU64::new(150),
            query_verify_timeout_ms: AtomicU64::new(75),
            query_allow_sync_readdir: AtomicBool::new(false),
            runtime_subtree_tombstones: Mutex::new(Vec::new()),
            memory_report_cache: Mutex::new(super::MemoryReportCache::default()),
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
        Self::load_with_options_follow_excludes_and_fs_policy(
            store,
            roots,
            include_hidden,
            ignore_enabled,
            follow_symlinks,
            exclude_dirs,
            FsPolicyConfig::default(),
        )
        .await
    }

    #[allow(clippy::too_many_arguments)]
    pub async fn load_with_options_follow_excludes_and_fs_policy<S: StorageBackend + ?Sized>(
        store: &S,
        roots: Vec<PathBuf>,
        include_hidden: bool,
        ignore_enabled: bool,
        follow_symlinks: bool,
        exclude_dirs: Vec<String>,
        fs_policy_config: FsPolicyConfig,
    ) -> anyhow::Result<Arc<Self>> {
        Self::load_with_options_follow_excludes_fs_policy_and_io_governor(
            store,
            roots,
            include_hidden,
            ignore_enabled,
            follow_symlinks,
            exclude_dirs,
            fs_policy_config,
            IoGovernorConfig::default(),
        )
        .await
    }

    #[allow(clippy::too_many_arguments)]
    pub async fn load_with_options_follow_excludes_fs_policy_and_io_governor<
        S: StorageBackend + ?Sized,
    >(
        store: &S,
        roots: Vec<PathBuf>,
        include_hidden: bool,
        ignore_enabled: bool,
        follow_symlinks: bool,
        exclude_dirs: Vec<String>,
        fs_policy_config: FsPolicyConfig,
        io_governor_config: IoGovernorConfig,
    ) -> anyhow::Result<Arc<Self>> {
        let index = Arc::new(
            Self::load_or_empty_with_options_follow_excludes_fs_policy_and_io_governor(
                store,
                roots,
                include_hidden,
                ignore_enabled,
                follow_symlinks,
                exclude_dirs,
                fs_policy_config,
                io_governor_config,
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
        Self::load_or_empty_with_options_follow_excludes_and_fs_policy(
            store,
            roots,
            include_hidden,
            ignore_enabled,
            follow_symlinks,
            exclude_dirs,
            FsPolicyConfig::default(),
        )
        .await
    }

    #[allow(clippy::too_many_arguments)]
    pub async fn load_or_empty_with_options_follow_excludes_and_fs_policy<
        S: StorageBackend + ?Sized,
    >(
        store: &S,
        roots: Vec<PathBuf>,
        include_hidden: bool,
        ignore_enabled: bool,
        follow_symlinks: bool,
        exclude_dirs: Vec<String>,
        fs_policy_config: FsPolicyConfig,
    ) -> anyhow::Result<Self> {
        Self::load_or_empty_with_options_follow_excludes_fs_policy_and_io_governor(
            store,
            roots,
            include_hidden,
            ignore_enabled,
            follow_symlinks,
            exclude_dirs,
            fs_policy_config,
            IoGovernorConfig::default(),
        )
        .await
    }

    #[allow(clippy::too_many_arguments)]
    pub async fn load_or_empty_with_options_follow_excludes_fs_policy_and_io_governor<
        S: StorageBackend + ?Sized,
    >(
        store: &S,
        roots: Vec<PathBuf>,
        include_hidden: bool,
        ignore_enabled: bool,
        follow_symlinks: bool,
        exclude_dirs: Vec<String>,
        fs_policy_config: FsPolicyConfig,
        io_governor_config: IoGovernorConfig,
    ) -> anyhow::Result<Self> {
        let l1 = L1Cache::with_capacity(1000);
        let l3 = IndexBuilder::new_with_options_follow_and_excludes(
            roots.clone(),
            include_hidden,
            ignore_enabled,
            follow_symlinks,
            exclude_dirs.clone(),
        )
        .with_fs_policy_config(fs_policy_config.clone());
        let io_governor = Arc::new(IoGovernor::from_config(&io_governor_config));

        let runtime_state = read_recovery_runtime_state(store.path()).unwrap_or_else(|e| {
            tracing::warn!("recovery runtime state read failed: {}", e);
            Default::default()
        });
        let audit = audit_recovery_ledger(store.path(), roots.as_slice(), &runtime_state);

        // 优先加载 stable.v7 / stable.prev.v7，再回退到 legacy v7 单文件快照。
        let stable_path = stable_v7_path_for(store.path());
        let stable_prev_path = stable_prev_v7_path_for(store.path());
        let legacy_v7_path = store.path().with_extension("v7");
        let loaded_candidate = match crate::storage::snapshot_v7::try_load_v7_cold(
            stable_path.as_path(),
            roots.as_slice(),
        ) {
            Ok(Some(stable_data)) => {
                let needs_prev_check = snapshot_file_suspiciously_smaller(
                    stable_path.as_path(),
                    stable_prev_path.as_path(),
                );
                if needs_prev_check {
                    match crate::storage::snapshot_v7::try_load_v7_cold(
                        stable_prev_path.as_path(),
                        roots.as_slice(),
                    ) {
                        Ok(Some(prev_data))
                            if snapshot_suspiciously_smaller(
                                stable_data.file_count(),
                                prev_data.file_count(),
                            ) =>
                        {
                            tracing::warn!(
                                "stable snapshot has {} entries but stable-prev has {}; using stable-prev",
                                stable_data.file_count(),
                                prev_data.file_count()
                            );
                            Some(("stable-prev", prev_data))
                        }
                        Ok(Some(_)) | Ok(None) => Some(("stable", stable_data)),
                        Err(e) => {
                            tracing::warn!("stable-prev load failed during shrink check: {}", e);
                            Some(("stable", stable_data))
                        }
                    }
                } else {
                    Some(("stable", stable_data))
                }
            }
            Ok(None) => None,
            Err(e) => {
                tracing::warn!("stable load failed: {}", e);
                None
            }
        }
        .or_else(|| {
            match crate::storage::snapshot_v7::try_load_v7_cold(
                stable_prev_path.as_path(),
                roots.as_slice(),
            ) {
                Ok(Some(v7_data)) => Some(("stable-prev", v7_data)),
                Ok(None) => None,
                Err(e) => {
                    tracing::warn!("stable-prev load failed: {}", e);
                    None
                }
            }
        })
        .or_else(|| {
            match crate::storage::snapshot_v7::try_load_v7_cold(
                legacy_v7_path.as_path(),
                roots.as_slice(),
            ) {
                Ok(Some(v7_data)) => Some(("legacy-v7", v7_data)),
                Ok(None) => None,
                Err(e) => {
                    tracing::warn!("legacy-v7 load failed: {}", e);
                    None
                }
            }
        });

        if let Some((source, v7_data)) = loaded_candidate {
            tracing::info!(
                "{} snapshot mounted as cold base: {} entries, {} manifest segment(s)",
                source,
                v7_data.file_count(),
                v7_data.cold_segments.len()
            );
            let mut startup_audit = audit.clone();
            if snapshot_too_small_for_roots(
                v7_data.file_count(),
                roots.as_slice(),
                include_hidden,
                ignore_enabled,
                follow_symlinks,
                exclude_dirs.as_slice(),
                &fs_policy_config,
            ) {
                tracing::warn!(
                    "{} snapshot has only {} entries but root probe exceeded the rebuild threshold",
                    source,
                    v7_data.file_count()
                );
                startup_audit.requires_repair = true;
                startup_audit.requires_rebuild = true;
                startup_audit
                    .reasons
                    .push("snapshot_too_small_for_roots".to_string());
            }
            let l2 = Arc::new(PersistentIndex::new_with_roots(roots.clone()));
            let idx = Self::new_with_base_and_io_governor(
                l1,
                l2,
                l3,
                roots,
                include_hidden,
                ignore_enabled,
                follow_symlinks,
                exclude_dirs,
                Some(v7_data),
                io_governor.clone(),
            );
            idx.attach_wal(store)?;
            let sidecar_path = quarantine_sidecar_path_for(store.path());
            if let Err(e) = idx.restore_quarantine_from_sidecar_path(&sidecar_path) {
                tracing::warn!(
                    "quarantine sidecar restore failed for {}: {}",
                    sidecar_path.display(),
                    e
                );
            }
            idx.set_root_case_policy_diagnostics(runtime_state.root_case_policies.clone());
            let checkpoint =
                choose_checkpoint(SnapshotCheckpointSource::from_label(source), &startup_audit);
            let replay = idx.replay_wal_if_any(checkpoint);
            idx.set_startup_recovery_report(startup_report(
                source,
                &runtime_state,
                &startup_audit,
                replay,
            ));
            maybe_trim_rss();
            return Ok(idx);
        }

        // 无可用快照：回退到空索引启动（由上层触发 rebuild）。
        let l2 = Arc::new(PersistentIndex::new_with_roots(roots.clone()));
        let idx = Self::new_with_base_and_io_governor(
            l1,
            l2,
            l3,
            roots,
            include_hidden,
            ignore_enabled,
            follow_symlinks,
            exclude_dirs,
            None,
            io_governor,
        );
        idx.attach_wal(store)?;
        let sidecar_path = quarantine_sidecar_path_for(store.path());
        if let Err(e) = idx.restore_quarantine_from_sidecar_path(&sidecar_path) {
            tracing::warn!(
                "quarantine sidecar restore failed for {}: {}",
                sidecar_path.display(),
                e
            );
        }
        idx.set_root_case_policy_diagnostics(runtime_state.root_case_policies.clone());
        let replay = idx.replay_wal_if_any(0);
        idx.set_startup_recovery_report(startup_report("empty", &runtime_state, &audit, replay));
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
                    events_replayed: r.events_replayed,
                    sealed_used: r.sealed_used,
                    truncated_tail_records: r.truncated_tail_records,
                    damage: r.damage.clone(),
                    gap_detected: r.gap_detected,
                    checkpoint_used: r.checkpoint_used,
                };
                if !r.records.is_empty() {
                    tracing::info!(
                        "WAL replay: events={} root_state_events={} sealed_used={} truncated_tail={}",
                        r.events_replayed,
                        r.root_events_replayed,
                        r.sealed_used,
                        r.truncated_tail_records
                    );
                    let mut pending_events = Vec::new();
                    for record in &r.records {
                        match record {
                            WalReplayRecord::File(event) => pending_events.push(event.clone()),
                            WalReplayRecord::Root(root) => {
                                if !pending_events.is_empty() {
                                    self.apply_events_inner(&pending_events, false);
                                    pending_events.clear();
                                }
                                self.restore_quarantine_from_wal(std::slice::from_ref(root));
                            }
                        }
                    }
                    if !pending_events.is_empty() {
                        self.apply_events_inner(&pending_events, false);
                    }
                }
                summary
            }
            Err(e) => {
                tracing::warn!("WAL replay failed, ignoring: {}", e);
                WalReplaySummary {
                    events_replayed: 0,
                    sealed_used: 0,
                    truncated_tail_records: 1,
                    damage: WalReplayDamage {
                        truncated_tail_records: 1,
                        unknown_scope: true,
                        dirty_dirs: Vec::new(),
                    },
                    gap_detected: false,
                    checkpoint_used: checkpoint_seal_id,
                }
            }
        }
    }
}

fn snapshot_too_small_for_roots(
    snapshot_count: usize,
    roots: &[PathBuf],
    include_hidden: bool,
    ignore_enabled: bool,
    follow_symlinks: bool,
    exclude_dirs: &[String],
    fs_policy_config: &FsPolicyConfig,
) -> bool {
    const MAX_BASE_COUNT_FOR_ROOT_PROBE: usize = 10_000;
    if snapshot_count == 0 || snapshot_count >= MAX_BASE_COUNT_FOR_ROOT_PROBE {
        return false;
    }

    let allowed_gap = (snapshot_count / 10).max(10_000);
    let probe_limit = snapshot_count.saturating_add(allowed_gap).saturating_add(1);
    let fs_policy = FsPolicy::current_with_config(fs_policy_config.clone());
    let mut observed = 0usize;

    for root in roots {
        let mut builder = ignore::WalkBuilder::new(root);
        builder
            .hidden(!include_hidden)
            .follow_links(follow_symlinks)
            .ignore(ignore_enabled)
            .git_ignore(ignore_enabled)
            .git_global(ignore_enabled)
            .git_exclude(ignore_enabled);

        let filter_root = root.clone();
        let exclude_dirs = exclude_dirs.to_vec();
        let fs_policy = fs_policy.clone();
        builder.filter_entry(move |entry| {
            (exclude_dirs.is_empty() || !path_has_excluded_component(entry.path(), &exclude_dirs))
                && fs_policy
                    .as_ref()
                    .map(|policy| {
                        policy
                            .check_path(entry.path(), Some(filter_root.as_path()))
                            .is_allowed()
                    })
                    .unwrap_or(true)
        });

        for entry in builder.build().filter_map(Result::ok) {
            let Some(file_type) = entry.file_type() else {
                continue;
            };
            if !file_type.is_file() && !file_type.is_dir() {
                continue;
            }
            if file_type.is_dir() && entry.path() == root.as_path() {
                continue;
            }
            let Ok(meta) = entry.metadata() else {
                continue;
            };
            if FileKey::from_path_and_metadata(entry.path(), &meta).is_none() {
                continue;
            }
            observed = observed.saturating_add(1);
            if observed >= probe_limit {
                return true;
            }
        }
    }

    false
}

fn snapshot_suspiciously_smaller(current: usize, previous: usize) -> bool {
    if previous < 10_000 || current >= previous {
        return false;
    }
    let allowed_loss = (previous / 10).max(10_000);
    current < previous.saturating_sub(allowed_loss)
}

fn snapshot_file_suspiciously_smaller(current: &Path, previous: &Path) -> bool {
    let Ok(current_len) = std::fs::metadata(current).map(|meta| meta.len()) else {
        return false;
    };
    let Ok(previous_len) = std::fs::metadata(previous).map(|meta| meta.len()) else {
        return false;
    };
    if previous_len < 1024 * 1024 || current_len >= previous_len {
        return false;
    }
    current_len < previous_len.saturating_sub(previous_len / 10)
}

#[derive(Clone, Debug, Default)]
struct WalReplaySummary {
    events_replayed: usize,
    sealed_used: usize,
    truncated_tail_records: usize,
    damage: WalReplayDamage,
    gap_detected: bool,
    checkpoint_used: u64,
}

fn startup_report(
    source: &str,
    runtime_state: &crate::storage::snapshot::RecoveryRuntimeState,
    audit: &RecoveryAuditReport,
    replay: WalReplaySummary,
) -> StartupRecoveryReport {
    let mut audit = audit.clone();
    audit.wal_checkpoint = replay.checkpoint_used;
    audit.sealed_wal_used_after_checkpoint = audit
        .sealed_wal_ids
        .iter()
        .filter(|id| **id > replay.checkpoint_used)
        .count();

    let mut reasons = audit.reasons.clone();
    if !runtime_state.last_clean_shutdown {
        reasons.push("unclean_shutdown".to_string());
    }
    if replay.truncated_tail_records > 0 {
        reasons.push("wal_tail_truncated".to_string());
    }
    if replay.gap_detected {
        reasons.push("wal_gap".to_string());
    }
    if source == "empty" {
        reasons.push("no_snapshot".to_string());
    }
    reasons.sort();
    reasons.dedup();
    let soft_reasons = recovery_reasons_matching(&reasons, is_soft_repair_reason);
    let hard_reasons = recovery_reasons_matching(&reasons, is_hard_rebuild_reason);
    let soft_repair_needed = !soft_reasons.is_empty();
    let hard_rebuild_needed = !hard_reasons.is_empty() || audit.requires_rebuild;
    let startup_scan_required =
        source == "empty" || hard_rebuild_needed || replay.gap_detected || !hard_reasons.is_empty();
    let deferred_dirty_dirs = replay.damage.dirty_dirs.clone();
    let deferred_unknown_scope = !startup_scan_required
        && (replay.damage.unknown_scope
            || (!runtime_state.last_clean_shutdown && deferred_dirty_dirs.is_empty()));
    let deferred_repair = soft_repair_needed
        && !startup_scan_required
        && (deferred_unknown_scope || !deferred_dirty_dirs.is_empty());
    let repair_reason_counts = repair_reason_counts(&reasons);

    StartupRecoveryReport {
        snapshot_source: source.to_string(),
        wal_events_replayed: replay.events_replayed,
        wal_sealed_used: replay.sealed_used,
        wal_truncated_tail_records: replay.truncated_tail_records,
        wal_gap_detected: audit.wal_gap_detected || replay.gap_detected,
        wal_checkpoint_used: replay.checkpoint_used,
        startup_scan_required,
        requires_repair: source == "empty"
            || audit.requires_repair
            || !runtime_state.last_clean_shutdown
            || replay.truncated_tail_records > 0
            || replay.gap_detected,
        requires_rebuild: audit.requires_rebuild,
        soft_repair_needed,
        deferred_repair,
        deferred_dirty_dirs,
        deferred_unknown_scope,
        hard_rebuild_needed,
        previous_clean_shutdown: runtime_state.last_clean_shutdown,
        reasons,
        soft_reasons,
        hard_reasons,
        repair_reason_counts,
        audit,
    }
}

fn recovery_reasons_matching(reasons: &[String], predicate: impl Fn(&str) -> bool) -> Vec<String> {
    reasons
        .iter()
        .filter(|reason| predicate(reason.as_str()))
        .cloned()
        .collect()
}

fn is_soft_repair_reason(reason: &str) -> bool {
    matches!(reason, "unclean_shutdown" | "wal_tail_truncated")
}

fn is_hard_rebuild_reason(reason: &str) -> bool {
    matches!(
        reason,
        "wal_gap"
            | "bad_current_wal"
            | "missing_segment"
            | "bad_segment_sidecar"
            | "lsm_dir_unreadable"
    ) || reason.starts_with("bad_manifest:")
}

fn repair_reason_counts(reasons: &[String]) -> Vec<super::RecoveryReasonCount> {
    let mut counts = std::collections::BTreeMap::<String, usize>::new();
    for reason in reasons {
        *counts.entry(reason.clone()).or_default() += 1;
    }
    counts
        .into_iter()
        .map(|(reason, count)| super::RecoveryReasonCount { reason, count })
        .collect()
}
