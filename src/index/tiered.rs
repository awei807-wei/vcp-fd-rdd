use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use arc_swap::ArcSwap;
use parking_lot::{Mutex, RwLock};

use crate::core::{AdaptiveScheduler, EventRecord, EventType, FileMeta, Task};
use crate::index::l1_cache::L1Cache;
use crate::index::l2_partition::PersistentIndex;
use crate::index::l3_cold::IndexBuilder;
use crate::index::mmap_index::MmapIndex;
use crate::query::matcher::create_matcher;
use crate::stats::{EventPipelineStats, MemoryReport, OverlayStats, RebuildStats};
use crate::storage::snapshot::{LoadedSnapshot, SnapshotStore};
use crate::storage::wal::WalStore;

const REBUILD_COOLDOWN: Duration = Duration::from_secs(60);
const COMPACTION_DELTA_THRESHOLD: usize = 4;

#[cfg(feature = "mimalloc")]
fn maybe_trim_rss() {
    // mimalloc 作为全局分配器时，glibc 的 malloc_trim 无效，需要调用 mimalloc 自己的回收。
    extern "C" {
        fn mi_collect(force: bool);
    }
    unsafe { mi_collect(true) };
}

#[cfg(all(not(feature = "mimalloc"), target_os = "linux", target_env = "gnu"))]
fn maybe_trim_rss() {
    // glibc malloc 的主动回吐：释放尽可能多的空闲块回 OS。
    unsafe {
        libc::malloc_trim(0);
    }
}

#[cfg(all(
    not(feature = "mimalloc"),
    not(all(target_os = "linux", target_env = "gnu"))
))]
fn maybe_trim_rss() {}

#[derive(Clone)]
struct DiskLayer {
    id: u64,
    idx: Arc<MmapIndex>,
    deleted_paths: Vec<Vec<u8>>,
}

#[derive(Debug)]
struct RebuildState {
    in_progress: bool,
    pending_events: std::collections::HashMap<PathBuf, EventRecord>,
    /// 最近一次 rebuild 开始时间（用于冷却/合并）
    last_started_at: Option<Instant>,
    /// 冷却期内收到 rebuild 请求时，设置该标记；在冷却到期后合并执行一次
    requested: bool,
    /// 冷却期触发的延迟 rebuild 是否已调度（避免重复 spawn sleep 线程）
    scheduled: bool,
}

impl Default for RebuildState {
    fn default() -> Self {
        Self {
            in_progress: false,
            pending_events: std::collections::HashMap::new(),
            last_started_at: None,
            requested: false,
            scheduled: false,
        }
    }
}

#[derive(Debug, Default)]
struct OverlayState {
    /// delete / rename-from：需要跨段屏蔽更老 segment 结果，并在 flush 时写入 seg-*.del
    deleted_paths: std::collections::HashSet<Vec<u8>>,
    /// create/modify/rename-to：用于抵消同一路径的 deleted（delete→recreate）
    upserted_paths: std::collections::HashSet<Vec<u8>>,
    /// deleted_paths 中路径字节总量（不含 HashSet 结构开销）
    deleted_bytes: u64,
    /// upserted_paths 中路径字节总量（不含 HashSet 结构开销）
    upserted_bytes: u64,
}

/// 三级索引：L1 热缓存 → L2 持久索引（内存常驻）→ L3 构建器（不在查询链路）
pub struct TieredIndex {
    pub l1: L1Cache,
    pub l2: ArcSwap<PersistentIndex>,
    disk_layers: RwLock<Vec<DiskLayer>>,
    pub l3: IndexBuilder,
    scheduler: Mutex<AdaptiveScheduler>,
    wal: Mutex<Option<Arc<WalStore>>>,
    pub event_seq: AtomicU64,
    rebuild_state: Mutex<RebuildState>,
    overlay_state: Mutex<OverlayState>,
    apply_gate: RwLock<()>,
    compaction_in_progress: AtomicBool,
    pub roots: Vec<PathBuf>,
}

impl TieredIndex {
    fn new(
        l1: L1Cache,
        l2: Arc<PersistentIndex>,
        l3: IndexBuilder,
        roots: Vec<PathBuf>,
        disk_layers: Vec<DiskLayer>,
    ) -> Self {
        Self {
            l1,
            l2: ArcSwap::from(l2),
            disk_layers: RwLock::new(disk_layers),
            l3,
            scheduler: Mutex::new(AdaptiveScheduler::new()),
            wal: Mutex::new(None),
            event_seq: AtomicU64::new(0),
            rebuild_state: Mutex::new(RebuildState::default()),
            overlay_state: Mutex::new(OverlayState::default()),
            apply_gate: RwLock::new(()),
            compaction_in_progress: AtomicBool::new(false),
            roots,
        }
    }

    /// 直接以空索引启动（显式忽略快照加载）
    pub fn empty(roots: Vec<PathBuf>) -> Self {
        let l1 = L1Cache::with_capacity(1000);
        let l2 = Arc::new(PersistentIndex::new_with_roots(roots.clone()));
        let l3 = IndexBuilder::new(roots.clone());
        Self::new(l1, l2, l3, roots, Vec::new())
    }

    /// 从快照加载或空索引启动
    pub async fn load_or_empty(store: &SnapshotStore, roots: Vec<PathBuf>) -> anyhow::Result<Self> {
        let l1 = L1Cache::with_capacity(1000);
        let l3 = IndexBuilder::new(roots.clone());

        // 阶段 C：优先加载 LSM 目录布局（base + delta segments），启动后不做全量 hydration。
        if let Ok(Some(lsm)) = store.load_lsm_if_valid(&roots) {
            let mut layers: Vec<DiskLayer> = Vec::new();
            if let Some(b) = lsm.base {
                layers.push(DiskLayer {
                    id: b.id,
                    idx: Arc::new(MmapIndex::new(b.snap)),
                    deleted_paths: b.deleted_paths,
                });
            }
            for d in lsm.deltas {
                layers.push(DiskLayer {
                    id: d.id,
                    idx: Arc::new(MmapIndex::new(d.snap)),
                    deleted_paths: d.deleted_paths,
                });
            }

            let idx = Self::new(
                l1,
                Arc::new(PersistentIndex::new_with_roots(roots.clone())),
                l3,
                roots,
                layers,
            );
            idx.attach_wal(store)?;
            idx.replay_wal_if_any(lsm.wal_seal_id);
            return Ok(idx);
        }

        // 兼容：legacy v6 单文件（mmap + lazy decode），作为长期 base 使用（不再 hydration）。
        if let Ok(Some(snap)) = store.load_v6_mmap_if_valid(&roots) {
            let base = DiskLayer {
                id: 0,
                idx: Arc::new(MmapIndex::new(snap)),
                deleted_paths: Vec::new(),
            };
            let idx = Self::new(
                l1,
                Arc::new(PersistentIndex::new_with_roots(roots.clone())),
                l3,
                roots,
                vec![base],
            );
            idx.attach_wal(store)?;
            // legacy v6 没有 LSM manifest checkpoint：保守回放全部 WAL（如果存在）。
            idx.replay_wal_if_any(0);
            return Ok(idx);
        }

        let l2 = match store.load_if_valid().await {
            Ok(Some(LoadedSnapshot::V5(snap))) => {
                tracing::info!("Loaded index snapshot v5: {} docs", snap.metas.len());
                PersistentIndex::from_snapshot_v5(snap, roots.clone())
            }
            Ok(Some(LoadedSnapshot::V4(snap))) => {
                tracing::info!("Loaded index snapshot v4: {} docs", snap.metas.len());
                PersistentIndex::from_snapshot_v4(snap, roots.clone())
            }
            Ok(Some(LoadedSnapshot::V3(snap))) => {
                tracing::info!("Loaded index snapshot v3: {} files", snap.files.len());
                PersistentIndex::from_snapshot_v3(snap, roots.clone())
            }
            Ok(Some(LoadedSnapshot::V2(snap))) => {
                tracing::info!("Loaded index snapshot v2: {} files", snap.files.len());
                PersistentIndex::from_snapshot_v2(snap, roots.clone())
            }
            Ok(None) => {
                tracing::info!("No valid snapshot, starting with empty index");
                PersistentIndex::new_with_roots(roots.clone())
            }
            Err(e) => {
                tracing::warn!("Failed to load snapshot: {}, starting empty", e);
                PersistentIndex::new_with_roots(roots.clone())
            }
        };

        let idx = Self::new(l1, Arc::new(l2), l3, roots, Vec::new());
        idx.attach_wal(store)?;
        idx.replay_wal_if_any(0);
        Ok(idx)
    }

    pub fn attach_wal(&self, store: &SnapshotStore) -> anyhow::Result<()> {
        let mut g = self.wal.lock();
        if g.is_some() {
            return Ok(());
        }
        let wal = WalStore::open_in_dir(store.derived_lsm_dir_path())?;
        *g = Some(Arc::new(wal));
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

    fn try_start_rebuild_force(&self) -> bool {
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
        let mut schedule_after: Option<Duration> = None;
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
                return true;
            }
        }

        if let Some(wait) = schedule_after {
            let idx = self.clone();
            std::thread::spawn(move || {
                std::thread::sleep(wait);
                idx.spawn_rebuild("cooldown elapsed (merged)");
            });
        }
        false
    }

    fn finish_rebuild(self: &Arc<Self>, new_l2: Arc<PersistentIndex>) -> bool {
        loop {
            let batch = {
                let mut st = self.rebuild_state.lock();
                if st.pending_events.is_empty() {
                    // 切换点：持锁判空 -> 原子切换，避免丢事件窗口。
                    self.l1.clear();
                    self.l2.store(new_l2.clone());
                    // rebuild 语义：新索引为权威数据源，旧 mmap segments 可能已过期，清空以避免双基座。
                    self.disk_layers.write().clear();
                    {
                        let mut ov = self.overlay_state.lock();
                        ov.deleted_paths.clear();
                        ov.upserted_paths.clear();
                        ov.deleted_bytes = 0;
                        ov.upserted_bytes = 0;
                    }
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
                    .map(|(_, ev)| ev)
                    .collect::<Vec<_>>();
                v.sort_by_key(|e| e.seq);
                v
            };

            new_l2.apply_events(&batch);
        }
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
                idx.spawn_rebuild("merged rebuild request after full build");
            }
        });
    }

    /// overflow / watcher 异常时的兜底：清空索引并后台全量重建，避免索引长期漂移。
    pub fn spawn_rebuild(self: &Arc<Self>, reason: &'static str) {
        if !self.try_start_rebuild_with_cooldown(reason) {
            // 冷却/合并：不立即执行
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
                idx.spawn_rebuild("merged rebuild request after rebuild");
            }
        });
    }

    /// 查询入口：L1 → L2，不扫盘
    pub fn query(&self, keyword: &str) -> Vec<FileMeta> {
        let matcher = create_matcher(keyword);

        // L1: 热缓存
        if let Some(results) = self.l1.query(matcher.as_ref()) {
            tracing::debug!("L1 hit: {} results", results.len());
            return results;
        }

        use std::os::unix::ffi::OsStrExt;

        // blocked：newest→oldest 合并语义的“屏蔽集合”（delete + 已输出路径）
        let mut blocked: std::collections::HashSet<Vec<u8>> = std::collections::HashSet::new();
        {
            let st = self.overlay_state.lock();
            for p in &st.deleted_paths {
                blocked.insert(p.clone());
            }
        }

        let mut results: Vec<FileMeta> = Vec::new();

        // L2: 内存 Delta（newest）
        let l2 = self.l2.load_full();
        for r in l2.query(matcher.as_ref()) {
            let pb = r.path.as_os_str().as_bytes();
            if blocked.contains(pb) {
                continue;
            }
            blocked.insert(pb.to_vec());
            results.push(r);
        }

        // Disk segments: newest → oldest
        let layers = self.disk_layers.read().clone();
        for layer in layers.iter().rev() {
            for p in &layer.deleted_paths {
                blocked.insert(p.clone());
            }
            for r in layer.idx.query(matcher.as_ref()) {
                let pb = r.path.as_os_str().as_bytes();
                if blocked.contains(pb) {
                    continue;
                }
                blocked.insert(pb.to_vec());
                results.push(r);
            }
        }

        if !results.is_empty() {
            tracing::debug!("Query hit: {} results", results.len());
            // 回填 L1（有界）
            for meta in results.iter().take(10) {
                self.l1.insert(meta.clone());
            }
            return results;
        }

        // miss：不在查询链路扫盘，返回空
        // 可选：触发后台补扫
        l2.maybe_schedule_repair();
        Vec::new()
    }

    /// 批量应用事件到索引
    pub fn apply_events(&self, events: &[EventRecord]) {
        self.apply_events_inner(events, true);
    }

    fn apply_events_inner(&self, events: &[EventRecord], log_to_wal: bool) {
        // flush/compaction 期间需要短暂阻塞写入，避免“指针 swap 后仍写旧 delta”的竞态。
        let _g = self.apply_gate.read();

        // WAL：先写后用（best-effort）。replay 场景下禁用写回，避免重复追加。
        if log_to_wal {
            if let Some(wal) = self.wal.lock().clone() {
                if let Err(e) = wal.append(events) {
                    tracing::warn!("WAL append failed (continuing without durability): {}", e);
                }
            }
        }

        use std::os::unix::ffi::OsStrExt;

        // 若 rebuild 在进行：先缓冲 pending 事件；并在持锁期间捕获当前 l2 指针，
        // 避免切换窗口导致“事件已缓冲但应用到了新索引”而重复回放。
        let l2 = {
            let mut st = self.rebuild_state.lock();
            if st.in_progress {
                // 有界化：按 path 去重，只保留每条路径的最新事件（避免 rebuild 期间无限堆积）。
                for ev in events {
                    let key = ev.path.clone();
                    match st.pending_events.get(&key) {
                        Some(old) if old.seq >= ev.seq => {}
                        _ => {
                            st.pending_events.insert(key, ev.clone());
                        }
                    }
                }
            }

            // overlay：记录跨段 delete/upsert（用于屏蔽更老 segment 结果 + flush sidecar）
            {
                let mut ov = self.overlay_state.lock();
                for ev in events {
                    let path_bytes = ev.path.as_os_str().as_bytes();
                    match &ev.event_type {
                        EventType::Delete => {
                            if ov.upserted_paths.remove(path_bytes) {
                                ov.upserted_bytes =
                                    ov.upserted_bytes.saturating_sub(path_bytes.len() as u64);
                            }
                            if !ov.deleted_paths.contains(path_bytes) {
                                ov.deleted_paths.insert(path_bytes.to_vec());
                                ov.deleted_bytes += path_bytes.len() as u64;
                            }
                        }
                        EventType::Create | EventType::Modify => {
                            if ov.deleted_paths.remove(path_bytes) {
                                ov.deleted_bytes =
                                    ov.deleted_bytes.saturating_sub(path_bytes.len() as u64);
                            }
                            if !ov.upserted_paths.contains(path_bytes) {
                                ov.upserted_paths.insert(path_bytes.to_vec());
                                ov.upserted_bytes += path_bytes.len() as u64;
                            }
                        }
                        EventType::Rename { from } => {
                            let from_bytes = from.as_os_str().as_bytes();
                            if ov.upserted_paths.remove(from_bytes) {
                                ov.upserted_bytes =
                                    ov.upserted_bytes.saturating_sub(from_bytes.len() as u64);
                            }
                            if !ov.deleted_paths.contains(from_bytes) {
                                ov.deleted_paths.insert(from_bytes.to_vec());
                                ov.deleted_bytes += from_bytes.len() as u64;
                            }

                            if ov.deleted_paths.remove(path_bytes) {
                                ov.deleted_bytes =
                                    ov.deleted_bytes.saturating_sub(path_bytes.len() as u64);
                            }
                            if !ov.upserted_paths.contains(path_bytes) {
                                ov.upserted_paths.insert(path_bytes.to_vec());
                                ov.upserted_bytes += path_bytes.len() as u64;
                            }
                        }
                    }
                }
            }

            self.l2.load_full()
        };

        l2.apply_events(events);

        // 同步更新 L1
        for ev in events {
            match &ev.event_type {
                EventType::Delete => {
                    self.l1.remove_by_path(&ev.path);
                }
                EventType::Rename { from } => {
                    self.l1.remove_by_path(from);
                }
                _ => {}
            }
        }

        self.event_seq
            .fetch_add(events.len() as u64, Ordering::Relaxed);
    }

    /// 原子快照
    pub async fn snapshot_now(self: &Arc<Self>, store: &SnapshotStore) -> anyhow::Result<()> {
        // Flush：把当前内存 Delta 刷盘为新 Segment；必要时触发后台 compaction。
        let (old_delta, deleted_paths, layers_snapshot, wal_seal_id) = {
            let _wg = self.apply_gate.write();

            let delta = self.l2.load_full();
            let delta_dirty = delta.is_dirty();

            let mut ov = self.overlay_state.lock();
            let overlay_dirty = !ov.deleted_paths.is_empty();
            if !delta_dirty && !overlay_dirty {
                tracing::debug!("No delta/overlay changes, skipping flush");
                return Ok(());
            }

            // WAL：在 snapshot 边界 seal，确保新事件进入新 WAL（并可由 manifest checkpoint 判定回放范围）。
            let wal_seal_id = match self.wal.lock().clone() {
                Some(w) => match w.seal() {
                    Ok(id) => id,
                    Err(e) => {
                        tracing::warn!("WAL seal failed, continuing: {}", e);
                        0
                    }
                },
                None => 0,
            };

            let old = self.l2.swap(Arc::new(PersistentIndex::new_with_roots(
                self.roots.clone(),
            )));

            // 只保留“仍然有效”的 delete：若本轮 delta 又 upsert 了同一路径，则认为 delete 被抵消。
            let mut deleted: Vec<Vec<u8>> = Vec::new();
            for p in ov.deleted_paths.iter() {
                if !ov.upserted_paths.contains(p) {
                    deleted.push(p.clone());
                }
            }
            ov.deleted_paths.clear();
            ov.upserted_paths.clear();
            ov.deleted_bytes = 0;
            ov.upserted_bytes = 0;

            (old, deleted, self.disk_layers.read().clone(), wal_seal_id)
        };

        // 判断是否已有 LSM manifest：无则先 bootstrap 为 base（避免 legacy base 被“遗忘”）。
        let roots = self.roots.clone();
        let lsm_present = store.load_lsm_if_valid(&roots).ok().flatten().is_some();

        if !lsm_present {
            tracing::info!("LSM manifest not found, bootstrapping a new base segment...");

            let merged = PersistentIndex::new_with_roots(roots.clone());

            // 先灌入现有 disk base（可能是 legacy v6）。
            for layer in &layers_snapshot {
                layer.idx.for_each_live_meta(|m| merged.upsert_rename(m));
            }

            // 再应用跨段 delete（delete/rename-from）。
            for p in &deleted_paths {
                let pb = pathbuf_from_bytes(p);
                merged.mark_deleted_by_path(&pb);
            }

            // 最后灌入本次 delta（newest）。
            old_delta.for_each_live_meta(|m| merged.upsert_rename(m));

            let segs = merged.export_segments_v6();
            let base = store
                .lsm_replace_base_v6(&segs, None, &roots, wal_seal_id)
                .await?;

            let new_layer = DiskLayer {
                id: base.id,
                idx: Arc::new(MmapIndex::new(base.snap)),
                deleted_paths: base.deleted_paths,
            };

            *self.disk_layers.write() = vec![new_layer];
            self.l1.clear();
            if let Some(w) = self.wal.lock().clone() {
                let _ = w.cleanup_sealed_up_to(wal_seal_id);
            }
            return Ok(());
        }

        let segs = old_delta.export_segments_v6();
        let seg = store
            .lsm_append_delta_v6(&segs, &deleted_paths, &roots, wal_seal_id)
            .await?;

        self.disk_layers.write().push(DiskLayer {
            id: seg.id,
            idx: Arc::new(MmapIndex::new(seg.snap)),
            deleted_paths: seg.deleted_paths,
        });
        self.l1.clear();
        if let Some(w) = self.wal.lock().clone() {
            let _ = w.cleanup_sealed_up_to(wal_seal_id);
        }

        // compaction：段数达到阈值后后台合并
        self.maybe_spawn_compaction(store.path().to_path_buf());
        Ok(())
    }

    /// 定期快照循环
    pub async fn snapshot_loop(self: Arc<Self>, store: Arc<SnapshotStore>, interval_secs: u64) {
        loop {
            tokio::time::sleep(std::time::Duration::from_secs(interval_secs)).await;
            if let Err(e) = self.snapshot_now(&store).await {
                tracing::error!("Snapshot failed: {}", e);
            }
        }
    }

    pub fn file_count(&self) -> usize {
        let l2 = self.l2.load_full().file_count();
        if l2 > 0 {
            return l2;
        }
        self.disk_layers
            .read()
            .first()
            .map(|b| b.idx.file_count_estimate())
            .unwrap_or(0)
    }

    /// 生成完整内存报告
    pub fn memory_report(&self, pipeline_stats: EventPipelineStats) -> MemoryReport {
        let overlay = {
            let ov = self.overlay_state.lock();
            OverlayStats {
                deleted_paths: ov.deleted_paths.len(),
                upserted_paths: ov.upserted_paths.len(),
                deleted_bytes: ov.deleted_bytes,
                upserted_bytes: ov.upserted_bytes,
            }
        };

        let rebuild = {
            let st = self.rebuild_state.lock();
            RebuildStats {
                in_progress: st.in_progress,
                pending_paths: st.pending_events.len(),
            }
        };

        MemoryReport {
            l1: self.l1.memory_stats(),
            l2: self.l2.load_full().memory_stats(),
            disk_segments: self.disk_layers.read().len(),
            event_pipeline: pipeline_stats,
            overlay,
            rebuild,
            process_rss_bytes: MemoryReport::read_process_rss(),
        }
    }

    /// 定期内存报告循环
    pub async fn memory_report_loop(
        self: Arc<Self>,
        pipeline_stats_fn: Arc<dyn Fn() -> EventPipelineStats + Send + Sync>,
        interval_secs: u64,
    ) {
        // 首次报告延迟 5 秒
        tokio::time::sleep(std::time::Duration::from_secs(5)).await;

        loop {
            let stats = pipeline_stats_fn();
            let report = self.memory_report(stats);
            tracing::info!("\n{}", report);
            tokio::time::sleep(std::time::Duration::from_secs(interval_secs)).await;
        }
    }

    fn maybe_spawn_compaction(self: &Arc<Self>, store_path: PathBuf) {
        let layers = self.disk_layers.read().clone();
        let delta_count = layers.len().saturating_sub(1);
        if delta_count < COMPACTION_DELTA_THRESHOLD {
            return;
        }

        // 避免并发 compaction
        if self.compaction_in_progress.swap(true, Ordering::AcqRel) {
            return;
        }

        let idx = self.clone();
        tokio::spawn(async move {
            if let Err(e) = idx.compact_layers(store_path, layers).await {
                tracing::error!("Compaction failed: {}", e);
            }
            idx.compaction_in_progress.store(false, Ordering::Release);
        });
    }

    async fn compact_layers(
        self: &Arc<Self>,
        store_path: PathBuf,
        layers_snapshot: Vec<DiskLayer>,
    ) -> anyhow::Result<()> {
        if layers_snapshot.is_empty() {
            return Ok(());
        }
        if layers_snapshot[0].id == 0 {
            // legacy base 只能通过 bootstrap 进入 LSM；此处不做跨体系 compaction。
            return Ok(());
        }

        tracing::info!(
            "Starting compaction: base={} deltas={}",
            layers_snapshot[0].id,
            layers_snapshot.len().saturating_sub(1)
        );

        let roots = self.roots.clone();
        let merged = PersistentIndex::new_with_roots(roots.clone());

        for layer in &layers_snapshot {
            for p in &layer.deleted_paths {
                let pb = pathbuf_from_bytes(p);
                merged.mark_deleted_by_path(&pb);
            }
            layer.idx.for_each_live_meta(|m| merged.upsert_rename(m));
        }

        let segs = merged.export_segments_v6();
        let store = SnapshotStore::new(store_path.clone());
        let wal_seal_id = store.lsm_manifest_wal_seal_id().unwrap_or(0);

        let base_id = layers_snapshot[0].id;
        let delta_ids = layers_snapshot
            .iter()
            .skip(1)
            .map(|l| l.id)
            .collect::<Vec<_>>();
        let new_base = store
            .lsm_replace_base_v6(
                &segs,
                Some((base_id, delta_ids.clone())),
                &roots,
                wal_seal_id,
            )
            .await?;

        let new_layer = DiskLayer {
            id: new_base.id,
            idx: Arc::new(MmapIndex::new(new_base.snap)),
            deleted_paths: new_base.deleted_paths,
        };

        // 仅当段列表未变化时才替换（弱 CAS）
        {
            let mut cur = self.disk_layers.write();
            let cur_ids = cur.iter().map(|l| l.id).collect::<Vec<_>>();
            let snap_ids = layers_snapshot.iter().map(|l| l.id).collect::<Vec<_>>();
            if cur_ids == snap_ids {
                *cur = vec![new_layer];
                self.l1.clear();
            }
        }

        // 清理旧段文件（best-effort；失败不影响正确性）
        let dir = lsm_dir_from_store_path(&store_path);
        for id in layers_snapshot.iter().map(|l| l.id) {
            if id == 0 || id == new_base.id {
                continue;
            }
            let _ = std::fs::remove_file(dir.join(format!("seg-{id:016x}.db")));
            let _ = std::fs::remove_file(dir.join(format!("seg-{id:016x}.del")));
        }

        tracing::info!("Compaction complete: new_base={}", new_base.id);
        Ok(())
    }

    #[cfg(test)]
    fn rebuild_in_progress(&self) -> bool {
        self.rebuild_state.lock().in_progress
    }
}

fn pathbuf_from_bytes(bytes: &[u8]) -> PathBuf {
    use std::os::unix::ffi::OsStringExt;
    PathBuf::from(std::ffi::OsString::from_vec(bytes.to_vec()))
}

fn lsm_dir_from_store_path(path: &PathBuf) -> PathBuf {
    let ext = path.extension().and_then(|s| s.to_str());
    if ext == Some("d") || path.is_dir() {
        return path.clone();
    }
    path.with_extension("d")
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::{EventRecord, EventType};
    use crate::storage::snapshot::SnapshotStore;
    use std::path::PathBuf;

    fn mk_event(seq: u64, event_type: EventType, path: PathBuf) -> EventRecord {
        EventRecord {
            seq,
            timestamp: std::time::SystemTime::now(),
            event_type,
            path,
        }
    }

    fn unique_tmp_dir(tag: &str) -> PathBuf {
        let nanos = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        std::env::temp_dir().join(format!("fd-rdd-{}-{}", tag, nanos))
    }

    #[test]
    fn rebuild_with_pending_events_no_loss() {
        let root = unique_tmp_dir("rebuild");
        std::fs::create_dir_all(&root).unwrap();

        let old_path = root.join("old_aaa.txt");
        std::fs::write(&old_path, b"old").unwrap();

        let idx = Arc::new(TieredIndex::empty(vec![root.clone()]));

        // 先让旧索引里有内容（模拟在线服务已有数据）。
        idx.apply_events(&[mk_event(1, EventType::Create, old_path.clone())]);
        assert!(!idx.query("old_aaa").is_empty());

        // 开始 rebuild：此时 apply_events 会进入 pending 缓冲。
        assert!(idx.try_start_rebuild_force());
        assert!(idx.rebuild_in_progress());

        // new L2：模拟 full_build 的结果（这里直接应用一次 Create）。
        let new_l2 = Arc::new(PersistentIndex::new_with_roots(vec![root.clone()]));
        new_l2.apply_events(&[mk_event(2, EventType::Create, old_path.clone())]);

        // rebuild 期间新增文件：必须在切换后仍可查询到。
        let new_path = root.join("new_bbb.txt");
        std::fs::write(&new_path, b"new").unwrap();
        idx.apply_events(&[mk_event(3, EventType::Create, new_path.clone())]);

        // 完成：回放 pending -> 原子切换
        idx.finish_rebuild(new_l2);
        assert!(!idx.rebuild_in_progress());

        assert!(!idx.query("old_aaa").is_empty());
        assert!(!idx.query("new_bbb").is_empty());
    }

    #[tokio::test]
    async fn lsm_layering_delete_blocks_base() {
        use crate::core::{FileKey, FileMeta};
        use crate::index::PersistentIndex;
        use std::os::unix::ffi::OsStrExt;

        let root = unique_tmp_dir("lsm-del");
        std::fs::create_dir_all(&root).unwrap();

        let alpha = root.join("alpha_test.txt");
        let gamma = root.join("gamma_test.txt");

        let store = SnapshotStore::new(root.join("index.db"));

        // base: alpha
        let base_idx = PersistentIndex::new_with_roots(vec![root.clone()]);
        base_idx.upsert(FileMeta {
            file_key: FileKey { dev: 1, ino: 1 },
            path: alpha.clone(),
            size: 1,
            mtime: None,
        });
        store
            .lsm_replace_base_v6(&base_idx.export_segments_v6(), None, &[root.clone()], 0)
            .await
            .unwrap();

        // delta seg: gamma + delete(alpha)
        let delta_idx = PersistentIndex::new_with_roots(vec![root.clone()]);
        delta_idx.upsert(FileMeta {
            file_key: FileKey { dev: 1, ino: 2 },
            path: gamma.clone(),
            size: 1,
            mtime: None,
        });
        let deleted = vec![alpha.as_os_str().as_bytes().to_vec()];
        store
            .lsm_append_delta_v6(
                &delta_idx.export_segments_v6(),
                &deleted,
                &[root.clone()],
                0,
            )
            .await
            .unwrap();

        let idx = TieredIndex::load_or_empty(&store, vec![root.clone()])
            .await
            .unwrap();

        assert!(idx.query("alpha").is_empty());
        assert_eq!(idx.query("gamma").len(), 1);
    }

    #[tokio::test]
    async fn lsm_delete_then_recreate_prefers_newest() {
        use crate::core::{FileKey, FileMeta};
        use crate::index::PersistentIndex;
        use std::os::unix::ffi::OsStrExt;

        let root = unique_tmp_dir("lsm-recreate");
        std::fs::create_dir_all(&root).unwrap();

        let alpha = root.join("alpha_test.txt");
        let store = SnapshotStore::new(root.join("index.db"));

        // base: alpha
        let base_idx = PersistentIndex::new_with_roots(vec![root.clone()]);
        base_idx.upsert(FileMeta {
            file_key: FileKey { dev: 1, ino: 1 },
            path: alpha.clone(),
            size: 1,
            mtime: None,
        });
        store
            .lsm_replace_base_v6(&base_idx.export_segments_v6(), None, &[root.clone()], 0)
            .await
            .unwrap();

        // delta1: delete(alpha)
        let d1 = PersistentIndex::new_with_roots(vec![root.clone()]);
        let deleted = vec![alpha.as_os_str().as_bytes().to_vec()];
        store
            .lsm_append_delta_v6(&d1.export_segments_v6(), &deleted, &[root.clone()], 0)
            .await
            .unwrap();

        // delta2: recreate(alpha)
        let d2 = PersistentIndex::new_with_roots(vec![root.clone()]);
        d2.upsert(FileMeta {
            file_key: FileKey { dev: 1, ino: 42 },
            path: alpha.clone(),
            size: 2,
            mtime: None,
        });
        store
            .lsm_append_delta_v6(&d2.export_segments_v6(), &[], &[root.clone()], 0)
            .await
            .unwrap();

        let idx = TieredIndex::load_or_empty(&store, vec![root.clone()])
            .await
            .unwrap();

        let r = idx.query("alpha");
        assert_eq!(r.len(), 1);
        assert_eq!(r[0].size, 2);
    }
}
