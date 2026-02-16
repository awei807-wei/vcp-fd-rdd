use std::hash::{Hash, Hasher};
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use arc_swap::ArcSwap;
use parking_lot::{Mutex, RwLock};
use tokio::sync::Notify;

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
// 更激进的合并阈值：用于百万文件后的“瘦身期”，加速 delta 段收敛。
const COMPACTION_DELTA_THRESHOLD: usize = 2;

fn dir_tree_changed_since(roots: &[PathBuf], ignore_prefixes: &[PathBuf], cutoff_ns: u64) -> bool {
    use std::time::{Duration, UNIX_EPOCH};

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

        if let Ok(modified) = md.modified() {
            if modified > cutoff {
                return true;
            }
        }

        let rd = match std::fs::read_dir(&dir) {
            Ok(rd) => rd,
            Err(e) => {
                // 权限/竞态等错误不应导致“永远判 stale”；保守地跳过不可读子树。
                tracing::debug!("offline mtime crawl: skip unreadable dir {:?}: {}", dir, e);
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

#[derive(Copy, Clone, Debug, Eq, PartialEq)]
struct Span {
    off: u32,
    len: u32,
}

impl Span {
    fn range(self) -> std::ops::Range<usize> {
        let off = self.off as usize;
        let len = self.len as usize;
        off..(off + len)
    }
}

#[derive(Clone, Debug)]
enum OneOrManySpan {
    One(Span),
    Many(Vec<Span>),
}

impl OneOrManySpan {
    fn iter(&self) -> impl Iterator<Item = &Span> {
        match self {
            OneOrManySpan::One(s) => std::slice::from_ref(s).iter(),
            OneOrManySpan::Many(v) => v.iter(),
        }
    }

    fn push(&mut self, s: Span) {
        match self {
            OneOrManySpan::One(existing) => {
                let old = *existing;
                *self = OneOrManySpan::Many(vec![old, s]);
            }
            OneOrManySpan::Many(v) => v.push(s),
        }
    }
}

fn hash_bytes64(bytes: &[u8]) -> u64 {
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    bytes.hash(&mut hasher);
    hasher.finish()
}

/// 一次 flush 周期内的“路径集合”：
/// - 以 hash 为索引（正确性靠 byte-compare，不依赖 hash 不碰撞）
/// - 路径字节统一落在 arena（append-only），避免每条路径一个独立堆分配
#[derive(Clone, Debug, Default)]
struct PathArenaSet {
    arena: Vec<u8>,
    map: std::collections::HashMap<u64, OneOrManySpan>,
    paths_len: usize,
    active_bytes: u64,
}

impl PathArenaSet {
    fn len_paths(&self) -> usize {
        self.paths_len
    }

    fn active_bytes(&self) -> u64 {
        self.active_bytes
    }

    fn arena_len(&self) -> usize {
        self.arena.len()
    }

    fn arena_cap(&self) -> usize {
        self.arena.capacity()
    }

    fn map_len(&self) -> usize {
        self.map.len()
    }

    fn map_cap(&self) -> usize {
        self.map.capacity()
    }

    fn bytes_at(&self, span: Span) -> Option<&[u8]> {
        let r = span.range();
        self.arena.get(r)
    }

    fn contains(&self, bytes: &[u8]) -> bool {
        let h = hash_bytes64(bytes);
        let Some(v) = self.map.get(&h) else {
            return false;
        };
        v.iter()
            .filter_map(|s| self.bytes_at(*s))
            .any(|b| b == bytes)
    }

    /// 返回 true 表示本次为“新路径”插入（用于统计）
    fn insert(&mut self, bytes: &[u8]) -> bool {
        let h = hash_bytes64(bytes);
        if let Some(v) = self.map.get(&h) {
            if v.iter()
                .filter_map(|s| self.bytes_at(*s))
                .any(|b| b == bytes)
            {
                return false;
            }
        }

        let off: u32 = match self.arena.len().try_into() {
            Ok(v) => v,
            Err(_) => {
                // 极端：arena 超过 4GiB。为避免溢出导致错误索引，直接清空本轮 overlay。
                // 这会丢失“跨段屏蔽集合”，但能阻止进程继续无界增长；后续 flush 会重建磁盘真相。
                tracing::warn!("Overlay arena exceeded 4GiB, clearing overlay to avoid overflow");
                self.clear();
                0
            }
        };
        let len: u32 = bytes.len().try_into().unwrap_or(u32::MAX);
        self.arena.extend_from_slice(bytes);
        let span = Span { off, len };

        match self.map.get_mut(&h) {
            Some(v) => v.push(span),
            None => {
                self.map.insert(h, OneOrManySpan::One(span));
            }
        }

        self.paths_len += 1;
        self.active_bytes += bytes.len() as u64;
        true
    }

    /// 返回 true 表示存在并移除
    fn remove(&mut self, bytes: &[u8]) -> bool {
        let h = hash_bytes64(bytes);
        let arena: &[u8] = &self.arena;

        let span_eq = |s: Span, bytes: &[u8]| -> bool {
            let r = s.range();
            arena.get(r).is_some_and(|b| b == bytes)
        };

        let mut removed = false;
        let mut drop_key = false;
        {
            let Some(v) = self.map.get_mut(&h) else {
                return false;
            };
            match v {
                OneOrManySpan::One(s) => {
                    if span_eq(*s, bytes) {
                        removed = true;
                        drop_key = true;
                    }
                }
                OneOrManySpan::Many(vs) => {
                    if let Some(i) = vs.iter().position(|s| span_eq(*s, bytes)) {
                        vs.swap_remove(i);
                        removed = true;
                    }
                    if removed {
                        if vs.is_empty() {
                            drop_key = true;
                        } else if vs.len() == 1 {
                            let only = vs[0];
                            *v = OneOrManySpan::One(only);
                        }
                    }
                }
            }
        }

        if !removed {
            return false;
        }
        if drop_key {
            self.map.remove(&h);
        }

        self.paths_len = self.paths_len.saturating_sub(1);
        self.active_bytes = self.active_bytes.saturating_sub(bytes.len() as u64);
        true
    }

    fn for_each_bytes(&self, mut f: impl FnMut(&[u8])) {
        for v in self.map.values() {
            for s in v.iter() {
                if let Some(b) = self.bytes_at(*s) {
                    f(b);
                }
            }
        }
    }

    fn clear(&mut self) {
        self.map.clear();
        self.arena.clear();
        self.paths_len = 0;
        self.active_bytes = 0;
    }

    /// 估算 overlay 堆占用（粗估、偏保守）：arena + HashMap 桶 + collision Vec 容量。
    fn estimated_bytes(&self) -> u64 {
        use std::mem::size_of;

        // HashMap 真实实现细节与装载因子会影响开销；这里取“桶数组 + 控制字节”的保守估算。
        // 该值用于解释 RSS，目标是“不低估”，而不是字节级精确。
        let bucket = size_of::<(u64, OneOrManySpan)>() as u64;
        let ctrl = 16u64; // 经验保守常数：控制字节/对齐等摊销
        let map_bytes = self.map.capacity() as u64 * (bucket + ctrl);

        let mut many_bytes = 0u64;
        for v in self.map.values() {
            if let OneOrManySpan::Many(vs) = v {
                many_bytes += vs.capacity() as u64 * size_of::<Span>() as u64;
            }
        }

        self.arena.capacity() as u64 + map_bytes + many_bytes
    }
}

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
    pending_events: std::collections::HashMap<PathBuf, PendingEvent>,
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

#[derive(Clone, Debug)]
struct PendingEvent {
    seq: u64,
    timestamp: std::time::SystemTime,
    event_type: EventType,
}

#[derive(Debug, Default)]
struct OverlayState {
    /// delete / rename-from：需要跨段屏蔽更老 segment 结果，并在 flush 时写入 seg-*.del
    deleted_paths: PathArenaSet,
    /// create/modify/rename-to：用于抵消同一路径的 deleted（delete→recreate）
    upserted_paths: PathArenaSet,
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
    flush_requested: AtomicBool,
    flush_notify: Notify,
    auto_flush_overlay_paths: AtomicU64,
    auto_flush_overlay_bytes: AtomicU64,
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
            flush_requested: AtomicBool::new(false),
            flush_notify: Notify::new(),
            auto_flush_overlay_paths: AtomicU64::new(250_000),
            auto_flush_overlay_bytes: AtomicU64::new(64 * 1024 * 1024),
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

    /// 从快照加载（或回退为空），并在返回前执行启动清扫：
    /// 1) 物理清理 manifest 未引用的孤儿段文件（best-effort）
    /// 2) 若现有 delta 段达到阈值则触发后台 compaction（best-effort）
    pub async fn load(store: &SnapshotStore, roots: Vec<PathBuf>) -> anyhow::Result<Arc<Self>> {
        let index = Arc::new(Self::load_or_empty(store, roots).await?);

        // 1) 物理清理不在 MANIFEST 里的孤儿文件（best-effort）
        let _ = store.gc_stale_segments();

        // 2) 检查是否需要合并现有的碎片段（best-effort）
        index.maybe_spawn_compaction(store.path().to_path_buf());

        Ok(index)
    }

    /// 从快照加载或空索引启动
    pub async fn load_or_empty(store: &SnapshotStore, roots: Vec<PathBuf>) -> anyhow::Result<Self> {
        let l1 = L1Cache::with_capacity(1000);
        let l3 = IndexBuilder::new(roots.clone());

        // 冷启动离线变更检测（仅 LSM 目录布局）：
        // - LSM 段可能包含停机期间的“幽灵记录”（已删除文件但索引仍在）。
        // - 查询会触发 mmap 触页把历史段读入 RSS（即使 L2 很小），造成突发内存暴涨与脏结果。
        // - 这里在加载任何 disk segments 之前做一次“目录 mtime crawl”（仅 stat 目录，O(目录数)）；
        //   若发现离线变更，则判定快照不可信：不挂载旧段进查询链路，从空索引启动（由上层触发 rebuild）。
        if let Ok(Some(last_build_ns)) = store.lsm_last_build_ns() {
            let ignores = vec![store.derived_lsm_dir_path()];
            if last_build_ns == 0 || dir_tree_changed_since(&roots, &ignores, last_build_ns) {
                tracing::warn!(
                    "LSM snapshot considered stale (offline dir mtime changed since last_build_ns={}), starting empty (will rebuild)",
                    last_build_ns
                );
                return Ok(Self::new(
                    l1,
                    Arc::new(PersistentIndex::new_with_roots(roots.clone())),
                    l3,
                    roots,
                    Vec::new(),
                ));
            }
        }

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
                    .map(|(path, ev)| EventRecord {
                        seq: ev.seq,
                        timestamp: ev.timestamp,
                        event_type: ev.event_type,
                        path,
                    })
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
            st.deleted_paths.for_each_bytes(|p| {
                blocked.insert(p.to_vec());
            });
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

    /// 设置 overlay 强制 flush 阈值（0 表示禁用对应阈值）。
    pub fn set_auto_flush_limits(&self, overlay_paths: u64, overlay_bytes: u64) {
        self.auto_flush_overlay_paths
            .store(overlay_paths, Ordering::Relaxed);
        self.auto_flush_overlay_bytes
            .store(overlay_bytes, Ordering::Relaxed);
    }

    fn maybe_request_flush(&self, overlay_paths: usize, overlay_arena_bytes: u64) {
        let limit_paths = self.auto_flush_overlay_paths.load(Ordering::Relaxed);
        let limit_bytes = self.auto_flush_overlay_bytes.load(Ordering::Relaxed);
        if limit_paths == 0 && limit_bytes == 0 {
            return;
        }

        let hit = (limit_paths > 0 && overlay_paths as u64 >= limit_paths)
            || (limit_bytes > 0 && overlay_arena_bytes >= limit_bytes);
        if !hit {
            return;
        }

        // 合并触发：只有从 false->true 才唤醒一次，避免 event 风暴下 notify 风暴。
        if !self.flush_requested.swap(true, Ordering::AcqRel) {
            self.flush_notify.notify_one();
        }
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
                            st.pending_events.insert(
                                key,
                                PendingEvent {
                                    seq: ev.seq,
                                    timestamp: ev.timestamp,
                                    event_type: ev.event_type.clone(),
                                },
                            );
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
                            let _ = ov.upserted_paths.remove(path_bytes);
                            let _ = ov.deleted_paths.insert(path_bytes);
                        }
                        EventType::Create | EventType::Modify => {
                            let _ = ov.deleted_paths.remove(path_bytes);
                            let _ = ov.upserted_paths.insert(path_bytes);
                        }
                        EventType::Rename { from } => {
                            let from_bytes = from.as_os_str().as_bytes();
                            let _ = ov.upserted_paths.remove(from_bytes);
                            let _ = ov.deleted_paths.insert(from_bytes);

                            let _ = ov.deleted_paths.remove(path_bytes);
                            let _ = ov.upserted_paths.insert(path_bytes);
                        }
                    }
                }

                // overlay 达阈值时请求强制 flush（合并触发，避免无界膨胀）。
                let overlay_paths = ov.deleted_paths.len_paths() + ov.upserted_paths.len_paths();
                let overlay_arena_bytes =
                    (ov.deleted_paths.arena_len() + ov.upserted_paths.arena_len()) as u64;
                drop(ov);
                self.maybe_request_flush(overlay_paths, overlay_arena_bytes);
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
            let overlay_dirty =
                ov.deleted_paths.len_paths() != 0 || ov.upserted_paths.len_paths() != 0;
            if !delta_dirty && !overlay_dirty {
                tracing::debug!("No delta/overlay changes, skipping flush");
                self.flush_requested.store(false, Ordering::Release);
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
            ov.deleted_paths.for_each_bytes(|p| {
                if !ov.upserted_paths.contains(p) {
                    deleted.push(p.to_vec());
                }
            });
            ov.deleted_paths.clear();
            ov.upserted_paths.clear();
            self.flush_requested.store(false, Ordering::Release);

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
            if let Err(e) = store.gc_stale_segments() {
                tracing::warn!("LSM gc stale segments failed after replace-base: {}", e);
            }

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
        let interval = std::time::Duration::from_secs(interval_secs);
        loop {
            // flush 请求优先：避免 overlay 长期积压。
            if self.flush_requested.load(Ordering::Acquire) {
                if let Err(e) = self.snapshot_now(&store).await {
                    tracing::error!("Snapshot failed (flush requested): {}", e);
                    // 避免失败后自旋
                    tokio::time::sleep(std::time::Duration::from_secs(1)).await;
                }
                continue;
            }

            tokio::select! {
                _ = tokio::time::sleep(interval) => {},
                _ = self.flush_notify.notified() => {},
            }

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
                deleted_paths: ov.deleted_paths.len_paths(),
                upserted_paths: ov.upserted_paths.len_paths(),
                deleted_bytes: ov.deleted_paths.active_bytes(),
                upserted_bytes: ov.upserted_paths.active_bytes(),
                deleted_arena_len: ov.deleted_paths.arena_len(),
                deleted_arena_cap: ov.deleted_paths.arena_cap(),
                upserted_arena_len: ov.upserted_paths.arena_len(),
                upserted_arena_cap: ov.upserted_paths.arena_cap(),
                deleted_map_len: ov.deleted_paths.map_len(),
                deleted_map_cap: ov.deleted_paths.map_cap(),
                upserted_map_len: ov.upserted_paths.map_len(),
                upserted_map_cap: ov.upserted_paths.map_cap(),
                estimated_bytes: ov.deleted_paths.estimated_bytes()
                    + ov.upserted_paths.estimated_bytes(),
            }
        };

        let rebuild = {
            use std::mem::size_of;
            use std::os::unix::ffi::OsStrExt;

            let st = self.rebuild_state.lock();
            let mut key_bytes = 0u64;
            let mut from_bytes = 0u64;
            for (k, v) in st.pending_events.iter() {
                key_bytes += k.as_os_str().as_bytes().len() as u64;
                if let EventType::Rename { from } = &v.event_type {
                    from_bytes += from.as_os_str().as_bytes().len() as u64;
                }
            }
            let cap = st.pending_events.capacity();
            let entry = size_of::<(PathBuf, PendingEvent)>() as u64;
            let estimated = cap as u64 * (entry + 16) + key_bytes + from_bytes;

            RebuildStats {
                in_progress: st.in_progress,
                pending_paths: st.pending_events.len(),
                pending_map_cap: st.pending_events.capacity(),
                pending_key_bytes: key_bytes,
                pending_from_bytes: from_bytes,
                estimated_bytes: estimated,
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
        if let Err(e) = store.gc_stale_segments() {
            tracing::warn!(
                "LSM gc stale segments failed after compaction replace-base: {}",
                e
            );
        }

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
    use crate::stats::EventPipelineStats;
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

    #[test]
    fn overlay_delete_then_recreate_cancels_deleted() {
        let root = unique_tmp_dir("overlay-cancel");
        std::fs::create_dir_all(&root).unwrap();

        let p = root.join("x.txt");
        std::fs::write(&p, b"x").unwrap();

        let idx = TieredIndex::empty(vec![root.clone()]);
        idx.apply_events(&[mk_event(1, EventType::Delete, p.clone())]);
        idx.apply_events(&[mk_event(2, EventType::Create, p.clone())]);

        let r = idx.memory_report(EventPipelineStats::default());
        assert_eq!(r.overlay.deleted_paths, 0);
        assert_eq!(r.overlay.upserted_paths, 1);
    }

    #[test]
    fn overlay_rename_tracks_from_as_delete_and_to_as_upsert() {
        let root = unique_tmp_dir("overlay-rename");
        std::fs::create_dir_all(&root).unwrap();

        let from = root.join("old_aaa.txt");
        std::fs::write(&from, b"old").unwrap();
        let to = root.join("new_bbb.txt");
        std::fs::rename(&from, &to).unwrap();

        let idx = TieredIndex::empty(vec![root.clone()]);
        idx.apply_events(&[mk_event(
            1,
            EventType::Rename { from: from.clone() },
            to.clone(),
        )]);

        let r = idx.memory_report(EventPipelineStats::default());
        assert_eq!(r.overlay.deleted_paths, 1);
        assert_eq!(r.overlay.upserted_paths, 1);
    }

    #[test]
    fn rebuild_pending_rename_applied_after_switch() {
        let root = unique_tmp_dir("rebuild-rename");
        std::fs::create_dir_all(&root).unwrap();

        let old_path = root.join("old_aaa.txt");
        std::fs::write(&old_path, b"old").unwrap();

        let idx = Arc::new(TieredIndex::empty(vec![root.clone()]));
        idx.apply_events(&[mk_event(1, EventType::Create, old_path.clone())]);
        assert!(!idx.query("old_aaa").is_empty());

        assert!(idx.try_start_rebuild_force());
        let new_l2 = Arc::new(PersistentIndex::new_with_roots(vec![root.clone()]));
        new_l2.apply_events(&[mk_event(2, EventType::Create, old_path.clone())]);

        let new_path = root.join("new_bbb.txt");
        std::fs::rename(&old_path, &new_path).unwrap();
        idx.apply_events(&[mk_event(
            3,
            EventType::Rename {
                from: old_path.clone(),
            },
            new_path.clone(),
        )]);

        idx.finish_rebuild(new_l2);
        assert!(idx.query("old_aaa").is_empty());
        assert!(!idx.query("new_bbb").is_empty());
    }

    #[tokio::test]
    async fn auto_flush_overlay_wakes_snapshot_loop() {
        let root = unique_tmp_dir("auto-flush");
        std::fs::create_dir_all(&root).unwrap();

        let store = Arc::new(SnapshotStore::new(root.join("index.db")));
        let idx = Arc::new(TieredIndex::empty(vec![root.clone()]));
        // 低阈值：1 条路径/1 字节即可触发
        idx.set_auto_flush_limits(1, 1);

        let h = tokio::spawn(idx.clone().snapshot_loop(store.clone(), 3600));

        let p = root.join("a.txt");
        std::fs::write(&p, b"a").unwrap();
        idx.apply_events(&[mk_event(1, EventType::Create, p.clone())]);

        let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(5);
        loop {
            if !idx.disk_layers.read().is_empty() {
                break;
            }
            if tokio::time::Instant::now() >= deadline {
                panic!("auto flush did not produce a disk layer in time");
            }
            tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        }

        h.abort();
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
        store.gc_stale_segments().unwrap();

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
        store.gc_stale_segments().unwrap();

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

    #[tokio::test]
    async fn lsm_offline_dir_mtime_change_skips_disk_segments() {
        use crate::core::{FileKey, FileMeta};
        use crate::index::PersistentIndex;

        let base = unique_tmp_dir("lsm-offline-mtime");
        let content_root = base.join("content");
        let state_root = base.join("state");
        std::fs::create_dir_all(&content_root).unwrap();
        std::fs::create_dir_all(&state_root).unwrap();

        // 预先创建深层目录（用于验证“深层变更不冒泡到 root”的场景）。
        let deep = content_root.join("deep");
        std::fs::create_dir_all(&deep).unwrap();

        let alpha = deep.join("alpha_test.txt");
        std::fs::write(&alpha, b"a").unwrap();

        let store = SnapshotStore::new(state_root.join("index.db"));

        // base: alpha（写入 LSM，生成 last_build_ns）
        let base_idx = PersistentIndex::new_with_roots(vec![content_root.clone()]);
        base_idx.upsert(FileMeta {
            file_key: FileKey { dev: 1, ino: 1 },
            path: alpha.clone(),
            size: 1,
            mtime: None,
        });
        store
            .lsm_replace_base_v6(
                &base_idx.export_segments_v6(),
                None,
                &[content_root.clone()],
                0,
            )
            .await
            .unwrap();
        store.gc_stale_segments().unwrap();

        // 离线变更：在 deep 下新增文件（只会更新 deep 的 mtime，不会更新 content_root 的 mtime）
        std::thread::sleep(std::time::Duration::from_millis(20));
        std::fs::write(deep.join("offline_new.txt"), b"x").unwrap();

        // 重新启动加载：应判定快照不可信，不挂载 disk segments。
        let idx = TieredIndex::load_or_empty(&store, vec![content_root.clone()])
            .await
            .unwrap();
        assert_eq!(idx.disk_layers.read().len(), 0);
    }
}
