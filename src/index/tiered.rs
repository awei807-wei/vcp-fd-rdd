use std::hash::{Hash, Hasher};
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use std::{collections::VecDeque, time::UNIX_EPOCH};

use arc_swap::ArcSwap;
use parking_lot::{Mutex, RwLock, RwLockReadGuard};
use tokio::sync::Notify;

use crate::core::{
    AdaptiveScheduler, EventRecord, EventType, FileIdentifier, FileKey, FileMeta, Task,
};
use crate::event::recovery::{DirtyScope, DirtyTracker};
use crate::index::l1_cache::L1Cache;
use crate::index::l2_partition::PersistentIndex;
use crate::index::l3_cold::IndexBuilder;
use crate::index::mmap_index::MmapIndex;
use crate::index::IndexLayer;
use crate::query::dsl::{compile_query, CompiledQuery};
use crate::query::matcher::{create_matcher, Matcher};
use crate::stats::{
    infer_heap_high_water, EventPipelineStats, MemoryReport, OverlayStats, RebuildStats,
};
use crate::storage::snapshot::LoadedSnapshot;
use crate::storage::traits::{StorageBackend, WriteAheadLog};
use crate::util::maybe_trim_rss;

const REBUILD_COOLDOWN: Duration = Duration::from_secs(60);
// 更激进的合并阈值：用于百万文件后的“瘦身期”，加速 delta 段收敛。
const COMPACTION_DELTA_THRESHOLD: usize = 2;
// 每次 compaction 最多合并多少个 delta（避免“delta 很多时一次合并过重”导致常驻/临时分配抖动）。
const COMPACTION_MAX_DELTAS_PER_RUN: usize = 2;
// 防抖：避免 flush 高频阶段反复启动 compaction 造成临时大分配抖动。
const COMPACTION_COOLDOWN: Duration = Duration::from_secs(30);

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

        if let Ok(modified) = md.modified() {
            let changed = cutoff_ns == 0 || modified > cutoff;
            if on_dir(&dir, changed) {
                return true;
            }
        }

        let rd = match std::fs::read_dir(&dir) {
            Ok(rd) => rd,
            Err(e) => {
                // 权限/竞态等错误不应导致“永远判 stale”；保守地跳过不可读子树。
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

fn dir_tree_changed_since(roots: &[PathBuf], ignore_prefixes: &[PathBuf], cutoff_ns: u64) -> bool {
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

fn path_deleted_by_any(path_bytes: &[u8], deleted_sets: &[Arc<PathArenaSet>]) -> bool {
    deleted_sets.iter().any(|paths| paths.contains(path_bytes))
}

#[derive(Debug, Default)]
pub(crate) struct FastSyncReport {
    dirs_scanned: usize,
    upsert_events: usize,
    delete_events: usize,
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
    const ARENA_KEEP_BYTES: usize = 256 * 1024;
    const MAP_KEEP_CAP: usize = 1024;

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

    /// flush/rebuild 后按阈值回收容量，避免历史高水位长期常驻。
    fn maybe_shrink_after_clear(&mut self) {
        if self.arena.capacity() > Self::ARENA_KEEP_BYTES * 2 {
            self.arena.shrink_to(Self::ARENA_KEEP_BYTES);
        }
        if self.map.capacity() > Self::MAP_KEEP_CAP * 2 {
            self.map.shrink_to(Self::MAP_KEEP_CAP);
        }
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

#[derive(Clone)]
struct DiskLayer {
    id: u64,
    idx: Arc<MmapIndex>,
    deleted_paths: Arc<PathArenaSet>,
    deleted_paths_count: usize,
    deleted_paths_bytes: u64,
    deleted_paths_estimated_bytes: u64,
}

fn path_arena_set_from_paths(paths: Vec<Vec<u8>>) -> PathArenaSet {
    let mut set = PathArenaSet::default();

    for path in paths {
        let _ = set.insert(&path);
    }
    set
}

fn deleted_paths_stats(paths: &PathArenaSet) -> (usize, u64, u64) {
    (
        paths.len_paths(),
        paths.active_bytes(),
        paths.estimated_bytes(),
    )
}

fn file_identifier_estimated_bytes(id: &FileIdentifier) -> u64 {
    match id {
        FileIdentifier::Path(p) => p.as_os_str().as_encoded_bytes().len() as u64,
        FileIdentifier::Fid { .. } => 16,
    }
}

fn event_record_estimated_bytes(ev: &EventRecord) -> u64 {
    let mut bytes = file_identifier_estimated_bytes(&ev.id);
    if let Some(p) = &ev.path_hint {
        bytes = bytes.saturating_add(p.as_os_str().as_encoded_bytes().len() as u64);
    }
    if let EventType::Rename {
        from,
        from_path_hint,
    } = &ev.event_type
    {
        bytes = bytes.saturating_add(file_identifier_estimated_bytes(from));
        if let Some(p) = from_path_hint {
            bytes = bytes.saturating_add(p.as_os_str().as_encoded_bytes().len() as u64);
        }
    }
    bytes
}

enum QueryEvaluator {
    Legacy(Arc<dyn Matcher>),
    Compiled(CompiledQuery),
}

struct QueryPlan {
    anchors: Vec<Arc<dyn Matcher>>,
    evaluator: QueryEvaluator,
}

impl QueryPlan {
    fn compiled(compiled: CompiledQuery) -> Self {
        Self {
            anchors: compiled.anchors().to_vec(),
            evaluator: QueryEvaluator::Compiled(compiled),
        }
    }

    fn legacy(matcher: Arc<dyn Matcher>) -> Self {
        Self {
            anchors: vec![matcher.clone()],
            evaluator: QueryEvaluator::Legacy(matcher),
        }
    }

    fn anchors(&self) -> &[Arc<dyn Matcher>] {
        &self.anchors
    }

    fn matches(&self, meta: &FileMeta) -> bool {
        match &self.evaluator {
            QueryEvaluator::Legacy(matcher) => matcher.matches(&meta.path.to_string_lossy()),
            QueryEvaluator::Compiled(compiled) => compiled.matches(meta),
        }
    }
}

#[derive(Debug)]
struct RebuildState {
    in_progress: bool,
    pending_events: std::collections::HashMap<FileIdentifier, PendingEvent>,
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
    path_hint: Option<PathBuf>,
}

struct ApplyBatchState<'a> {
    _gate: RwLockReadGuard<'a, ()>,
    l2: Arc<PersistentIndex>,
    rebuild_in_progress: bool,
    event_count: usize,
}

#[derive(Debug, Default)]
struct OverlayState {
    /// delete / rename-from：需要跨段屏蔽更老 segment 结果，并在 flush 时写入 seg-*.del
    deleted_paths: Arc<PathArenaSet>,
    /// create/modify/rename-to：用于抵消同一路径的 deleted（delete→recreate）
    upserted_paths: Arc<PathArenaSet>,
}

/// 三级索引：L1 热缓存 → L2 持久索引（内存常驻）→ L3 构建器（不在查询链路）
pub struct TieredIndex {
    pub l1: L1Cache,
    pub l2: ArcSwap<PersistentIndex>,
    disk_layers: RwLock<Vec<DiskLayer>>,
    pub l3: IndexBuilder,
    scheduler: Mutex<AdaptiveScheduler>,
    wal: Mutex<Option<Arc<dyn WriteAheadLog + Send + Sync>>>,
    pub event_seq: AtomicU64,
    rebuild_state: Mutex<RebuildState>,
    overlay_state: Mutex<OverlayState>,
    apply_gate: RwLock<()>,
    compaction_in_progress: AtomicBool,
    compaction_last_started_at: Mutex<Option<Instant>>,
    flush_requested: AtomicBool,
    flush_notify: Notify,
    auto_flush_overlay_paths: AtomicU64,
    auto_flush_overlay_bytes: AtomicU64,
    periodic_flush_min_events: AtomicU64,
    periodic_flush_min_bytes: AtomicU64,
    pending_flush_events: AtomicU64,
    pending_flush_bytes: AtomicU64,
    last_snapshot_time: AtomicU64,
    pub roots: Vec<PathBuf>,
    pub include_hidden: bool,
    pub ignore_enabled: bool,
}

impl TieredIndex {
    fn new(
        l1: L1Cache,
        l2: Arc<PersistentIndex>,
        l3: IndexBuilder,
        roots: Vec<PathBuf>,
        include_hidden: bool,
        ignore_enabled: bool,
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
            compaction_last_started_at: Mutex::new(None),
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
        let l3 = IndexBuilder::new_with_options(roots.clone(), include_hidden, ignore_enabled);
        Self::new(
            l1,
            l2,
            l3,
            roots,
            include_hidden,
            ignore_enabled,
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
        let _ = store.gc_stale_segments();

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
                    include_hidden,
                    ignore_enabled,
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
                    id: b.id,
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
                    id: d.id,
                    idx: Arc::new(MmapIndex::new(d.snap)),
                    deleted_paths,
                    deleted_paths_count: cnt,
                    deleted_paths_bytes: bytes,
                    deleted_paths_estimated_bytes: est,
                });
            }

            let idx = Self::new(
                l1,
                Arc::new(PersistentIndex::new_with_roots(roots.clone())),
                l3,
                roots,
                include_hidden,
                ignore_enabled,
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
                deleted_paths: Arc::new(PathArenaSet::default()),
                deleted_paths_count: 0,
                deleted_paths_bytes: 0,
                deleted_paths_estimated_bytes: 0,
            };
            let idx = Self::new(
                l1,
                Arc::new(PersistentIndex::new_with_roots(roots.clone())),
                l3,
                roots,
                include_hidden,
                ignore_enabled,
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

        let idx = Self::new(
            l1,
            Arc::new(l2),
            l3,
            roots,
            include_hidden,
            ignore_enabled,
            Vec::new(),
        );
        idx.attach_wal(store)?;
        idx.replay_wal_if_any(0);
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
                        Arc::make_mut(&mut ov.deleted_paths).clear();
                        Arc::make_mut(&mut ov.upserted_paths).clear();
                        Arc::make_mut(&mut ov.deleted_paths).maybe_shrink_after_clear();
                        Arc::make_mut(&mut ov.upserted_paths).maybe_shrink_after_clear();
                    }
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

    /// overflow 兜底：dirty region + cooldown/max-staleness 触发后执行一次 fast-sync（best-effort）。
    ///
    /// 设计目标：
    /// - 避免 “overflow → 立刻全盘 rebuild” 在风暴中触发大分配/高水位；
    /// - 允许查询短暂陈旧，但不阻塞查询、不 OOM；
    /// - fast-sync 以“目录为单位”做对齐：只需要 read_dir + 必要的 metadata，不假设 mtime 冒泡。
    pub fn spawn_fast_sync(
        self: &Arc<Self>,
        scope: DirtyScope,
        ignore_prefixes: Vec<PathBuf>,
        tracker: Arc<DirtyTracker>,
    ) {
        let idx = self.clone();
        std::thread::spawn(move || {
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
            DirtyScope::Dirs { dirs, .. } => {
                let mut v = dirs;
                v.sort();
                v.dedup();
                v
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
        // 说明：这里不再构建“文件名集合（HashSet<OsString>）”用于删除对齐，
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

                let path = ent.path().to_path_buf();
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

        // 3) 删除对齐：只对齐“被标记 dirty 的目录”下的条目（但对文件做轻量存在性检查，避免构建巨大的 names set）。
        // 注意：for_each_live_meta 内部持有读锁，期间不能调用 apply_events（会死锁）。
        let l2 = self.l2.load_full();
        let mut delete_events: Vec<EventRecord> = Vec::new();
        l2.for_each_live_meta(|m| {
            let Some(parent) = m.path.parent() else {
                return;
            };
            if !dirty_dirs.contains(parent) {
                return;
            };

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

    /// 即时扫描指定目录并更新索引（同步执行，不走 debounce/channel）。
    ///
    /// 限制：最多 10 个目录，每目录最多 10000 条目。
    /// 返回 (scanned_files, elapsed_ms)。
    pub fn scan_dirs_immediate(&self, dirs: &[PathBuf]) -> (usize, u64) {
        let start = Instant::now();
        let dirs: Vec<&PathBuf> = dirs.iter().take(10).collect();

        let mut upsert_events: Vec<EventRecord> = Vec::new();
        let mut upsert_metas: Vec<FileMeta> = Vec::new();
        let mut scanned: usize = 0;
        let mut seq: u64 = 0;

        for dir in dirs {
            let mut dir_count = 0;
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
                if dir_count >= 10_000 {
                    break;
                }
                dir_count += 1;

                let path = ent.path().to_path_buf();
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

    /// 查询入口：L1 → L2 → DiskSegments（mmap），不扫真实文件系统
    pub fn query(&self, keyword: &str) -> Vec<FileMeta> {
        self.query_limit(keyword, usize::MAX)
    }

    /// 查询入口（带 limit）：用于 IPC/HTTP 等“结果集可能很大”的场景，避免一次性聚合造成内存峰值。
    pub fn query_limit(&self, keyword: &str, limit: usize) -> Vec<FileMeta> {
        if limit == 0 {
            return Vec::new();
        }

        let plan = match compile_query(keyword) {
            Ok(compiled) => QueryPlan::compiled(compiled),
            Err(e) => {
                tracing::warn!(
                    "query dsl compile failed, fallback to legacy matcher: {}",
                    e
                );
                let case_sensitive =
                    keyword.contains("case:") || keyword.chars().any(|c| c.is_uppercase());
                let matcher = create_matcher(keyword, case_sensitive);

                if let Some(results) = self.l1.query(matcher.as_ref()) {
                    tracing::debug!("L1 hit: {} results", results.len());
                    return results.into_iter().take(limit).collect();
                }

                QueryPlan::legacy(matcher)
            }
        };

        let results = self.execute_query_plan(&plan, limit);
        if !results.is_empty() {
            tracing::debug!("Query hit: {} results", results.len());
            for meta in results.iter().take(10) {
                self.l1.insert(meta.clone());
            }
            return results;
        }

        self.l2.load_full().maybe_schedule_repair();
        Vec::new()
    }

    fn execute_query_plan(&self, plan: &QueryPlan, limit: usize) -> Vec<FileMeta> {
        let l2 = self.l2.load_full();
        let layers = self.disk_layers.read().clone();
        let overlay_deleted = { self.overlay_state.lock().deleted_paths.clone() };
        let mut blocked_paths = PathArenaSet::default();
        let mut deleted_sources: Vec<Arc<PathArenaSet>> = vec![overlay_deleted];
        let mut seen: std::collections::HashSet<FileKey> =
            std::collections::HashSet::with_capacity(l2.file_count().saturating_add(256));
        let mut results: Vec<FileMeta> = Vec::with_capacity(limit.min(128));

        if self.query_layer(
            plan,
            l2.as_ref(),
            None,
            deleted_sources.as_slice(),
            &mut seen,
            &mut blocked_paths,
            &mut results,
            limit,
        ) {
            return results;
        }

        for layer in layers.iter().rev() {
            if self.query_layer(
                plan,
                layer.idx.as_ref(),
                Some(layer.deleted_paths.as_ref()),
                deleted_sources.as_slice(),
                &mut seen,
                &mut blocked_paths,
                &mut results,
                limit,
            ) {
                return results;
            }
            deleted_sources.push(layer.deleted_paths.clone());
        }

        results
    }

    fn query_layer(
        &self,
        plan: &QueryPlan,
        layer: &dyn IndexLayer,
        layer_deleted: Option<&PathArenaSet>,
        deleted_sources: &[Arc<PathArenaSet>],
        seen: &mut std::collections::HashSet<FileKey>,
        blocked_paths: &mut PathArenaSet,
        results: &mut Vec<FileMeta>,
        limit: usize,
    ) -> bool {
        for anchor in plan.anchors() {
            for key in layer.query_keys(anchor.as_ref()) {
                if !seen.insert(key) {
                    continue;
                }

                let Some(meta) = layer.get_meta(key) else {
                    continue;
                };
                let path_bytes = meta.path.as_os_str().as_encoded_bytes();
                if blocked_paths.contains(path_bytes)
                    || layer_deleted.is_some_and(|paths| paths.contains(path_bytes))
                    || path_deleted_by_any(path_bytes, deleted_sources)
                {
                    continue;
                }

                let _ = blocked_paths.insert(path_bytes);
                if plan.matches(&meta) {
                    results.push(meta);
                    if results.len() >= limit {
                        return true;
                    }
                }
            }
        }

        false
    }

    /// 批量应用事件到索引
    pub fn apply_events(&self, events: &[EventRecord]) {
        self.apply_events_inner(events, true);
    }

    /// 批量应用事件到索引（drain 版本）：消费 `Vec<EventRecord>`，用于减少 PathBuf 克隆带来的非索引 PD 高水位。
    ///
    /// 说明：
    /// - 仅用于“事件生产者本就不需要保留 EventRecord”的路径（EventPipeline / fast-sync）。
    /// - 内部会清空 `events`，但保留 capacity 以便复用。
    pub fn apply_events_drain(&self, events: &mut Vec<EventRecord>) {
        self.apply_events_inner_drain(events, true);
    }

    /// 设置 overlay 强制 flush 阈值（0 表示禁用对应阈值）。
    pub fn set_auto_flush_limits(&self, overlay_paths: u64, overlay_bytes: u64) {
        self.auto_flush_overlay_paths
            .store(overlay_paths, Ordering::Relaxed);
        self.auto_flush_overlay_bytes
            .store(overlay_bytes, Ordering::Relaxed);
    }

    /// 设置“定时 flush”的最小批量门槛。
    ///
    /// - 仅影响 snapshot_loop 的周期性 flush
    /// - overlay 强制 flush / 退出前最终 snapshot 不受影响
    pub fn set_periodic_flush_batch_limits(&self, min_events: u64, min_bytes: u64) {
        self.periodic_flush_min_events
            .store(min_events, Ordering::Relaxed);
        self.periodic_flush_min_bytes
            .store(min_bytes, Ordering::Relaxed);
    }

    fn note_pending_flush_batch(&self, events: &[EventRecord]) {
        if events.is_empty() {
            return;
        }
        let bytes = events
            .iter()
            .map(event_record_estimated_bytes)
            .fold(0u64, u64::saturating_add);
        self.pending_flush_events
            .fetch_add(events.len() as u64, Ordering::Relaxed);
        self.pending_flush_bytes.fetch_add(bytes, Ordering::Relaxed);
    }

    fn note_pending_flush_rebuild(&self, idx: &PersistentIndex) {
        self.pending_flush_events
            .store(idx.file_count() as u64, Ordering::Relaxed);
        self.pending_flush_bytes
            .store(idx.memory_stats().estimated_bytes, Ordering::Relaxed);
    }

    fn reset_pending_flush_batch(&self) {
        self.pending_flush_events.store(0, Ordering::Relaxed);
        self.pending_flush_bytes.store(0, Ordering::Relaxed);
    }

    fn periodic_flush_batch_ready(&self) -> bool {
        let min_events = self.periodic_flush_min_events.load(Ordering::Relaxed);
        let min_bytes = self.periodic_flush_min_bytes.load(Ordering::Relaxed);
        if min_events == 0 && min_bytes == 0 {
            return true;
        }
        let pending_events = self.pending_flush_events.load(Ordering::Relaxed);
        let pending_bytes = self.pending_flush_bytes.load(Ordering::Relaxed);
        (min_events > 0 && pending_events >= min_events)
            || (min_bytes > 0 && pending_bytes >= min_bytes)
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

    fn append_events_to_wal(&self, events: &[EventRecord], log_to_wal: bool) {
        if !log_to_wal {
            return;
        }

        if let Some(wal) = self.wal.lock().clone() {
            if let Err(e) = wal.append(events) {
                tracing::warn!("WAL append failed (continuing without durability): {}", e);
            }
        }
    }

    fn capture_l2_for_apply(&self, events: &[EventRecord]) -> (Arc<PersistentIndex>, bool) {
        let mut st = self.rebuild_state.lock();
        if !st.in_progress {
            drop(st);
            return (self.l2.load_full(), false);
        }

        // 有界化：按身份去重，只保留每条身份的最新事件（避免 rebuild 期间无限堆积）。
        for ev in events {
            let key = ev.id.clone();
            match st.pending_events.get_mut(&key) {
                Some(old) if old.seq >= ev.seq => {
                    // 旧记录更新：忽略（避免乱序覆盖）。
                }
                Some(old) => {
                    // 新事件覆盖旧事件；path_hint 仅在新事件提供时覆盖（“最后一次非空覆盖”）。
                    old.seq = ev.seq;
                    old.timestamp = ev.timestamp;
                    old.event_type = ev.event_type.clone();
                    if ev.path_hint.is_some() {
                        old.path_hint = ev.path_hint.clone();
                    }
                }
                None => {
                    st.pending_events.insert(
                        key,
                        PendingEvent {
                            seq: ev.seq,
                            timestamp: ev.timestamp,
                            event_type: ev.event_type.clone(),
                            path_hint: ev.path_hint.clone(),
                        },
                    );
                }
            }
        }

        (self.l2.load_full(), true)
    }

    fn update_overlay_for_events(&self, events: &[EventRecord]) {
        let mut ov = self.overlay_state.lock();
        for ev in events {
            let Some(path) = ev.best_path() else {
                // FID-only 且无路径：阶段 1 保守跳过 overlay 更新（后续 fanotify 反查完善）。
                continue;
            };
            let path_bytes = path.as_os_str().as_encoded_bytes();
            match &ev.event_type {
                EventType::Delete => {
                    let _ = Arc::make_mut(&mut ov.upserted_paths).remove(path_bytes);
                    let _ = Arc::make_mut(&mut ov.deleted_paths).insert(path_bytes);
                }
                EventType::Create | EventType::Modify => {
                    let _ = Arc::make_mut(&mut ov.deleted_paths).remove(path_bytes);
                    let _ = Arc::make_mut(&mut ov.upserted_paths).insert(path_bytes);
                }
                EventType::Rename {
                    from,
                    from_path_hint,
                } => {
                    let from_best = from_path_hint.as_deref().or_else(|| from.as_path());
                    if let Some(from_path) = from_best {
                        let from_bytes = from_path.as_os_str().as_encoded_bytes();
                        let _ = Arc::make_mut(&mut ov.upserted_paths).remove(from_bytes);
                        let _ = Arc::make_mut(&mut ov.deleted_paths).insert(from_bytes);
                    }

                    let _ = Arc::make_mut(&mut ov.deleted_paths).remove(path_bytes);
                    let _ = Arc::make_mut(&mut ov.upserted_paths).insert(path_bytes);
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

    fn invalidate_l1_for_events(&self, events: &[EventRecord]) {
        for ev in events {
            match &ev.event_type {
                EventType::Delete => {
                    if let Some(p) = ev.best_path() {
                        self.l1.remove_by_path(p);
                    } else if let Some(fid) = ev.id.as_file_key() {
                        self.l1.remove(&fid);
                    }
                }
                EventType::Rename {
                    from,
                    from_path_hint,
                } => {
                    let from_best = from_path_hint.as_deref().or_else(|| from.as_path());
                    if let Some(p) = from_best {
                        self.l1.remove_by_path(p);
                    } else if let Some(fid) = from.as_file_key() {
                        self.l1.remove(&fid);
                    }
                }
                _ => {}
            }
        }
    }

    fn begin_apply_batch(
        &self,
        events: &[EventRecord],
        log_to_wal: bool,
    ) -> Option<ApplyBatchState<'_>> {
        if events.is_empty() {
            return None;
        }

        // flush/compaction 期间需要短暂阻塞写入，避免“指针 swap 后仍写旧 delta”的竞态。
        let gate = self.apply_gate.read();

        // WAL：先写后用（best-effort）。replay 场景下禁用写回，避免重复追加。
        self.append_events_to_wal(events, log_to_wal);

        // 若 rebuild 在进行：先缓冲 pending 事件；并在持锁期间捕获当前 l2 指针，
        // 避免切换窗口导致“事件已缓冲但应用到了新索引”而重复回放。
        let (l2, rebuild_in_progress) = self.capture_l2_for_apply(events);
        self.update_overlay_for_events(events);
        self.note_pending_flush_batch(events);
        self.invalidate_l1_for_events(events);

        Some(ApplyBatchState {
            _gate: gate,
            l2,
            rebuild_in_progress,
            event_count: events.len(),
        })
    }

    fn apply_events_inner(&self, events: &[EventRecord], log_to_wal: bool) {
        let Some(batch) = self.begin_apply_batch(events, log_to_wal) else {
            return;
        };
        batch.l2.apply_events(events);
        self.event_seq
            .fetch_add(batch.event_count as u64, Ordering::Relaxed);
    }

    fn apply_events_inner_drain(&self, events: &mut Vec<EventRecord>, log_to_wal: bool) {
        let Some(batch) = self.begin_apply_batch(events.as_slice(), log_to_wal) else {
            return;
        };
        if batch.rebuild_in_progress {
            batch.l2.apply_events(events.as_slice());
            events.clear();
        } else {
            batch.l2.apply_events_drain(events);
        }
        self.event_seq
            .fetch_add(batch.event_count as u64, Ordering::Relaxed);
    }

    fn apply_upserted_metas_inner(
        &self,
        events: &[EventRecord],
        metas: &mut Vec<FileMeta>,
        log_to_wal: bool,
    ) {
        let Some(batch) = self.begin_apply_batch(events, log_to_wal) else {
            metas.clear();
            return;
        };
        if batch.rebuild_in_progress {
            batch.l2.apply_file_metas(metas.as_slice());
            metas.clear();
        } else {
            batch.l2.apply_file_metas_drain(metas);
        }
        self.event_seq
            .fetch_add(batch.event_count as u64, Ordering::Relaxed);
    }

    /// 原子快照
    pub async fn snapshot_now<S>(self: &Arc<Self>, store: Arc<S>) -> anyhow::Result<()>
    where
        S: StorageBackend + 'static,
    {
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
                self.reset_pending_flush_batch();
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
            Arc::make_mut(&mut ov.deleted_paths).clear();
            Arc::make_mut(&mut ov.upserted_paths).clear();
            Arc::make_mut(&mut ov.deleted_paths).maybe_shrink_after_clear();
            Arc::make_mut(&mut ov.upserted_paths).maybe_shrink_after_clear();
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
            drop(layers_snapshot);

            // 再应用跨段 delete（delete/rename-from）。
            for p in &deleted_paths {
                let pb = pathbuf_from_bytes(p);
                merged.mark_deleted_by_path(&pb);
            }
            drop(deleted_paths);

            // 最后灌入本次 delta（newest）。
            old_delta.for_each_live_meta(|m| merged.upsert_rename(m));
            drop(old_delta);

            let segs = merged.export_segments_v6_compacted();
            drop(merged);
            let base = store
                .replace_base_v6(&segs, None, &roots, wal_seal_id)
                .await?;
            drop(segs);
            if let Err(e) = store.gc_stale_segments() {
                tracing::warn!("LSM gc stale segments failed after replace-base: {}", e);
            }

            // deleted_paths 在 append/replace-base 后通常会经历增长与扩容；这里 shrink 一次，避免把 capacity 高水位长期带到常驻层。
            let mut base_deleted_paths = base.deleted_paths;
            base_deleted_paths.shrink_to_fit();
            let deleted_paths = Arc::new(path_arena_set_from_paths(base_deleted_paths));
            let (cnt, bytes, est) = deleted_paths_stats(deleted_paths.as_ref());
            let new_layer = DiskLayer {
                id: base.id,
                idx: Arc::new(MmapIndex::new(base.snap)),
                deleted_paths,
                deleted_paths_count: cnt,
                deleted_paths_bytes: bytes,
                deleted_paths_estimated_bytes: est,
            };

            *self.disk_layers.write() = vec![new_layer];
            self.l1.clear();
            if let Some(w) = self.wal.lock().clone() {
                let _ = w.cleanup_sealed_up_to(wal_seal_id);
            }
            self.record_snapshot_success();
            self.reset_pending_flush_batch();
            // snapshot/flush 是临时分配大户；完成后尝试回吐。
            maybe_trim_rss();
            return Ok(());
        }

        drop(layers_snapshot);
        let segs = old_delta.export_segments_v6();
        drop(old_delta);
        let seg = store
            .append_delta_v6(&segs, &deleted_paths, &roots, wal_seal_id)
            .await?;
        drop(segs);
        drop(deleted_paths);

        // deleted_paths 在 append 后通常会经历增长与扩容；这里 shrink 一次，避免把 capacity 高水位长期带到常驻层。
        let mut seg_deleted_paths = seg.deleted_paths;
        seg_deleted_paths.shrink_to_fit();
        let deleted_paths = Arc::new(path_arena_set_from_paths(seg_deleted_paths));
        let (cnt, bytes, est) = deleted_paths_stats(deleted_paths.as_ref());
        self.disk_layers.write().push(DiskLayer {
            id: seg.id,
            idx: Arc::new(MmapIndex::new(seg.snap)),
            deleted_paths,
            deleted_paths_count: cnt,
            deleted_paths_bytes: bytes,
            deleted_paths_estimated_bytes: est,
        });
        self.l1.clear();
        if let Some(w) = self.wal.lock().clone() {
            let _ = w.cleanup_sealed_up_to(wal_seal_id);
        }
        self.record_snapshot_success();
        self.reset_pending_flush_batch();

        // compaction：段数达到阈值后后台合并
        self.maybe_spawn_compaction(store);
        // snapshot/flush 是临时分配大户；完成后尝试回吐。
        maybe_trim_rss();
        Ok(())
    }

    /// 定期快照循环
    pub async fn snapshot_loop<S>(self: Arc<Self>, store: Arc<S>, interval_secs: u64)
    where
        S: StorageBackend + 'static,
    {
        // interval_secs==0 is treated as "disabled" to avoid a busy loop.
        let interval = if interval_secs == 0 {
            None
        } else {
            Some(std::time::Duration::from_secs(interval_secs))
        };
        loop {
            // flush 请求优先：避免 overlay 长期积压。
            if self.flush_requested.load(Ordering::Acquire) {
                if let Err(e) = self.snapshot_now(store.clone()).await {
                    tracing::error!("Snapshot failed (flush requested): {}", e);
                    // 避免失败后自旋
                    tokio::time::sleep(std::time::Duration::from_secs(1)).await;
                }
                continue;
            }

            let periodic_tick = match interval {
                Some(interval) => {
                    tokio::select! {
                        _ = tokio::time::sleep(interval) => true,
                        _ = self.flush_notify.notified() => false,
                    }
                }
                None => {
                    self.flush_notify.notified().await;
                    false
                }
            };

            if periodic_tick && !self.periodic_flush_batch_ready() {
                tracing::debug!(
                    "Periodic flush skipped: pending_events={} pending_bytes={} min_events={} min_bytes={}",
                    self.pending_flush_events.load(Ordering::Relaxed),
                    self.pending_flush_bytes.load(Ordering::Relaxed),
                    self.periodic_flush_min_events.load(Ordering::Relaxed),
                    self.periodic_flush_min_bytes.load(Ordering::Relaxed),
                );
                continue;
            }

            if let Err(e) = self.snapshot_now(store.clone()).await {
                tracing::error!("Snapshot failed: {}", e);
            }
        }
    }

    pub fn last_snapshot_time(&self) -> u64 {
        self.last_snapshot_time.load(Ordering::Relaxed)
    }

    fn record_snapshot_success(&self) {
        let ts = std::time::SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_secs())
            .unwrap_or(0);
        self.last_snapshot_time.store(ts, Ordering::Relaxed);
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
        let l1 = self.l1.memory_stats();
        let l2 = self.l2.load_full().memory_stats();
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

            let st = self.rebuild_state.lock();
            let mut key_bytes = 0u64;
            let mut from_bytes = 0u64;
            for (k, v) in st.pending_events.iter() {
                key_bytes += match k {
                    FileIdentifier::Path(p) => p.as_os_str().as_encoded_bytes().len() as u64,
                    FileIdentifier::Fid { .. } => 16,
                };
                if let EventType::Rename {
                    from,
                    from_path_hint,
                } = &v.event_type
                {
                    from_bytes += match from {
                        FileIdentifier::Path(p) => p.as_os_str().as_encoded_bytes().len() as u64,
                        FileIdentifier::Fid { .. } => 16,
                    };
                    if let Some(p) = from_path_hint {
                        from_bytes += p.as_os_str().as_encoded_bytes().len() as u64;
                    }
                }
                if let Some(p) = &v.path_hint {
                    key_bytes += p.as_os_str().as_encoded_bytes().len() as u64;
                }
            }
            let cap = st.pending_events.capacity();
            let entry = size_of::<(FileIdentifier, PendingEvent)>() as u64;
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

        let (
            disk_segments,
            disk_deleted_paths,
            disk_deleted_bytes,
            disk_deleted_estimated_bytes,
            disk_deleted_estimated_bytes_max,
        ) = {
            let layers = self.disk_layers.read();
            let mut total_paths: usize = 0;
            let mut total_bytes: u64 = 0;
            let mut total_est: u64 = 0;
            let mut max_est: u64 = 0;
            for l in layers.iter() {
                total_paths = total_paths.saturating_add(l.deleted_paths_count);
                total_bytes = total_bytes.saturating_add(l.deleted_paths_bytes);
                total_est = total_est.saturating_add(l.deleted_paths_estimated_bytes);
                max_est = max_est.max(l.deleted_paths_estimated_bytes);
            }
            (layers.len(), total_paths, total_bytes, total_est, max_est)
        };

        let index_estimated_bytes = l1.estimated_bytes
            + l2.estimated_bytes
            + disk_deleted_estimated_bytes
            + overlay.estimated_bytes
            + rebuild.estimated_bytes;
        let process_smaps_rollup = MemoryReport::read_smaps_rollup();
        let (non_index_private_dirty_bytes, heap_high_water_suspected) = process_smaps_rollup
            .as_ref()
            .map(|s| {
                let (non, suspected) =
                    infer_heap_high_water(s.private_dirty_bytes, index_estimated_bytes);
                (Some(non), suspected)
            })
            .unwrap_or((None, false));

        MemoryReport {
            l1,
            l2,
            disk_segments,
            disk_deleted_paths,
            disk_deleted_bytes,
            disk_deleted_estimated_bytes,
            disk_deleted_estimated_bytes_max,
            event_pipeline: pipeline_stats,
            overlay,
            rebuild,
            process_rss_bytes: MemoryReport::read_process_rss(),
            process_smaps_rollup,
            process_faults: MemoryReport::read_faults(),
            index_estimated_bytes,
            non_index_private_dirty_bytes,
            heap_high_water_suspected,
        }
    }

    /// 定期内存报告循环
    pub async fn memory_report_loop(
        self: Arc<Self>,
        pipeline_stats_fn: Arc<dyn Fn() -> EventPipelineStats + Send + Sync>,
        interval_secs: u64,
    ) {
        if interval_secs == 0 {
            tracing::info!("Memory reporting disabled (interval_secs=0)");
            return;
        }
        // 首次报告延迟 5 秒
        tokio::time::sleep(std::time::Duration::from_secs(5)).await;

        let interval = std::time::Duration::from_secs(interval_secs);
        let mut rss_window: VecDeque<u64> = VecDeque::with_capacity(12);

        loop {
            let stats = pipeline_stats_fn();
            let report = self.memory_report(stats);

            rss_window.push_back(report.process_rss_bytes);
            while rss_window.len() > 12 {
                rss_window.pop_front();
            }

            let trend_mb_per_min = if rss_window.len() >= 2 {
                let first = *rss_window.front().unwrap_or(&0) as f64;
                let last = *rss_window.back().unwrap_or(&0) as f64;
                let minutes = ((rss_window.len() - 1) as f64 * interval_secs as f64) / 60.0;
                if minutes > 0.0 {
                    (last - first) / (1024.0 * 1024.0) / minutes
                } else {
                    0.0
                }
            } else {
                0.0
            };

            tracing::info!(
                "\n{}\n[heap-signal] index_est_bytes={} non_index_pd_bytes={} suspected={} rss_trend_mb_per_min={:+.2}",
                report,
                report.index_estimated_bytes,
                report.non_index_private_dirty_bytes.unwrap_or(0),
                report.heap_high_water_suspected,
                trend_mb_per_min
            );
            tokio::time::sleep(interval).await;
        }
    }

    /// 条件性 RSS trim 循环：
    /// - 周期检查 smaps 的 Private_Dirty
    /// - 当“非索引脏页偏高”且超过阈值时触发一次 trim
    pub async fn rss_trim_loop(self: Arc<Self>, interval_secs: u64, trim_pd_threshold_mb: u64) {
        if interval_secs == 0 || trim_pd_threshold_mb == 0 {
            tracing::info!(
                "RSS trim disabled (interval_secs={}, trim_pd_threshold_mb={})",
                interval_secs,
                trim_pd_threshold_mb
            );
            return;
        }

        let interval = Duration::from_secs(interval_secs);
        let threshold_bytes = trim_pd_threshold_mb.saturating_mul(1024 * 1024);
        let mut non_idx_pd_window: VecDeque<u64> = VecDeque::with_capacity(6);

        loop {
            tokio::time::sleep(interval).await;

            let report = self.memory_report(EventPipelineStats::default());
            let Some(smaps) = report.process_smaps_rollup.as_ref() else {
                continue;
            };
            let non_idx_pd = report
                .non_index_private_dirty_bytes
                .unwrap_or(smaps.private_dirty_bytes);
            non_idx_pd_window.push_back(non_idx_pd);
            while non_idx_pd_window.len() > 6 {
                non_idx_pd_window.pop_front();
            }

            let trend_mb_per_min = if non_idx_pd_window.len() >= 2 {
                let first = *non_idx_pd_window.front().unwrap_or(&0) as f64;
                let last = *non_idx_pd_window.back().unwrap_or(&0) as f64;
                let minutes = ((non_idx_pd_window.len() - 1) as f64 * interval_secs as f64) / 60.0;
                if minutes > 0.0 {
                    (last - first) / (1024.0 * 1024.0) / minutes
                } else {
                    0.0
                }
            } else {
                0.0
            };

            let growing = trend_mb_per_min >= 0.5;
            let very_high = smaps.private_dirty_bytes >= threshold_bytes.saturating_mul(2);
            if smaps.private_dirty_bytes < threshold_bytes
                || !report.heap_high_water_suspected
                || (!growing && !very_high)
            {
                continue;
            }

            tracing::warn!(
                "RSS trim trigger: private_dirty_bytes={} threshold_bytes={} index_est_bytes={} non_index_pd_bytes={} trend_mb_per_min={:+.2}",
                smaps.private_dirty_bytes,
                threshold_bytes,
                report.index_estimated_bytes,
                report.non_index_private_dirty_bytes.unwrap_or(0),
                trend_mb_per_min
            );
            maybe_trim_rss();

            tokio::time::sleep(Duration::from_millis(120)).await;
            let after = MemoryReport::read_smaps_rollup()
                .map(|s| s.private_dirty_bytes)
                .unwrap_or(0);
            tracing::info!(
                "RSS trim done: private_dirty_bytes={} -> {}",
                smaps.private_dirty_bytes,
                after
            );
        }
    }

    fn maybe_spawn_compaction<S>(self: &Arc<Self>, store: Arc<S>)
    where
        S: StorageBackend + 'static,
    {
        let mut layers = self.disk_layers.read().clone();
        let delta_count = layers.len().saturating_sub(1);
        if delta_count < COMPACTION_DELTA_THRESHOLD {
            return;
        }
        // 为了避免一次 compaction 过重：只合并 base + 最老的一小段 delta，剩余新 delta 保留在 suffix。
        let max_layers = 1 + COMPACTION_MAX_DELTAS_PER_RUN;
        if layers.len() > max_layers {
            layers.truncate(max_layers);
        }

        // 防抖：冷却期内不重复启动 compaction（尤其是 manifest changed 场景）。
        {
            let mut g = self.compaction_last_started_at.lock();
            if let Some(last) = *g {
                if last.elapsed() < COMPACTION_COOLDOWN {
                    return;
                }
            }
            *g = Some(Instant::now());
        }

        // 避免并发 compaction
        if self.compaction_in_progress.swap(true, Ordering::AcqRel) {
            return;
        }

        let idx = self.clone();
        tokio::spawn(async move {
            struct CompactionInProgressGuard(Arc<TieredIndex>);
            impl Drop for CompactionInProgressGuard {
                fn drop(&mut self) {
                    self.0
                        .compaction_in_progress
                        .store(false, Ordering::Release);
                }
            }
            let _guard = CompactionInProgressGuard(idx.clone());
            match idx.compact_layers(store, layers).await {
                Ok(()) => tracing::debug!("Compaction attempt finished"),
                Err(e) => {
                    // manifest changed 是并发下的预期分支：并不意味着数据损坏。
                    let msg = e.to_string();
                    if msg.contains("LSM manifest changed, aborting compaction") {
                        tracing::info!("Compaction skipped due to concurrent manifest change");
                    } else {
                        tracing::error!("Compaction failed: {}", e);
                    }
                }
            }
            // compaction 是临时分配大户；无论成功/跳过/失败都尝试一次回吐。
            maybe_trim_rss();
        });
    }

    async fn compact_layers<S>(
        self: &Arc<Self>,
        store: Arc<S>,
        layers_snapshot: Vec<DiskLayer>,
    ) -> anyhow::Result<()>
    where
        S: StorageBackend + 'static,
    {
        if layers_snapshot.is_empty() {
            return Ok(());
        }
        // 若进入执行时层列表“前缀”已变化，直接放弃本轮（避免无意义重活）。
        // 允许并发 append 新 delta：只要当前层列表仍以本次 snapshot 作为前缀，本轮 compaction 仍然有意义。
        {
            let cur_ids = self
                .disk_layers
                .read()
                .iter()
                .map(|l| l.id)
                .collect::<Vec<_>>();
            let snap_ids = layers_snapshot.iter().map(|l| l.id).collect::<Vec<_>>();
            let snap_len = snap_ids.len();
            if cur_ids.len() < snap_len || cur_ids[..snap_len] != snap_ids[..] {
                return Ok(());
            }
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
            layer.deleted_paths.for_each_bytes(|p| {
                let pb = pathbuf_from_bytes(p);
                merged.mark_deleted_by_path(&pb);
            });
            layer.idx.for_each_live_meta(|m| merged.upsert_rename(m));
        }

        let segs = merged.export_segments_v6_compacted();
        let wal_seal_id = store.lsm_manifest_wal_seal_id().unwrap_or(0);

        let base_id = layers_snapshot[0].id;
        let delta_ids = layers_snapshot
            .iter()
            .skip(1)
            .map(|l| l.id)
            .collect::<Vec<_>>();
        let new_base = store
            .replace_base_v6(
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

        let deleted_paths = Arc::new(path_arena_set_from_paths(new_base.deleted_paths));
        let (cnt, bytes, est) = deleted_paths_stats(deleted_paths.as_ref());
        let new_layer = DiskLayer {
            id: new_base.id,
            idx: Arc::new(MmapIndex::new(new_base.snap)),
            deleted_paths,
            deleted_paths_count: cnt,
            deleted_paths_bytes: bytes,
            deleted_paths_estimated_bytes: est,
        };

        // 仅当段列表未变化时才替换（弱 CAS）
        {
            let mut cur = self.disk_layers.write();
            let snap_len = layers_snapshot.len();
            let prefix_matches = cur.len() >= snap_len
                && cur
                    .iter()
                    .take(snap_len)
                    .map(|l| l.id)
                    .eq(layers_snapshot.iter().map(|l| l.id));
            if prefix_matches {
                // 保留并发 append 的新 delta（suffix）；用 new_base 替换掉本次 compaction 的 prefix。
                let suffix: Vec<DiskLayer> = cur.drain(snap_len..).collect();
                cur.clear();
                cur.push(new_layer);
                cur.extend(suffix);
                self.l1.clear();
            }
        }

        // 清理旧段文件（best-effort；失败不影响正确性）
        let dir = store.derived_lsm_dir_path();
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

fn pathbuf_from_bytes(bytes: impl AsRef<[u8]>) -> PathBuf {
    let bytes = bytes.as_ref();
    #[cfg(unix)]
    {
        use std::ffi::OsString;
        use std::os::unix::ffi::OsStringExt;
        return PathBuf::from(OsString::from_vec(bytes.to_vec()));
    }
    #[cfg(not(unix))]
    {
        PathBuf::from(String::from_utf8_lossy(bytes).into_owned())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::{EventRecord, EventType, FileIdentifier};
    use crate::stats::EventPipelineStats;
    use crate::storage::snapshot::SnapshotStore;
    use std::path::PathBuf;

    fn mk_event(seq: u64, event_type: EventType, path: PathBuf) -> EventRecord {
        EventRecord {
            seq,
            timestamp: std::time::SystemTime::now(),
            event_type,
            id: FileIdentifier::Path(path.clone()),
            path_hint: Some(path),
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
            EventType::Rename {
                from: FileIdentifier::Path(from.clone()),
                from_path_hint: Some(from.clone()),
            },
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
                from: FileIdentifier::Path(old_path.clone()),
                from_path_hint: Some(old_path.clone()),
            },
            new_path.clone(),
        )]);

        idx.finish_rebuild(new_l2);
        assert!(idx.query("old_aaa").is_empty());
        assert!(!idx.query("new_bbb").is_empty());
    }

    #[test]
    fn fast_sync_reconciles_add_and_delete() {
        let root = unique_tmp_dir("fast-sync");
        std::fs::create_dir_all(&root).unwrap();

        let a = root.join("a_match.txt");
        let b = root.join("b_match.txt");
        std::fs::write(&a, b"a").unwrap();
        std::fs::write(&b, b"b").unwrap();

        let idx = TieredIndex::empty(vec![root.clone()]);
        idx.apply_events(&[
            mk_event(1, EventType::Create, a.clone()),
            mk_event(2, EventType::Create, b.clone()),
        ]);
        assert!(!idx.query("a_match").is_empty());
        assert!(!idx.query("b_match").is_empty());

        // 离线变更：不经过事件管道直接修改文件系统
        std::fs::remove_file(&b).unwrap();
        let c = root.join("c_match.txt");
        std::fs::write(&c, b"c").unwrap();

        let r = idx.fast_sync(
            DirtyScope::Dirs {
                cutoff_ns: 0,
                dirs: vec![root.clone()],
            },
            &[],
        );
        assert!(r.dirs_scanned >= 1);
        assert!(r.upsert_events >= 1);
        assert!(r.delete_events >= 1);

        assert!(!idx.query("a_match").is_empty());
        assert!(idx.query("b_match").is_empty());
        assert!(!idx.query("c_match").is_empty());
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
    async fn query_filters_do_not_leak_old_segment_meta() -> anyhow::Result<()> {
        let root = unique_tmp_dir("query-dsl-no-leak");
        std::fs::create_dir_all(&root)?;

        let store = Arc::new(SnapshotStore::new(root.join("index.db")));
        let idx = Arc::new(TieredIndex::empty(vec![root.clone()]));

        let p = root.join("x.txt");
        std::fs::write(&p, b"x")?;
        idx.apply_events(&[mk_event(1, EventType::Create, p.clone())]);

        // flush: 让旧元数据进入 disk layer
        idx.snapshot_now(store.clone()).await?;
        assert!(!idx.disk_layers.read().is_empty());

        // 修改文件：新元数据进入 L2（size 变大）
        std::fs::write(&p, vec![b'a'; 128])?;
        idx.apply_events(&[mk_event(2, EventType::Modify, p.clone())]);

        // 若未在 miss 时也 block path，旧段的 size 可能会“误命中”并被返回
        let r = idx.query_limit("size:<10b", 100);
        assert!(r.is_empty(), "should not return stale disk meta");

        let _ = std::fs::remove_dir_all(&root);
        Ok(())
    }

    #[tokio::test]
    async fn periodic_flush_batch_threshold_skips_then_flushes() {
        let root = unique_tmp_dir("periodic-batch-events");
        std::fs::create_dir_all(&root).unwrap();

        let store = Arc::new(SnapshotStore::new(root.join("index.db")));
        let idx = Arc::new(TieredIndex::empty(vec![root.clone()]));
        idx.set_auto_flush_limits(0, 0);
        idx.set_periodic_flush_batch_limits(2, 0);

        let h = tokio::spawn(idx.clone().snapshot_loop(store.clone(), 1));

        let p1 = root.join("a.txt");
        std::fs::write(&p1, b"a").unwrap();
        idx.apply_events(&[mk_event(1, EventType::Create, p1.clone())]);

        tokio::time::sleep(std::time::Duration::from_millis(1300)).await;
        assert!(idx.disk_layers.read().is_empty());

        let p2 = root.join("b.txt");
        std::fs::write(&p2, b"b").unwrap();
        idx.apply_events(&[mk_event(2, EventType::Create, p2.clone())]);

        let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(5);
        loop {
            if !idx.disk_layers.read().is_empty() {
                break;
            }
            if tokio::time::Instant::now() >= deadline {
                panic!("periodic flush did not flush after event threshold was met");
            }
            tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        }

        h.abort();
    }

    #[tokio::test]
    async fn periodic_flush_batch_byte_threshold_skips_then_flushes() {
        let root = unique_tmp_dir("periodic-batch-bytes");
        std::fs::create_dir_all(&root).unwrap();

        let store = Arc::new(SnapshotStore::new(root.join("index.db")));
        let idx = Arc::new(TieredIndex::empty(vec![root.clone()]));
        idx.set_auto_flush_limits(0, 0);

        let p1 = root.join("alpha-long-name.txt");
        let ev1 = mk_event(1, EventType::Create, p1.clone());
        let threshold = event_record_estimated_bytes(&ev1).saturating_add(1);
        idx.set_periodic_flush_batch_limits(0, threshold);

        let h = tokio::spawn(idx.clone().snapshot_loop(store.clone(), 1));

        std::fs::write(&p1, b"a").unwrap();
        idx.apply_events(&[ev1]);

        tokio::time::sleep(std::time::Duration::from_millis(1300)).await;
        assert!(idx.disk_layers.read().is_empty());

        let p2 = root.join("beta-long-name.txt");
        std::fs::write(&p2, b"b").unwrap();
        idx.apply_events(&[mk_event(2, EventType::Create, p2.clone())]);

        let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(5);
        loop {
            if !idx.disk_layers.read().is_empty() {
                break;
            }
            if tokio::time::Instant::now() >= deadline {
                panic!("periodic flush did not flush after byte threshold was met");
            }
            tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        }

        h.abort();
    }

    #[tokio::test]
    async fn lsm_layering_delete_blocks_base() {
        use crate::core::{FileKey, FileMeta};
        use crate::index::PersistentIndex;

        let root = unique_tmp_dir("lsm-del");
        std::fs::create_dir_all(&root).unwrap();

        let alpha = root.join("alpha_test.txt");
        let gamma = root.join("gamma_test.txt");

        let store = Arc::new(SnapshotStore::new(root.join("index.db")));

        // base: alpha
        let base_idx = PersistentIndex::new_with_roots(vec![root.clone()]);
        base_idx.upsert(FileMeta {
            file_key: FileKey { dev: 1, ino: 1 },
            path: alpha.clone(),
            size: 1,
            mtime: None,
            ctime: None,
            atime: None,
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
            ctime: None,
            atime: None,
        });
        let deleted = vec![alpha.as_os_str().as_encoded_bytes().to_vec()];
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

        let root = unique_tmp_dir("lsm-recreate");
        std::fs::create_dir_all(&root).unwrap();

        let alpha = root.join("alpha_test.txt");
        let store = Arc::new(SnapshotStore::new(root.join("index.db")));

        // base: alpha
        let base_idx = PersistentIndex::new_with_roots(vec![root.clone()]);
        base_idx.upsert(FileMeta {
            file_key: FileKey { dev: 1, ino: 1 },
            path: alpha.clone(),
            size: 1,
            mtime: None,
            ctime: None,
            atime: None,
        });
        store
            .lsm_replace_base_v6(&base_idx.export_segments_v6(), None, &[root.clone()], 0)
            .await
            .unwrap();
        store.gc_stale_segments().unwrap();

        // delta1: delete(alpha)
        let d1 = PersistentIndex::new_with_roots(vec![root.clone()]);
        let deleted = vec![alpha.as_os_str().as_encoded_bytes().to_vec()];
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
            ctime: None,
            atime: None,
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
    async fn query_same_path_different_filekey_prefers_newest_segment() {
        use crate::core::{FileKey, FileMeta};
        use crate::index::PersistentIndex;

        let root = unique_tmp_dir("q-samepath-newest");
        std::fs::create_dir_all(&root).unwrap();

        let a = root.join("a.txt");
        std::fs::write(&a, b"x").unwrap();

        let store = Arc::new(SnapshotStore::new(root.join("index.db")));

        // seg1 (older): (dev=1, ino=100, path=/a.txt)
        let seg1 = PersistentIndex::new_with_roots(vec![root.clone()]);
        seg1.upsert(FileMeta {
            file_key: FileKey { dev: 1, ino: 100 },
            path: a.clone(),
            size: 1,
            mtime: None,
            ctime: None,
            atime: None,
        });
        store
            .lsm_replace_base_v6(&seg1.export_segments_v6(), None, &[root.clone()], 0)
            .await
            .unwrap();

        // seg2 (newer): (dev=1, ino=200, path=/a.txt) -- no delete sidecar
        let seg2 = PersistentIndex::new_with_roots(vec![root.clone()]);
        seg2.upsert(FileMeta {
            file_key: FileKey { dev: 1, ino: 200 },
            path: a.clone(),
            size: 2,
            mtime: None,
            ctime: None,
            atime: None,
        });
        store
            .lsm_append_delta_v6(&seg2.export_segments_v6(), &[], &[root.clone()], 0)
            .await
            .unwrap();

        let idx = TieredIndex::load_or_empty(&store, vec![root.clone()])
            .await
            .unwrap();

        let r = idx.query("a.txt");
        assert_eq!(r.len(), 1);
        assert_eq!(r[0].file_key.ino, 200);
    }

    #[tokio::test]
    async fn query_rename_from_tombstone_blocks_old_path() {
        use crate::core::{FileKey, FileMeta};
        use crate::index::PersistentIndex;

        let root = unique_tmp_dir("q-rename-tombstone");
        std::fs::create_dir_all(&root).unwrap();

        let old = root.join("old.txt");
        let newp = root.join("new.txt");
        std::fs::write(&old, b"old").unwrap();
        std::fs::write(&newp, b"new").unwrap();

        let store = SnapshotStore::new(root.join("index.db"));

        // seg1 (older): /old.txt
        let seg1 = PersistentIndex::new_with_roots(vec![root.clone()]);
        seg1.upsert(FileMeta {
            file_key: FileKey { dev: 1, ino: 10 },
            path: old.clone(),
            size: 1,
            mtime: None,
            ctime: None,
            atime: None,
        });
        store
            .lsm_replace_base_v6(&seg1.export_segments_v6(), None, &[root.clone()], 0)
            .await
            .unwrap();

        // seg2 (newer): /new.txt + tombstone(/old.txt)
        let seg2 = PersistentIndex::new_with_roots(vec![root.clone()]);
        seg2.upsert(FileMeta {
            file_key: FileKey { dev: 1, ino: 11 },
            path: newp.clone(),
            size: 1,
            mtime: None,
            ctime: None,
            atime: None,
        });
        let deleted = vec![old.as_os_str().as_encoded_bytes().to_vec()];
        store
            .lsm_append_delta_v6(&seg2.export_segments_v6(), &deleted, &[root.clone()], 0)
            .await
            .unwrap();

        let idx = TieredIndex::load_or_empty(&store, vec![root.clone()])
            .await
            .unwrap();

        assert!(idx.query("old.txt").is_empty());
        assert_eq!(idx.query("new.txt").len(), 1);

        // Query wide pattern that would match both paths: old must remain blocked.
        let all = idx.query(".txt");
        assert_eq!(all.len(), 1);
        assert!(all[0].path.to_string_lossy().ends_with("new.txt"));
    }

    #[tokio::test]
    async fn query_same_filekey_multiple_paths_only_returns_newest_path() {
        use crate::core::{FileKey, FileMeta};
        use crate::index::PersistentIndex;

        let root = unique_tmp_dir("q-samekey-newestpath");
        std::fs::create_dir_all(&root).unwrap();

        let p1 = root.join("ghost_v1.txt");
        let p2 = root.join("ghost_v2.txt");
        let p3 = root.join("ghost_v3.txt");
        std::fs::write(&p1, b"1").unwrap();
        std::fs::write(&p2, b"2").unwrap();
        std::fs::write(&p3, b"3").unwrap();

        let store = SnapshotStore::new(root.join("index.db"));

        let k = FileKey { dev: 1, ino: 999 };

        // seg1 (older): k -> p1
        let seg1 = PersistentIndex::new_with_roots(vec![root.clone()]);
        seg1.upsert(FileMeta {
            file_key: k,
            path: p1.clone(),
            size: 1,
            mtime: None,
            ctime: None,
            atime: None,
        });
        store
            .lsm_replace_base_v6(&seg1.export_segments_v6(), None, &[root.clone()], 0)
            .await
            .unwrap();

        // seg2: k -> p2
        let seg2 = PersistentIndex::new_with_roots(vec![root.clone()]);
        seg2.upsert(FileMeta {
            file_key: k,
            path: p2.clone(),
            size: 2,
            mtime: None,
            ctime: None,
            atime: None,
        });
        store
            .lsm_append_delta_v6(&seg2.export_segments_v6(), &[], &[root.clone()], 0)
            .await
            .unwrap();

        // seg3 (newest): k -> p3
        let seg3 = PersistentIndex::new_with_roots(vec![root.clone()]);
        seg3.upsert(FileMeta {
            file_key: k,
            path: p3.clone(),
            size: 3,
            mtime: None,
            ctime: None,
            atime: None,
        });
        store
            .lsm_append_delta_v6(&seg3.export_segments_v6(), &[], &[root.clone()], 0)
            .await
            .unwrap();

        let idx = TieredIndex::load_or_empty(&store, vec![root.clone()])
            .await
            .unwrap();

        // 如果 seen(FileKey) 去重语义回退，这里会返回 3 条（路径不同，blocked 兜不住）。
        let r = idx.query("ghost_");
        assert_eq!(r.len(), 1);
        assert_eq!(r[0].file_key.ino, 999);
        assert!(r[0].path.to_string_lossy().ends_with("ghost_v3.txt"));
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
            ctime: None,
            atime: None,
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

    #[tokio::test]
    async fn compaction_prefix_replaces_base_and_keeps_suffix_deltas() {
        use crate::core::{FileKey, FileMeta};
        use crate::index::PersistentIndex;

        let root = unique_tmp_dir("lsm-compact-prefix");
        std::fs::create_dir_all(&root).unwrap();
        let store = Arc::new(SnapshotStore::new(root.join("index.db")));

        let mk_seg = |ino: u64, name: &str| {
            let idx = PersistentIndex::new_with_roots(vec![root.clone()]);
            idx.upsert(FileMeta {
                file_key: FileKey { dev: 1, ino },
                path: root.join(name),
                size: ino,
                mtime: None,
                ctime: None,
                atime: None,
            });
            idx
        };

        store
            .lsm_replace_base_v6(
                &mk_seg(1, "base.txt").export_segments_v6(),
                None,
                &[root.clone()],
                10,
            )
            .await
            .unwrap();
        store
            .lsm_append_delta_v6(
                &mk_seg(2, "delta-1.txt").export_segments_v6(),
                &[],
                &[root.clone()],
                11,
            )
            .await
            .unwrap();
        store
            .lsm_append_delta_v6(
                &mk_seg(3, "delta-2.txt").export_segments_v6(),
                &[],
                &[root.clone()],
                12,
            )
            .await
            .unwrap();
        store
            .lsm_append_delta_v6(
                &mk_seg(4, "delta-3.txt").export_segments_v6(),
                &[],
                &[root.clone()],
                13,
            )
            .await
            .unwrap();
        store
            .lsm_append_delta_v6(
                &mk_seg(5, "delta-4.txt").export_segments_v6(),
                &[],
                &[root.clone()],
                14,
            )
            .await
            .unwrap();

        let idx = Arc::new(
            TieredIndex::load_or_empty(&store, vec![root.clone()])
                .await
                .unwrap(),
        );
        assert_eq!(idx.disk_layers.read().len(), 5);

        let prefix = idx
            .disk_layers
            .read()
            .iter()
            .take(3)
            .cloned()
            .collect::<Vec<_>>();
        assert_eq!(
            prefix.iter().map(|l| l.id).collect::<Vec<_>>(),
            vec![1, 2, 3]
        );

        idx.compact_layers(store.clone(), prefix).await.unwrap();

        let layer_ids = idx
            .disk_layers
            .read()
            .iter()
            .map(|l| l.id)
            .collect::<Vec<_>>();
        assert_eq!(layer_ids.len(), 3);
        assert_eq!(layer_ids[1..], [4, 5]);
        assert!(layer_ids[0] > 5);

        let loaded = store.load_lsm_if_valid(&[root.clone()]).unwrap().unwrap();
        assert_eq!(loaded.base.as_ref().map(|b| b.id), Some(layer_ids[0]));
        assert_eq!(
            loaded.deltas.iter().map(|d| d.id).collect::<Vec<_>>(),
            vec![4, 5]
        );
        assert_eq!(loaded.wal_seal_id, 14);
    }
}
