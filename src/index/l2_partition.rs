use parking_lot::RwLock;
use roaring::RoaringTreemap;
use std::borrow::Cow;
use std::collections::HashMap;
use std::hash::{Hash, Hasher};
use std::path::{Component, Path, PathBuf};
use std::sync::Arc;

#[cfg(feature = "rkyv")]
use crate::core::FileKeyEntry;
use crate::core::{EventRecord, EventType, FileIdentifier, FileKey, FileMeta};
use crate::index::base_index::BaseIndex;
use crate::index::delta_buffer::DeltaBuffer;
use crate::index::file_entry::FileEntry;
use crate::index::parent_index::ParentIndex;
use crate::index::path_table::PathTable;
use crate::index::IndexLayer;
use crate::query::matcher::Matcher;
use crate::stats::L2Stats;
use crate::storage::snapshot_v7::V7Snapshot;
use crate::util::{compose_abs_path_bytes, pathbuf_from_encoded_vec, root_bytes_for_id};

/// Trigram：3 字节子串，用于倒排索引加速查询
type Trigram = [u8; 3];

/// 将 1-2 字节的短路径组件编码为 u16（大端序，零填充高位）。
/// 1 字节: `[b]` → `[b, 0x00]`；2 字节: `[b0, b1]` → `[b0, b1]`。
#[inline]
fn encode_short_component(bytes: &[u8]) -> u16 {
    u16::from_be_bytes([bytes[0], bytes.get(1).copied().unwrap_or(0)])
}

fn normalize_short_hint(hint: &[u8]) -> Option<Vec<u8>> {
    let normalized = String::from_utf8_lossy(hint).to_lowercase().into_bytes();
    if (1..=2).contains(&normalized.len()) {
        Some(normalized)
    } else {
        None
    }
}

fn trigram_matches_short_hint(tri: Trigram, hint: &[u8]) -> bool {
    match hint.len() {
        1 => tri.contains(&hint[0]),
        2 => tri[0..2] == hint[..] || tri[1..3] == hint[..],
        _ => false,
    }
}

fn short_component_matches(encoded: u16, hint: &[u8]) -> bool {
    if hint.is_empty() || hint.len() > 2 {
        return false;
    }
    let bytes = encoded.to_be_bytes();
    if hint.len() == 1 {
        bytes[0] == hint[0] || bytes[1] == hint[0]
    } else {
        bytes == hint
    }
}

#[derive(Clone, Copy)]
struct ResolvedFsMeta {
    file_key: FileKey,
    size: u64,
    mtime: Option<std::time::SystemTime>,
}

fn mtime_to_ns(mtime: Option<std::time::SystemTime>) -> i64 {
    mtime
        .and_then(|t| t.duration_since(std::time::UNIX_EPOCH).ok())
        .and_then(|d| i64::try_from(d.as_nanos()).ok())
        .unwrap_or(-1)
}

fn mtime_from_ns(ns: i64) -> Option<std::time::SystemTime> {
    if ns < 0 {
        None
    } else {
        Some(std::time::UNIX_EPOCH + std::time::Duration::from_nanos(ns as u64))
    }
}

/// DocId：L2 内部紧凑文档编号（posting 的元素类型）
pub type DocId = u64;

/// 从查询词中提取 trigram 列表
fn query_trigrams(query: &str) -> Vec<Trigram> {
    let lower = query.to_lowercase();
    let bytes = lower.as_bytes();
    let mut tris = Vec::new();
    if bytes.len() >= 3 {
        for w in bytes.windows(3) {
            tris.push([w[0], w[1], w[2]]);
        }
    }
    tris
}

/// 从路径的所有“洁净组件”（`Component::Normal`）中枚举 trigram（可能重复）。
///
/// - 标准化：lossy UTF-8 + to_lowercase
/// - 目的：让 trigram 候选集成为 Segment/contains 等精确匹配的严格超集（避免假阴性）
fn for_each_component_trigram(path: &Path, mut f: impl FnMut(Trigram)) {
    for c in path.components() {
        let Component::Normal(os) = c else {
            continue;
        };
        let lower = os.to_string_lossy().to_lowercase();
        let bytes = lower.as_bytes();
        if bytes.len() < 3 {
            continue;
        }
        for w in bytes.windows(3) {
            f([w[0], w[1], w[2]]);
        }
    }
}

fn for_each_short_component(path: &Path, mut f: impl FnMut(u16)) {
    for c in path.components() {
        let Component::Normal(os) = c else {
            continue;
        };
        let lower = os.to_string_lossy().to_lowercase();
        let bytes = lower.as_bytes();
        if (1..=2).contains(&bytes.len()) {
            f(encode_short_component(bytes));
        }
    }
}

/// 仅用于 v6 段落盘前的本地 PathArena 合成。M4-C 之后不再作为 PersistentIndex
/// 字段或 IndexSnapshotV4/V5 字段——这些类型已删除。
struct V6ExportArena {
    data: Vec<u8>,
}

impl V6ExportArena {
    fn new() -> Self {
        Self { data: Vec::new() }
    }
    fn push_bytes(&mut self, bytes: &[u8]) -> Option<(u32, u16)> {
        let len: u16 = bytes.len().try_into().ok()?;
        let off: u32 = self.data.len().try_into().ok()?;
        self.data.extend_from_slice(bytes);
        Some((off, len))
    }
    fn into_bytes(self) -> Vec<u8> {
        self.data
    }
}

/// v6 段式快照：由 PersistentIndex 导出为一组“可独立校验”的段（供 storage/snapshot 写入）。
///
/// 说明：
/// - v6 的核心目标是：冷启动 mmap + lazy decode（posting 按需解码）
/// - 这里仅导出段的 raw bytes；物理布局/校验与原子替换由 storage 层负责
#[derive(Clone, Debug)]
pub struct V6Segments {
    pub roots_bytes: Arc<Vec<u8>>,
    pub path_arena_bytes: Arc<Vec<u8>>,
    pub metas_bytes: Arc<Vec<u8>>,
    pub trigram_table_bytes: Arc<Vec<u8>>,
    pub postings_blob_bytes: Arc<Vec<u8>>,
    pub tombstones_bytes: Arc<Vec<u8>>,
    pub filekey_map_bytes: Arc<Vec<u8>>,
}

#[derive(Clone, Debug)]
enum OneOrManyDocId {
    One(DocId),
    Many(Vec<DocId>),
}

impl OneOrManyDocId {
    fn iter(&self) -> impl Iterator<Item = &DocId> {
        match self {
            OneOrManyDocId::One(id) => std::slice::from_ref(id).iter(),
            OneOrManyDocId::Many(v) => v.iter(),
        }
    }

    fn insert(&mut self, id: DocId) {
        match self {
            OneOrManyDocId::One(existing) => {
                if *existing == id {
                    return;
                }
                *self = OneOrManyDocId::Many(vec![*existing, id]);
            }
            OneOrManyDocId::Many(v) => {
                if !v.contains(&id) {
                    v.push(id);
                }
            }
        }
    }

    /// 返回 true 表示变为空，需要从 map 移除
    fn remove(&mut self, id: DocId) -> bool {
        match self {
            OneOrManyDocId::One(existing) => *existing == id,
            OneOrManyDocId::Many(v) => {
                v.retain(|x| *x != id);
                if v.len() == 1 {
                    let only = v[0];
                    *self = OneOrManyDocId::One(only);
                    false
                } else {
                    v.is_empty()
                }
            }
        }
    }

    fn len(&self) -> usize {
        match self {
            OneOrManyDocId::One(_) => 1,
            OneOrManyDocId::Many(v) => v.len(),
        }
    }
}

/// L2: 持久索引（内存常驻，可直接查询；trigram 倒排加速）
///
/// ## 单路径策略 (Single-Path Policy)
/// 一个 `FileKey(dev, ino)` 只存储一条路径（最先发现的那个）。
/// Hardlink 的其他路径视为"不在索引中"。
/// 理由：简单、可预测、够用。如需多路径支持，需扩展为 `FileKey -> Vec<PathBuf>`。
pub struct PersistentIndex {
    /// root 列表（root_id -> root Path）。root_id=0 固定为 "/" 作为兜底。
    roots: Vec<PathBuf>,
    roots_bytes: Vec<Vec<u8>>,
    /// FileKey -> DocId
    filekey_to_docid: RwLock<HashMap<FileKey, DocId>>,

    /// 路径反查：hash(path_bytes) -> DocId（或少量冲突列表）
    path_hash_to_id: RwLock<HashMap<u64, OneOrManyDocId>>,

    /// Trigram 倒排索引：trigram -> RoaringTreemap(DocId)
    trigram_index: RwLock<HashMap<Trigram, RoaringTreemap>>,
    /// 短组件索引：长度 1-2 的标准化路径组件 -> RoaringTreemap(DocId)
    short_component_index: RwLock<HashMap<u16, RoaringTreemap>>,

    /// 墓碑标记（DocId）
    tombstones: RwLock<RoaringTreemap>,

    /// 父目录倒排索引：parent_path_hash → RoaringTreemap of DocIds.
    ///
    /// 用于 [`Self::for_each_live_meta_in_dirs`] 把"按目录定位 live 文件"从
    /// O(N_total_metas) 降到 O(dir_size + bitmap)。语义参见
    /// [`crate::index::parent_index::ParentIndex`] 与 `重构方案包/causal-chain-report.md` §一·链路1。
    parent_index: RwLock<ParentIndex>,

    /// upsert 写锁：保护 alloc_docid → insert_trigrams / insert_path_hash 的原子性，
    /// 防止写入-查询竞态导致 trigram 索引与 metas 不一致。
    upsert_lock: RwLock<()>,

    /// 脏标记（自上次快照后是否有变更）
    dirty: std::sync::atomic::AtomicBool,

    /// BaseDelta 内核（参见 `重构方案包/causal-chain-report.md` §8.6 第二阶段）。
    ///
    /// **M4 阶段**：此字段始终持有 [`BaseDeltaKernel`]——`new_with_roots` 初始化为
    /// 空 base + 空 delta，所有 `from_snapshot_*` 加载完 metas+arena 后立即重建一次。
    /// 写路径已经 M3 双写到 delta；M4-B/C 后续会让 BaseDelta 接管所有读路径并物理
    /// 删除 metas+arena 字段。
    base_delta: parking_lot::RwLock<BaseDeltaKernel>,
}

/// BaseDelta 内核：[`crate::index::base_index::BaseIndex`] 只读快照 +
/// [`crate::index::delta_buffer::DeltaBuffer`] 运行时可写增量。
///
/// docid 空间分割：
/// - `[0, base.by_path_idx().len())` 由 base 持有；通过 `base.resolve_path(docid)` 解析。
/// - `[base.by_path_idx().len(), ...)` 由 delta 持有；通过
///   `delta.added_path_bytes(docid - base_count)` 解析。
///
/// 这与 [`crate::index::l2_partition::PersistentIndex.metas`] 的 docid 空间是**独立**的——
/// M1 阶段 BaseDelta 与 Legacy 并存，docid 含义不互通；切换需依赖
/// [`Self::resolve_abs_path_bytes_for_docid`] 这个抽象。
#[derive(Debug)]
pub struct BaseDeltaKernel {
    pub base: Arc<BaseIndex>,
    pub delta: DeltaBuffer,
}

impl Default for PersistentIndex {
    fn default() -> Self {
        Self::new()
    }
}

impl PersistentIndex {
    pub fn new() -> Self {
        Self::new_with_roots(Vec::new())
    }

    pub fn new_with_roots(roots: Vec<PathBuf>) -> Self {
        let roots = normalize_roots_with_fallback(roots);
        let roots_bytes = roots
            .iter()
            .map(|p| p.as_os_str().as_encoded_bytes().to_vec())
            .collect::<Vec<_>>();

        Self {
            roots,
            roots_bytes,
            filekey_to_docid: RwLock::new(HashMap::new()),
            path_hash_to_id: RwLock::new(HashMap::new()),
            trigram_index: RwLock::new(HashMap::new()),
            short_component_index: RwLock::new(HashMap::new()),
            tombstones: RwLock::new(RoaringTreemap::new()),
            parent_index: RwLock::new(ParentIndex::new()),
            upsert_lock: RwLock::new(()),
            dirty: std::sync::atomic::AtomicBool::new(false),
            base_delta: parking_lot::RwLock::new(BaseDeltaKernel {
                base: Arc::new(BaseIndex::empty()),
                delta: DeltaBuffer::new(),
            }),
        }
    }

    /// 计算路径父目录字节的哈希，与 [`crate::index::parent_index::parent_hash_bytes`] 等价。
    /// 返回 None 表示路径无父目录（极少见，例如根 `/` 或空路径）。
    fn parent_hash_for(abs_path: &Path) -> Option<u64> {
        abs_path
            .parent()
            .map(|p| path_hash_bytes(p.as_os_str().as_encoded_bytes()))
    }

    /// 从 v7 snapshot 加载（参见 `重构方案包/causal-chain-report.md` §8.9）。
    ///
    /// 转换策略
    /// - V7 的 entries 数组按 path_idx 升序排列；本入口把每条 entry 翻译回旧的
    ///   `CompactMeta + PathArena` 内存模型，让现有 query/upsert/snapshot 路径完全无感。
    /// - 新分配的 docid = V7 entries 的下标，因此 V7 中的 tombstones 可以原样套用。
    /// - 若某条 entry 的 `path_idx` 损坏导致 PathTable 解析失败，整次加载视为损坏并放弃；
    ///   宁可启动回退到全量重扫，也不要让索引和实际数据错位。
    pub fn from_snapshot_v7(snap: V7Snapshot, roots: Vec<PathBuf>) -> Self {
        let idx = Self::new_with_roots(roots);

        // roots 兼容性：v7 文件里冻结了 roots 字节序列，必须与运行时一致。
        let roots_match = snap.roots.len() == idx.roots_bytes.len()
            && snap
                .roots
                .iter()
                .zip(idx.roots_bytes.iter())
                .all(|(a, b)| a == b);
        if !roots_match {
            tracing::warn!(
                "V7 snapshot roots mismatch ({} entries vs {}); ignoring snapshot",
                snap.roots.len(),
                idx.roots_bytes.len()
            );
            return idx;
        }

        // Phase 4：BaseDelta 是唯一权威。直接复用 v7 的 path_table + entries（已排序），
        // 不再走 BaseIndexBuilder 重建路径——节省 ≈ 800MB 临时分配 + 8-15s CPU
        // （8M 文件场景）。v7 的 tombstones 转移到外层 PersistentIndex.tombstones，
        // 因为查询/upsert 路径都在那里查表。
        let mut snap = snap;
        let snap_tombstones = std::mem::take(&mut snap.tombstones);
        let entry_count = snap.entries.len() as u64;
        let base_index = BaseIndex::from_v7_snapshot(snap);
        let mut delta = DeltaBuffer::new();
        delta.set_base_file_count(entry_count);

        *idx.base_delta.write() = BaseDeltaKernel {
            base: Arc::new(base_index),
            delta,
        };
        *idx.tombstones.write() = snap_tombstones;
        idx.dirty.store(false, std::sync::atomic::Ordering::Release);

        idx.rebuild_derived_indexes();
        idx
    }

    /// 把当前内存状态导出为 V7 snapshot（参见 `重构方案包/causal-chain-report.md` §8.9）。
    ///
    /// 注意：导出时按 abs_path 字典序对所有条目重新分配 docid（V7 的硬约束），
    /// 因此**当前 PersistentIndex 里的 docid ≠ V7 里的 docid**；tombstones 在转换中
    /// 一并按新 docid 重映射。
    ///
    /// M4-B：数据源从 metas+arena 切到 BaseDelta（base 段 + delta.added 段）；docid
    /// 编号继承 legacy 空间，只在导出时重新按 abs path 字典序分配。
    pub fn to_snapshot_v7(&self) -> V7Snapshot {
        let bd = self.base_delta.read();
        let tombstones = self.tombstones.read();
        let base_count = bd.base.by_path_idx().len() as u64;
        let total = base_count + bd.delta.added().len() as u64;

        // 1) 收集 (abs_path_bytes, FileKey, size, mtime_ns, was_tombstoned)。
        // Phase 4：base 半段走 path_table.for_each_path 顺序扫描，省 ~30× CPU。
        let base_entries = bd.base.by_path_idx().entries();
        let mut items: Vec<(Vec<u8>, FileKey, u64, i64, bool)> = Vec::with_capacity(total as usize);
        bd.base.path_table().for_each_path(|path_idx, abs_bytes| {
            let docid = path_idx as u64;
            let Some(entry) = base_entries.get(path_idx as usize) else {
                return;
            };
            items.push((
                abs_bytes.to_vec(),
                entry.file_key(),
                entry.size,
                entry.mtime_ns,
                tombstones.contains(docid),
            ));
        });
        for idx in 0..bd.delta.added().len() {
            let docid = base_count + idx as u64;
            let Some(entry) = bd.delta.added().get(idx) else {
                continue;
            };
            let Some(abs_bytes) = bd.delta.added_path_bytes(idx) else {
                continue;
            };
            items.push((
                abs_bytes.to_vec(),
                entry.file_key(),
                entry.size,
                entry.mtime_ns,
                tombstones.contains(docid),
            ));
        }
        let _ = total;

        // 2) 字典序排序（v7 PathTable 的硬要求；同时 entries 按这个顺序分配新 docid）。
        items.sort_by(|a, b| a.0.as_slice().cmp(b.0.as_slice()));

        // 3) 写 PathTable + 生成 FileEntries（path_idx == 新 docid）+ tombstones 重映射。
        let mut path_table = PathTable::new();
        let mut entries: Vec<FileEntry> = Vec::with_capacity(items.len());
        let mut new_tombstones = roaring::RoaringTreemap::new();
        for (i, (path, fk, size, mtime_ns, was_tomb)) in items.iter().enumerate() {
            let path_idx = path_table.push(path);
            entries.push(FileEntry::new(*fk, path_idx, *size, *mtime_ns));
            if *was_tomb {
                new_tombstones.insert(i as u64);
            }
        }

        V7Snapshot {
            roots: self.roots_bytes.clone(),
            path_table,
            entries,
            tombstones: new_tombstones,
            wal_seal_id: 0,
        }
    }

    /// 同 [`Self::to_snapshot_v7`]，但写入指定的 `wal_seal_id`。供 LSM `replace_base_v6`
    /// 成功后立即生成 v7 companion 时使用——v7 与 LSM 共享同一个 wal_seal_id 才能
    /// 在启动时被判断为"与最新 LSM 同步"。
    pub fn to_snapshot_v7_with_seal(&self, wal_seal_id: u64) -> V7Snapshot {
        let mut snap = self.to_snapshot_v7();
        snap.wal_seal_id = wal_seal_id;
        snap
    }

    // M4-C: from_snapshot_v2/v3/v4/v5 + IndexSnapshotV2-V5 + CompactMeta(V4) + PathArena
    // 已删除。带旧 bincode index.db 的用户会在启动时被 load_if_valid 拒绝（返回
    // Ok(None)），上层降级到全量重扫，等同于一次性丢索引重建。

    /// M4-B: 重建派生索引（trigram/path_hash/parent_index 等）。
    /// 数据源：BaseDelta（base + delta.added，docid 空间与 legacy `tombstones` 对齐）。
    /// 与 metas+arena 不再耦合——下游 M4-C 删除 Legacy 字段后这条路径仍能用。
    ///
    /// Phase 4：base 半段用 [`PathTable::for_each_path`] 顺序遍历——单次 O(data.len())
    /// 扫描代替每条目独立 resolve（每条 O(anchor_interval)）。8M 文件冷启动估算
    /// 节省 ≈ 30× CPU on this loop。
    fn rebuild_derived_indexes(&self) {
        let tomb = self.tombstones.read();
        let bd = self.base_delta.read();
        let base_entries = bd.base.by_path_idx().entries();
        let base_count = base_entries.len() as u64;

        let mut filekey_to_docid = self.filekey_to_docid.write();
        let mut path_hash_to_id = self.path_hash_to_id.write();
        let mut trigram_index = self.trigram_index.write();
        let mut short_component_index = self.short_component_index.write();
        let mut parent_index = self.parent_index.write();

        filekey_to_docid.clear();
        path_hash_to_id.clear();
        trigram_index.clear();
        short_component_index.clear();
        parent_index.clear();

        // base 与 delta 共用："插 trigram + parent + 可选 path_hash"。
        // abs_bytes 在 base 半段是 path_table 内部 buffer 的借用（不分配），
        // 在 delta 半段是 delta.added_path_bytes 的借用——两边都不再 to_vec。
        //
        // Phase 4：
        // - filekey_to_docid 仅 delta 半段维护，base 半段走 BaseIndex.by_file_key 二分。
        // - path_hash_to_id 仅 delta 半段维护，base 半段走 PathTable::find_path_idx。
        //   8M 文件场景下省 ~160MB 常驻 HashMap（参见
        //   `重构方案包/causal-chain-report.md` §8.7 的内存表）。
        let mut insert_derived = |docid: u64,
                                  abs_bytes: &[u8],
                                  put_path_hash: bool,
                                  path_hash_to_id: &mut HashMap<u64, OneOrManyDocId>| {
            if tomb.contains(docid) {
                return;
            }
            if put_path_hash {
                let h = path_hash_bytes(abs_bytes);
                path_hash_to_id
                    .entry(h)
                    .and_modify(|v| v.insert(docid))
                    .or_insert(OneOrManyDocId::One(docid));
            }

            // PathBuf 仍要分配——`for_each_component_trigram` 走 `Path::components`
            // 是平台感知的（Windows 处理 drive 前缀/反斜杠），底层字节迭代在这里
            // 不容易替代。pathbuf_from_encoded_vec 在 Unix 上是 zero-copy move。
            let abs_path = pathbuf_from_encoded_vec(abs_bytes.to_vec());
            for_each_component_trigram(abs_path.as_path(), |tri| {
                trigram_index.entry(tri).or_default().insert(docid);
            });
            for_each_short_component(abs_path.as_path(), |component| {
                short_component_index
                    .entry(component)
                    .or_default()
                    .insert(docid);
            });
            if let Some(ph) = Self::parent_hash_for(&abs_path) {
                parent_index.insert(ph, docid);
            }
        };

        // Base 半段：不登记 filekey_to_docid（走 by_file_key 二分），
        // 也不登记 path_hash_to_id（走 PathTable::find_path_idx）。
        bd.base.path_table().for_each_path(|path_idx, abs_bytes| {
            let docid = path_idx as u64;
            if base_entries.get(path_idx as usize).is_none() {
                return;
            }
            insert_derived(docid, abs_bytes, false, &mut path_hash_to_id);
        });

        // Delta 半段：filekey_to_docid + path_hash_to_id 都登记。
        for idx in 0..bd.delta.added().len() {
            let docid = base_count + idx as u64;
            let Some(entry) = bd.delta.added().get(idx) else {
                continue;
            };
            let Some(abs_bytes) = bd.delta.added_path_bytes(idx) else {
                continue;
            };
            filekey_to_docid.insert(entry.file_key(), docid);
            insert_derived(docid, abs_bytes, true, &mut path_hash_to_id);
        }
    }

    /// 插入/更新一条文件记录
    ///
    /// ## 单路径策略 (first-seen wins)
    /// 如果该 FileKey 已存在且路径不同（hardlink 场景），
    /// 保留最先发现的路径，仅更新 size/mtime 等元数据。
    /// 只有显式 rename 事件，或补扫时检测到旧路径已消失的 reconcile 场景，才会更新路径。
    pub fn upsert(&self, meta: FileMeta) {
        self.upsert_inner(meta, false);
    }

    /// rename 专用：强制更新路径
    pub fn upsert_rename(&self, meta: FileMeta) {
        self.upsert_inner(meta, true);
    }

    fn upsert_inner(&self, mut meta: FileMeta, force_path_update: bool) {
        meta.path = crate::index::tiered::normalize_path(&meta.path);
        let fkey = meta.file_key;
        let (new_root_id, new_rel_bytes) = self.split_root_relative_bytes(meta.path.as_path());
        let new_abs_bytes = compose_abs_path_bytes(
            root_bytes_for_id(&self.roots_bytes, new_root_id),
            &new_rel_bytes,
        );

        // 先查 docid（只持有 mapping 的读锁）
        let existing_docid = self.lookup_docid_by_filekey(fkey);

        if let Some(docid) = existing_docid {
            // M4-C: 读旧路径走 BaseDelta（不再访问 metas+arena）。
            let old_abs_bytes = self.resolve_abs_path_bytes_for_docid(docid);
            let same_path = old_abs_bytes
                .as_deref()
                .map(|b| b == new_abs_bytes.as_slice())
                .unwrap_or(false);

            if same_path {
                // 同路径重复上报：只更新元数据，避免 posting 重复写入。
                self.update_meta_in_delta(
                    docid,
                    fkey,
                    &new_abs_bytes,
                    meta.size,
                    mtime_to_ns(meta.mtime),
                );
                self.dirty.store(true, std::sync::atomic::Ordering::Release);
                return;
            }

            let old_path_missing = if force_path_update || old_abs_bytes.is_none() {
                false
            } else {
                old_abs_bytes
                    .as_deref()
                    .and_then(|b| std::str::from_utf8(b).ok().map(PathBuf::from))
                    .map(|old_path| match std::fs::symlink_metadata(&old_path) {
                        Ok(_) => false,
                        Err(err) if err.kind() == std::io::ErrorKind::NotFound => true,
                        Err(_) => false,
                    })
                    .unwrap_or(false)
            };

            // 路径不同：hardlink、rename，或旧路径已消失后的 reconcile
            if !force_path_update && !old_path_missing {
                // hardlink/重复发现：保留旧路径，仅更新元数据。
                self.update_meta_in_delta(
                    docid,
                    fkey,
                    old_abs_bytes.as_deref().unwrap_or(&new_abs_bytes),
                    meta.size,
                    mtime_to_ns(meta.mtime),
                );
                self.dirty.store(true, std::sync::atomic::Ordering::Release);
                return;
            }

            // rename：先移除旧路径关联
            let _guard = self.upsert_lock.write();
            if let Some(ref old_abs) = old_abs_bytes {
                let old_path = pathbuf_from_encoded_vec(old_abs.clone());
                self.remove_trigrams(docid, &old_path);
                self.remove_path_hash(docid, &old_path);
                self.remove_parent(docid, &old_path);
            }

            // posting/path_hash 先写（与 query 锁顺序一致）。
            self.insert_trigrams(docid, meta.path.as_path());
            self.insert_path_hash(docid, meta.path.as_path());
            self.insert_parent(docid, meta.path.as_path());

            // M4-C: rename 写入 BaseDelta（不再写 metas+arena）。
            let renamed = self.maybe_rename_in_delta(
                docid,
                fkey,
                new_root_id,
                &new_rel_bytes,
                meta.size,
                mtime_to_ns(meta.mtime),
            );
            if !renamed {
                // 新 path 太长 / arena 溢出：把旧 docid tombstone 掉，避免 query 看到
                // 半 rename 的"旧 path 在 trigram、entry 还指向旧"的不一致状态。
                self.tombstones.write().insert(docid);
                self.dirty.store(true, std::sync::atomic::Ordering::Release);
                return;
            }

            // rename 视为"存在且活跃"
            self.tombstones.write().remove(docid);
            self.dirty.store(true, std::sync::atomic::Ordering::Release);
            return;
        }

        // 新文件：分配 docid 并写入
        let _guard = self.upsert_lock.write();
        let Some(docid) = self.alloc_docid(
            fkey,
            new_root_id,
            &new_rel_bytes,
            meta.size,
            mtime_to_ns(meta.mtime),
        ) else {
            return;
        };
        self.insert_trigrams(docid, meta.path.as_path());
        self.insert_path_hash(docid, meta.path.as_path());
        self.insert_parent(docid, meta.path.as_path());
        self.dirty.store(true, std::sync::atomic::Ordering::Release);
    }

    /// M4-C：分配新 docid，仅写 BaseDelta 一处（不再走 metas+arena）。
    ///
    /// docid = `base_count + delta.added.len()` （写之前的快照），
    /// 然后 [`DeltaBuffer::append_added`] 强制 push（**不**走 upsert_added，避免同
    /// file_key 原地替换破坏 docid 对齐——recreate-after-delete 也得拿一个新 docid）。
    fn alloc_docid(
        &self,
        file_key: FileKey,
        root_id: u16,
        rel_bytes: &[u8],
        size: u64,
        mtime_ns: i64,
    ) -> Option<DocId> {
        let abs_bytes =
            compose_abs_path_bytes(root_bytes_for_id(&self.roots_bytes, root_id), rel_bytes);
        let docid = {
            let mut bd = self.base_delta.write();
            let base_count = bd.base.by_path_idx().len() as u64;
            let docid = base_count + bd.delta.added().len() as u64;
            if !bd.delta.append_added(file_key, &abs_bytes, size, mtime_ns) {
                return None;
            }
            docid
        };

        self.filekey_to_docid.write().insert(file_key, docid);
        self.tombstones.write().remove(docid);

        // 注：parent_index 在调用方（upsert_inner / 提供完整 abs_path 的位置）维护，
        // 这里不重复插入，避免重新组装 absolute path。
        Some(docid)
    }

    /// M4-C：更新某 docid 对应 entry 的 size+mtime（path 保持原样）。
    /// 用于 same_path 重复上报、hardlink 等元数据-only 更新场景。
    /// - docid < base_count：base 段 immutable，**当前不更新**——FileEntry.size/mtime
    ///   会有轻微 staleness 直到下次 snapshot/merge。常见场景下 base 段只在启动时
    ///   从 v7 加载、运行时极少接收元数据更新，影响可忽略。
    /// - docid >= base_count：delta 段，直接 update_added_at 原地刷新。
    fn update_meta_in_delta(
        &self,
        legacy_docid: DocId,
        file_key: FileKey,
        path_bytes: &[u8],
        size: u64,
        mtime_ns: i64,
    ) {
        let mut bd = self.base_delta.write();
        let base_count = bd.base.by_path_idx().len() as u64;
        if legacy_docid < base_count {
            // base 段元数据 stale，留待下次 snapshot 重建。
            return;
        }
        let idx = (legacy_docid - base_count) as usize;
        bd.delta
            .update_added_at(idx, file_key, path_bytes, size, mtime_ns);
    }

    /// M3 双写：根据 legacy docid 在 BaseDelta 空间标记删除。
    /// - docid < base_count：base 段，调 `delta.mark_removed`
    /// - docid >= base_count：delta 段——**不**物理 drop，仅依赖 legacy `tombstones`
    ///   位图过滤。
    ///
    /// 不能 swap_remove 的原因：Legacy docid 与 `delta.added` 的下标在 delta 段是
    /// `docid - base_count = idx` 这条对齐关系。任何 swap_remove 都会让后续条目的
    /// 下标移位，从而打破 [`Self::resolve_entry_via_base_delta`] 等以 docid → idx
    /// 直接定位的读路径。
    ///
    /// 副作用：`delta.added` 会保留已删除条目（被 tombstones 过滤但物理仍在）。
    /// 这部分"幽灵"由后续 snapshot/merge 时再次过滤——见
    /// [`Self::to_snapshot_v7`]，它走 metas+tombstones 已自动跳过；merge_with_delta
    /// 路径若启用，需要在那里读 legacy tombstones 做对齐过滤。
    fn maybe_mark_base_delta_removed(&self, legacy_docid: DocId, _file_key: FileKey) {
        let mut bd = self.base_delta.write();
        let base_count = bd.base.by_path_idx().len() as u64;
        if legacy_docid < base_count {
            bd.delta.mark_removed(legacy_docid);
        }
        // delta 段：保留物理 entry，由 legacy tombstones 在查询层过滤。
    }

    /// M3/M4-C 双写：同步 rename。新 path 落入 delta.added。
    /// 返回 true 表示成功；false 表示路径过长/arena 满，调用方应把 docid 设为 tombstone。
    /// - docid < base_count：mark_removed(docid) + delta.append_added(new path) (push 新位置)
    /// - docid >= base_count：delta.update_added_at(idx, ...) —— 原地刷新当前 docid 对应 entry
    fn maybe_rename_in_delta(
        &self,
        legacy_docid: DocId,
        file_key: FileKey,
        new_root_id: u16,
        new_rel_bytes: &[u8],
        size: u64,
        mtime_ns: i64,
    ) -> bool {
        let abs_bytes = compose_abs_path_bytes(
            root_bytes_for_id(&self.roots_bytes, new_root_id),
            new_rel_bytes,
        );
        let mut bd = self.base_delta.write();
        let base_count = bd.base.by_path_idx().len() as u64;
        if legacy_docid < base_count {
            // base 段：当前 docid 在 base，无法原地修改。mark_removed 屏蔽旧条目，
            // append 新条目到 delta（注意 docid 不再对齐——见上方 update_meta_in_delta 注释）。
            bd.delta.mark_removed(legacy_docid);
            bd.delta.append_added(file_key, &abs_bytes, size, mtime_ns)
        } else {
            let idx = (legacy_docid - base_count) as usize;
            bd.delta
                .update_added_at(idx, file_key, &abs_bytes, size, mtime_ns)
        }
    }

    /// 标记删除（tombstone）
    pub fn mark_deleted(&self, file_key: FileKey) {
        let Some(docid) = self.lookup_docid_by_filekey(file_key) else {
            return;
        };

        // M2: 走统一 helper。BaseDelta 启用时从 base+delta 解析；否则 metas+arena。
        let path = self.resolve_abs_path_for_docid(docid);

        // Atomicity: mark tombstone first so queries see deleted before trigrams are removed.
        self.filekey_to_docid.write().remove(&file_key);
        self.tombstones.write().insert(docid);
        self.dirty.store(true, std::sync::atomic::Ordering::Release);

        // M3: 同步 BaseDelta（如启用）—— legacy docid 在 BaseDelta 空间里要么属于 base，
        // 要么属于 delta.added，分别对应 mark_removed / drop_added_by_file_key。
        self.maybe_mark_base_delta_removed(docid, file_key);

        if let Some(p) = path {
            self.remove_trigrams(docid, &p);
            self.remove_path_hash(docid, &p);
            self.remove_parent(docid, &p);
        }
    }

    /// 按路径删除（M4-B：file_key 从 BaseDelta 拿）
    pub fn mark_deleted_by_path(&self, path: &Path) {
        if let Some(docid) = self.lookup_docid_by_path(path) {
            if let Some((fk, _, _, _)) = self.resolve_entry_via_base_delta(docid) {
                self.mark_deleted(fk);
            }
        }
    }

    /// 查询：trigram 候选集（Roaring 交集）→ 精确过滤
    ///
    /// M4-B：数据源切到 BaseDelta（base + delta.added），不再读 metas+arena。
    pub fn query(&self, matcher: &dyn Matcher, limit: usize) -> Vec<FileMeta> {
        // 重要：先读取 trigram_index 计算候选集，再读取 base_delta/tombstones。
        // 写入路径通常是先更新 trigram_index 再更新 base_delta，如果这里反过来拿锁，
        // 在"边写边查"场景下可能形成死锁。
        let candidates = self
            .trigram_candidates(matcher)
            .or_else(|| self.short_hint_candidates(matcher));

        let bd = self.base_delta.read();
        let tombstones = self.tombstones.read();
        let base_count = bd.base.by_path_idx().len() as u64;
        let total = base_count + bd.delta.added().len() as u64;

        let try_resolve = |docid: u64| -> Option<(FileKey, Vec<u8>, u64, i64)> {
            if docid < base_count {
                let entry = bd.base.by_path_idx().entries().get(docid as usize)?;
                let abs = bd.base.resolve_path(docid)?;
                Some((entry.file_key(), abs, entry.size, entry.mtime_ns))
            } else {
                let idx = (docid - base_count) as usize;
                let entry = bd.delta.added().get(idx)?;
                let abs = bd.delta.added_path_bytes(idx)?.to_vec();
                Some((entry.file_key(), abs, entry.size, entry.mtime_ns))
            }
        };

        let into_meta = |fk: FileKey, abs: Vec<u8>, size: u64, mtime_ns: i64| FileMeta {
            file_key: fk,
            path: pathbuf_from_encoded_vec(abs),
            size,
            mtime: mtime_from_ns(mtime_ns),
            ctime: None,
            atime: None,
        };

        match candidates {
            Some(bitmap) => bitmap
                .iter()
                .filter(|docid| !tombstones.contains(*docid))
                .filter_map(&try_resolve)
                .filter(|(_, abs, _, _)| {
                    let s = std::str::from_utf8(abs)
                        .map(std::borrow::Cow::Borrowed)
                        .unwrap_or_else(|_| String::from_utf8_lossy(abs));
                    matcher.matches(&s)
                })
                .map(|(fk, abs, size, mtime_ns)| into_meta(fk, abs, size, mtime_ns))
                .collect(),
            None => {
                // 无法用 trigram 加速（查询词太短），全量过滤
                (0..total)
                    .filter(|docid| !tombstones.contains(*docid))
                    .filter_map(try_resolve)
                    .filter_map(|(fk, abs, size, mtime_ns)| {
                        let s = std::str::from_utf8(&abs)
                            .map(std::borrow::Cow::Borrowed)
                            .unwrap_or_else(|_| String::from_utf8_lossy(&abs));
                        if matcher.matches(&s) {
                            Some(into_meta(fk, abs, size, mtime_ns))
                        } else {
                            None
                        }
                    })
                    .take(limit)
                    .collect()
            }
        }
    }

    /// 遍历所有"活跃"文档（跳过 tombstone），用于 Flush/Compaction/重建等离线流程。
    /// M4-B：数据源切到 BaseDelta。
    /// Phase 4：base 半段走 path_table.for_each_path 顺序扫描（query "" 与 snapshot
    /// 合并都会调到这里，每次省 ~30× CPU on 8M 文件）。
    pub fn for_each_live_meta(&self, mut f: impl FnMut(FileMeta)) {
        let bd = self.base_delta.read();
        let tombstones = self.tombstones.read();
        let base_entries = bd.base.by_path_idx().entries();
        let base_count = base_entries.len() as u64;

        let mut emit = |fk: FileKey, abs_bytes: &[u8], size: u64, mtime_ns: i64| {
            f(FileMeta {
                file_key: fk,
                path: pathbuf_from_encoded_vec(abs_bytes.to_vec()),
                size,
                mtime: mtime_from_ns(mtime_ns),
                ctime: None,
                atime: None,
            });
        };

        bd.base.path_table().for_each_path(|path_idx, abs_bytes| {
            let docid = path_idx as u64;
            if tombstones.contains(docid) {
                return;
            }
            let Some(entry) = base_entries.get(path_idx as usize) else {
                return;
            };
            emit(entry.file_key(), abs_bytes, entry.size, entry.mtime_ns);
        });

        for idx in 0..bd.delta.added().len() {
            let docid = base_count + idx as u64;
            if tombstones.contains(docid) {
                continue;
            }
            let Some(entry) = bd.delta.added().get(idx) else {
                continue;
            };
            let Some(abs_bytes) = bd.delta.added_path_bytes(idx) else {
                continue;
            };
            emit(entry.file_key(), abs_bytes, entry.size, entry.mtime_ns);
        }
    }

    /// Iterate live metas whose path's parent is in `dirs`.
    ///
    /// 旧实现是 O(N_total_metas) 全量扫描，每条还要重组 abs_path 与执行 `dirs.contains`，
    /// 是 `重构方案包/causal-chain-report.md` 第三节排名第一（★★★★★）的 CPU/内存杀手。
    /// 现在改用 [`ParentIndex`] 倒排索引：先按 dir 哈希拿到候选 DocId 位图，再做
    /// tombstone 差集 + 防御性 parent 字符串校验（避免 hash collision 导致误回调）。
    pub fn for_each_live_meta_in_dirs<F>(
        &self,
        dirs: &std::collections::HashSet<PathBuf>,
        mut callback: F,
    ) where
        F: FnMut(FileMeta),
    {
        if dirs.is_empty() {
            return;
        }

        // 1) 按目录哈希取候选 DocIds 并集，**不**持有 parent_index 锁做后续工作。
        let parent_hashes: Vec<u64> = dirs
            .iter()
            .map(|d| path_hash_bytes(d.as_os_str().as_encoded_bytes()))
            .collect();
        let candidates = {
            let pi = self.parent_index.read();
            pi.files_in_dirs(parent_hashes)
        };
        if candidates.is_empty() {
            return;
        }

        // 2) 减去墓碑后再展开。tombstones 这里是只读快照。
        let live = {
            let tombstones = self.tombstones.read();
            &candidates - &*tombstones
        };
        if live.is_empty() {
            return;
        }

        // 3) 重组绝对路径并发回。注意防御性：哈希碰撞时父目录字符串可能不匹配，
        //   退化为旧版 `dirs.contains(parent)` 校验。
        // M4-B: 走 BaseDelta 一次性拿 (file_key, abs, size, mtime)，每条 docid
        // 只加一次 base_delta 锁。
        for docid in live.iter() {
            let Some((file_key, abs, size, mtime_ns)) = self.resolve_entry_via_base_delta(docid)
            else {
                continue;
            };
            let path = pathbuf_from_encoded_vec(abs);
            let parent_matches = path.parent().map(|p| dirs.contains(p)).unwrap_or(false);
            if !parent_matches {
                continue;
            }
            callback(FileMeta {
                file_key,
                path,
                size,
                mtime: mtime_from_ns(mtime_ns),
                ctime: None,
                atime: None,
            });
        }
    }

    /// 批量应用事件
    pub fn apply_events(&self, events: &[EventRecord]) {
        for ev in events {
            self.apply_event_ref(ev);
        }
    }

    /// 批量应用事件（drain 版本）：
    /// - 消费 `Vec<EventRecord>`，避免 `Create/Modify/Rename` 在这里再次 `to_path_buf()` 造成的额外分配。
    /// - 用于 EventPipeline / fast-sync 这类“事件量很大、且不需要保留 EventRecord”的路径。
    pub fn apply_events_drain(&self, events: &mut Vec<EventRecord>) {
        for ev in events.drain(..) {
            self.apply_event_owned(ev);
        }
    }

    pub fn apply_file_metas(&self, metas: &[FileMeta]) {
        for meta in metas.iter().cloned() {
            self.upsert(meta);
        }
    }

    pub fn apply_file_metas_drain(&self, metas: &mut Vec<FileMeta>) {
        for meta in metas.drain(..) {
            self.upsert(meta);
        }
    }

    fn resolve_path_meta(path: &Path) -> Option<ResolvedFsMeta> {
        let meta = std::fs::metadata(path).ok()?;
        Some(ResolvedFsMeta {
            file_key: FileKey::from_path_and_metadata(path, &meta)?,
            size: meta.len(),
            mtime: meta.modified().ok(),
        })
    }

    fn existing_path_for_file_key(&self, fk: FileKey) -> Option<PathBuf> {
        let docid = self.lookup_docid_by_filekey(fk)?;
        self.resolve_abs_path_for_docid(docid)
    }

    fn apply_event_ref(&self, ev: &EventRecord) {
        match &ev.event_type {
            EventType::Create | EventType::Modify => {
                self.handle_create_or_modify(
                    ev.best_path().map(Cow::Borrowed),
                    ev.id.as_file_key(),
                );
            }
            EventType::Delete => {
                self.handle_delete(ev.best_path(), ev.id.as_file_key());
            }
            EventType::Rename {
                from,
                from_path_hint,
            } => {
                self.handle_rename(
                    from_path_hint.as_deref().or_else(|| from.as_path()),
                    from.as_file_key(),
                    ev.best_path().map(Cow::Borrowed),
                );
            }
        }
    }

    fn apply_event_owned(&self, ev: EventRecord) {
        let EventRecord {
            event_type,
            id,
            path_hint,
            ..
        } = ev;

        match event_type {
            EventType::Create | EventType::Modify => match (path_hint, id) {
                (Some(path), _) => self.handle_create_or_modify(Some(Cow::Owned(path)), None),
                (None, FileIdentifier::Path(path)) => {
                    self.handle_create_or_modify(Some(Cow::Owned(path)), None)
                }
                (None, FileIdentifier::Fid { dev, ino }) => self.handle_create_or_modify(
                    None,
                    Some(FileKey {
                        dev,
                        ino,
                        generation: 0,
                    }),
                ),
            },
            EventType::Delete => match (path_hint, id) {
                (_, FileIdentifier::Fid { dev, ino }) => {
                    self.handle_delete(
                        None,
                        Some(FileKey {
                            dev,
                            ino,
                            generation: 0,
                        }),
                    );
                }
                (Some(path), _) => {
                    self.handle_delete(Some(path.as_path()), None);
                }
                (None, FileIdentifier::Path(path)) => {
                    self.handle_delete(Some(path.as_path()), None);
                }
            },
            EventType::Rename {
                from,
                from_path_hint,
            } => {
                let to_path = match (path_hint, id) {
                    (Some(path), _) => Some(Cow::Owned(path)),
                    (None, FileIdentifier::Path(path)) => Some(Cow::Owned(path)),
                    (None, FileIdentifier::Fid { .. }) => None,
                };

                self.handle_rename(
                    from_path_hint.as_deref().or_else(|| from.as_path()),
                    from.as_file_key(),
                    to_path,
                );
            }
        }
    }

    fn handle_create_or_modify(&self, path: Option<Cow<'_, Path>>, fid: Option<FileKey>) {
        if let Some(path) = path {
            let Some(meta) = Self::resolve_path_meta(path.as_ref()) else {
                return;
            };
            self.upsert(FileMeta {
                file_key: meta.file_key,
                path: path.into_owned(),
                size: meta.size,
                mtime: meta.mtime,
                ctime: None,
                atime: None,
            });
        }

        let Some(fk) = fid else {
            return;
        };
        let Some(path) = self.existing_path_for_file_key(fk) else {
            return;
        };
        let Some(meta) = Self::resolve_path_meta(&path) else {
            return;
        };
        if meta.file_key == fk {
            self.upsert(FileMeta {
                file_key: fk,
                path,
                size: meta.size,
                mtime: meta.mtime,
                ctime: None,
                atime: None,
            });
        }
    }

    fn handle_delete(&self, path: Option<&Path>, fid: Option<FileKey>) {
        if let Some(fk) = fid {
            self.mark_deleted(fk);
        } else if let Some(path) = path {
            self.mark_deleted_by_path(path);
        }
    }

    fn handle_rename(
        &self,
        from_best_path: Option<&Path>,
        from_fid: Option<FileKey>,
        to_path: Option<Cow<'_, Path>>,
    ) {
        let to_path = to_path.map(|p| Cow::Owned(crate::index::tiered::normalize_path(p.as_ref())));
        let from_best_path = from_best_path.map(crate::index::tiered::normalize_path);
        let to_meta = to_path.as_deref().and_then(Self::resolve_path_meta);
        let fallback_meta = if to_meta.is_none() {
            from_best_path.as_deref().and_then(Self::resolve_path_meta)
        } else {
            None
        };

        let docid_opt = if let Some(fk) = from_fid {
            self.lookup_docid_by_filekey(fk)
        } else {
            from_best_path
                .as_deref()
                .and_then(|p| self.lookup_docid_by_path(p))
        };

        if let Some(docid) = docid_opt {
            // M4-C: 走 BaseDelta 拿当前 entry。若 docid 在 BaseDelta 也不存在，跳过。
            let Some((file_key, _, cur_size, cur_mtime_ns)) =
                self.resolve_entry_via_base_delta(docid)
            else {
                return;
            };

            if let Some(old_path) = self.resolve_abs_path_for_docid(docid) {
                self.remove_trigrams(docid, &old_path);
                self.remove_path_hash(docid, &old_path);
            } else if let Some(ref path) = from_best_path {
                self.remove_trigrams(docid, path);
                self.remove_path_hash(docid, path);
            }

            if let Some(to_path) = to_path {
                let to_path = to_path.into_owned();
                let (root_id, rel_bytes) = self.split_root_relative_bytes(to_path.as_path());
                let (size, mtime_ns) = if let Some(meta) = to_meta {
                    (meta.size, mtime_to_ns(meta.mtime))
                } else {
                    (cur_size, cur_mtime_ns)
                };
                self.insert_trigrams(docid, to_path.as_path());
                self.insert_path_hash(docid, to_path.as_path());
                // M4-C: rename 写入 BaseDelta（不再写 metas+arena）。
                self.maybe_rename_in_delta(docid, file_key, root_id, &rel_bytes, size, mtime_ns);
            } else if let Some(meta) = fallback_meta {
                // 仅元数据更新（rename 没拿到 to_path，但 from_path 还在）。
                let abs_bytes = self
                    .resolve_abs_path_bytes_for_docid(docid)
                    .unwrap_or_default();
                self.update_meta_in_delta(
                    docid,
                    file_key,
                    &abs_bytes,
                    meta.size,
                    mtime_to_ns(meta.mtime),
                );
            }
            self.tombstones.write().remove(docid);
            self.dirty.store(true, std::sync::atomic::Ordering::Release);
            return;
        }

        self.handle_delete(from_best_path.as_deref(), from_fid);
        if let (Some(to_path), Some(meta)) = (to_path, to_meta) {
            self.upsert(FileMeta {
                file_key: meta.file_key,
                path: to_path.into_owned(),
                size: meta.size,
                mtime: meta.mtime,
                ctime: None,
                atime: None,
            });
        }
    }

    /// 导出 v6 段（物理 compaction 版）：仅包含 live metas（不携带 tombstones）。
    ///
    /// 用途：段合并/replace-base 时做“真·Tombstone GC”，让段文件尺寸随真实文件系统状态收敛。
    pub fn export_segments_v6_compacted(&self) -> V6Segments {
        let compact = PersistentIndex::new_with_roots(self.roots.clone());
        self.for_each_live_meta(|m| compact.upsert_rename(m));
        compact.export_segments_v6()
    }

    pub fn export_segments_v6(&self) -> V6Segments {
        // roots 段：u16 count + (u16 len + bytes)...
        let mut roots_bytes = Vec::new();
        let roots_count: u16 = self.roots_bytes.len().try_into().unwrap_or(u16::MAX);
        roots_bytes.extend_from_slice(&roots_count.to_le_bytes());
        for rb in self.roots_bytes.iter().take(roots_count as usize) {
            let len: u16 = rb.len().try_into().unwrap_or(u16::MAX);
            roots_bytes.extend_from_slice(&len.to_le_bytes());
            roots_bytes.extend_from_slice(&rb[..len as usize]);
        }

        // M4-C: PathArena + Metas 段：从 BaseDelta 现场 synthesize v6 二进制布局。
        //
        // MetaRecordV6 (40B):
        //   dev u64, ino u64, root_id u16, path_off u32, path_len u16, size u64, mtime_unix_ns i64
        //
        // 注意：v6 metas 段必须按 DocId 顺序排列，与 PathArena 偏移一一对应。
        // Phase 4：base 半段走 path_table.for_each_path（O(data_len) 单次扫描），
        // 比逐条 resolve_path 省 ~30× CPU（8M 文件场景）；delta 半段保持索引访问。
        let (metas_bytes, path_arena_bytes_vec) = {
            let bd = self.base_delta.read();
            let base_entries = bd.base.by_path_idx().entries();
            let base_count = base_entries.len() as u64;
            let total = base_count + bd.delta.added().len() as u64;

            let mut metas_bytes = Vec::with_capacity(total as usize * 40);
            let mut tmp_arena = V6ExportArena::new();
            let mut emit = |file_key: FileKey, abs_bytes: &[u8], size: u64, mtime_ns: i64| {
                let abs_path = pathbuf_from_encoded_vec(abs_bytes.to_vec());
                let (root_id, rel_bytes) = self.split_root_relative_bytes(&abs_path);
                let Some((path_off, path_len)) = tmp_arena.push_bytes(&rel_bytes) else {
                    return;
                };
                metas_bytes.extend_from_slice(&file_key.dev.to_le_bytes());
                metas_bytes.extend_from_slice(&file_key.ino.to_le_bytes());
                metas_bytes.extend_from_slice(&root_id.to_le_bytes());
                metas_bytes.extend_from_slice(&path_off.to_le_bytes());
                metas_bytes.extend_from_slice(&path_len.to_le_bytes());
                metas_bytes.extend_from_slice(&size.to_le_bytes());
                metas_bytes.extend_from_slice(&mtime_ns.to_le_bytes());
            };
            // Base 半段：path_idx == docid，path_table 顺序扫描 + 同步索引到 entries。
            bd.base.path_table().for_each_path(|path_idx, abs_bytes| {
                let Some(entry) = base_entries.get(path_idx as usize) else {
                    return;
                };
                emit(entry.file_key(), abs_bytes, entry.size, entry.mtime_ns);
            });
            // Delta 半段：原索引访问保持不变。
            for idx in 0..bd.delta.added().len() {
                let Some(entry) = bd.delta.added().get(idx) else {
                    continue;
                };
                let Some(abs_bytes) = bd.delta.added_path_bytes(idx) else {
                    continue;
                };
                emit(entry.file_key(), abs_bytes, entry.size, entry.mtime_ns);
            }
            let _ = total;
            (metas_bytes, Arc::new(tmp_arena.into_bytes()))
        };
        let path_arena_bytes = path_arena_bytes_vec;

        // Tombstones 段：RoaringBitmap serialized bytes（v6 兼容格式；v8 后再切 Treemap）
        let tombstones = self.tombstones.read();
        let mut tombstones_bytes = Vec::new();
        let tomb_bitmap: roaring::RoaringBitmap = tombstones.iter().map(|v| v as u32).collect();
        tomb_bitmap
            .serialize_into(&mut tombstones_bytes)
            .expect("write to vec");

        // TrigramTable + PostingsBlob 段
        //
        // TrigramEntryV6 (12B):
        //   trigram [u8;3]
        //   pad u8
        //   posting_off u32
        //   posting_len u32
        let tri_idx = self.trigram_index.read();
        let mut entries: Vec<([u8; 3], u32, u32)> = Vec::with_capacity(tri_idx.len());
        let mut postings_blob_bytes = Vec::new();
        for (tri, posting) in tri_idx.iter() {
            let off: u32 = postings_blob_bytes.len().try_into().unwrap_or(u32::MAX);
            let posting_bitmap: roaring::RoaringBitmap = posting.iter().map(|v| v as u32).collect();
            posting_bitmap
                .serialize_into(&mut postings_blob_bytes)
                .expect("write to vec");
            let len: u32 = postings_blob_bytes
                .len()
                .saturating_sub(off as usize)
                .try_into()
                .unwrap_or(u32::MAX);
            entries.push((*tri, off, len));
        }

        // 能力哨兵：用于 mmap layer 区分“新段（全组件 trigram）”与“旧段（仅 basename trigram）”。
        // - path 组件不允许包含 NUL，因此 [0,0,0] 不会与真实 trigram 冲突。
        // - posting 置空即可（只用 key 存在性探测）。
        const TRIGRAM_SENTINEL: [u8; 3] = [0, 0, 0];
        if !tri_idx.contains_key(&TRIGRAM_SENTINEL) {
            let off: u32 = postings_blob_bytes.len().try_into().unwrap_or(u32::MAX);
            roaring::RoaringBitmap::new()
                .serialize_into(&mut postings_blob_bytes)
                .expect("write to vec");
            let len: u32 = postings_blob_bytes
                .len()
                .saturating_sub(off as usize)
                .try_into()
                .unwrap_or(u32::MAX);
            entries.push((TRIGRAM_SENTINEL, off, len));
        }
        entries.sort_by_key(|(tri, _, _)| *tri);

        let mut trigram_table_bytes = Vec::with_capacity(entries.len() * 12);
        for (tri, off, len) in entries {
            trigram_table_bytes.extend_from_slice(&tri);
            trigram_table_bytes.push(0); // pad
            trigram_table_bytes.extend_from_slice(&off.to_le_bytes());
            trigram_table_bytes.extend_from_slice(&len.to_le_bytes());
        }

        // FileKeyMap 段：按 (dev,ino) 排序的 FileKey -> DocId 映射，用于 mmap layer 的反查。
        //
        // 统一 header（8B，LE）：
        //   magic [u8;4] = b"FKM\0"
        //   version u16  = 1
        //   flags u16    = 0 legacy table | 1 rkyv bytes
        //
        // legacy payload：固定记录 24B（LE）：
        //   dev u64
        //   ino u64
        //   generation u32
        //   docid u32
        //
        // Phase 4：base 段 file_key 现在从 BaseIndex.by_file_key 枚举，HashMap 仅承载 delta。
        let mut pairs = self.collect_filekey_docid_pairs();
        pairs.sort_unstable_by_key(|(k, _)| (k.dev, k.ino, k.generation));
        const FKM_MAGIC: [u8; 4] = *b"FKM\0";
        const FKM_VERSION: u16 = 1;
        #[cfg(not(feature = "rkyv"))]
        const FKM_FLAG_LEGACY: u16 = 0;
        #[cfg(feature = "rkyv")]
        const FKM_FLAG_RKYV: u16 = 1;

        let mut filekey_map_bytes = Vec::new();
        filekey_map_bytes.extend_from_slice(&FKM_MAGIC);
        filekey_map_bytes.extend_from_slice(&FKM_VERSION.to_le_bytes());

        #[cfg(feature = "rkyv")]
        {
            // rkyv：写入 Vec<FileKeyEntry> 的 archived bytes（可扩展）。
            filekey_map_bytes.extend_from_slice(&FKM_FLAG_RKYV.to_le_bytes());
            let entries: Vec<FileKeyEntry> = pairs
                .into_iter()
                .map(|(key, doc_id)| FileKeyEntry { key, doc_id })
                .collect();
            let bytes = rkyv::to_bytes::<_, 1024>(&entries).expect("rkyv to_bytes");
            filekey_map_bytes.extend_from_slice(bytes.as_ref());
        }

        #[cfg(not(feature = "rkyv"))]
        {
            // 默认：仍写 legacy 定长表（极致性能），但带 header 便于未来平滑切换。
            filekey_map_bytes.extend_from_slice(&FKM_FLAG_LEGACY.to_le_bytes());
            filekey_map_bytes.reserve(pairs.len() * 24);
            for (k, docid) in pairs {
                filekey_map_bytes.extend_from_slice(&k.dev.to_le_bytes());
                filekey_map_bytes.extend_from_slice(&k.ino.to_le_bytes());
                filekey_map_bytes.extend_from_slice(&k.generation.to_le_bytes());
                filekey_map_bytes.extend_from_slice(&(docid as u32).to_le_bytes());
            }
        }

        V6Segments {
            roots_bytes: Arc::new(roots_bytes),
            path_arena_bytes,
            metas_bytes: Arc::new(metas_bytes),
            trigram_table_bytes: Arc::new(trigram_table_bytes),
            postings_blob_bytes: Arc::new(postings_blob_bytes),
            tombstones_bytes: Arc::new(tombstones_bytes),
            filekey_map_bytes: Arc::new(filekey_map_bytes),
        }
    }

    fn write_segment(writer: &mut impl std::io::Write, bytes: &[u8]) -> std::io::Result<()> {
        writer.write_all(&(bytes.len() as u64).to_le_bytes())?;
        writer.write_all(bytes)?;
        Ok(())
    }

    /// 将 v6 段按固定顺序流式写入 writer，每段带 u64 LE 长度前缀。
    /// 顺序：roots → path_arena → metas → tombstones → trigram_table → postings_blob → filekey_map。
    pub fn export_segments_v6_to_writer(
        &self,
        writer: &mut impl std::io::Write,
    ) -> std::io::Result<()> {
        // roots 段：u16 count + (u16 len + bytes)...
        let mut roots_bytes = Vec::new();
        let roots_count: u16 = self.roots_bytes.len().try_into().unwrap_or(u16::MAX);
        roots_bytes.extend_from_slice(&roots_count.to_le_bytes());
        for rb in self.roots_bytes.iter().take(roots_count as usize) {
            let len: u16 = rb.len().try_into().unwrap_or(u16::MAX);
            roots_bytes.extend_from_slice(&len.to_le_bytes());
            roots_bytes.extend_from_slice(&rb[..len as usize]);
        }
        Self::write_segment(writer, &roots_bytes)?;

        // M4-C: PathArena + Metas 段从 BaseDelta synthesize（与 export_segments_v6 同逻辑）。
        // Phase 4：base 走 path_table.for_each_path 顺序扫描，省 ~30× CPU（8M 文件场景）。
        let (metas_bytes, path_arena_bytes) = {
            let bd = self.base_delta.read();
            let base_entries = bd.base.by_path_idx().entries();
            let base_count = base_entries.len() as u64;
            let total = base_count + bd.delta.added().len() as u64;

            let mut metas_bytes = Vec::with_capacity(total as usize * 40);
            let mut tmp_arena = V6ExportArena::new();
            let mut emit = |file_key: FileKey, abs_bytes: &[u8], size: u64, mtime_ns: i64| {
                let abs_path = pathbuf_from_encoded_vec(abs_bytes.to_vec());
                let (root_id, rel_bytes) = self.split_root_relative_bytes(&abs_path);
                let Some((path_off, path_len)) = tmp_arena.push_bytes(&rel_bytes) else {
                    return;
                };
                metas_bytes.extend_from_slice(&file_key.dev.to_le_bytes());
                metas_bytes.extend_from_slice(&file_key.ino.to_le_bytes());
                metas_bytes.extend_from_slice(&root_id.to_le_bytes());
                metas_bytes.extend_from_slice(&path_off.to_le_bytes());
                metas_bytes.extend_from_slice(&path_len.to_le_bytes());
                metas_bytes.extend_from_slice(&size.to_le_bytes());
                metas_bytes.extend_from_slice(&mtime_ns.to_le_bytes());
            };
            bd.base.path_table().for_each_path(|path_idx, abs_bytes| {
                let Some(entry) = base_entries.get(path_idx as usize) else {
                    return;
                };
                emit(entry.file_key(), abs_bytes, entry.size, entry.mtime_ns);
            });
            for idx in 0..bd.delta.added().len() {
                let Some(entry) = bd.delta.added().get(idx) else {
                    continue;
                };
                let Some(abs_bytes) = bd.delta.added_path_bytes(idx) else {
                    continue;
                };
                emit(entry.file_key(), abs_bytes, entry.size, entry.mtime_ns);
            }
            let _ = total;
            (metas_bytes, Arc::new(tmp_arena.into_bytes()))
        };
        Self::write_segment(writer, path_arena_bytes.as_ref())?;
        Self::write_segment(writer, &metas_bytes)?;

        // Tombstones 段：RoaringBitmap serialized bytes
        let tombstones = self.tombstones.read();
        let mut tombstones_bytes = Vec::new();
        let tomb_bitmap: roaring::RoaringBitmap = tombstones.iter().map(|v| v as u32).collect();
        tomb_bitmap
            .serialize_into(&mut tombstones_bytes)
            .expect("write to vec");
        drop(tombstones);
        Self::write_segment(writer, &tombstones_bytes)?;

        // TrigramTable + PostingsBlob 段
        let tri_idx = self.trigram_index.read();
        let mut entries: Vec<([u8; 3], u32, u32)> = Vec::with_capacity(tri_idx.len());
        let mut postings_blob_bytes = Vec::new();
        for (tri, posting) in tri_idx.iter() {
            let off: u32 = postings_blob_bytes.len().try_into().unwrap_or(u32::MAX);
            let posting_bitmap: roaring::RoaringBitmap = posting.iter().map(|v| v as u32).collect();
            posting_bitmap
                .serialize_into(&mut postings_blob_bytes)
                .expect("write to vec");
            let len: u32 = postings_blob_bytes
                .len()
                .saturating_sub(off as usize)
                .try_into()
                .unwrap_or(u32::MAX);
            entries.push((*tri, off, len));
        }

        const TRIGRAM_SENTINEL: [u8; 3] = [0, 0, 0];
        if !tri_idx.contains_key(&TRIGRAM_SENTINEL) {
            let off: u32 = postings_blob_bytes.len().try_into().unwrap_or(u32::MAX);
            roaring::RoaringBitmap::new()
                .serialize_into(&mut postings_blob_bytes)
                .expect("write to vec");
            let len: u32 = postings_blob_bytes
                .len()
                .saturating_sub(off as usize)
                .try_into()
                .unwrap_or(u32::MAX);
            entries.push((TRIGRAM_SENTINEL, off, len));
        }
        drop(tri_idx);
        entries.sort_by_key(|(tri, _, _)| *tri);

        let mut trigram_table_bytes = Vec::with_capacity(entries.len() * 12);
        for (tri, off, len) in entries {
            trigram_table_bytes.extend_from_slice(&tri);
            trigram_table_bytes.push(0); // pad
            trigram_table_bytes.extend_from_slice(&off.to_le_bytes());
            trigram_table_bytes.extend_from_slice(&len.to_le_bytes());
        }
        Self::write_segment(writer, &trigram_table_bytes)?;
        Self::write_segment(writer, &postings_blob_bytes)?;

        // FileKeyMap 段（参见 export_segments_v6 同段说明）。
        let mut pairs = self.collect_filekey_docid_pairs();
        pairs.sort_unstable_by_key(|(k, _)| (k.dev, k.ino, k.generation));
        const FKM_MAGIC: [u8; 4] = *b"FKM\0";
        const FKM_VERSION: u16 = 1;
        #[cfg(not(feature = "rkyv"))]
        const FKM_FLAG_LEGACY: u16 = 0;
        #[cfg(feature = "rkyv")]
        const FKM_FLAG_RKYV: u16 = 1;

        let mut filekey_map_bytes = Vec::new();
        filekey_map_bytes.extend_from_slice(&FKM_MAGIC);
        filekey_map_bytes.extend_from_slice(&FKM_VERSION.to_le_bytes());

        #[cfg(feature = "rkyv")]
        {
            filekey_map_bytes.extend_from_slice(&FKM_FLAG_RKYV.to_le_bytes());
            let entries: Vec<FileKeyEntry> = pairs
                .into_iter()
                .map(|(key, doc_id)| FileKeyEntry { key, doc_id })
                .collect();
            let bytes = rkyv::to_bytes::<_, 1024>(&entries).expect("rkyv to_bytes");
            filekey_map_bytes.extend_from_slice(bytes.as_ref());
        }

        #[cfg(not(feature = "rkyv"))]
        {
            filekey_map_bytes.extend_from_slice(&FKM_FLAG_LEGACY.to_le_bytes());
            filekey_map_bytes.reserve(pairs.len() * 24);
            for (k, docid) in pairs {
                filekey_map_bytes.extend_from_slice(&k.dev.to_le_bytes());
                filekey_map_bytes.extend_from_slice(&k.ino.to_le_bytes());
                filekey_map_bytes.extend_from_slice(&k.generation.to_le_bytes());
                filekey_map_bytes.extend_from_slice(&(docid as u32).to_le_bytes());
            }
        }
        Self::write_segment(writer, &filekey_map_bytes)?;

        Ok(())
    }

    /// 导出 v6 段（物理 compaction 版）并流式写入 writer。
    pub fn export_segments_v6_compacted_to_writer(
        &self,
        writer: &mut impl std::io::Write,
    ) -> std::io::Result<()> {
        let compact = PersistentIndex::new_with_roots(self.roots.clone());
        self.for_each_live_meta(|m| compact.upsert_rename(m));
        compact.export_segments_v6_to_writer(writer)
    }

    pub fn is_dirty(&self) -> bool {
        self.dirty.load(std::sync::atomic::Ordering::Acquire)
    }

    pub fn file_count(&self) -> usize {
        let bd = self.base_delta.read();
        let total = bd.base.by_path_idx().len() + bd.delta.added().len();
        let tomb = self.tombstones.read().len() as usize;
        total.saturating_sub(tomb)
    }

    /// 内存占用统计（粗估）
    pub fn memory_stats(&self) -> L2Stats {
        use std::mem::size_of;

        let bd = self.base_delta.read();
        let filekey_to_docid = self.filekey_to_docid.read();
        let path_hash_to_id = self.path_hash_to_id.read();
        let trigram_index = self.trigram_index.read();
        let short_component_index = self.short_component_index.read();
        let tombstones = self.tombstones.read();

        // M4-C: 内存统计走 BaseDelta（base.estimated_bytes() + delta.estimated_bytes()）
        // 替代 metas+arena 估算。
        let total_docs = bd.base.by_path_idx().len() + bd.delta.added().len();
        let tombstone_count = tombstones.len() as usize;
        let file_count = total_docs.saturating_sub(tombstone_count);

        let path_to_id_count: usize = path_hash_to_id.values().map(|v| v.len()).sum();
        let trigram_distinct = trigram_index.len();

        let mut trigram_postings_total: usize = 0;
        let mut trigram_heap_bytes: u64 = 0;
        for posting in trigram_index.values() {
            trigram_postings_total += posting.len() as usize;
            // serialized_size 更接近 Roaring 的真实压缩存储量（但仍不等同于实际 heap bytes）
            trigram_heap_bytes += posting.serialized_size() as u64;
        }

        // ── 更贴近真实的估算策略（以 capacity 为主，避免 len 低估） ──
        //
        // 说明：
        // - 这是“近似占用”，不包含 allocator 产生的碎片/空闲块（RSS 高水位常驻的主要来源）。
        // - HashMap 的真实 bucket/ctrl 布局由 hashbrown 决定，这里按“entry + 1B ctrl”做近似。

        // BaseDelta（base+delta）的内存占用——取代旧的 metas+arena 估算。
        // base.estimated_bytes() = path_table + by_path_idx*2 + parent_index + tombstones
        // delta.estimated_bytes() = path_arena + added vec + lens vec + removed bitmap
        let metas_bytes = bd.base.estimated_bytes() + bd.delta.estimated_bytes();

        // mapping: HashMap<FileKey, DocId>
        let map_entry_bytes = size_of::<(FileKey, DocId)>() as u64;
        let filekey_to_docid_bytes = filekey_to_docid.len() as u64 * (map_entry_bytes + 1)
            + size_of::<HashMap<FileKey, DocId>>() as u64;

        // arena 字段已删除——占位为 0 兼容 L2Stats 结构。
        let arena_bytes: u64 = 0;

        // path hash 反查：HashMap<u64, OneOrManyDocId> + Many 的 Vec<DocId> 堆分配
        let path_entry_bytes = size_of::<(u64, OneOrManyDocId)>() as u64;
        let mut path_many_bytes: u64 = 0;
        for v in path_hash_to_id.values() {
            if let OneOrManyDocId::Many(ids) = v {
                path_many_bytes += ids.capacity() as u64 * size_of::<DocId>() as u64
                    + size_of::<Vec<DocId>>() as u64;
            }
        }
        let path_to_id_bytes = path_hash_to_id.len() as u64 * (path_entry_bytes + 1)
            + size_of::<HashMap<u64, OneOrManyDocId>>() as u64
            + path_many_bytes;

        // trigram：HashMap<Trigram, RoaringTreemap> 的 entry + Roaring 的压缩存储量（serialized_size）
        let trigram_entry_bytes = size_of::<(Trigram, RoaringTreemap)>() as u64;
        let trigram_map_bytes = trigram_index.capacity() as u64 * (trigram_entry_bytes + 1)
            + size_of::<HashMap<Trigram, RoaringTreemap>>() as u64;
        let short_component_entry_bytes = size_of::<(u16, RoaringTreemap)>() as u64;
        let mut short_component_heap_bytes: u64 = 0;
        for (component, posting) in short_component_index.iter() {
            short_component_heap_bytes += if component.to_be_bytes()[1] == 0 {
                1
            } else {
                2
            };
            short_component_heap_bytes += posting.serialized_size() as u64;
        }
        let short_component_bytes = short_component_index.capacity() as u64
            * (short_component_entry_bytes + 1)
            + size_of::<HashMap<u16, RoaringTreemap>>() as u64
            + short_component_heap_bytes;
        let trigram_bytes = trigram_map_bytes + trigram_heap_bytes + short_component_bytes;

        // tombstones：RoaringTreemap
        let tomb_bytes = size_of::<RoaringTreemap>() as u64 + tombstones.serialized_size() as u64;
        let roaring_serialized_bytes = trigram_heap_bytes + tombstones.serialized_size() as u64;

        let core_table_bytes = metas_bytes + filekey_to_docid_bytes;
        let estimated_bytes =
            core_table_bytes + arena_bytes + path_to_id_bytes + trigram_bytes + tomb_bytes;

        L2Stats {
            file_count,
            path_to_id_count,
            trigram_distinct,
            trigram_postings_total,
            tombstone_count,
            metas_capacity: total_docs,
            filekey_to_docid_capacity: filekey_to_docid.len(),
            path_hash_to_id_capacity: path_hash_to_id.len(),
            trigram_index_capacity: trigram_index.capacity(),
            arena_capacity: 0,

            core_table_bytes,
            metas_bytes,
            filekey_to_docid_bytes,
            arena_bytes,
            path_to_id_bytes,
            trigram_bytes,
            roaring_serialized_bytes,
            estimated_bytes,
        }
    }

    /// Compaction：清理墓碑（阶段 A：只清 tombstone 位图，不重排 DocId）
    pub fn compact(&self) {
        self.tombstones.write().clear();
        self.dirty.store(true, std::sync::atomic::Ordering::Release);
        tracing::info!("Compaction complete, tombstones cleared");
    }

    pub fn maybe_schedule_repair(&self) {
        // 占位：检测索引健康度，触发后台补扫
    }

    /// 清空索引并标记为 dirty（用于 overflow 后的重建兜底）
    pub fn reset(&self) {
        // 统一按固定顺序清理，避免读写并发下出现锁顺序反转。
        self.trigram_index.write().clear();
        self.short_component_index.write().clear();
        self.path_hash_to_id.write().clear();
        self.filekey_to_docid.write().clear();
        *self.base_delta.write() = BaseDeltaKernel {
            base: Arc::new(BaseIndex::empty()),
            delta: DeltaBuffer::new(),
        };
        self.tombstones.write().clear();
        self.dirty.store(true, std::sync::atomic::Ordering::Release);
    }

    // ── 内部方法 ──

    fn split_root_relative_bytes(&self, abs_path: &Path) -> (u16, Vec<u8>) {
        // 选择“最长匹配”的 root（避免 /home 与 /home/user 的歧义）。
        let mut best: Option<(usize, usize, PathBuf)> = None; // (root_id, root_bytes_len, rel_path)
        for (i, root) in self.roots.iter().enumerate() {
            if let Ok(rel) = abs_path.strip_prefix(root) {
                let root_len = self.roots_bytes.get(i).map(|b| b.len()).unwrap_or(0);
                let take = match &best {
                    Some((_, best_len, _)) => root_len > *best_len,
                    None => true,
                };
                if take {
                    best = Some((i, root_len, rel.to_path_buf()));
                }
            }
        }

        let (root_id_usize, rel_path) = if let Some((i, _, rel)) = best {
            (i, rel)
        } else {
            // 兜底：认为 path 在 "/" 下（绝对路径时去掉 leading "/"，否则保留原样）
            let rel = abs_path
                .strip_prefix(Path::new("/"))
                .unwrap_or(abs_path)
                .to_path_buf();
            (0, rel)
        };

        let rel_bytes = rel_path.as_os_str().as_encoded_bytes().to_vec();
        let root_id: u16 = root_id_usize.try_into().unwrap_or(0);
        (root_id, rel_bytes)
    }

    /// 通过 docid 解析绝对路径字节（参见 `重构方案包/causal-chain-report.md` §8.6 第二阶段）。
    ///
    /// **M4-B**: BaseDelta 是唯一权威数据源；不再 fallback 到 metas+arena。
    /// 所有 from_snapshot_* 加载器与运行时写路径都把 BaseDelta 当作 first-class，
    /// 而 metas+arena 已降级为"待删除字段"。
    pub(super) fn resolve_abs_path_bytes_for_docid(&self, docid: DocId) -> Option<Vec<u8>> {
        self.resolve_abs_path_bytes_for_docid_via_base_delta(docid)
    }

    /// `resolve_abs_path_bytes_for_docid` 的 PathBuf 包装。
    pub(super) fn resolve_abs_path_for_docid(&self, docid: DocId) -> Option<PathBuf> {
        self.resolve_abs_path_bytes_for_docid(docid)
            .map(pathbuf_from_encoded_vec)
    }

    /// 在 BaseDelta 内核（M1 影子）上独立解析 docid → path，用于双内核校验。
    /// 仅 BaseDelta 启用时返回 Some，否则返回 None；docid 越界也返回 None。
    pub(super) fn resolve_abs_path_bytes_for_docid_via_base_delta(
        &self,
        docid: DocId,
    ) -> Option<Vec<u8>> {
        let bd = self.base_delta.read();
        let base_count = bd.base.by_path_idx().len() as u64;
        if docid < base_count {
            bd.base.resolve_path(docid)
        } else {
            // delta 段：docid - base_count 是 added 数组下标。
            let delta_idx = (docid - base_count) as usize;
            bd.delta.added_path_bytes(delta_idx).map(|s| s.to_vec())
        }
    }

    /// 枚举当前所有 (FileKey, DocId)——base 段从 by_file_key 取，delta 段从 HashMap 取。
    /// 用于 v6 segment 写入时合成 FileKeyMap 段。
    /// 不过滤 tombstone（与旧 HashMap-only 实现行为一致）。
    pub(super) fn collect_filekey_docid_pairs(&self) -> Vec<(FileKey, DocId)> {
        let bd = self.base_delta.read();
        let base_entries = bd.base.by_path_idx().entries();
        let map = self.filekey_to_docid.read();
        let mut pairs: Vec<(FileKey, DocId)> = Vec::with_capacity(base_entries.len() + map.len());
        // Base：path_idx 当 docid。
        for entry in base_entries.iter() {
            pairs.push((entry.file_key(), entry.path_idx as DocId));
        }
        // Delta：HashMap 里就是 delta 段 file_key → docid。
        pairs.extend(map.iter().map(|(k, v)| (*k, *v)));
        pairs
    }

    /// 通过 file_key 查 docid——Phase 4 之后 `filekey_to_docid` 仅承载 delta，
    /// base 半段走 [`BaseIndex::by_file_key`] 二分。两边都查不到返回 None。
    ///
    /// 不变量：base 段 docid == path_idx == entries 数组下标（v7/Builder 都按字典序
    /// 分配 idx）。所以 `entry.path_idx as u64` 直接当 docid 用。
    ///
    /// 行为对齐旧版 HashMap 的 last-wins 语义：
    /// - 若 file_key 同时在 delta（HashMap）和 base（by_file_key）中存在
    ///   （rename/hardlink 短暂状态），返回 delta 那个——更"新"。
    /// - 单路径策略下不应出现重复 file_key，但保守按 last-wins。
    pub(super) fn lookup_docid_by_filekey(&self, fk: FileKey) -> Option<DocId> {
        if let Some(d) = self.filekey_to_docid.read().get(&fk).copied() {
            return Some(d);
        }
        let bd = self.base_delta.read();
        bd.base
            .by_file_key()
            .find(fk)
            .map(|(_, e)| e.path_idx as u64)
    }

    /// M4-B：BaseDelta 上的 docid → 完整条目解析（FileKey + abs_path + size + mtime）。
    /// 用于 `for_each_live_meta_in_dirs` 等需要一次性拿完整 FileMeta 的扫描路径，
    /// 避免反复加 base_delta 锁。docid 越界返回 None。
    pub(super) fn resolve_entry_via_base_delta(
        &self,
        docid: DocId,
    ) -> Option<(FileKey, Vec<u8>, u64, i64)> {
        let bd = self.base_delta.read();
        let base_count = bd.base.by_path_idx().len() as u64;
        if docid < base_count {
            let entry = bd.base.by_path_idx().entries().get(docid as usize)?;
            let abs = bd.base.resolve_path(docid)?;
            Some((entry.file_key(), abs, entry.size, entry.mtime_ns))
        } else {
            let delta_idx = (docid - base_count) as usize;
            let entry = bd.delta.added().get(delta_idx)?;
            let abs = bd.delta.added_path_bytes(delta_idx)?.to_vec();
            Some((entry.file_key(), abs, entry.size, entry.mtime_ns))
        }
    }

    fn remove_trigrams(&self, docid: DocId, path: &Path) {
        let mut tri_idx = self.trigram_index.write();
        let mut short_idx = self.short_component_index.write();
        for_each_component_trigram(path, |tri| {
            if let Some(posting) = tri_idx.get_mut(&tri) {
                posting.remove(docid);
                if posting.is_empty() {
                    tri_idx.remove(&tri);
                }
            }
        });
        for_each_short_component(path, |component| {
            if let Some(posting) = short_idx.get_mut(&component) {
                posting.remove(docid);
                if posting.is_empty() {
                    short_idx.remove(&component);
                }
            }
        });
    }

    fn insert_trigrams(&self, docid: DocId, path: &Path) {
        let mut tri_idx = self.trigram_index.write();
        let mut short_idx = self.short_component_index.write();
        for_each_component_trigram(path, |tri| {
            tri_idx.entry(tri).or_default().insert(docid);
        });
        for_each_short_component(path, |component| {
            short_idx.entry(component).or_default().insert(docid);
        });
    }

    /// path_hash_to_id 不变量（Phase 4）：仅 delta 范围 docid 进入。
    /// base 范围（docid < base_count）的反查走 [`crate::index::path_table::PathTable::find_path_idx`]。
    fn is_delta_docid(&self, docid: DocId) -> bool {
        docid >= self.base_delta.read().base.by_path_idx().len() as u64
    }

    fn insert_path_hash(&self, docid: DocId, path: &Path) {
        if !self.is_delta_docid(docid) {
            return;
        }
        let bytes = path.as_os_str().as_encoded_bytes();
        let h = path_hash_bytes(bytes);
        let mut map = self.path_hash_to_id.write();
        map.entry(h)
            .and_modify(|v| v.insert(docid))
            .or_insert(OneOrManyDocId::One(docid));
    }

    fn remove_path_hash(&self, docid: DocId, path: &Path) {
        if !self.is_delta_docid(docid) {
            return;
        }
        let bytes = path.as_os_str().as_encoded_bytes();
        let h = path_hash_bytes(bytes);
        let mut map = self.path_hash_to_id.write();
        if let Some(v) = map.get_mut(&h) {
            let empty = v.remove(docid);
            if empty {
                map.remove(&h);
            }
        }
    }

    /// 把 docid 注册到所属父目录的 ParentIndex 桶。
    fn insert_parent(&self, docid: DocId, abs_path: &Path) {
        if let Some(h) = Self::parent_hash_for(abs_path) {
            self.parent_index.write().insert(h, docid);
        }
    }

    /// 从所属父目录的 ParentIndex 桶里取出 docid（删除/迁移用）。
    fn remove_parent(&self, docid: DocId, abs_path: &Path) {
        if let Some(h) = Self::parent_hash_for(abs_path) {
            self.parent_index.write().remove(h, docid);
        }
    }

    fn lookup_docid_by_path(&self, path: &Path) -> Option<DocId> {
        let bytes = path.as_os_str().as_encoded_bytes();
        let h = path_hash_bytes(bytes);

        // delta 半段：先查 path_hash_to_id（仅含 delta 路径，不再含 base，
        // 参见 `重构方案包/causal-chain-report.md` §8.7 的内存表）。
        let delta_candidates: Vec<DocId> = {
            let map = self.path_hash_to_id.read();
            map.get(&h).map(|v| v.iter().copied().collect()).unwrap_or_default()
        };
        for docid in &delta_candidates {
            if self
                .resolve_abs_path_bytes_for_docid(*docid)
                .map(|abs| abs == bytes)
                .unwrap_or(false)
            {
                return Some(*docid);
            }
        }

        // base 半段：PathTable 已按字典序排序，直接二分。
        // 命中后还要确认该 docid 不在 tombstones / delta.removed 里。
        let bd = self.base_delta.read();
        let base_path_idx = bd.base.path_table().find_path_idx(bytes)?;
        let docid = base_path_idx as u64;
        if self.tombstones.read().contains(docid) {
            return None;
        }
        if bd.delta.removed().contains(docid) {
            return None;
        }
        Some(docid)
    }

    fn trigram_candidates(&self, matcher: &dyn Matcher) -> Option<RoaringTreemap> {
        let hint = matcher.literal_hint()?;
        let s = String::from_utf8_lossy(hint);
        let tris = query_trigrams(s.as_ref());
        if tris.is_empty() {
            return None;
        }

        let tri_idx = self.trigram_index.read();
        let mut sorted_tris = tris.clone();
        sorted_tris.sort_by_key(|t| tri_idx.get(t).map(|b| b.len()).unwrap_or(0));

        let mut acc: Option<RoaringTreemap> = None;
        for tri in &sorted_tris {
            let posting = tri_idx.get(tri)?;
            match acc {
                None => acc = Some(posting.clone()),
                Some(ref mut a) => {
                    *a &= posting;
                    if a.is_empty() {
                        return None;
                    }
                }
            }
        }
        Some(acc.unwrap_or_default())
    }

    fn short_hint_candidates(&self, matcher: &dyn Matcher) -> Option<RoaringTreemap> {
        let hint = normalize_short_hint(matcher.literal_hint()?)?;
        let tri_idx = self.trigram_index.read();
        let short_idx = self.short_component_index.read();
        let mut acc = RoaringTreemap::new();

        for (tri, posting) in tri_idx.iter() {
            if trigram_matches_short_hint(*tri, &hint) {
                acc |= posting.clone();
            }
        }

        for (component, posting) in short_idx.iter() {
            if short_component_matches(*component, &hint) {
                acc |= posting.clone();
            }
        }

        Some(acc)
    }
}

impl IndexLayer for PersistentIndex {
    fn query_keys(&self, matcher: &dyn Matcher) -> Vec<FileKey> {
        // 复用 L2 的 trigram 候选集计算，但只输出稳定身份（FileKey）。
        let candidates = self
            .trigram_candidates(matcher)
            .or_else(|| self.short_hint_candidates(matcher));

        // M4-B: 改走 BaseDelta，避免 metas+arena 锁。
        let bd = self.base_delta.read();
        let tombstones = self.tombstones.read();
        let base_entries = bd.base.by_path_idx().entries();
        let base_count = base_entries.len() as u64;

        let mut out: Vec<FileKey> = Vec::new();
        let push_if_match = |out: &mut Vec<FileKey>, fk: FileKey, abs: &[u8]| {
            let s = std::str::from_utf8(abs)
                .map(std::borrow::Cow::Borrowed)
                .unwrap_or_else(|_| String::from_utf8_lossy(abs));
            if matcher.matches(&s) {
                out.push(fk);
            }
        };

        match candidates {
            Some(bitmap) => {
                // candidates 已被 trigram 过滤后通常很小，逐 docid 解析无伤大雅。
                for docid in bitmap.iter() {
                    if tombstones.contains(docid) {
                        continue;
                    }
                    if docid < base_count {
                        let Some(entry) = base_entries.get(docid as usize) else {
                            continue;
                        };
                        let Some(abs) = bd.base.resolve_path(docid) else {
                            continue;
                        };
                        push_if_match(&mut out, entry.file_key(), &abs);
                    } else {
                        let idx = (docid - base_count) as usize;
                        let Some(entry) = bd.delta.added().get(idx) else {
                            continue;
                        };
                        let Some(abs) = bd.delta.added_path_bytes(idx) else {
                            continue;
                        };
                        push_if_match(&mut out, entry.file_key(), abs);
                    }
                }
            }
            None => {
                // Phase 4：无 trigram 候选时全量扫描，base 半段走 path_table.for_each_path
                // 顺序解码（~30× CPU 节省）。短查询命中此分支，节省可观。
                bd.base.path_table().for_each_path(|path_idx, abs_bytes| {
                    let docid = path_idx as u64;
                    if tombstones.contains(docid) {
                        return;
                    }
                    let Some(entry) = base_entries.get(path_idx as usize) else {
                        return;
                    };
                    push_if_match(&mut out, entry.file_key(), abs_bytes);
                });
                for idx in 0..bd.delta.added().len() {
                    let docid = base_count + idx as u64;
                    if tombstones.contains(docid) {
                        continue;
                    }
                    let Some(entry) = bd.delta.added().get(idx) else {
                        continue;
                    };
                    let Some(abs) = bd.delta.added_path_bytes(idx) else {
                        continue;
                    };
                    push_if_match(&mut out, entry.file_key(), abs);
                }
            }
        }

        out
    }

    fn get_meta(&self, key: FileKey) -> Option<FileMeta> {
        let docid = self.lookup_docid_by_filekey(key)?;
        if self.tombstones.read().contains(docid) {
            return None;
        }
        let (fk, abs, size, mtime_ns) = self.resolve_entry_via_base_delta(docid)?;
        Some(FileMeta {
            file_key: fk,
            path: pathbuf_from_encoded_vec(abs),
            size,
            mtime: mtime_from_ns(mtime_ns),
            ctime: None,
            atime: None,
        })
    }
}

fn path_hash_bytes(bytes: &[u8]) -> u64 {
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    bytes.hash(&mut hasher);
    hasher.finish()
}

fn normalize_roots_with_fallback(mut roots: Vec<PathBuf>) -> Vec<PathBuf> {
    use unicode_normalization::UnicodeNormalization;
    for r in &mut roots {
        let s = r.to_string_lossy();
        *r = PathBuf::from(s.nfc().collect::<String>());
    }

    // 去重 + 排序，保证 root_id 的解释在"同一组 roots"下稳定。
    roots.sort_by(|a, b| {
        a.as_os_str()
            .as_encoded_bytes()
            .cmp(b.as_os_str().as_encoded_bytes())
    });
    roots.dedup();

    // root_id=0 固定为 "/"（用于兜底匹配与快照兼容）。
    let slash = PathBuf::from("/");
    roots.retain(|p| p != &slash);
    let mut out = Vec::with_capacity(roots.len() + 1);
    out.push(slash);
    out.extend(roots);
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::query::matcher::create_matcher;

    #[test]
    fn roaring_posting_basic_query() {
        let idx = PersistentIndex::new();
        idx.upsert(FileMeta {
            file_key: FileKey {
                dev: 1,
                ino: 1,
                generation: 0,
            },
            path: PathBuf::from("/tmp/alpha_test.txt"),
            size: 1,
            mtime: None,
            ctime: None,
            atime: None,
        });
        idx.upsert(FileMeta {
            file_key: FileKey {
                dev: 1,
                ino: 2,
                generation: 0,
            },
            path: PathBuf::from("/tmp/beta_test.txt"),
            size: 1,
            mtime: None,
            ctime: None,
            atime: None,
        });

        let m = create_matcher("alpha", true);
        let r = idx.query(m.as_ref(), 100);
        assert_eq!(r.len(), 1);
        assert!(r[0].path.to_string_lossy().contains("alpha_test"));
    }

    #[test]
    fn short_literal_query_uses_short_component_candidates() {
        let idx = PersistentIndex::new();
        idx.upsert(FileMeta {
            file_key: FileKey {
                dev: 1,
                ino: 1,
                generation: 0,
            },
            path: PathBuf::from("/tmp/ab"),
            size: 1,
            mtime: None,
            ctime: None,
            atime: None,
        });
        idx.upsert(FileMeta {
            file_key: FileKey {
                dev: 1,
                ino: 2,
                generation: 0,
            },
            path: PathBuf::from("/tmp/cabd.txt"),
            size: 1,
            mtime: None,
            ctime: None,
            atime: None,
        });

        let m = create_matcher("ab", true);
        let r = idx.query(m.as_ref(), 100);
        assert_eq!(r.len(), 2);

        let m = create_matcher("a", true);
        let r = idx.query(m.as_ref(), 100);
        assert_eq!(r.len(), 2);
    }

    #[test]
    fn overlong_new_paths_are_skipped_without_placeholder_doc() {
        let idx = PersistentIndex::new();
        let path = PathBuf::from(format!("/tmp/{}", "a".repeat(u16::MAX as usize + 1)));
        idx.upsert(FileMeta {
            file_key: FileKey {
                dev: 1,
                ino: 1,
                generation: 0,
            },
            path,
            size: 1,
            mtime: None,
            ctime: None,
            atime: None,
        });

        assert_eq!(idx.file_count(), 0);
        let m = create_matcher("aaaa", true);
        assert!(idx.query(m.as_ref(), 100).is_empty());
    }

    #[test]
    fn rename_to_overlong_path_tombstones_old_entry() {
        let idx = PersistentIndex::new();
        idx.upsert(FileMeta {
            file_key: FileKey {
                dev: 1,
                ino: 1,
                generation: 0,
            },
            path: PathBuf::from("/tmp/short-name.txt"),
            size: 1,
            mtime: None,
            ctime: None,
            atime: None,
        });

        let long_path = PathBuf::from(format!("/tmp/{}", "b".repeat(u16::MAX as usize + 1)));
        idx.upsert_rename(FileMeta {
            file_key: FileKey {
                dev: 1,
                ino: 1,
                generation: 0,
            },
            path: long_path,
            size: 2,
            mtime: None,
            ctime: None,
            atime: None,
        });

        assert_eq!(idx.file_count(), 0);
        let m = create_matcher("short-name", true);
        assert!(idx.query(m.as_ref(), 100).is_empty());
    }

    #[test]
    fn same_filekey_updates_to_new_path_when_old_path_is_missing() {
        let root = std::env::temp_dir().join(format!(
            "fd-rdd-reconcile-{}",
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        let old_project = root.join("old_project");
        let old_dir = old_project.join("node_modules/libA/dist");
        std::fs::create_dir_all(&old_dir).unwrap();

        let old_path = old_dir.join("bundle_1.js");
        std::fs::write(&old_path, b"bundle").unwrap();

        let idx = PersistentIndex::new_with_roots(vec![root.clone()]);
        let file_key = FileKey {
            dev: 1,
            ino: 42,
            generation: 0,
        };

        idx.upsert(FileMeta {
            file_key,
            path: old_path.clone(),
            size: 6,
            mtime: None,
            ctime: None,
            atime: None,
        });

        let new_project = root.join("new_project");
        std::fs::rename(&old_project, &new_project).unwrap();
        let new_path = new_project.join("node_modules/libA/dist/bundle_1.js");

        idx.upsert(FileMeta {
            file_key,
            path: new_path.clone(),
            size: 6,
            mtime: None,
            ctime: None,
            atime: None,
        });

        let meta = idx.get_meta(file_key).expect("file should remain indexed");
        assert_eq!(meta.path, new_path);

        let matcher = create_matcher("bundle_1", false);
        let results = idx.query(matcher.as_ref(), 10);
        assert!(
            results.iter().any(|m| m.path == new_path),
            "new path should be queryable after reconcile: {results:?}"
        );
        assert!(
            results.iter().all(|m| m.path != old_path),
            "old path should be removed after reconcile: {results:?}"
        );

        let _ = std::fs::remove_dir_all(&root);
    }

    #[test]
    fn chinese_exact_query_via_trigram() {
        let idx = PersistentIndex::new();
        idx.upsert(FileMeta {
            file_key: FileKey {
                dev: 1,
                ino: 1,
                generation: 0,
            },
            path: PathBuf::from("/tmp/中文文件.txt"),
            size: 1,
            mtime: None,
            ctime: None,
            atime: None,
        });

        let m = create_matcher("中文", true);
        let r = idx.query(m.as_ref(), 100);
        assert_eq!(r.len(), 1, "expected 1 result for '中文', got {}", r.len());
        assert!(r[0].path.to_string_lossy().contains("中文文件"));

        let m2 = create_matcher("文件", true);
        let r2 = idx.query(m2.as_ref(), 100);
        assert_eq!(
            r2.len(),
            1,
            "expected 1 result for '文件', got {}",
            r2.len()
        );
    }

    /// 直接验证 `for_each_live_meta_in_dirs` 在 ParentIndex 介入后的语义：
    /// - 同目录新增：bitmap 立刻看到。
    /// - 删除：bitmap 不再返回。
    /// - 改名跨目录：从旧 parent bitmap 移出、新 parent bitmap 加入。
    /// - 多目录并集：返回所有指定父目录下的 live 条目。
    #[test]
    fn for_each_live_meta_in_dirs_uses_parent_index_lifecycle() {
        use std::collections::HashSet;

        let idx = PersistentIndex::new_with_roots(vec![PathBuf::from("/tmp")]);
        let p_a = PathBuf::from("/tmp/d1/a.txt");
        let p_b = PathBuf::from("/tmp/d1/b.txt");
        let p_c = PathBuf::from("/tmp/d2/c.txt");

        for (ino, path) in [(1u64, &p_a), (2, &p_b), (3, &p_c)] {
            idx.upsert(FileMeta {
                file_key: FileKey {
                    dev: 1,
                    ino,
                    generation: 0,
                },
                path: path.clone(),
                size: 1,
                mtime: None,
                ctime: None,
                atime: None,
            });
        }

        let mut dirs = HashSet::new();
        dirs.insert(PathBuf::from("/tmp/d1"));

        let mut seen: Vec<PathBuf> = Vec::new();
        idx.for_each_live_meta_in_dirs(&dirs, |m| seen.push(m.path));
        seen.sort();
        assert_eq!(seen, vec![p_a.clone(), p_b.clone()]);

        // delete /tmp/d1/a.txt
        idx.mark_deleted(FileKey {
            dev: 1,
            ino: 1,
            generation: 0,
        });
        let mut seen: Vec<PathBuf> = Vec::new();
        idx.for_each_live_meta_in_dirs(&dirs, |m| seen.push(m.path));
        assert_eq!(seen, vec![p_b.clone()]);

        // rename /tmp/d1/b.txt → /tmp/d2/b.txt
        let p_b_new = PathBuf::from("/tmp/d2/b.txt");
        idx.upsert_rename(FileMeta {
            file_key: FileKey {
                dev: 1,
                ino: 2,
                generation: 0,
            },
            path: p_b_new.clone(),
            size: 1,
            mtime: None,
            ctime: None,
            atime: None,
        });

        // /tmp/d1 现在为空。
        let mut seen: Vec<PathBuf> = Vec::new();
        idx.for_each_live_meta_in_dirs(&dirs, |m| seen.push(m.path));
        assert!(seen.is_empty(), "/tmp/d1 should now be empty: {seen:?}");

        // /tmp/d2 应能看到 c.txt 和 b.txt（被 rename 进来的）。
        let mut dirs2 = HashSet::new();
        dirs2.insert(PathBuf::from("/tmp/d2"));
        let mut seen: Vec<PathBuf> = Vec::new();
        idx.for_each_live_meta_in_dirs(&dirs2, |m| seen.push(m.path));
        seen.sort();
        assert_eq!(seen, vec![p_b_new, p_c.clone()]);

        // 多目录并集：传入 d1 + d2，应等于 d2 的结果（d1 已空）。
        let mut both = HashSet::new();
        both.insert(PathBuf::from("/tmp/d1"));
        both.insert(PathBuf::from("/tmp/d2"));
        let mut seen: Vec<PathBuf> = Vec::new();
        idx.for_each_live_meta_in_dirs(&both, |m| seen.push(m.path));
        assert_eq!(seen.len(), 2);

        // 不存在的目录返回空。
        let mut nope = HashSet::new();
        nope.insert(PathBuf::from("/tmp/no_such_dir"));
        let mut seen: Vec<PathBuf> = Vec::new();
        idx.for_each_live_meta_in_dirs(&nope, |m| seen.push(m.path));
        assert!(seen.is_empty());
    }

    /// `mark_deleted_by_path` 同样要从 ParentIndex 中删除，否则 fast_sync delete 对齐
    /// 会持续把已删文件错认为 live（hash 校验兜底但走 expensive 路径）。
    ///
    /// 注：路径用 [`Path::join`] 构造而不是 `"/tmp/d/x.txt"` 字面量。原因是
    /// `lookup_docid_by_path` 内部用 [`compose_abs_path_bytes`] 重组绝对路径并做
    /// 字节级 `==` 校验，而 `compose_abs_path_bytes` 在 Windows 上使用
    /// `MAIN_SEPARATOR = '\\'`。如果用全斜线字面量 `/tmp/d/x.txt`，在 Windows 上
    /// 字节级比对会失败，`mark_deleted_by_path` 会静默 no-op。
    #[test]
    fn mark_deleted_by_path_evicts_from_parent_index() {
        use std::collections::HashSet;

        let root = PathBuf::from("/tmp");
        let dir = root.join("d");
        let p = dir.join("x.txt");

        let idx = PersistentIndex::new_with_roots(vec![root]);
        idx.upsert(FileMeta {
            file_key: FileKey {
                dev: 1,
                ino: 1,
                generation: 0,
            },
            path: p.clone(),
            size: 0,
            mtime: None,
            ctime: None,
            atime: None,
        });
        let mut dirs = HashSet::new();
        dirs.insert(dir);

        let mut seen: Vec<PathBuf> = Vec::new();
        idx.for_each_live_meta_in_dirs(&dirs, |m| seen.push(m.path));
        assert_eq!(seen, vec![p.clone()]);

        idx.mark_deleted_by_path(&p);
        let mut seen: Vec<PathBuf> = Vec::new();
        idx.for_each_live_meta_in_dirs(&dirs, |m| seen.push(m.path));
        assert!(seen.is_empty());
    }

    /// V7 round-trip 验证（参见 `重构方案包/causal-chain-report.md` §8.9）。
    /// 路径：upsert 若干 → to_snapshot_v7 → encode → decode → from_snapshot_v7
    /// → 比对 query 结果。
    #[test]
    fn v7_snapshot_round_trip_preserves_query_results() {
        let root = PathBuf::from("/tmp");
        let p_a = root.join("d1").join("a.txt");
        let p_b = root.join("d1").join("b.txt");
        let p_c = root.join("d2").join("c.txt");

        let idx = PersistentIndex::new_with_roots(vec![root.clone()]);
        for (ino, path) in [(1u64, &p_a), (2, &p_b), (3, &p_c)] {
            idx.upsert(FileMeta {
                file_key: FileKey {
                    dev: 1,
                    ino,
                    generation: 0,
                },
                path: path.clone(),
                size: 100 + ino,
                mtime: None,
                ctime: None,
                atime: None,
            });
        }
        // 删一条以验证 tombstones 也能 round-trip。
        idx.mark_deleted(FileKey {
            dev: 1,
            ino: 2,
            generation: 0,
        });

        // V7 字节流 round-trip。
        let snap = idx.to_snapshot_v7();
        let bytes = snap.encode().expect("v7 encode");
        let decoded = crate::storage::snapshot_v7::V7Snapshot::decode(&bytes).expect("v7 decode");

        let restored = PersistentIndex::from_snapshot_v7(decoded, vec![root.clone()]);

        // live 文件数：原来 3 条减 1 条墓碑 = 2 条。
        // Phase 4：base 段的 file_key 现在不再登记到 `filekey_to_docid`，需走
        // `lookup_docid_by_filekey`（fallback 到 BaseIndex.by_file_key 二分）。
        let live_a = restored
            .lookup_docid_by_filekey(FileKey {
                dev: 1,
                ino: 1,
                generation: 0,
            })
            .is_some();
        assert!(live_a, "ino=1 should survive round-trip");

        // ino=2 已删除：BaseIndex.by_file_key 仍持有它（不区分 tombstone），
        // 但 tombstones 应包含它的 docid。
        let docid_for_ino2 = restored.lookup_docid_by_filekey(FileKey {
            dev: 1,
            ino: 2,
            generation: 0,
        });
        if let Some(d) = docid_for_ino2 {
            assert!(
                restored.tombstones.read().contains(d),
                "ino=2 must be tombstoned"
            );
        }

        // 目录倒排：/tmp/d1 应只剩 a.txt（b 已删），/tmp/d2 仍有 c.txt。
        use std::collections::HashSet;
        let mut d1 = HashSet::new();
        d1.insert(root.join("d1"));
        let mut seen: Vec<PathBuf> = Vec::new();
        restored.for_each_live_meta_in_dirs(&d1, |m| seen.push(m.path));
        assert_eq!(
            seen,
            vec![p_a.clone()],
            "after round-trip /tmp/d1 should have only a.txt"
        );

        let mut d2 = HashSet::new();
        d2.insert(root.join("d2"));
        let mut seen: Vec<PathBuf> = Vec::new();
        restored.for_each_live_meta_in_dirs(&d2, |m| seen.push(m.path));
        assert_eq!(seen, vec![p_c.clone()]);
    }

    /// 空索引的 v7 round-trip：仍应能加密 / 解密 / 重建出空 PersistentIndex。
    #[test]
    fn v7_empty_round_trip() {
        let root = PathBuf::from("/tmp");
        let idx = PersistentIndex::new_with_roots(vec![root.clone()]);

        let snap = idx.to_snapshot_v7();
        let bytes = snap.encode().expect("v7 encode");
        let decoded = crate::storage::snapshot_v7::V7Snapshot::decode(&bytes).expect("v7 decode");
        let restored = PersistentIndex::from_snapshot_v7(decoded, vec![root]);

        assert_eq!(restored.file_count(), 0);
        assert_eq!(restored.tombstones.read().len(), 0);
    }

    /// roots 不匹配时 from_snapshot_v7 应返回空索引并打 warn（而不是把垃圾路径写进去）。
    #[test]
    fn v7_roots_mismatch_aborts_load() {
        let idx = PersistentIndex::new_with_roots(vec![PathBuf::from("/tmp")]);
        idx.upsert(FileMeta {
            file_key: FileKey {
                dev: 1,
                ino: 1,
                generation: 0,
            },
            path: PathBuf::from("/tmp").join("x.txt"),
            size: 1,
            mtime: None,
            ctime: None,
            atime: None,
        });
        let snap = idx.to_snapshot_v7();
        // 用一个不同的 root 加载——应得到空索引。
        let restored =
            PersistentIndex::from_snapshot_v7(snap, vec![PathBuf::from("/different/root")]);
        assert_eq!(restored.file_count(), 0);
    }

    /// M1：from_snapshot_v7 后 base_delta 内核应被启用，且
    /// `resolve_abs_path_bytes_for_docid` 与
    /// `resolve_abs_path_bytes_for_docid_via_base_delta` 在 live 范围内一致。
    #[test]
    fn v7_load_enables_base_delta_kernel_with_consistent_path_resolution() {
        let root = PathBuf::from("/tmp");
        let p_a = root.join("d1").join("a.txt");
        let p_b = root.join("d1").join("b.txt");
        let p_c = root.join("d2").join("c.txt");

        let idx = PersistentIndex::new_with_roots(vec![root.clone()]);
        for (ino, path) in [(1u64, &p_a), (2, &p_b), (3, &p_c)] {
            idx.upsert(FileMeta {
                file_key: FileKey {
                    dev: 1,
                    ino,
                    generation: 0,
                },
                path: path.clone(),
                size: 100 + ino,
                mtime: None,
                ctime: None,
                atime: None,
            });
        }

        // 走完整 round-trip：to_v7 → encode → decode → from_v7。
        let snap = idx.to_snapshot_v7();
        let bytes = snap.encode().unwrap();
        let decoded = crate::storage::snapshot_v7::V7Snapshot::decode(&bytes).unwrap();
        let restored = PersistentIndex::from_snapshot_v7(decoded, vec![root]);

        // BaseDelta 内核应已填充——base 不再为空。
        assert!(
            !restored.base_delta.read().base.by_path_idx().is_empty(),
            "BaseDelta kernel must be populated after from_snapshot_v7"
        );

        // 对每条 live docid，两条 helper 应返回相同 path bytes。
        let total = restored.file_count();
        assert_eq!(total, 3);
        for d in 0..total as u64 {
            let legacy = restored.resolve_abs_path_bytes_for_docid(d);
            let bd = restored.resolve_abs_path_bytes_for_docid_via_base_delta(d);
            assert_eq!(
                legacy, bd,
                "docid {d}: legacy and base_delta paths must agree"
            );
            assert!(legacy.is_some(), "docid {d} should have a path");
        }
    }

    /// M1：BaseDelta 启用后，docid 越界（超过 base_count）应优雅返回 None；
    /// 当前未启用 delta 写入，所以 [base_count, ...) 区间永远是 None。
    #[test]
    fn base_delta_oob_docid_returns_none() {
        let root = PathBuf::from("/tmp");
        let idx = PersistentIndex::new_with_roots(vec![root.clone()]);
        idx.upsert(FileMeta {
            file_key: FileKey {
                dev: 1,
                ino: 1,
                generation: 0,
            },
            path: root.join("only.txt"),
            size: 1,
            mtime: None,
            ctime: None,
            atime: None,
        });
        let snap = idx.to_snapshot_v7();
        let bytes = snap.encode().unwrap();
        let decoded = crate::storage::snapshot_v7::V7Snapshot::decode(&bytes).unwrap();
        let restored = PersistentIndex::from_snapshot_v7(decoded, vec![root]);

        // 唯一 live docid = 0：应可解析。
        assert!(restored
            .resolve_abs_path_bytes_for_docid_via_base_delta(0)
            .is_some());
        // docid = 1 越界：返回 None（不 panic）。
        assert!(restored
            .resolve_abs_path_bytes_for_docid_via_base_delta(1)
            .is_none());
    }

    /// M4：默认 PersistentIndex 的 BaseDelta 内核应是空的（base/delta 均空），
    /// 而非 None——helper 对任意 docid 仍返回 None（base 为空）。
    #[test]
    fn fresh_index_has_empty_base_delta_kernel() {
        let idx = PersistentIndex::new_with_roots(vec![PathBuf::from("/tmp")]);
        {
            let bd = idx.base_delta.read();
            assert_eq!(bd.base.by_path_idx().len(), 0);
            assert!(bd.delta.added().is_empty());
            assert!(bd.delta.removed().is_empty());
        }
        // 空 kernel 对任意 docid 返回 None（base 为空 + delta 也为空）。
        assert!(idx
            .resolve_abs_path_bytes_for_docid_via_base_delta(0)
            .is_none());
    }

    /// M3：BaseDelta 启用后，新建文件应同步写入 `delta.added`；helper 解析新 docid
    /// 应从 delta 解析得到正确 path。
    #[test]
    fn m3_upsert_synchronizes_base_delta_added() {
        let root = PathBuf::from("/tmp");
        let idx = PersistentIndex::new_with_roots(vec![root.clone()]);

        // 走 v7 round-trip 启用 BaseDelta（base_count = 0，delta 干净）。
        let snap = idx.to_snapshot_v7();
        let bytes = snap.encode().unwrap();
        let decoded = crate::storage::snapshot_v7::V7Snapshot::decode(&bytes).unwrap();
        let restored = PersistentIndex::from_snapshot_v7(decoded, vec![root.clone()]);

        // 新建一条文件——应同步进 delta。
        let p = root.join("new.txt");
        restored.upsert(FileMeta {
            file_key: FileKey {
                dev: 1,
                ino: 42,
                generation: 0,
            },
            path: p.clone(),
            size: 7,
            mtime: None,
            ctime: None,
            atime: None,
        });

        // BaseDelta 现在应有 1 条 delta.added。
        {
            let bd = restored.base_delta.read();
            let kernel = &*bd;
            assert_eq!(kernel.delta.added().len(), 1);
            // delta.added[0] 的 path 应等于 p 的字节。
            let path_bytes = kernel.delta.added_path_bytes(0).unwrap();
            assert_eq!(path_bytes, p.as_os_str().as_encoded_bytes());
        }

        // helper 通过 docid（base_count=0 + delta 0 = 0）应能解析 path。
        let resolved = restored.resolve_abs_path_bytes_for_docid_via_base_delta(0);
        assert_eq!(resolved.as_deref(), Some(p.as_os_str().as_encoded_bytes()));
    }

    /// M3：BaseDelta 启用后，mark_deleted 应同步：
    /// - base 段 docid（< base_count）→ delta.removed += docid
    /// - delta 段 docid（>= base_count）→ delta.added 中对应 file_key 被 drop
    #[test]
    fn m3_mark_deleted_synchronizes_base_delta() {
        let root = PathBuf::from("/tmp");
        // 先插一条到原 idx 然后 round-trip，让 base_count=1。
        let idx = PersistentIndex::new_with_roots(vec![root.clone()]);
        idx.upsert(FileMeta {
            file_key: FileKey {
                dev: 1,
                ino: 1,
                generation: 0,
            },
            path: root.join("a.txt"),
            size: 1,
            mtime: None,
            ctime: None,
            atime: None,
        });
        let snap = idx.to_snapshot_v7();
        let bytes = snap.encode().unwrap();
        let decoded = crate::storage::snapshot_v7::V7Snapshot::decode(&bytes).unwrap();
        let restored = PersistentIndex::from_snapshot_v7(decoded, vec![root.clone()]);

        {
            let bd = restored.base_delta.read();
            let kernel = &*bd;
            assert_eq!(kernel.base.by_path_idx().len(), 1, "base_count = 1");
            assert_eq!(kernel.delta.added().len(), 0);
        }

        // 删 base 段那条（docid=0）→ delta.removed 应包含 0。
        restored.mark_deleted(FileKey {
            dev: 1,
            ino: 1,
            generation: 0,
        });
        {
            let bd = restored.base_delta.read();
            let kernel = &*bd;
            assert!(
                kernel.delta.removed().contains(0),
                "base docid 0 should be marked removed in delta"
            );
        }

        // 新建一条到 delta 段（docid=1）→ 然后再删它。
        // M4-B: delta 段不再物理 drop（避免 swap_remove 打乱 docid→idx 对齐）；
        // 由 legacy tombstones 过滤——所以 delta.added 会保留 entry，但
        // tombstones 包含其 docid。
        restored.upsert(FileMeta {
            file_key: FileKey {
                dev: 1,
                ino: 2,
                generation: 0,
            },
            path: root.join("b.txt"),
            size: 1,
            mtime: None,
            ctime: None,
            atime: None,
        });
        {
            let bd = restored.base_delta.read();
            let kernel = &*bd;
            assert_eq!(kernel.delta.added().len(), 1);
        }
        let new_docid = restored
            .filekey_to_docid
            .read()
            .get(&FileKey {
                dev: 1,
                ino: 2,
                generation: 0,
            })
            .copied()
            .unwrap();
        restored.mark_deleted(FileKey {
            dev: 1,
            ino: 2,
            generation: 0,
        });
        {
            let bd = restored.base_delta.read();
            let kernel = &*bd;
            // 物理 entry 仍在（保 docid 对齐）。
            assert_eq!(
                kernel.delta.added().len(),
                1,
                "delta.added entry should be retained for docid alignment"
            );
        }
        // tombstones 应包含这个 docid。
        assert!(
            restored.tombstones.read().contains(new_docid),
            "tombstones should include the deleted delta-segment docid"
        );
    }

    /// M3：base 段条目被 rename 时，delta.removed 应屏蔽 base 旧 docid，
    /// 同时 delta.added 应包含新 path。
    #[test]
    fn m3_rename_base_entry_synchronizes_base_delta() {
        let root = PathBuf::from("/tmp");
        let idx = PersistentIndex::new_with_roots(vec![root.clone()]);
        let fk = FileKey {
            dev: 1,
            ino: 5,
            generation: 0,
        };
        idx.upsert(FileMeta {
            file_key: fk,
            path: root.join("old.txt"),
            size: 1,
            mtime: None,
            ctime: None,
            atime: None,
        });
        let snap = idx.to_snapshot_v7();
        let bytes = snap.encode().unwrap();
        let decoded = crate::storage::snapshot_v7::V7Snapshot::decode(&bytes).unwrap();
        let restored = PersistentIndex::from_snapshot_v7(decoded, vec![root.clone()]);

        // base 段 docid=0 持有 old.txt；走 upsert_rename 改名到 new.txt。
        let new_path = root.join("new.txt");
        restored.upsert_rename(FileMeta {
            file_key: fk,
            path: new_path.clone(),
            size: 2,
            mtime: None,
            ctime: None,
            atime: None,
        });

        let bd = restored.base_delta.read();
        let kernel = &*bd;
        assert!(
            kernel.delta.removed().contains(0),
            "base docid 0 should be marked removed after rename"
        );
        assert_eq!(
            kernel.delta.added().len(),
            1,
            "renamed entry should appear in delta.added"
        );
        let new_bytes = kernel.delta.added_path_bytes(0).unwrap();
        assert_eq!(new_bytes, new_path.as_os_str().as_encoded_bytes());
    }

    /// M3：delta 段条目被 rename 时，delta.added 中对应 file_key 应原地替换为新 path。
    /// 不应往 delta.removed 写任何东西（旧 entry 本来就在 delta，不在 base）。
    #[test]
    fn m3_rename_delta_entry_replaces_in_place() {
        let root = PathBuf::from("/tmp");
        // 空 base：v7 round-trip 后启用 BaseDelta，base_count = 0。
        let idx = PersistentIndex::new_with_roots(vec![root.clone()]);
        let snap = idx.to_snapshot_v7();
        let bytes = snap.encode().unwrap();
        let decoded = crate::storage::snapshot_v7::V7Snapshot::decode(&bytes).unwrap();
        let restored = PersistentIndex::from_snapshot_v7(decoded, vec![root.clone()]);

        // 先 upsert 一条进 delta（docid=0，base_count=0 → 在 delta 段）。
        let fk = FileKey {
            dev: 1,
            ino: 9,
            generation: 0,
        };
        restored.upsert(FileMeta {
            file_key: fk,
            path: root.join("old.txt"),
            size: 1,
            mtime: None,
            ctime: None,
            atime: None,
        });
        // 再 rename。
        let new_path = root.join("renamed.txt");
        restored.upsert_rename(FileMeta {
            file_key: fk,
            path: new_path.clone(),
            size: 2,
            mtime: None,
            ctime: None,
            atime: None,
        });

        let bd = restored.base_delta.read();
        let kernel = &*bd;
        assert_eq!(
            kernel.delta.added().len(),
            1,
            "should still be one entry (in-place upsert)"
        );
        assert!(
            kernel.delta.removed().is_empty(),
            "rename inside delta segment should not touch removed"
        );
        let bytes_now = kernel.delta.added_path_bytes(0).unwrap();
        assert_eq!(bytes_now, new_path.as_os_str().as_encoded_bytes());
    }
}
