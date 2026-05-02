use parking_lot::RwLock;
use roaring::RoaringTreemap;
use serde::{Deserialize, Serialize};
use std::borrow::Cow;
use std::collections::{HashMap, HashSet};
use std::hash::{Hash, Hasher};
use std::path::{Component, Path, PathBuf};
use std::sync::Arc;

#[cfg(feature = "rkyv")]
use crate::core::FileKeyEntry;
use crate::core::{EventRecord, EventType, FileIdentifier, FileKey, FileMeta};
use crate::index::file_entry_v2::FileEntry;
use crate::index::parent_index::PathTable as PathTableTrait;
use crate::index::IndexLayer;
use crate::query::matcher::Matcher;
use crate::stats::L2Stats;
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

/// Path blob arena：所有路径的连续字节存储
#[derive(Clone, Debug, Default)]
pub struct PathArena {
    pub data: Arc<Vec<u8>>,
}

impl serde::Serialize for PathArena {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        self.data.as_ref().serialize(serializer)
    }
}

impl<'de> serde::Deserialize<'de> for PathArena {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let data = Vec::<u8>::deserialize(deserializer)?;
        Ok(PathArena {
            data: Arc::new(data),
        })
    }
}

impl PathArena {
    pub fn new() -> Self {
        Self {
            data: Arc::new(Vec::new()),
        }
    }

    pub fn push_bytes(&mut self, bytes: &[u8]) -> Option<(u32, u16)> {
        let len: u16 = match bytes.len().try_into() {
            Ok(len) => len,
            Err(_) => {
                tracing::warn!(
                    "Skipping indexed path longer than {} bytes: {} bytes",
                    u16::MAX,
                    bytes.len()
                );
                return None;
            }
        };
        let data = Arc::make_mut(&mut self.data);
        let off: u32 = data.len().try_into().ok()?;
        data.extend_from_slice(bytes);
        Some((off, len))
    }

    pub fn push_path(&mut self, path: &Path) -> Option<(u32, u16)> {
        let bytes = path.as_os_str().as_encoded_bytes();
        self.push_bytes(bytes)
    }

    pub fn get_bytes(&self, off: u32, len: u16) -> Option<&[u8]> {
        let start: usize = off as usize;
        let end: usize = start.checked_add(len as usize)?;
        self.data.get(start..end)
    }

    pub fn get_path_buf(&self, off: u32, len: u16) -> Option<PathBuf> {
        let bytes = self.get_bytes(off, len)?.to_vec();
        Some(pathbuf_from_encoded_vec(bytes))
    }
}

/// 旧紧凑元数据（v4 快照）：不包含 root_id（存储的是绝对路径字节）
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct CompactMetaV4 {
    pub file_key: FileKey,
    pub path_off: u32,
    pub path_len: u16,
    pub size: u64,
    pub mtime: Option<std::time::SystemTime>,
}

/// 紧凑元数据（v5 起）：以 DocId 为下标（Vec 紧凑布局）
///
/// - arena 存储 root 相对路径 bytes（不含 root 前缀）
/// - root_id 指向 `PersistentIndex.roots[root_id]`
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct CompactMeta {
    pub file_key: FileKey,
    pub root_id: u16,
    pub path_off: u32,
    pub path_len: u16,
    pub size: u64,
    pub mtime_ns: i64,
}

/// 旧快照格式 v2（兼容读取）
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct IndexSnapshotV2 {
    pub files: HashMap<FileKey, FileMeta>,
    pub path_to_id: HashMap<PathBuf, FileKey>,
    pub tombstones: HashSet<FileKey>,
}

impl IndexSnapshotV2 {
    pub fn new() -> Self {
        Self {
            files: HashMap::new(),
            path_to_id: HashMap::new(),
            tombstones: HashSet::new(),
        }
    }
}

/// 旧快照格式 v3（兼容读取）：不落盘 path_to_id（可从 files 重建）
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct IndexSnapshotV3 {
    pub files: HashMap<FileKey, FileMeta>,
    pub tombstones: HashSet<FileKey>,
}

impl IndexSnapshotV3 {
    pub fn new() -> Self {
        Self {
            files: HashMap::new(),
            tombstones: HashSet::new(),
        }
    }
}

/// 新快照格式 v4：落盘紧凑布局（arena + metas + tombstones）
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct IndexSnapshotV4 {
    pub arena: PathArena,
    pub metas: Vec<CompactMetaV4>,
    pub tombstones: Vec<u32>,
}

impl IndexSnapshotV4 {
    pub fn new() -> Self {
        Self {
            arena: PathArena::new(),
            metas: Vec::new(),
            tombstones: Vec::new(),
        }
    }
}

/// 新快照格式 v5：落盘紧凑布局（arena(root-relative) + metas(root_id+offset/len) + tombstones）
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct IndexSnapshotV5 {
    /// 用于校验：root 列表（含 "/" 兜底）是否与当前运行时一致
    pub roots_hash: u64,
    pub arena: PathArena,
    pub metas: Vec<CompactMeta>,
    pub tombstones: Vec<u32>,
}

impl IndexSnapshotV5 {
    pub fn new(roots_hash: u64) -> Self {
        Self {
            roots_hash,
            arena: PathArena::new(),
            metas: Vec::new(),
            tombstones: Vec::new(),
        }
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
    /// DocId -> FileEntry
    entries: RwLock<Vec<FileEntry>>,
    /// DocId -> absolute path bytes
    paths: RwLock<Vec<Vec<u8>>>,
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

    /// 脏标记（自上次快照后是否有变更）
    dirty: std::sync::atomic::AtomicBool,
    /// 阶段 2: ParentIndex，替代 for_each_live_meta_in_dirs
    parent_index: RwLock<Option<crate::index::parent_index::ParentIndex>>,
    /// 配套 PathTable，用于将 PathBuf 映射到 path_idx
    parent_path_table: RwLock<Option<RebuildPathTable>>,
}

impl Default for PersistentIndex {
    fn default() -> Self {
        Self::new()
    }
}

#[derive(Clone, Debug, Default)]
struct RebuildPathTable {
    path_to_id: HashMap<Vec<u8>, u32>,
    id_to_path: Vec<Vec<u8>>,
    dirs: HashSet<u32>,
}

impl RebuildPathTable {
    fn new() -> Self {
        Self::default()
    }

    fn intern(&mut self, path_bytes: Vec<u8>, is_dir: bool) -> u32 {
        if let Some(&id) = self.path_to_id.get(&path_bytes) {
            if is_dir {
                self.dirs.insert(id);
            }
            id
        } else {
            let id = self.id_to_path.len() as u32;
            self.path_to_id.insert(path_bytes.clone(), id);
            self.id_to_path.push(path_bytes);
            if is_dir {
                self.dirs.insert(id);
            }
            id
        }
    }

    fn lookup(&self, path_bytes: &[u8]) -> Option<u32> {
        self.path_to_id.get(path_bytes).copied()
    }
}

impl PathTableTrait for RebuildPathTable {
    fn parent_idx(&self, path_idx: u32) -> Option<u32> {
        let path = self.id_to_path.get(path_idx as usize)?;
        if path.as_slice() == b"/" {
            return None;
        }
        let parent_len = match path.iter().rposition(|&b| b == b'/') {
            Some(0) => 1,
            Some(pos) => pos,
            None => return None,
        };
        let parent_bytes = &path[..parent_len];
        self.path_to_id.get(parent_bytes).copied()
    }

    fn is_dir(&self, path_idx: u32) -> bool {
        self.dirs.contains(&path_idx)
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
            entries: RwLock::new(Vec::new()),
            paths: RwLock::new(Vec::new()),
            filekey_to_docid: RwLock::new(HashMap::new()),
            path_hash_to_id: RwLock::new(HashMap::new()),
            trigram_index: RwLock::new(HashMap::new()),
            short_component_index: RwLock::new(HashMap::new()),
            tombstones: RwLock::new(RoaringTreemap::new()),
            dirty: std::sync::atomic::AtomicBool::new(false),
            parent_index: RwLock::new(None),
            parent_path_table: RwLock::new(None),
        }
    }

    pub fn from_snapshot_v5(snap: IndexSnapshotV5, roots: Vec<PathBuf>) -> Self {
        let idx = Self::new_with_roots(roots);

        if snap.roots_hash != idx.roots_hash() {
            tracing::warn!(
                "Snapshot roots_hash mismatch, ignoring snapshot ({} != {})",
                snap.roots_hash,
                idx.roots_hash()
            );
            return idx;
        }

        {
            let mut entries = Vec::with_capacity(snap.metas.len());
            let mut paths = Vec::with_capacity(snap.metas.len());
            for (docid_usize, meta) in snap.metas.iter().enumerate() {
                let docid = docid_usize as u32;
                let abs_bytes = snap
                    .arena
                    .get_bytes(meta.path_off, meta.path_len)
                    .map(|rel| {
                        compose_abs_path_bytes(
                            root_bytes_for_id(&idx.roots_bytes, meta.root_id),
                            rel,
                        )
                    })
                    .unwrap_or_default();
                entries.push(FileEntry::from_file_key(
                    meta.file_key,
                    docid,
                    meta.size,
                    meta.mtime_ns,
                ));
                paths.push(abs_bytes);
            }
            *idx.entries.write() = entries;
            *idx.paths.write() = paths;
            *idx.tombstones.write() = snap.tombstones.into_iter().map(|v| v as u64).collect();
            idx.dirty.store(false, std::sync::atomic::Ordering::Release);
        }

        // rebuild derived indexes
        idx.rebuild_derived_indexes();
        idx
    }

    pub fn from_snapshot_v4(snap: IndexSnapshotV4, roots: Vec<PathBuf>) -> Self {
        // v4 arena 里存的是绝对路径字节；这里迁移为 v5（root-relative + root_id）
        let idx = Self::new_with_roots(roots);

        let IndexSnapshotV4 {
            arena: old_arena,
            metas: old_metas,
            tombstones,
        } = snap;

        let mut entries: Vec<FileEntry> = Vec::with_capacity(old_metas.len());
        let mut paths: Vec<Vec<u8>> = Vec::with_capacity(old_metas.len());

        for m in old_metas {
            let abs_path = old_arena.get_path_buf(m.path_off, m.path_len);
            let Some(abs_path) = abs_path else {
                continue;
            };
            let docid = entries.len() as u32;
            let mtime_ns = mtime_to_ns(m.mtime);
            entries.push(FileEntry::from_file_key(
                m.file_key, docid, m.size, mtime_ns,
            ));
            paths.push(abs_path.as_os_str().as_encoded_bytes().to_vec());
        }

        {
            *idx.entries.write() = entries;
            *idx.paths.write() = paths;
            *idx.tombstones.write() = tombstones.into_iter().map(|v| v as u64).collect();
            idx.dirty.store(false, std::sync::atomic::Ordering::Release);
        }

        idx.rebuild_derived_indexes();
        idx
    }

    pub fn from_snapshot_v3(snap: IndexSnapshotV3, roots: Vec<PathBuf>) -> Self {
        // v3 的 tombstones 不携带对应文档记录；阶段 A 的 DocId tombstone 以“保留 doc 槽位”实现，
        // 因此这里仅重建 files，本质上等价于“干净加载”。
        let idx = Self::new_with_roots(roots);
        for (_k, meta) in snap.files {
            idx.upsert(meta);
        }
        idx
    }

    pub fn from_snapshot_v2(snap: IndexSnapshotV2, roots: Vec<PathBuf>) -> Self {
        let idx = Self::new_with_roots(roots);
        for (_k, meta) in snap.files {
            idx.upsert(meta);
        }
        idx
    }

    fn rebuild_derived_indexes(&self) {
        let entries = self.entries.read();
        let paths = self.paths.read();
        let tomb = self.tombstones.read();

        let mut filekey_to_docid = self.filekey_to_docid.write();
        let mut path_hash_to_id = self.path_hash_to_id.write();
        let mut trigram_index = self.trigram_index.write();
        let mut short_component_index = self.short_component_index.write();

        filekey_to_docid.clear();
        path_hash_to_id.clear();
        trigram_index.clear();
        short_component_index.clear();

        for (docid_usize, entry) in entries.iter().enumerate() {
            let docid: DocId = docid_usize as DocId;

            if tomb.contains(docid) {
                continue;
            }

            filekey_to_docid.insert(entry.file_key(), docid);

            let Some(abs_bytes) = paths.get(docid_usize) else {
                continue;
            };
            if !abs_bytes.is_empty() {
                let h = path_hash_bytes(&abs_bytes);
                path_hash_to_id
                    .entry(h)
                    .and_modify(|v| v.insert(docid))
                    .or_insert(OneOrManyDocId::One(docid));

                let abs_path = pathbuf_from_encoded_vec(abs_bytes.clone());
                for_each_component_trigram(abs_path.as_path(), |tri| {
                    trigram_index.entry(tri).or_default().insert(docid);
                });
                for_each_short_component(abs_path.as_path(), |component| {
                    short_component_index
                        .entry(component)
                        .or_default()
                        .insert(docid);
                });
            }
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
        let new_abs_bytes = meta.path.as_os_str().as_encoded_bytes().to_vec();
        let new_mtime_ns = mtime_to_ns(meta.mtime);

        // 先查 docid（只持有 mapping 的读锁）
        let existing_docid = { self.filekey_to_docid.read().get(&fkey).copied() };

        if let Some(docid) = existing_docid {
            // 读旧路径 bytes（不持有 trigram/path_hash 锁）
            let old_path_bytes = { self.paths.read().get(docid as usize).cloned() };
            let same_path = old_path_bytes
                .as_deref()
                .map(|old| old == new_abs_bytes.as_slice())
                .unwrap_or(false);

            if same_path {
                // 同路径重复上报：只更新元数据，避免 posting 重复写入
                self.update_entry_metadata(docid, meta.size, new_mtime_ns);
                self.dirty.store(true, std::sync::atomic::Ordering::Release);
                return;
            }

            let old_path_missing = if force_path_update {
                false
            } else {
                old_path_bytes
                    .as_ref()
                    .map(|old| pathbuf_from_encoded_vec(old.clone()))
                    .map(|old_path| match std::fs::symlink_metadata(&old_path) {
                        Ok(_) => false,
                        Err(err) if err.kind() == std::io::ErrorKind::NotFound => true,
                        Err(_) => false,
                    })
                    .unwrap_or(false)
            };

            // 路径不同：hardlink、rename，或旧路径已消失后的 reconcile
            if !force_path_update && !old_path_missing {
                // hardlink/重复发现：保留旧路径，仅更新元数据
                self.update_entry_metadata(docid, meta.size, new_mtime_ns);
                self.dirty.store(true, std::sync::atomic::Ordering::Release);
                return;
            }

            // rename：先移除旧路径关联
            if let Some(old_path_bytes) = old_path_bytes {
                let old_path = pathbuf_from_encoded_vec(old_path_bytes);
                self.remove_trigrams(docid, &old_path);
                self.remove_path_hash(docid, &old_path);
            };

            // posting/path_hash 先写（与 query 锁顺序一致：trigram -> entries/paths）
            self.insert_trigrams(docid, meta.path.as_path());
            self.insert_path_hash(docid, meta.path.as_path());

            if !self.update_entry_path(docid, &new_abs_bytes, meta.size, new_mtime_ns) {
                // 极端情况：docid 槽位不存在，降级为 append
                if let Some(docid_new) =
                    self.alloc_docid(fkey, &new_abs_bytes, meta.size, new_mtime_ns)
                {
                    self.insert_trigrams(docid_new, meta.path.as_path());
                    self.insert_path_hash(docid_new, meta.path.as_path());
                }
            }

            // rename 视为“存在且活跃”
            self.tombstones.write().remove(docid);
            self.dirty.store(true, std::sync::atomic::Ordering::Release);
            return;
        }

        // 新文件：分配 docid 并写入
        let Some(docid) = self.alloc_docid(fkey, &new_abs_bytes, meta.size, new_mtime_ns) else {
            return;
        };
        self.insert_trigrams(docid, meta.path.as_path());
        self.insert_path_hash(docid, meta.path.as_path());
        self.dirty.store(true, std::sync::atomic::Ordering::Release);
    }

    fn alloc_docid(
        &self,
        file_key: FileKey,
        abs_path_bytes: &[u8],
        size: u64,
        mtime_ns: i64,
    ) -> Option<DocId> {
        let mut entries = self.entries.write();
        let docid: DocId = entries.len() as DocId;
        let path_idx: u32 = docid.try_into().ok()?;
        entries.push(FileEntry::from_file_key(file_key, path_idx, size, mtime_ns));
        self.paths.write().push(abs_path_bytes.to_vec());

        self.filekey_to_docid.write().insert(file_key, docid);
        self.tombstones.write().remove(docid);
        Some(docid)
    }

    /// 标记删除（tombstone）
    pub fn mark_deleted(&self, file_key: FileKey) {
        let docid = { self.filekey_to_docid.read().get(&file_key).copied() };
        let Some(docid) = docid else {
            return;
        };

        let path = { self.path_buf_for_docid(docid) };

        // Atomicity: mark tombstone first so queries see deleted before trigrams are removed.
        self.filekey_to_docid.write().remove(&file_key);
        self.tombstones.write().insert(docid);
        self.dirty.store(true, std::sync::atomic::Ordering::Release);

        if let Some(p) = path {
            self.remove_trigrams(docid, &p);
            self.remove_path_hash(docid, &p);
        }
    }

    /// 按路径删除
    pub fn mark_deleted_by_path(&self, path: &Path) {
        if let Some(docid) = self.lookup_docid_by_path(path) {
            let file_key = {
                let entries = self.entries.read();
                entries.get(docid as usize).map(|e| e.file_key())
            };
            if let Some(k) = file_key {
                self.mark_deleted(k);
            }
        }
    }

    /// 查询：trigram 候选集（Roaring 交集）→ 精确过滤
    pub fn query(&self, matcher: &dyn Matcher, limit: usize) -> Vec<FileMeta> {
        // 重要：先读取 trigram_index 计算候选集，再读取 entries/tombstones/paths。
        // 写入路径通常是先更新 trigram_index 再更新 entries，如果这里反过来拿锁，
        // 在“边写边查”场景下可能形成死锁。
        let candidates = self
            .trigram_candidates(matcher)
            .or_else(|| self.short_hint_candidates(matcher));

        let entries = self.entries.read();
        let paths = self.paths.read();
        let tombstones = self.tombstones.read();

        match candidates {
            Some(bitmap) => bitmap
                .iter()
                .filter(|docid| !tombstones.contains(*docid))
                .filter_map(|docid| {
                    let entry = entries.get(docid as usize)?;
                    let path_bytes = paths.get(docid as usize)?;
                    Some((entry, path_bytes))
                })
                .filter(|(_, path_bytes)| {
                    let s = std::str::from_utf8(path_bytes)
                        .map(std::borrow::Cow::Borrowed)
                        .unwrap_or_else(|_| String::from_utf8_lossy(path_bytes));
                    matcher.matches(&s)
                })
                .map(|(entry, path_bytes)| {
                    Self::meta_from_entry_and_path(entry, path_bytes.as_slice())
                })
                .take(limit)
                .collect(),
            None => {
                // 无法用 trigram 加速（查询词太短），全量过滤
                entries
                    .iter()
                    .enumerate()
                    .filter_map(|(i, entry)| {
                        let docid: DocId = i as DocId;
                        if tombstones.contains(docid) {
                            return None;
                        }
                        let path_bytes = paths.get(i)?;
                        let s = std::str::from_utf8(path_bytes)
                            .map(std::borrow::Cow::Borrowed)
                            .unwrap_or_else(|_| String::from_utf8_lossy(path_bytes));
                        if matcher.matches(&s) {
                            Some(Self::meta_from_entry_and_path(entry, path_bytes))
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
    pub fn for_each_live_meta(&self, mut f: impl FnMut(FileMeta)) {
        let entries = self.entries.read();
        let paths = self.paths.read();
        let tombstones = self.tombstones.read();

        for (i, entry) in entries.iter().enumerate() {
            let docid: DocId = i as DocId;
            if tombstones.contains(docid) {
                continue;
            }
            let Some(path_bytes) = paths.get(i) else {
                continue;
            };
            f(Self::meta_from_entry_and_path(entry, path_bytes));
        }
    }

    /// 构建/重建 ParentIndex
    pub fn rebuild_parent_index(&self) {
        let mut path_table = RebuildPathTable::new();
        let mut entries: Vec<(u32, u64)> = Vec::new();

        let paths = self.paths.read();
        let tombstones = self.tombstones.read();

        for (i, abs) in paths.iter().enumerate() {
            let doc_id = i as DocId;
            if tombstones.contains(doc_id) {
                continue;
            }
            let path_idx = path_table.intern(abs.to_vec(), false);
            entries.push((path_idx, doc_id as u64));
        }

        // Also intern root directories so parent lookup terminates correctly
        for root in &self.roots_bytes {
            if !root.is_empty() {
                let _ = path_table.intern(root.clone(), true);
            }
        }

        let new_index =
            crate::index::parent_index::ParentIndex::build_from_entries(&entries, &path_table);
        *self.parent_index.write() = Some(new_index);
        *self.parent_path_table.write() = Some(path_table);
    }

    /// 使用 ParentIndex 的删除对齐
    pub fn delete_alignment_with_parent_index(
        &self,
        dirty_dirs: &std::collections::HashSet<PathBuf>,
    ) -> Vec<(DocId, PathBuf)> {
        let parent_idx = self.parent_index.read();
        let path_table = self.parent_path_table.read();
        if let (Some(ref index), Some(ref pt)) = (&*parent_idx, &*path_table) {
            let mut dir_idxs = Vec::new();
            for dir in dirty_dirs {
                let dir_bytes = dir.as_os_str().as_encoded_bytes().to_vec();
                if let Some(idx) = pt.lookup(&dir_bytes) {
                    dir_idxs.push(idx);
                }
            }
            let to_check = index.files_in_dirs(&dir_idxs);
            let mut result = Vec::new();
            let paths = self.paths.read();
            for doc_id in to_check {
                let Some(path_bytes) = paths.get(doc_id as usize) else {
                    continue;
                };
                let path = pathbuf_from_encoded_vec(path_bytes.clone());
                result.push((doc_id as u64, path));
            }
            result
        } else {
            Vec::new()
        }
    }

    /// 使用 ParentIndex 查询某目录下的文件候选（Query 加速）
    pub fn parent_candidates(&self, parent_path: &str) -> Vec<FileKey> {
        let parent_idx = self.parent_index.read();
        let path_table = self.parent_path_table.read();
        let (index, pt) = match (parent_idx.as_ref(), path_table.as_ref()) {
            (Some(i), Some(p)) => (i, p),
            _ => return Vec::new(),
        };
        let parent_bytes = PathBuf::from(parent_path)
            .as_os_str()
            .as_encoded_bytes()
            .to_vec();
        let parent_idx = match pt.lookup(&parent_bytes) {
            Some(idx) => idx,
            None => return Vec::new(),
        };
        let bitmap = match index.files_in_dir(parent_idx) {
            Some(b) => b,
            None => return Vec::new(),
        };

        let entries = self.entries.read();
        let mut keys = Vec::with_capacity(bitmap.len());
        for &doc_id in bitmap {
            if let Some(entry) = entries.get(doc_id as usize) {
                keys.push(entry.file_key());
            }
        }
        keys
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
        let docid = { self.filekey_to_docid.read().get(&fk).copied()? };
        self.path_buf_for_docid(docid)
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
            self.filekey_to_docid.read().get(&fk).copied()
        } else {
            from_best_path
                .as_deref()
                .and_then(|p| self.lookup_docid_by_path(p))
        };

        if let Some(docid) = docid_opt {
            if let Some(old_path) = self.path_buf_for_docid(docid) {
                self.remove_trigrams(docid, &old_path);
                self.remove_path_hash(docid, &old_path);
            } else if let Some(ref path) = from_best_path {
                self.remove_trigrams(docid, path);
                self.remove_path_hash(docid, path);
            }

            if let Some(ref to_path) = to_path {
                let to_path_owned = to_path.clone();
                let (size, mtime_ns) = if let Some(meta) = to_meta {
                    (meta.size, mtime_to_ns(meta.mtime))
                } else {
                    self.entry_size_mtime(docid).unwrap_or((0, -1))
                };
                self.insert_trigrams(docid, &*to_path_owned);
                self.insert_path_hash(docid, &*to_path_owned);
                let abs_path_bytes = to_path_owned.as_os_str().as_encoded_bytes().to_vec();
                self.update_entry_path(docid, &abs_path_bytes, size, mtime_ns);
            } else if let Some(meta) = fallback_meta {
                self.update_entry_metadata(docid, meta.size, mtime_to_ns(meta.mtime));
                if let Some(old_path) = self.path_buf_for_docid(docid) {
                    self.insert_trigrams(docid, &old_path);
                    self.insert_path_hash(docid, &old_path);
                }
            } else {
                // 没有新路径时恢复旧路径倒排，避免仅凭 FID rename 事件造成误删。
                if let Some(old_path) = self.path_buf_for_docid(docid) {
                    self.insert_trigrams(docid, &old_path);
                    self.insert_path_hash(docid, &old_path);
                }
            }

            if (self.entries.read().get(docid as usize)).is_some() {
                self.tombstones.write().remove(docid);
                self.dirty.store(true, std::sync::atomic::Ordering::Release);
            }
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

    /// 导出 v5 快照数据
    pub fn export_snapshot_v5(&self) -> IndexSnapshotV5 {
        let (arena, metas) = self.build_legacy_metas(false);
        let tombstones = self
            .tombstones
            .read()
            .iter()
            .map(|v| v as u32)
            .collect::<Vec<u32>>();
        self.dirty
            .store(false, std::sync::atomic::Ordering::Release);
        IndexSnapshotV5 {
            roots_hash: self.roots_hash(),
            arena,
            metas,
            tombstones,
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

        let (arena, metas) = self.build_legacy_metas(false);

        // PathArena 段：raw bytes（root-relative）
        let path_arena_bytes = Arc::clone(&arena.data);

        // Metas 段：按 DocId 顺序顺排，固定记录大小（little-endian）
        //
        // MetaRecordV6:
        //   dev u64
        //   ino u64
        //   root_id u16
        //   path_off u32
        //   path_len u16
        //   size u64
        //   mtime_unix_ns i64 (-1 表示 None)
        let mut metas_bytes = Vec::with_capacity(metas.len() * 40);
        for m in metas.iter() {
            metas_bytes.extend_from_slice(&m.file_key.dev.to_le_bytes());
            metas_bytes.extend_from_slice(&m.file_key.ino.to_le_bytes());
            metas_bytes.extend_from_slice(&m.root_id.to_le_bytes());
            metas_bytes.extend_from_slice(&m.path_off.to_le_bytes());
            metas_bytes.extend_from_slice(&m.path_len.to_le_bytes());
            metas_bytes.extend_from_slice(&m.size.to_le_bytes());
            metas_bytes.extend_from_slice(&m.mtime_ns.to_le_bytes());
        }

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
        // 注意：来源为 filekey_to_docid（天然排除 tombstone）。
        let mut pairs: Vec<(FileKey, DocId)> = {
            let m = self.filekey_to_docid.read();
            m.iter().map(|(k, v)| (*k, *v)).collect()
        };
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

        let (arena, metas) = self.build_legacy_metas(false);

        // PathArena 段：raw bytes（root-relative）
        Self::write_segment(writer, arena.data.as_ref())?;

        // Metas 段：按 DocId 顺序顺排，固定记录大小（little-endian）
        let mut metas_bytes = Vec::with_capacity(metas.len() * 40);
        for m in metas.iter() {
            metas_bytes.extend_from_slice(&m.file_key.dev.to_le_bytes());
            metas_bytes.extend_from_slice(&m.file_key.ino.to_le_bytes());
            metas_bytes.extend_from_slice(&m.root_id.to_le_bytes());
            metas_bytes.extend_from_slice(&m.path_off.to_le_bytes());
            metas_bytes.extend_from_slice(&m.path_len.to_le_bytes());
            metas_bytes.extend_from_slice(&m.size.to_le_bytes());
            metas_bytes.extend_from_slice(&m.mtime_ns.to_le_bytes());
        }
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

        // FileKeyMap 段
        let mut pairs: Vec<(FileKey, DocId)> = {
            let m = self.filekey_to_docid.read();
            m.iter().map(|(k, v)| (*k, *v)).collect()
        };
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
        let total = self.entries.read().len();
        let tomb = self.tombstones.read().len() as usize;
        total.saturating_sub(tomb)
    }

    /// 内存占用统计（粗估）
    pub fn memory_stats(&self) -> L2Stats {
        use std::mem::size_of;

        let entries = self.entries.read();
        let paths = self.paths.read();
        let filekey_to_docid = self.filekey_to_docid.read();
        let path_hash_to_id = self.path_hash_to_id.read();
        let trigram_index = self.trigram_index.read();
        let short_component_index = self.short_component_index.read();
        let tombstones = self.tombstones.read();

        let total_docs = entries.len();
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

        // entries: Vec<FileEntry>
        let metas_bytes = entries.capacity() as u64 * size_of::<FileEntry>() as u64
            + size_of::<Vec<FileEntry>>() as u64;

        // mapping: HashMap<FileKey, DocId>
        let map_entry_bytes = size_of::<(FileKey, DocId)>() as u64;
        let filekey_to_docid_bytes = filekey_to_docid.len() as u64 * (map_entry_bytes + 1)
            + size_of::<HashMap<FileKey, DocId>>() as u64;

        // paths: Vec<Vec<u8>>
        let mut arena_bytes = paths.capacity() as u64 * size_of::<Vec<u8>>() as u64
            + size_of::<Vec<Vec<u8>>>() as u64;
        for path in paths.iter() {
            arena_bytes += path.capacity() as u64 + size_of::<Vec<u8>>() as u64;
        }

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
            metas_capacity: entries.capacity(),
            filekey_to_docid_capacity: filekey_to_docid.len(),
            path_hash_to_id_capacity: path_hash_to_id.len(),
            trigram_index_capacity: trigram_index.capacity(),
            arena_capacity: paths.iter().map(|p| p.capacity()).sum(),

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
        self.tombstones.write().clear();
        self.entries.write().clear();
        self.paths.write().clear();
        self.dirty.store(true, std::sync::atomic::Ordering::Release);
    }

    // ── 内部方法 ──

    fn roots_hash(&self) -> u64 {
        // 稳定哈希：按 root bytes 顺序（含 "/" 兜底 + 其余 root 的排序规则）拼接后 hash。
        // 目的：避免 root 顺序变化导致 root_id 解释错位。
        let mut hasher = std::collections::hash_map::DefaultHasher::new();
        for b in &self.roots_bytes {
            b.hash(&mut hasher);
            0xff_u8.hash(&mut hasher); // 分隔符，避免 ["ab","c"] 与 ["a","bc"] 冲突
        }
        hasher.finish()
    }

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

    fn meta_from_entry_and_path(entry: &FileEntry, path_bytes: &[u8]) -> FileMeta {
        FileMeta {
            file_key: entry.file_key(),
            path: pathbuf_from_encoded_vec(path_bytes.to_vec()),
            size: entry.size,
            mtime: mtime_from_ns(entry.mtime_ns),
            ctime: None,
            atime: None,
        }
    }

    fn path_buf_for_docid(&self, docid: DocId) -> Option<PathBuf> {
        let paths = self.paths.read();
        paths
            .get(docid as usize)
            .map(|bytes| pathbuf_from_encoded_vec(bytes.clone()))
    }

    fn entry_size_mtime(&self, docid: DocId) -> Option<(u64, i64)> {
        let entries = self.entries.read();
        let entry = entries.get(docid as usize)?;
        Some((entry.size, entry.mtime_ns))
    }

    fn update_entry_metadata(&self, docid: DocId, size: u64, mtime_ns: i64) -> bool {
        let mut entries = self.entries.write();
        let Some(entry) = entries.get_mut(docid as usize) else {
            return false;
        };
        entry.size = size;
        entry.mtime_ns = mtime_ns;
        true
    }

    fn update_entry_path(
        &self,
        docid: DocId,
        abs_path_bytes: &[u8],
        size: u64,
        mtime_ns: i64,
    ) -> bool {
        {
            let mut entries = self.entries.write();
            let Some(entry) = entries.get_mut(docid as usize) else {
                return false;
            };
            entry.path_idx = match docid.try_into() {
                Ok(path_idx) => path_idx,
                Err(_) => return false,
            };
            entry.size = size;
            entry.mtime_ns = mtime_ns;
        }
        let mut paths = self.paths.write();
        let Some(path) = paths.get_mut(docid as usize) else {
            return false;
        };
        *path = abs_path_bytes.to_vec();
        true
    }

    fn build_legacy_metas(&self, live_only: bool) -> (PathArena, Vec<CompactMeta>) {
        let entries = self.entries.read();
        let paths = self.paths.read();
        let tombstones = self.tombstones.read();
        let mut arena = PathArena::new();
        let mut metas = Vec::with_capacity(entries.len());

        for (docid, entry) in entries.iter().enumerate() {
            if live_only && tombstones.contains(docid as DocId) {
                continue;
            }
            let path = paths
                .get(docid)
                .map(|bytes| pathbuf_from_encoded_vec(bytes.clone()))
                .unwrap_or_default();
            let (root_id, rel_bytes) = self.split_root_relative_bytes(path.as_path());
            let (path_off, path_len) = arena.push_bytes(&rel_bytes).unwrap_or((0, 0));
            metas.push(CompactMeta {
                file_key: entry.file_key(),
                root_id,
                path_off,
                path_len,
                size: entry.size,
                mtime_ns: entry.mtime_ns,
            });
        }

        (arena, metas)
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

    fn insert_path_hash(&self, docid: DocId, path: &Path) {
        let bytes = path.as_os_str().as_encoded_bytes();
        let h = path_hash_bytes(bytes);
        let mut map = self.path_hash_to_id.write();
        map.entry(h)
            .and_modify(|v| v.insert(docid))
            .or_insert(OneOrManyDocId::One(docid));
    }

    fn remove_path_hash(&self, docid: DocId, path: &Path) {
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

    fn lookup_docid_by_path(&self, path: &Path) -> Option<DocId> {
        let bytes = path.as_os_str().as_encoded_bytes();
        let h = path_hash_bytes(bytes);

        // 先复制候选 DocId（避免同时持有 path_hash_to_id 与 paths 的锁）
        let candidates: Vec<DocId> = {
            let map = self.path_hash_to_id.read();
            let v = map.get(&h)?;
            v.iter().copied().collect()
        };

        if candidates.is_empty() {
            return None;
        }

        let paths = self.paths.read();
        candidates.into_iter().find(|docid| {
            paths
                .get(*docid as usize)
                .map(|path_bytes| path_bytes.as_slice() == bytes)
                .unwrap_or(false)
        })
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

    pub fn to_base_index_data(&self) -> crate::index::base_index::BaseIndexData {
        let entries_v2 = self.entries.read();
        let paths_v2 = self.paths.read();
        let tombstones = self.tombstones.read();
        let trigram_index = self.trigram_index.read();

        let mut rebuild_path_table = RebuildPathTable::new();
        for root in &self.roots_bytes {
            if !root.is_empty() {
                let _ = rebuild_path_table.intern(root.clone(), true);
            }
        }
        let mut entry_path_idxs: Vec<u32> = Vec::with_capacity(entries_v2.len());
        let mut parent_entries: Vec<(u32, u64)> = Vec::with_capacity(entries_v2.len());

        for (docid_usize, abs_bytes) in paths_v2.iter().enumerate() {
            intern_parent_dirs(&mut rebuild_path_table, abs_bytes);
            let path_idx = rebuild_path_table.intern(abs_bytes.clone(), false);
            entry_path_idxs.push(path_idx);
            let docid = docid_usize as DocId;
            if !tombstones.contains(docid) {
                parent_entries.push((path_idx, docid as u64));
            }
        }

        let mut path_table_builder = crate::index::path_table_v2::PathTableBuilder::with_capacity(
            rebuild_path_table.id_to_path.len(),
        );
        for (idx, path_bytes) in rebuild_path_table.id_to_path.iter().enumerate() {
            path_table_builder.push(idx as u32, path_bytes);
        }
        let mut entry_index =
            crate::index::file_entry_v2::FileEntryIndex::with_capacity(entries_v2.len());

        for (docid_usize, entry) in entries_v2.iter().enumerate() {
            let Some(&path_idx) = entry_path_idxs.get(docid_usize) else {
                continue;
            };
            let new_entry = crate::index::file_entry_v2::FileEntry::from_file_key(
                entry.file_key(),
                path_idx,
                entry.size,
                entry.mtime_ns,
            );
            entry_index.push(new_entry);
        }

        let path_table = path_table_builder.build();
        let entries_by_key = entry_index.build();
        let parent_index = crate::index::parent_index::ParentIndex::build_from_entries(
            &parent_entries,
            &rebuild_path_table,
        );

        let mut tri = crate::index::base_index::TrigramIndex::new();
        for (trigram, posting) in trigram_index.iter() {
            let bitmap: roaring::RoaringBitmap = posting.iter().map(|v| v as u32).collect();
            tri.insert(*trigram, bitmap);
        }

        let tombstones_bitmap: roaring::RoaringBitmap =
            tombstones.iter().map(|v| v as u32).collect();

        crate::index::base_index::BaseIndexData {
            path_table,
            entries_by_key,
            trigram_index: tri,
            parent_index,
            tombstones: tombstones_bitmap,
        }
    }
}

fn intern_parent_dirs(path_table: &mut RebuildPathTable, path: &[u8]) {
    let mut end = match path.iter().rposition(|&b| b == b'/') {
        Some(0) => {
            let _ = path_table.intern(b"/".to_vec(), true);
            return;
        }
        Some(pos) => pos,
        None => return,
    };

    loop {
        if end == 0 {
            let _ = path_table.intern(b"/".to_vec(), true);
            break;
        }
        let _ = path_table.intern(path[..end].to_vec(), true);
        end = match path[..end].iter().rposition(|&b| b == b'/') {
            Some(0) => 0,
            Some(pos) => pos,
            None => break,
        };
    }
}

impl IndexLayer for PersistentIndex {
    fn query_keys(&self, matcher: &dyn Matcher) -> Vec<FileKey> {
        // 复用 L2 的 trigram 候选集计算，但只输出稳定身份（FileKey）。
        let candidates = self
            .trigram_candidates(matcher)
            .or_else(|| self.short_hint_candidates(matcher));

        let entries = self.entries.read();
        let paths = self.paths.read();
        let tombstones = self.tombstones.read();

        let mut out: Vec<FileKey> = Vec::new();

        match candidates {
            Some(bitmap) => {
                for docid in bitmap.iter() {
                    if tombstones.contains(docid) {
                        continue;
                    }
                    let Some(entry) = entries.get(docid as usize) else {
                        continue;
                    };
                    let Some(path_bytes) = paths.get(docid as usize) else {
                        continue;
                    };
                    let s = std::str::from_utf8(path_bytes)
                        .map(std::borrow::Cow::Borrowed)
                        .unwrap_or_else(|_| String::from_utf8_lossy(path_bytes));
                    if matcher.matches(&s) {
                        out.push(entry.file_key());
                    }
                }
            }
            None => {
                // 无法用 trigram 加速（查询词太短），全量过滤（不构造 PathBuf）。
                for (i, entry) in entries.iter().enumerate() {
                    let docid: DocId = i as DocId;
                    if tombstones.contains(docid) {
                        continue;
                    }
                    let Some(path_bytes) = paths.get(i) else {
                        continue;
                    };
                    let s = std::str::from_utf8(path_bytes)
                        .map(std::borrow::Cow::Borrowed)
                        .unwrap_or_else(|_| String::from_utf8_lossy(path_bytes));
                    if matcher.matches(&s) {
                        out.push(entry.file_key());
                    }
                }
            }
        }

        out
    }

    fn get_meta(&self, key: FileKey) -> Option<FileMeta> {
        let docid = { self.filekey_to_docid.read().get(&key).copied()? };
        if self.tombstones.read().contains(docid) {
            return None;
        }
        let entries = self.entries.read();
        let paths = self.paths.read();
        let entry = entries.get(docid as usize)?;
        let path_bytes = paths.get(docid as usize)?;
        Some(PersistentIndex::meta_from_entry_and_path(entry, path_bytes))
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
    fn overlong_new_paths_are_indexed_in_runtime_paths_store() {
        let idx = PersistentIndex::new();
        let path = PathBuf::from(format!("/tmp/{}", "a".repeat(u16::MAX as usize + 1)));
        idx.upsert(FileMeta {
            file_key: FileKey {
                dev: 1,
                ino: 1,
                generation: 0,
            },
            path: path.clone(),
            size: 1,
            mtime: None,
            ctime: None,
            atime: None,
        });

        assert_eq!(idx.file_count(), 1);
        let m = create_matcher("aaaa", true);
        let results = idx.query(m.as_ref(), 100);
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].path, path);
    }

    #[test]
    fn rename_to_overlong_path_keeps_entry_live() {
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
            path: long_path.clone(),
            size: 2,
            mtime: None,
            ctime: None,
            atime: None,
        });

        assert_eq!(idx.file_count(), 1);
        let m = create_matcher("short-name", true);
        assert!(idx.query(m.as_ref(), 100).is_empty());
        let m = create_matcher("bbbb", true);
        let results = idx.query(m.as_ref(), 100);
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].path, long_path);
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
}
