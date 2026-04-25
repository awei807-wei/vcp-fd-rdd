use parking_lot::RwLock;
use roaring::RoaringTreemap;
use serde::{Deserialize, Serialize};
use std::borrow::Cow;
use std::collections::{BTreeMap, HashMap, HashSet};
use std::hash::{Hash, Hasher};
use std::path::{Component, Path, PathBuf};
use std::sync::Arc;

#[cfg(feature = "rkyv")]
use crate::core::FileKeyEntry;
use crate::core::{EventRecord, EventType, FileIdentifier, FileKey, FileMeta};
use crate::index::IndexLayer;
use crate::query::matcher::Matcher;
use crate::stats::L2Stats;
use crate::util::{
    compose_abs_path_buf, compose_abs_path_bytes, pathbuf_from_encoded_vec, root_bytes_for_id,
};

/// Trigram：3 字节子串，用于倒排索引加速查询
type Trigram = [u8; 3];

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

fn short_component_matches(component: &[u8], hint: &[u8]) -> bool {
    if hint.is_empty() || component.len() < hint.len() {
        return false;
    }
    component.windows(hint.len()).any(|window| window == hint)
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

fn for_each_short_component(path: &Path, mut f: impl FnMut(&[u8])) {
    for c in path.components() {
        let Component::Normal(os) = c else {
            continue;
        };
        let lower = os.to_string_lossy().to_lowercase();
        let bytes = lower.as_bytes();
        if (1..=2).contains(&bytes.len()) {
            f(bytes);
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
    /// DocId -> CompactMeta
    metas: RwLock<Vec<CompactMeta>>,
    /// FileKey -> DocId
    filekey_to_docid: RwLock<BTreeMap<FileKey, DocId>>,
    /// 路径 blob
    arena: RwLock<PathArena>,

    /// 路径反查：hash(path_bytes) -> DocId（或少量冲突列表）
    path_hash_to_id: RwLock<BTreeMap<u64, OneOrManyDocId>>,

    /// Trigram 倒排索引：trigram -> RoaringTreemap(DocId)
    trigram_index: RwLock<HashMap<Trigram, RoaringTreemap>>,
    /// 短组件索引：长度 1-2 的标准化路径组件 -> RoaringTreemap(DocId)
    short_component_index: RwLock<HashMap<Box<[u8]>, RoaringTreemap>>,

    /// 墓碑标记（DocId）
    tombstones: RwLock<RoaringTreemap>,

    /// upsert 写锁：保护 alloc_docid → insert_trigrams / insert_path_hash 的原子性，
    /// 防止写入-查询竞态导致 trigram 索引与 metas 不一致。
    upsert_lock: RwLock<()>,

    /// 脏标记（自上次快照后是否有变更）
    dirty: std::sync::atomic::AtomicBool,
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
            metas: RwLock::new(Vec::new()),
            filekey_to_docid: RwLock::new(BTreeMap::new()),
            arena: RwLock::new(PathArena::new()),
            path_hash_to_id: RwLock::new(BTreeMap::new()),
            trigram_index: RwLock::new(HashMap::new()),
            short_component_index: RwLock::new(HashMap::new()),
            tombstones: RwLock::new(RoaringTreemap::new()),
            upsert_lock: RwLock::new(()),
            dirty: std::sync::atomic::AtomicBool::new(false),
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
            *idx.metas.write() = snap.metas;
            *idx.arena.write() = snap.arena;
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

        let mut new_arena = PathArena::new();
        let mut new_metas: Vec<CompactMeta> = Vec::with_capacity(old_metas.len());

        for m in old_metas {
            let abs_path = old_arena.get_path_buf(m.path_off, m.path_len);
            let Some(abs_path) = abs_path else {
                continue;
            };
            let (root_id, rel_bytes) = idx.split_root_relative_bytes(&abs_path);
            let Some((off, len)) = new_arena.push_bytes(&rel_bytes) else {
                continue;
            };
            new_metas.push(CompactMeta {
                file_key: m.file_key,
                root_id,
                path_off: off,
                path_len: len,
                size: m.size,
                mtime_ns: mtime_to_ns(m.mtime),
            });
        }

        {
            *idx.metas.write() = new_metas;
            *idx.arena.write() = new_arena;
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
        let metas = self.metas.read();
        let arena = self.arena.read();
        let tomb = self.tombstones.read();

        let mut filekey_to_docid = self.filekey_to_docid.write();
        let mut path_hash_to_id = self.path_hash_to_id.write();
        let mut trigram_index = self.trigram_index.write();
        let mut short_component_index = self.short_component_index.write();

        filekey_to_docid.clear();
        path_hash_to_id.clear();
        trigram_index.clear();
        short_component_index.clear();

        for (docid_usize, meta) in metas.iter().enumerate() {
            let docid: DocId = docid_usize as DocId;
            filekey_to_docid.insert(meta.file_key, docid);

            if tomb.contains(docid) {
                continue;
            }

            if let Some(rel_bytes) = arena.get_bytes(meta.path_off, meta.path_len) {
                let abs_bytes = compose_abs_path_bytes(
                    root_bytes_for_id(&self.roots_bytes, meta.root_id),
                    rel_bytes,
                );
                let h = path_hash_bytes(&abs_bytes);
                path_hash_to_id
                    .entry(h)
                    .and_modify(|v| v.insert(docid))
                    .or_insert(OneOrManyDocId::One(docid));

                let abs_path = compose_abs_path_buf(
                    root_bytes_for_id(&self.roots_bytes, meta.root_id),
                    rel_bytes,
                );
                for_each_component_trigram(abs_path.as_path(), |tri| {
                    trigram_index.entry(tri).or_default().insert(docid);
                });
                for_each_short_component(abs_path.as_path(), |component| {
                    short_component_index
                        .entry(Box::<[u8]>::from(component))
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
        let (new_root_id, new_rel_bytes) = self.split_root_relative_bytes(meta.path.as_path());

        // 先查 docid（只持有 mapping 的读锁）
        let existing_docid = { self.filekey_to_docid.read().get(&fkey).copied() };

        if let Some(docid) = existing_docid {
            // 读旧路径 bytes（不持有 trigram/path_hash 锁）
            let (old_root_id, old_off, old_len) = {
                let metas = self.metas.read();
                if let Some(old) = metas.get(docid as usize) {
                    (old.root_id, old.path_off, old.path_len)
                } else {
                    (0, 0, 0)
                }
            };

            let same_path = {
                let arena = self.arena.read();
                arena
                    .get_bytes(old_off, old_len)
                    .map(|b| old_root_id == new_root_id && b == new_rel_bytes.as_slice())
                    .unwrap_or(false)
            };

            if same_path {
                // 同路径重复上报：只更新元数据，避免 posting 重复写入
                let mut metas = self.metas.write();
                if let Some(existing) = metas.get_mut(docid as usize) {
                    existing.size = meta.size;
                    existing.mtime_ns = mtime_to_ns(meta.mtime);
                }
                self.dirty.store(true, std::sync::atomic::Ordering::Release);
                return;
            }

            let old_path_missing = if force_path_update || old_len == 0 {
                false
            } else {
                self.absolute_path_buf(old_root_id, old_off, old_len)
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
                let mut metas = self.metas.write();
                if let Some(existing) = metas.get_mut(docid as usize) {
                    existing.size = meta.size;
                    existing.mtime_ns = mtime_to_ns(meta.mtime);
                }
                self.dirty.store(true, std::sync::atomic::Ordering::Release);
                return;
            }

            // rename：先移除旧路径关联
            let _guard = self.upsert_lock.write();
            if old_len != 0 {
                if let Some(old_path) = self.absolute_path_buf(old_root_id, old_off, old_len) {
                    self.remove_trigrams(docid, &old_path);
                    self.remove_path_hash(docid, &old_path);
                }
            }

            let Some((new_off, new_len)) = self.arena.write().push_bytes(&new_rel_bytes) else {
                self.tombstones.write().insert(docid);
                self.dirty.store(true, std::sync::atomic::Ordering::Release);
                return;
            };

            // posting/path_hash 先写（与 query 锁顺序一致：trigram -> metas）
            self.insert_trigrams(docid, meta.path.as_path());
            self.insert_path_hash(docid, meta.path.as_path());

            let mut metas = self.metas.write();
            if let Some(existing) = metas.get_mut(docid as usize) {
                existing.root_id = new_root_id;
                existing.path_off = new_off;
                existing.path_len = new_len;
                existing.size = meta.size;
                existing.mtime_ns = mtime_to_ns(meta.mtime);
            } else {
                // 极端情况：docid 槽位不存在，降级为 append
                if let Some(docid_new) = self.alloc_docid(
                    fkey,
                    new_root_id,
                    &new_rel_bytes,
                    meta.size,
                    mtime_to_ns(meta.mtime),
                ) {
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
        self.dirty.store(true, std::sync::atomic::Ordering::Release);
    }

    fn alloc_docid(
        &self,
        file_key: FileKey,
        root_id: u16,
        rel_bytes: &[u8],
        size: u64,
        mtime_ns: i64,
    ) -> Option<DocId> {
        let (off, len) = self.arena.write().push_bytes(rel_bytes)?;

        let mut metas = self.metas.write();
        let docid: DocId = metas.len() as DocId;
        metas.push(CompactMeta {
            file_key,
            root_id,
            path_off: off,
            path_len: len,
            size,
            mtime_ns,
        });

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

        let path = {
            let metas = self.metas.read();
            metas
                .get(docid as usize)
                .and_then(|m| self.absolute_path_buf(m.root_id, m.path_off, m.path_len))
        };

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
                let metas = self.metas.read();
                metas.get(docid as usize).map(|m| m.file_key)
            };
            if let Some(k) = file_key {
                self.mark_deleted(k);
            }
        }
    }

    /// 查询：trigram 候选集（Roaring 交集）→ 精确过滤
    pub fn query(&self, matcher: &dyn Matcher, limit: usize) -> Vec<FileMeta> {
        // 重要：先读取 trigram_index 计算候选集，再读取 metas/tombstones/arena。
        // 写入路径通常是先更新 trigram_index 再更新 metas，如果这里反过来拿锁，
        // 在“边写边查”场景下可能形成死锁。
        let candidates = self
            .trigram_candidates(matcher)
            .or_else(|| self.short_hint_candidates(matcher));

        let metas = self.metas.read();
        let arena = self.arena.read();
        let tombstones = self.tombstones.read();

        match candidates {
            Some(bitmap) => bitmap
                .iter()
                .filter(|docid| !tombstones.contains(*docid))
                .filter_map(|docid| metas.get(docid as usize).map(|m| (docid, m)))
                .filter(|(_, m)| {
                    let rel = arena.get_bytes(m.path_off, m.path_len).unwrap_or(&[]);
                    let abs = compose_abs_path_bytes(
                        root_bytes_for_id(&self.roots_bytes, m.root_id),
                        rel,
                    );
                    let s = std::str::from_utf8(&abs)
                        .map(std::borrow::Cow::Borrowed)
                        .unwrap_or_else(|_| String::from_utf8_lossy(&abs));
                    matcher.matches(&s)
                })
                .filter_map(|(_, m)| {
                    let rel = arena.get_bytes(m.path_off, m.path_len)?;
                    let path =
                        compose_abs_path_buf(root_bytes_for_id(&self.roots_bytes, m.root_id), rel);
                    Some(FileMeta {
                        file_key: m.file_key,
                        path,
                        size: m.size,
                        mtime: mtime_from_ns(m.mtime_ns),
                        ctime: None,
                        atime: None,
                    })
                })
                .collect(),
            None => {
                // 无法用 trigram 加速（查询词太短），全量过滤
                metas
                    .iter()
                    .enumerate()
                    .filter_map(|(i, m)| {
                        let docid: DocId = i as DocId;
                        if tombstones.contains(docid) {
                            return None;
                        }
                        let rel = arena.get_bytes(m.path_off, m.path_len).unwrap_or(&[]);
                        let abs = compose_abs_path_bytes(
                            root_bytes_for_id(&self.roots_bytes, m.root_id),
                            rel,
                        );
                        let s = std::str::from_utf8(&abs)
                            .map(std::borrow::Cow::Borrowed)
                            .unwrap_or_else(|_| String::from_utf8_lossy(&abs));
                        if matcher.matches(&s) {
                            let path = compose_abs_path_buf(
                                root_bytes_for_id(&self.roots_bytes, m.root_id),
                                rel,
                            );
                            Some(FileMeta {
                                file_key: m.file_key,
                                path,
                                size: m.size,
                                mtime: mtime_from_ns(m.mtime_ns),
                                ctime: None,
                                atime: None,
                            })
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
        let metas = self.metas.read();
        let arena = self.arena.read();
        let tombstones = self.tombstones.read();

        for (i, m) in metas.iter().enumerate() {
            let docid: DocId = i as DocId;
            if tombstones.contains(docid) {
                continue;
            }
            let Some(rel) = arena.get_bytes(m.path_off, m.path_len) else {
                continue;
            };
            let path = compose_abs_path_buf(root_bytes_for_id(&self.roots_bytes, m.root_id), rel);
            f(FileMeta {
                file_key: m.file_key,
                path,
                size: m.size,
                mtime: mtime_from_ns(m.mtime_ns),
                ctime: None,
                atime: None,
            });
        }
    }

    /// Iterate live metas whose path's parent is in `dirs`.
    pub fn for_each_live_meta_in_dirs<F>(
        &self,
        dirs: &std::collections::HashSet<PathBuf>,
        mut callback: F,
    ) where
        F: FnMut(FileMeta),
    {
        let metas = self.metas.read();
        let arena = self.arena.read();
        let tombstones = self.tombstones.read();
        for (i, m) in metas.iter().enumerate() {
            let docid = i as DocId;
            if tombstones.contains(docid) {
                continue;
            }
            let rel = match arena.get_bytes(m.path_off, m.path_len) {
                Some(r) => r,
                None => continue,
            };
            let abs = compose_abs_path_bytes(root_bytes_for_id(&self.roots_bytes, m.root_id), rel);
            let path = pathbuf_from_encoded_vec(abs.to_vec());
            if let Some(parent) = path.parent() {
                if dirs.contains(parent) {
                    callback(FileMeta {
                        file_key: m.file_key,
                        path,
                        size: m.size,
                        mtime: mtime_from_ns(m.mtime_ns),
                        ctime: None,
                        atime: None,
                    });
                }
            }
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
        let docid = { self.filekey_to_docid.read().get(&fk).copied()? };
        let metas = self.metas.read();
        match metas.get(docid as usize) {
            Some(m) => self.absolute_path_buf(m.root_id, m.path_off, m.path_len),
            None => None,
        }
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
            let old = {
                let metas = self.metas.read();
                metas.get(docid as usize).cloned()
            };

            if let Some(mut m) = old {
                if let Some(old_path) = self.absolute_path_buf(m.root_id, m.path_off, m.path_len) {
                    self.remove_trigrams(docid, &old_path);
                    self.remove_path_hash(docid, &old_path);
                } else if let Some(ref path) = from_best_path {
                    self.remove_trigrams(docid, path);
                    self.remove_path_hash(docid, path);
                }

                if let Some(to_path) = to_path {
                    let to_path = to_path.into_owned();
                    let (root_id, rel_bytes) = self.split_root_relative_bytes(to_path.as_path());
                    if let Some((off, len)) = self.arena.write().push_bytes(&rel_bytes) {
                        m.root_id = root_id;
                        m.path_off = off;
                        m.path_len = len;
                    }
                    if let Some(meta) = to_meta {
                        m.size = meta.size;
                        m.mtime_ns = mtime_to_ns(meta.mtime);
                    }
                    self.insert_trigrams(docid, to_path.as_path());
                    self.insert_path_hash(docid, to_path.as_path());
                } else if let Some(meta) = fallback_meta {
                    m.size = meta.size;
                    m.mtime_ns = mtime_to_ns(meta.mtime);
                }

                if let Some(slot) = self.metas.write().get_mut(docid as usize) {
                    *slot = m;
                }
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
        let arena = self.arena.read().clone();
        let metas = self.metas.read().clone();
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

        // PathArena 段：raw bytes（root-relative）
        let path_arena_bytes = Arc::clone(&self.arena.read().data);

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
        let metas = self.metas.read();
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

        // PathArena 段：raw bytes（root-relative）
        let arena_guard = self.arena.read();
        let path_arena_bytes = arena_guard.data.as_ref();
        Self::write_segment(writer, path_arena_bytes)?;

        // Metas 段：按 DocId 顺序顺排，固定记录大小（little-endian）
        let metas = self.metas.read();
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
        drop(metas);
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
        let total = self.metas.read().len();
        let tomb = self.tombstones.read().len() as usize;
        total.saturating_sub(tomb)
    }

    /// 内存占用统计（粗估）
    pub fn memory_stats(&self) -> L2Stats {
        use std::mem::size_of;

        let metas = self.metas.read();
        let filekey_to_docid = self.filekey_to_docid.read();
        let path_hash_to_id = self.path_hash_to_id.read();
        let trigram_index = self.trigram_index.read();
        let short_component_index = self.short_component_index.read();
        let tombstones = self.tombstones.read();
        let arena = self.arena.read();

        let total_docs = metas.len();
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

        // metas: Vec<CompactMeta>
        let metas_bytes = metas.capacity() as u64 * size_of::<CompactMeta>() as u64
            + size_of::<Vec<CompactMeta>>() as u64;

        // mapping: BTreeMap<FileKey, DocId>
        let map_entry_bytes = size_of::<(FileKey, DocId)>() as u64;
        let filekey_to_docid_bytes = filekey_to_docid.len() as u64 * (map_entry_bytes + 1)
            + size_of::<BTreeMap<FileKey, DocId>>() as u64;

        // arena：Vec<u8>
        let arena_bytes = arena.data.capacity() as u64 + size_of::<Vec<u8>>() as u64;

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
            + size_of::<BTreeMap<u64, OneOrManyDocId>>() as u64
            + path_many_bytes;

        // trigram：HashMap<Trigram, RoaringTreemap> 的 entry + Roaring 的压缩存储量（serialized_size）
        let trigram_entry_bytes = size_of::<(Trigram, RoaringTreemap)>() as u64;
        let trigram_map_bytes = trigram_index.capacity() as u64 * (trigram_entry_bytes + 1)
            + size_of::<HashMap<Trigram, RoaringTreemap>>() as u64;
        let short_component_entry_bytes = size_of::<(Box<[u8]>, RoaringTreemap)>() as u64;
        let mut short_component_heap_bytes: u64 = 0;
        for (component, posting) in short_component_index.iter() {
            short_component_heap_bytes += component.len() as u64;
            short_component_heap_bytes += posting.serialized_size() as u64;
        }
        let short_component_bytes = short_component_index.capacity() as u64
            * (short_component_entry_bytes + 1)
            + size_of::<HashMap<Box<[u8]>, RoaringTreemap>>() as u64
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
            metas_capacity: metas.capacity(),
            filekey_to_docid_capacity: filekey_to_docid.len(),
            path_hash_to_id_capacity: path_hash_to_id.len(),
            trigram_index_capacity: trigram_index.capacity(),
            arena_capacity: arena.data.capacity(),

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
        self.metas.write().clear();
        Arc::make_mut(&mut self.arena.write().data).clear();
        self.tombstones.write().clear();
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

    fn absolute_path_buf(&self, root_id: u16, off: u32, len: u16) -> Option<PathBuf> {
        let arena = self.arena.read();
        let rel = arena.get_bytes(off, len)?;
        Some(compose_abs_path_buf(
            root_bytes_for_id(&self.roots_bytes, root_id),
            rel,
        ))
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
            if let Some(posting) = short_idx.get_mut(component) {
                posting.remove(docid);
                if posting.is_empty() {
                    short_idx.remove(component);
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
            short_idx
                .entry(Box::<[u8]>::from(component))
                .or_default()
                .insert(docid);
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

        // 先复制候选 DocId（避免同时持有 path_hash_to_id 与 metas/arena 的锁）
        let candidates: Vec<DocId> = {
            let map = self.path_hash_to_id.read();
            let v = map.get(&h)?;
            v.iter().copied().collect()
        };

        if candidates.is_empty() {
            return None;
        }

        let metas = self.metas.read();
        let arena = self.arena.read();
        candidates.into_iter().find(|docid| {
            metas
                .get(*docid as usize)
                .and_then(|m| {
                    let rel = arena.get_bytes(m.path_off, m.path_len)?;
                    Some(
                        compose_abs_path_bytes(
                            root_bytes_for_id(&self.roots_bytes, m.root_id),
                            rel,
                        ) == bytes,
                    )
                })
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
            if short_component_matches(component.as_ref(), &hint) {
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

        let metas = self.metas.read();
        let arena = self.arena.read();
        let tombstones = self.tombstones.read();

        let mut out: Vec<FileKey> = Vec::new();

        match candidates {
            Some(bitmap) => {
                for docid in bitmap.iter() {
                    if tombstones.contains(docid) {
                        continue;
                    }
                    let Some(m) = metas.get(docid as usize) else {
                        continue;
                    };
                    let rel = arena.get_bytes(m.path_off, m.path_len).unwrap_or(&[]);
                    let abs = compose_abs_path_bytes(
                        root_bytes_for_id(&self.roots_bytes, m.root_id),
                        rel,
                    );
                    let s = std::str::from_utf8(&abs)
                        .map(std::borrow::Cow::Borrowed)
                        .unwrap_or_else(|_| String::from_utf8_lossy(&abs));
                    if matcher.matches(&s) {
                        out.push(m.file_key);
                    }
                }
            }
            None => {
                // 无法用 trigram 加速（查询词太短），全量过滤（不构造 PathBuf）。
                for (i, m) in metas.iter().enumerate() {
                    let docid: DocId = i as DocId;
                    if tombstones.contains(docid) {
                        continue;
                    }
                    let rel = arena.get_bytes(m.path_off, m.path_len).unwrap_or(&[]);
                    let abs = compose_abs_path_bytes(
                        root_bytes_for_id(&self.roots_bytes, m.root_id),
                        rel,
                    );
                    let s = std::str::from_utf8(&abs)
                        .map(std::borrow::Cow::Borrowed)
                        .unwrap_or_else(|_| String::from_utf8_lossy(&abs));
                    if matcher.matches(&s) {
                        out.push(m.file_key);
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
        let m = { self.metas.read().get(docid as usize).cloned()? };
        let arena = self.arena.read();
        let rel = arena.get_bytes(m.path_off, m.path_len)?;
        let path = compose_abs_path_buf(root_bytes_for_id(&self.roots_bytes, m.root_id), rel);
        Some(FileMeta {
            file_key: m.file_key,
            path,
            size: m.size,
            mtime: mtime_from_ns(m.mtime_ns),
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

impl PersistentIndex {
    /// Fast-path bulk fill used by compaction dimension reduction.
    /// Directly replaces metas, arena, trigram_index, and derived maps.
    /// Paths are taken from `metas` (full FileMeta with absolute paths).
    /// `trigrams` contains pre-merged DocId posting lists (DocId matches index in `metas`).
    pub(crate) fn fill_from_compaction(
        &self,
        _roots: Vec<PathBuf>,
        metas: Vec<FileMeta>,
        trigrams: std::collections::HashMap<[u8; 3], roaring::RoaringTreemap>,
    ) {
        // Reset internal state
        {
            *self.metas.write() = Vec::new();
            *self.arena.write() = PathArena::new();
            *self.filekey_to_docid.write() = BTreeMap::new();
            *self.path_hash_to_id.write() = BTreeMap::new();
            *self.trigram_index.write() = HashMap::new();
            *self.short_component_index.write() = HashMap::new();
            *self.tombstones.write() = RoaringTreemap::new();
            self.dirty.store(true, std::sync::atomic::Ordering::Release);
        }

        let mut new_metas = Vec::with_capacity(metas.len());
        let mut new_arena = PathArena::new();
        let mut new_filekey_to_docid = BTreeMap::new();
        let mut new_path_hash_to_id = BTreeMap::new();
        let mut new_short_component_index: HashMap<Box<[u8]>, RoaringTreemap> = HashMap::new();

        for (docid, meta) in metas.into_iter().enumerate() {
            let docid = docid as DocId;
            let (root_id, rel_bytes) = self.split_root_relative_bytes(&meta.path);

            let Some((off, len)) = new_arena.push_bytes(&rel_bytes) else {
                continue;
            };

            new_metas.push(CompactMeta {
                file_key: meta.file_key,
                root_id,
                path_off: off,
                path_len: len,
                size: meta.size,
                mtime_ns: mtime_to_ns(meta.mtime),
            });

            new_filekey_to_docid.insert(meta.file_key, docid);

            let bytes = meta.path.as_os_str().as_encoded_bytes();
            let h = path_hash_bytes(bytes);
            new_path_hash_to_id
                .entry(h)
                .and_modify(|v: &mut OneOrManyDocId| v.insert(docid))
                .or_insert(OneOrManyDocId::One(docid));

            for_each_short_component(meta.path.as_path(), |component| {
                new_short_component_index
                    .entry(Box::<[u8]>::from(component))
                    .or_default()
                    .insert(docid);
            });
        }

        {
            *self.metas.write() = new_metas;
            *self.arena.write() = new_arena;
            *self.filekey_to_docid.write() = new_filekey_to_docid;
            *self.path_hash_to_id.write() = new_path_hash_to_id;
            *self.trigram_index.write() = trigrams;
            *self.short_component_index.write() = new_short_component_index;
        }
    }
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
}
