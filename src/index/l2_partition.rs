use parking_lot::RwLock;
use roaring::RoaringBitmap;
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
use std::hash::{Hash, Hasher};
use std::path::{Path, PathBuf};

#[cfg(feature = "rkyv")]
use crate::core::FileKeyEntry;
use crate::core::{EventRecord, EventType, FileKey, FileMeta};
use crate::index::IndexLayer;
use crate::query::matcher::Matcher;
use crate::stats::L2Stats;

/// Trigram：3 字节子串，用于倒排索引加速查询
type Trigram = [u8; 3];

/// DocId：L2 内部紧凑文档编号（posting 的元素类型）
pub type DocId = u32;

/// 从文件名中提取 trigram 集合
fn extract_trigrams(name: &str) -> HashSet<Trigram> {
    let lower = name.to_lowercase();
    let bytes = lower.as_bytes();
    let mut set = HashSet::new();
    if bytes.len() >= 3 {
        for w in bytes.windows(3) {
            set.insert([w[0], w[1], w[2]]);
        }
    }
    set
}

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

fn basename_bytes(path_bytes: &[u8]) -> &[u8] {
    match path_bytes.iter().rposition(|b| *b == b'/') {
        Some(pos) if pos + 1 < path_bytes.len() => &path_bytes[pos + 1..],
        _ => path_bytes,
    }
}

/// Path blob arena：所有路径的连续字节存储
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct PathArena {
    pub data: Vec<u8>,
}

impl PathArena {
    pub fn new() -> Self {
        Self { data: Vec::new() }
    }

    pub fn push_bytes(&mut self, bytes: &[u8]) -> Option<(u32, u16)> {
        let len: u16 = bytes.len().try_into().ok()?;
        let off: u32 = self.data.len().try_into().ok()?;
        self.data.extend_from_slice(bytes);
        Some((off, len))
    }

    pub fn push_path(&mut self, path: &Path) -> Option<(u32, u16)> {
        use std::os::unix::ffi::OsStrExt;

        let bytes = path.as_os_str().as_bytes();
        self.push_bytes(bytes)
    }

    pub fn get_bytes(&self, off: u32, len: u16) -> Option<&[u8]> {
        let start: usize = off as usize;
        let end: usize = start.checked_add(len as usize)?;
        self.data.get(start..end)
    }

    pub fn get_path_buf(&self, off: u32, len: u16) -> Option<PathBuf> {
        use std::os::unix::ffi::OsStringExt;

        let bytes = self.get_bytes(off, len)?.to_vec();
        Some(PathBuf::from(std::ffi::OsString::from_vec(bytes)))
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
    pub mtime: Option<std::time::SystemTime>,
}

/// 旧快照格式 v2（兼容读取）
#[derive(Clone, Debug, Serialize, Deserialize)]
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
#[derive(Clone, Debug, Serialize, Deserialize)]
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
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct IndexSnapshotV4 {
    pub arena: PathArena,
    pub metas: Vec<CompactMetaV4>,
    pub tombstones: Vec<DocId>,
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
    pub tombstones: Vec<DocId>,
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
    pub roots_bytes: Vec<u8>,
    pub path_arena_bytes: Vec<u8>,
    pub metas_bytes: Vec<u8>,
    pub trigram_table_bytes: Vec<u8>,
    pub postings_blob_bytes: Vec<u8>,
    pub tombstones_bytes: Vec<u8>,
    pub filekey_map_bytes: Vec<u8>,
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
    filekey_to_docid: RwLock<HashMap<FileKey, DocId>>,
    /// 路径 blob
    arena: RwLock<PathArena>,

    /// 路径反查：hash(path_bytes) -> DocId（或少量冲突列表）
    path_hash_to_id: RwLock<HashMap<u64, OneOrManyDocId>>,

    /// Trigram 倒排索引：trigram -> RoaringBitmap(DocId)
    trigram_index: RwLock<HashMap<Trigram, RoaringBitmap>>,

    /// 墓碑标记（DocId）
    tombstones: RwLock<RoaringBitmap>,

    /// 脏标记（自上次快照后是否有变更）
    dirty: RwLock<bool>,
}

impl PersistentIndex {
    pub fn new() -> Self {
        Self::new_with_roots(Vec::new())
    }

    pub fn new_with_roots(roots: Vec<PathBuf>) -> Self {
        let roots = normalize_roots_with_fallback(roots);
        let roots_bytes = roots
            .iter()
            .map(|p| {
                use std::os::unix::ffi::OsStrExt;
                p.as_os_str().as_bytes().to_vec()
            })
            .collect::<Vec<_>>();

        Self {
            roots,
            roots_bytes,
            metas: RwLock::new(Vec::new()),
            filekey_to_docid: RwLock::new(HashMap::new()),
            arena: RwLock::new(PathArena::new()),
            path_hash_to_id: RwLock::new(HashMap::new()),
            trigram_index: RwLock::new(HashMap::new()),
            tombstones: RwLock::new(RoaringBitmap::new()),
            dirty: RwLock::new(false),
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
            *idx.tombstones.write() = snap.tombstones.into_iter().collect();
            *idx.dirty.write() = false;
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
            let (off, len) = new_arena.push_bytes(&rel_bytes).unwrap_or((0, 0));
            new_metas.push(CompactMeta {
                file_key: m.file_key,
                root_id,
                path_off: off,
                path_len: len,
                size: m.size,
                mtime: m.mtime,
            });
        }

        {
            *idx.metas.write() = new_metas;
            *idx.arena.write() = new_arena;
            *idx.tombstones.write() = tombstones.into_iter().collect();
            *idx.dirty.write() = false;
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

        filekey_to_docid.clear();
        path_hash_to_id.clear();
        trigram_index.clear();

        for (docid_usize, meta) in metas.iter().enumerate() {
            let docid: DocId = match docid_usize.try_into() {
                Ok(v) => v,
                Err(_) => continue,
            };
            filekey_to_docid.insert(meta.file_key, docid);

            if tomb.contains(docid) {
                continue;
            }

            if let Some(rel_bytes) = arena.get_bytes(meta.path_off, meta.path_len) {
                let abs_bytes = self.compose_abs_path_bytes(meta.root_id, rel_bytes);
                let h = path_hash_bytes(&abs_bytes);
                path_hash_to_id
                    .entry(h)
                    .and_modify(|v| v.insert(docid))
                    .or_insert(OneOrManyDocId::One(docid));

                let name_bytes = basename_bytes(rel_bytes);
                let name = std::str::from_utf8(name_bytes)
                    .map(|s| s.to_string())
                    .unwrap_or_else(|_| String::from_utf8_lossy(name_bytes).into_owned());

                for tri in extract_trigrams(&name) {
                    trigram_index
                        .entry(tri)
                        .or_insert_with(RoaringBitmap::new)
                        .insert(docid);
                }
            }
        }
    }

    /// 插入/更新一条文件记录
    ///
    /// ## 单路径策略 (first-seen wins)
    /// 如果该 FileKey 已存在且路径不同（hardlink 场景），
    /// 保留最先发现的路径，仅更新 size/mtime 等元数据。
    /// 只有显式 rename 事件才会更新路径。
    pub fn upsert(&self, meta: FileMeta) {
        self.upsert_inner(meta, false);
    }

    /// rename 专用：强制更新路径
    pub fn upsert_rename(&self, meta: FileMeta) {
        self.upsert_inner(meta, true);
    }

    fn upsert_inner(&self, meta: FileMeta, force_path_update: bool) {
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
                    existing.mtime = meta.mtime;
                }
                *self.dirty.write() = true;
                return;
            }

            // 路径不同：hardlink 或 rename
            if !force_path_update {
                // hardlink/重复发现：保留旧路径，仅更新元数据
                let mut metas = self.metas.write();
                if let Some(existing) = metas.get_mut(docid as usize) {
                    existing.size = meta.size;
                    existing.mtime = meta.mtime;
                }
                *self.dirty.write() = true;
                return;
            }

            // rename：先移除旧路径关联
            if old_len != 0 {
                if let Some(old_path) = self.absolute_path_buf(old_root_id, old_off, old_len) {
                    self.remove_trigrams(docid, &old_path);
                    self.remove_path_hash(docid, &old_path);
                }
            }

            // 再写入新路径
            let (new_off, new_len) = self
                .arena
                .write()
                .push_bytes(&new_rel_bytes)
                .unwrap_or((0, 0));

            // posting/path_hash 先写（与 query 锁顺序一致：trigram -> metas）
            self.insert_trigrams(docid, meta.path.as_path());
            self.insert_path_hash(docid, meta.path.as_path());

            let mut metas = self.metas.write();
            if let Some(existing) = metas.get_mut(docid as usize) {
                existing.root_id = new_root_id;
                existing.path_off = new_off;
                existing.path_len = new_len;
                existing.size = meta.size;
                existing.mtime = meta.mtime;
            } else {
                // 极端情况：docid 槽位不存在，降级为 append
                let docid_new =
                    self.alloc_docid(fkey, new_root_id, &new_rel_bytes, meta.size, meta.mtime);
                self.insert_trigrams(docid_new, meta.path.as_path());
                self.insert_path_hash(docid_new, meta.path.as_path());
            }

            // rename 视为“存在且活跃”
            self.tombstones.write().remove(docid);
            *self.dirty.write() = true;
            return;
        }

        // 新文件：分配 docid 并写入
        let docid = self.alloc_docid(fkey, new_root_id, &new_rel_bytes, meta.size, meta.mtime);
        self.insert_trigrams(docid, meta.path.as_path());
        self.insert_path_hash(docid, meta.path.as_path());
        *self.dirty.write() = true;
    }

    fn alloc_docid(
        &self,
        file_key: FileKey,
        root_id: u16,
        rel_bytes: &[u8],
        size: u64,
        mtime: Option<std::time::SystemTime>,
    ) -> DocId {
        let (off, len) = self.arena.write().push_bytes(rel_bytes).unwrap_or((0, 0));

        let mut metas = self.metas.write();
        let docid: DocId = metas.len().try_into().unwrap_or(u32::MAX);
        metas.push(CompactMeta {
            file_key,
            root_id,
            path_off: off,
            path_len: len,
            size,
            mtime,
        });

        self.filekey_to_docid.write().insert(file_key, docid);
        self.tombstones.write().remove(docid);
        docid
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

        if let Some(p) = path {
            self.remove_trigrams(docid, &p);
            self.remove_path_hash(docid, &p);
        }

        // 保留 doc 槽位，但移除 filekey 映射，避免 inode 复用/重新扫描复用 docid
        self.filekey_to_docid.write().remove(&file_key);
        self.tombstones.write().insert(docid);
        *self.dirty.write() = true;
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
    pub fn query(&self, matcher: &dyn Matcher) -> Vec<FileMeta> {
        // 重要：先读取 trigram_index 计算候选集，再读取 metas/tombstones/arena。
        // 写入路径通常是先更新 trigram_index 再更新 metas，如果这里反过来拿锁，
        // 在“边写边查”场景下可能形成死锁。
        let candidates = self.trigram_candidates(matcher);

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
                    let abs = self.compose_abs_path_bytes(m.root_id, rel);
                    let s = std::str::from_utf8(&abs)
                        .map(std::borrow::Cow::Borrowed)
                        .unwrap_or_else(|_| String::from_utf8_lossy(&abs));
                    matcher.matches(&s)
                })
                .filter_map(|(_, m)| {
                    let rel = arena.get_bytes(m.path_off, m.path_len)?;
                    let path = self.compose_abs_path_buf(m.root_id, rel)?;
                    Some(FileMeta {
                        file_key: m.file_key,
                        path,
                        size: m.size,
                        mtime: m.mtime,
                    })
                })
                .collect(),
            None => {
                // 无法用 trigram 加速（查询词太短），全量过滤
                metas
                    .iter()
                    .enumerate()
                    .filter_map(|(i, m)| {
                        let docid: DocId = i.try_into().ok()?;
                        if tombstones.contains(docid) {
                            return None;
                        }
                        let rel = arena.get_bytes(m.path_off, m.path_len).unwrap_or(&[]);
                        let abs = self.compose_abs_path_bytes(m.root_id, rel);
                        let s = std::str::from_utf8(&abs)
                            .map(std::borrow::Cow::Borrowed)
                            .unwrap_or_else(|_| String::from_utf8_lossy(&abs));
                        if matcher.matches(&s) {
                            let path = self.compose_abs_path_buf(m.root_id, rel)?;
                            Some(FileMeta {
                                file_key: m.file_key,
                                path,
                                size: m.size,
                                mtime: m.mtime,
                            })
                        } else {
                            None
                        }
                    })
                    .collect()
            }
        }
    }

    /// 遍历所有“活跃”文档（跳过 tombstone），用于 Flush/Compaction/重建等离线流程。
    pub fn for_each_live_meta(&self, mut f: impl FnMut(FileMeta)) {
        let metas = self.metas.read();
        let arena = self.arena.read();
        let tombstones = self.tombstones.read();

        for (i, m) in metas.iter().enumerate() {
            let docid: DocId = match i.try_into() {
                Ok(v) => v,
                Err(_) => continue,
            };
            if tombstones.contains(docid) {
                continue;
            }
            let Some(rel) = arena.get_bytes(m.path_off, m.path_len) else {
                continue;
            };
            let Some(path) = self.compose_abs_path_buf(m.root_id, rel) else {
                continue;
            };
            f(FileMeta {
                file_key: m.file_key,
                path,
                size: m.size,
                mtime: m.mtime,
            });
        }
    }

    /// 批量应用事件
    pub fn apply_events(&self, events: &[EventRecord]) {
        use std::os::unix::fs::MetadataExt;

        for ev in events {
            match &ev.event_type {
                EventType::Create | EventType::Modify => {
                    if let Some(p) = ev.best_path() {
                        if let Ok(meta) = std::fs::metadata(p) {
                            self.upsert(FileMeta {
                                file_key: FileKey {
                                    dev: meta.dev(),
                                    ino: meta.ino(),
                                },
                                path: p.to_path_buf(),
                                size: meta.len(),
                                mtime: meta.modified().ok(),
                            });
                        }
                        continue;
                    }

                    // FID-only 且无 path_hint：若索引里已有路径，则用现有路径做保守更新。
                    if let Some(fk) = ev.id.as_file_key() {
                        let docid = { self.filekey_to_docid.read().get(&fk).copied() };
                        if let Some(docid) = docid {
                            if let Some(path) = {
                                let metas = self.metas.read();
                                match metas.get(docid as usize) {
                                    Some(m) => {
                                        self.absolute_path_buf(m.root_id, m.path_off, m.path_len)
                                    }
                                    None => None,
                                }
                            } {
                                if let Ok(meta) = std::fs::metadata(&path) {
                                    if meta.dev() == fk.dev && meta.ino() == fk.ino {
                                        self.upsert(FileMeta {
                                            file_key: fk,
                                            path,
                                            size: meta.len(),
                                            mtime: meta.modified().ok(),
                                        });
                                    }
                                }
                            }
                        }
                    }
                }
                EventType::Delete => {
                    if let Some(fk) = ev.id.as_file_key() {
                        self.mark_deleted(fk);
                    } else if let Some(p) = ev.best_path() {
                        self.mark_deleted_by_path(p);
                    }
                }
                EventType::Rename {
                    from,
                    from_path_hint,
                } => {
                    let from_best_path = from_path_hint.as_deref().or_else(|| from.as_path());
                    let to_path = ev.best_path().map(|p| p.to_path_buf());

                    let docid_opt = if let Some(fk) = from.as_file_key() {
                        self.filekey_to_docid.read().get(&fk).copied()
                    } else {
                        from_best_path.and_then(|p| self.lookup_docid_by_path(p))
                    };

                    if let Some(docid) = docid_opt {
                        let old = {
                            let metas = self.metas.read();
                            metas.get(docid as usize).cloned()
                        };
                        if let Some(mut m) = old {
                            // 移除旧路径 posting/hash：优先 meta 中存储的旧路径。
                            if let Some(old_path) =
                                self.absolute_path_buf(m.root_id, m.path_off, m.path_len)
                            {
                                self.remove_trigrams(docid, &old_path);
                                self.remove_path_hash(docid, &old_path);
                            } else if let Some(p) = from_best_path {
                                self.remove_trigrams(docid, p);
                                self.remove_path_hash(docid, p);
                            }

                            if let Some(to_path) = to_path {
                                let (root_id, rel_bytes) =
                                    self.split_root_relative_bytes(to_path.as_path());
                                if let Some((off, len)) = self.arena.write().push_bytes(&rel_bytes)
                                {
                                    m.root_id = root_id;
                                    m.path_off = off;
                                    m.path_len = len;
                                }

                                if let Ok(fs_meta) = std::fs::metadata(&to_path) {
                                    m.size = fs_meta.len();
                                    m.mtime = fs_meta.modified().ok();
                                }

                                self.insert_trigrams(docid, to_path.as_path());
                                self.insert_path_hash(docid, to_path.as_path());
                            } else if let Some(p) = from_best_path {
                                // 无 to path：保守更新元数据
                                if let Ok(fs_meta) = std::fs::metadata(p) {
                                    m.size = fs_meta.len();
                                    m.mtime = fs_meta.modified().ok();
                                }
                            }

                            if let Some(slot) = self.metas.write().get_mut(docid as usize) {
                                *slot = m;
                            }
                            self.tombstones.write().remove(docid);
                            *self.dirty.write() = true;
                        }
                    } else {
                        // 未命中：降级为 delete + create（若有路径）
                        if let Some(fk) = from.as_file_key() {
                            self.mark_deleted(fk);
                        } else if let Some(p) = from_best_path {
                            self.mark_deleted_by_path(p);
                        }

                        if let Some(to_path) = to_path {
                            if let Ok(meta) = std::fs::metadata(&to_path) {
                                self.upsert(FileMeta {
                                    file_key: FileKey {
                                        dev: meta.dev(),
                                        ino: meta.ino(),
                                    },
                                    path: to_path,
                                    size: meta.len(),
                                    mtime: meta.modified().ok(),
                                });
                            }
                        }
                    }
                }
            }
        }
    }

    /// 导出 v5 快照数据
    pub fn export_snapshot_v5(&self) -> IndexSnapshotV5 {
        let arena = self.arena.read().clone();
        let metas = self.metas.read().clone();
        let tombstones = self.tombstones.read().iter().collect::<Vec<DocId>>();
        *self.dirty.write() = false;
        IndexSnapshotV5 {
            roots_hash: self.roots_hash(),
            arena,
            metas,
            tombstones,
        }
    }

    pub fn export_segments_v6(&self) -> V6Segments {
        use std::io::Write;

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
        let path_arena_bytes = self.arena.read().data.clone();

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
            let ns: i64 = m
                .mtime
                .and_then(|t| t.duration_since(std::time::UNIX_EPOCH).ok())
                .and_then(|d| i64::try_from(d.as_nanos()).ok())
                .unwrap_or(-1);
            metas_bytes.extend_from_slice(&ns.to_le_bytes());
        }

        // Tombstones 段：Roaring serialized bytes
        let tombstones = self.tombstones.read();
        let mut tombstones_bytes = Vec::new();
        tombstones
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
            let mut buf = Vec::new();
            posting.serialize_into(&mut buf).expect("write to vec");
            let off: u32 = postings_blob_bytes.len().try_into().unwrap_or(u32::MAX);
            let len: u32 = buf.len().try_into().unwrap_or(u32::MAX);
            postings_blob_bytes.write_all(&buf).expect("write to vec");
            entries.push((*tri, off, len));
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
        // legacy payload：固定记录 20B（LE）：
        //   dev u64
        //   ino u64
        //   docid u32
        //
        // 注意：来源为 filekey_to_docid（天然排除 tombstone）。
        let mut pairs: Vec<(FileKey, DocId)> = {
            let m = self.filekey_to_docid.read();
            m.iter().map(|(k, v)| (*k, *v)).collect()
        };
        pairs.sort_unstable_by_key(|(k, _)| (k.dev, k.ino));
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
            filekey_map_bytes.reserve(pairs.len() * 20);
            for (k, docid) in pairs {
                filekey_map_bytes.extend_from_slice(&k.dev.to_le_bytes());
                filekey_map_bytes.extend_from_slice(&k.ino.to_le_bytes());
                filekey_map_bytes.extend_from_slice(&docid.to_le_bytes());
            }
        }

        V6Segments {
            roots_bytes,
            path_arena_bytes,
            metas_bytes,
            trigram_table_bytes,
            postings_blob_bytes,
            tombstones_bytes,
            filekey_map_bytes,
        }
    }

    pub fn is_dirty(&self) -> bool {
        *self.dirty.read()
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

        // mapping: HashMap<FileKey, DocId>
        let map_entry_bytes = size_of::<(FileKey, DocId)>() as u64;
        let filekey_to_docid_bytes = filekey_to_docid.capacity() as u64 * (map_entry_bytes + 1)
            + size_of::<HashMap<FileKey, DocId>>() as u64;

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
        let path_to_id_bytes = path_hash_to_id.capacity() as u64 * (path_entry_bytes + 1)
            + size_of::<HashMap<u64, OneOrManyDocId>>() as u64
            + path_many_bytes;

        // trigram：HashMap<Trigram, RoaringBitmap> 的 entry + Roaring 的压缩存储量（serialized_size）
        let trigram_entry_bytes = size_of::<(Trigram, RoaringBitmap)>() as u64;
        let trigram_map_bytes = trigram_index.capacity() as u64 * (trigram_entry_bytes + 1)
            + size_of::<HashMap<Trigram, RoaringBitmap>>() as u64;
        let trigram_bytes = trigram_map_bytes + trigram_heap_bytes;

        // tombstones：RoaringBitmap
        let tomb_bytes = size_of::<RoaringBitmap>() as u64 + tombstones.serialized_size() as u64;
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
            filekey_to_docid_capacity: filekey_to_docid.capacity(),
            path_hash_to_id_capacity: path_hash_to_id.capacity(),
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
        *self.dirty.write() = true;
        tracing::info!("Compaction complete, tombstones cleared");
    }

    pub fn maybe_schedule_repair(&self) {
        // 占位：检测索引健康度，触发后台补扫
    }

    /// 清空索引并标记为 dirty（用于 overflow 后的重建兜底）
    pub fn reset(&self) {
        // 统一按固定顺序清理，避免读写并发下出现锁顺序反转。
        self.trigram_index.write().clear();
        self.path_hash_to_id.write().clear();
        self.filekey_to_docid.write().clear();
        self.metas.write().clear();
        self.arena.write().data.clear();
        self.tombstones.write().clear();
        *self.dirty.write() = true;
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

        use std::os::unix::ffi::OsStrExt;
        let rel_bytes = rel_path.as_os_str().as_bytes().to_vec();
        let root_id: u16 = root_id_usize.try_into().unwrap_or(0);
        (root_id, rel_bytes)
    }

    fn compose_abs_path_bytes(&self, root_id: u16, rel_bytes: &[u8]) -> Vec<u8> {
        let root = self
            .roots_bytes
            .get(root_id as usize)
            .map(|v| v.as_slice())
            .unwrap_or(b"/");

        let mut out = Vec::with_capacity(root.len() + 1 + rel_bytes.len());
        out.extend_from_slice(root);
        if !out.ends_with(b"/") {
            out.push(b'/');
        }
        out.extend_from_slice(rel_bytes);
        out
    }

    fn compose_abs_path_buf(&self, root_id: u16, rel_bytes: &[u8]) -> Option<PathBuf> {
        use std::os::unix::ffi::OsStringExt;
        let abs = self.compose_abs_path_bytes(root_id, rel_bytes);
        Some(PathBuf::from(std::ffi::OsString::from_vec(abs)))
    }

    fn absolute_path_buf(&self, root_id: u16, off: u32, len: u16) -> Option<PathBuf> {
        let arena = self.arena.read();
        let rel = arena.get_bytes(off, len)?;
        self.compose_abs_path_buf(root_id, rel)
    }

    fn remove_trigrams(&self, docid: DocId, path: &Path) {
        use std::os::unix::ffi::OsStrExt;

        let path_bytes = path.as_os_str().as_bytes();
        let name_bytes = basename_bytes(path_bytes);
        let name = std::str::from_utf8(name_bytes)
            .map(|s| s.to_string())
            .unwrap_or_else(|_| String::from_utf8_lossy(name_bytes).into_owned());

        let mut tri_idx = self.trigram_index.write();
        for tri in extract_trigrams(&name) {
            if let Some(posting) = tri_idx.get_mut(&tri) {
                posting.remove(docid);
                if posting.is_empty() {
                    tri_idx.remove(&tri);
                }
            }
        }
    }

    fn insert_trigrams(&self, docid: DocId, path: &Path) {
        use std::os::unix::ffi::OsStrExt;

        let path_bytes = path.as_os_str().as_bytes();
        let name_bytes = basename_bytes(path_bytes);
        let name = std::str::from_utf8(name_bytes)
            .map(|s| s.to_string())
            .unwrap_or_else(|_| String::from_utf8_lossy(name_bytes).into_owned());

        let mut tri_idx = self.trigram_index.write();
        for tri in extract_trigrams(&name) {
            tri_idx
                .entry(tri)
                .or_insert_with(RoaringBitmap::new)
                .insert(docid);
        }
    }

    fn insert_path_hash(&self, docid: DocId, path: &Path) {
        use std::os::unix::ffi::OsStrExt;

        let bytes = path.as_os_str().as_bytes();
        let h = path_hash_bytes(bytes);
        let mut map = self.path_hash_to_id.write();
        map.entry(h)
            .and_modify(|v| v.insert(docid))
            .or_insert(OneOrManyDocId::One(docid));
    }

    fn remove_path_hash(&self, docid: DocId, path: &Path) {
        use std::os::unix::ffi::OsStrExt;

        let bytes = path.as_os_str().as_bytes();
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
        use std::os::unix::ffi::OsStrExt;

        let bytes = path.as_os_str().as_bytes();
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
                    Some(self.compose_abs_path_bytes(m.root_id, rel) == bytes)
                })
                .unwrap_or(false)
        })
    }

    fn trigram_candidates(&self, matcher: &dyn Matcher) -> Option<RoaringBitmap> {
        let prefix = matcher.prefix()?;
        let tris = query_trigrams(prefix);
        if tris.is_empty() {
            return None;
        }

        let tri_idx = self.trigram_index.read();
        let mut bitmaps: Vec<RoaringBitmap> = Vec::with_capacity(tris.len());
        for tri in &tris {
            let Some(posting) = tri_idx.get(tri) else {
                return Some(RoaringBitmap::new());
            };
            bitmaps.push(posting.clone());
        }
        drop(tri_idx);

        // 交集：先按基数排序，减少中间结果大小
        bitmaps.sort_by_key(|b| b.len());
        let mut iter = bitmaps.into_iter();
        let mut acc = iter.next().unwrap_or_else(RoaringBitmap::new);
        for b in iter {
            acc &= &b;
            if acc.is_empty() {
                break;
            }
        }
        Some(acc)
    }
}

impl IndexLayer for PersistentIndex {
    fn query_keys(&self, matcher: &dyn Matcher) -> Vec<FileKey> {
        // 复用 L2 的 trigram 候选集计算，但只输出稳定身份（FileKey）。
        let candidates = self.trigram_candidates(matcher);

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
                    let abs = self.compose_abs_path_bytes(m.root_id, rel);
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
                    let docid: DocId = match i.try_into() {
                        Ok(v) => v,
                        Err(_) => continue,
                    };
                    if tombstones.contains(docid) {
                        continue;
                    }
                    let rel = arena.get_bytes(m.path_off, m.path_len).unwrap_or(&[]);
                    let abs = self.compose_abs_path_bytes(m.root_id, rel);
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
        let path = self.compose_abs_path_buf(m.root_id, rel)?;
        Some(FileMeta {
            file_key: m.file_key,
            path,
            size: m.size,
            mtime: m.mtime,
        })
    }
}

fn path_hash_bytes(bytes: &[u8]) -> u64 {
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    bytes.hash(&mut hasher);
    hasher.finish()
}

fn normalize_roots_with_fallback(mut roots: Vec<PathBuf>) -> Vec<PathBuf> {
    use std::os::unix::ffi::OsStrExt;

    // 去重 + 排序，保证 root_id 的解释在“同一组 roots”下稳定。
    roots.sort_by(|a, b| a.as_os_str().as_bytes().cmp(b.as_os_str().as_bytes()));
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
            file_key: FileKey { dev: 1, ino: 1 },
            path: PathBuf::from("/tmp/alpha_test.txt"),
            size: 1,
            mtime: None,
        });
        idx.upsert(FileMeta {
            file_key: FileKey { dev: 1, ino: 2 },
            path: PathBuf::from("/tmp/beta_test.txt"),
            size: 1,
            mtime: None,
        });

        let m = create_matcher("alpha");
        let r = idx.query(m.as_ref());
        assert_eq!(r.len(), 1);
        assert!(r[0].path.to_string_lossy().contains("alpha_test"));
    }
}
