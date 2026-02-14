use std::collections::{HashMap, HashSet};
use std::hash::{Hash, Hasher};
use std::path::{Path, PathBuf};

use parking_lot::RwLock;
use roaring::RoaringBitmap;
use serde::{Deserialize, Serialize};

use crate::core::{EventRecord, EventType, FileKey, FileMeta};
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

    pub fn push_path(&mut self, path: &Path) -> Option<(u32, u16)> {
        use std::os::unix::ffi::OsStrExt;

        let bytes = path.as_os_str().as_bytes();
        let len: u16 = bytes.len().try_into().ok()?;
        let off: u32 = self.data.len().try_into().ok()?;
        self.data.extend_from_slice(bytes);
        Some((off, len))
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

/// 紧凑元数据：以 DocId 为下标（Vec 紧凑布局）
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct CompactMeta {
    pub file_key: FileKey,
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
    pub metas: Vec<CompactMeta>,
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
        Self {
            metas: RwLock::new(Vec::new()),
            filekey_to_docid: RwLock::new(HashMap::new()),
            arena: RwLock::new(PathArena::new()),
            path_hash_to_id: RwLock::new(HashMap::new()),
            trigram_index: RwLock::new(HashMap::new()),
            tombstones: RwLock::new(RoaringBitmap::new()),
            dirty: RwLock::new(false),
        }
    }

    pub fn from_snapshot_v4(snap: IndexSnapshotV4) -> Self {
        let idx = Self::new();

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

    pub fn from_snapshot_v3(snap: IndexSnapshotV3) -> Self {
        // v3 的 tombstones 不携带对应文档记录；阶段 A 的 DocId tombstone 以“保留 doc 槽位”实现，
        // 因此这里仅重建 files，本质上等价于“干净加载”。
        let idx = Self::new();
        for (_k, meta) in snap.files {
            idx.upsert(meta);
        }
        idx
    }

    pub fn from_snapshot_v2(snap: IndexSnapshotV2) -> Self {
        let idx = Self::new();
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

            if let Some(path_bytes) = arena.get_bytes(meta.path_off, meta.path_len) {
                let h = path_hash_bytes(path_bytes);
                path_hash_to_id
                    .entry(h)
                    .and_modify(|v| v.insert(docid))
                    .or_insert(OneOrManyDocId::One(docid));

                let name_bytes = basename_bytes(path_bytes);
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

        // 先查 docid（只持有 mapping 的读锁）
        let existing_docid = { self.filekey_to_docid.read().get(&fkey).copied() };

        if let Some(docid) = existing_docid {
            // 读旧路径 bytes（不持有 trigram/path_hash 锁）
            let (old_off, old_len) = {
                let metas = self.metas.read();
                if let Some(old) = metas.get(docid as usize) {
                    (old.path_off, old.path_len)
                } else {
                    (0, 0)
                }
            };

            let new_bytes_opt = {
                use std::os::unix::ffi::OsStrExt;
                Some(meta.path.as_os_str().as_bytes())
            };

            let same_path = {
                let arena = self.arena.read();
                let old_bytes = arena.get_bytes(old_off, old_len);
                match (old_bytes, new_bytes_opt) {
                    (Some(a), Some(b)) => a == b,
                    _ => false,
                }
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
                let old_path_buf = {
                    let arena = self.arena.read();
                    arena.get_path_buf(old_off, old_len)
                };
                if let Some(old_path) = old_path_buf {
                    self.remove_trigrams(docid, &old_path);
                    self.remove_path_hash(docid, &old_path);
                }
            }

            // 再写入新路径
            let (new_off, new_len) = {
                let mut arena = self.arena.write();
                arena.push_path(meta.path.as_path()).unwrap_or((0, 0))
            };

            // posting/path_hash 先写（与 query 锁顺序一致：trigram -> metas）
            self.insert_trigrams(docid, meta.path.as_path());
            self.insert_path_hash(docid, meta.path.as_path());

            let mut metas = self.metas.write();
            if let Some(existing) = metas.get_mut(docid as usize) {
                existing.path_off = new_off;
                existing.path_len = new_len;
                existing.size = meta.size;
                existing.mtime = meta.mtime;
            } else {
                // 极端情况：docid 槽位不存在，降级为 append
                let docid_new = self.alloc_docid(fkey, &meta.path, meta.size, meta.mtime);
                self.insert_trigrams(docid_new, meta.path.as_path());
                self.insert_path_hash(docid_new, meta.path.as_path());
            }

            // rename 视为“存在且活跃”
            self.tombstones.write().remove(docid);
            *self.dirty.write() = true;
            return;
        }

        // 新文件：分配 docid 并写入
        let docid = self.alloc_docid(fkey, &meta.path, meta.size, meta.mtime);
        self.insert_trigrams(docid, meta.path.as_path());
        self.insert_path_hash(docid, meta.path.as_path());
        *self.dirty.write() = true;
    }

    fn alloc_docid(
        &self,
        file_key: FileKey,
        path: &Path,
        size: u64,
        mtime: Option<std::time::SystemTime>,
    ) -> DocId {
        let (off, len) = {
            let mut arena = self.arena.write();
            arena.push_path(path).unwrap_or((0, 0))
        };

        let mut metas = self.metas.write();
        let docid: DocId = metas.len().try_into().unwrap_or(u32::MAX);
        metas.push(CompactMeta {
            file_key,
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
            let arena = self.arena.read();
            metas
                .get(docid as usize)
                .and_then(|m| arena.get_path_buf(m.path_off, m.path_len))
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
                    let bytes = arena.get_bytes(m.path_off, m.path_len).unwrap_or(&[]);
                    let s = std::str::from_utf8(bytes)
                        .map(std::borrow::Cow::Borrowed)
                        .unwrap_or_else(|_| String::from_utf8_lossy(bytes));
                    matcher.matches(&s)
                })
                .filter_map(|(_, m)| {
                    let path = arena.get_path_buf(m.path_off, m.path_len)?;
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
                        let bytes = arena.get_bytes(m.path_off, m.path_len).unwrap_or(&[]);
                        let s = std::str::from_utf8(bytes)
                            .map(std::borrow::Cow::Borrowed)
                            .unwrap_or_else(|_| String::from_utf8_lossy(bytes));
                        if matcher.matches(&s) {
                            let path = arena.get_path_buf(m.path_off, m.path_len)?;
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

    /// 批量应用事件
    pub fn apply_events(&self, events: &[EventRecord]) {
        use std::os::unix::fs::MetadataExt;

        for ev in events {
            match &ev.event_type {
                EventType::Create | EventType::Modify => {
                    if let Ok(meta) = std::fs::metadata(&ev.path) {
                        self.upsert(FileMeta {
                            file_key: FileKey {
                                dev: meta.dev(),
                                ino: meta.ino(),
                            },
                            path: ev.path.clone(),
                            size: meta.len(),
                            mtime: meta.modified().ok(),
                        });
                    }
                }
                EventType::Delete => {
                    self.mark_deleted_by_path(&ev.path);
                }
                EventType::Rename { from } => {
                    // rename 事件：强制更新路径（upsert_rename）
                    if let Some(docid) = self.lookup_docid_by_path(from.as_path()) {
                        let old_meta = self.metas.read().get(docid as usize).cloned();
                        if let Some(mut m) = old_meta {
                            // 更新 path
                            if let Some((off, len)) = self.arena.write().push_path(&ev.path) {
                                m.path_off = off;
                                m.path_len = len;
                            }
                            // 更新 size/mtime（文件可能在 rename 过程中被修改）
                            if let Ok(fs_meta) = std::fs::metadata(&ev.path) {
                                m.size = fs_meta.len();
                                m.mtime = fs_meta.modified().ok();
                            }
                            let path_buf = ev.path.clone();
                            self.remove_trigrams(docid, from.as_path());
                            self.remove_path_hash(docid, from.as_path());
                            self.insert_trigrams(docid, path_buf.as_path());
                            self.insert_path_hash(docid, path_buf.as_path());

                            if let Some(slot) = self.metas.write().get_mut(docid as usize) {
                                *slot = m;
                            }
                            self.tombstones.write().remove(docid);
                            *self.dirty.write() = true;
                        }
                    } else {
                        // 跨 FS rename = delete + create
                        self.mark_deleted_by_path(from);
                        if let Ok(meta) = std::fs::metadata(&ev.path) {
                            self.upsert(FileMeta {
                                file_key: FileKey {
                                    dev: meta.dev(),
                                    ino: meta.ino(),
                                },
                                path: ev.path.clone(),
                                size: meta.len(),
                                mtime: meta.modified().ok(),
                            });
                        }
                    }
                }
            }
        }
    }

    /// 导出 v4 快照数据
    pub fn export_snapshot_v4(&self) -> IndexSnapshotV4 {
        let arena = self.arena.read().clone();
        let metas = self.metas.read().clone();
        let tombstones = self.tombstones.read().iter().collect::<Vec<DocId>>();
        *self.dirty.write() = false;
        IndexSnapshotV4 {
            arena,
            metas,
            tombstones,
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
        let map_bytes = filekey_to_docid.capacity() as u64 * (map_entry_bytes + 1)
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

        let estimated_bytes =
            metas_bytes + map_bytes + arena_bytes + path_to_id_bytes + trigram_bytes + tomb_bytes;

        L2Stats {
            file_count,
            path_to_id_count,
            trigram_distinct,
            trigram_postings_total,
            tombstone_count,
            files_bytes: metas_bytes + map_bytes + arena_bytes,
            path_to_id_bytes,
            trigram_bytes,
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
                .and_then(|m| arena.get_bytes(m.path_off, m.path_len))
                .map(|b| b == bytes)
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

fn path_hash_bytes(bytes: &[u8]) -> u64 {
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    bytes.hash(&mut hasher);
    hasher.finish()
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
