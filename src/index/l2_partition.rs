use std::collections::{HashMap, HashSet};
use std::path::PathBuf;
use std::hash::{Hash, Hasher};
use std::collections::hash_map::DefaultHasher;
use parking_lot::RwLock;
use serde::{Serialize, Deserialize};

use crate::core::{FileId, FileMeta, EventRecord, EventType};
use crate::query::matcher::Matcher;
use crate::stats::L2Stats;

/// Trigram：3 字节子串，用于倒排索引加速查询
type Trigram = [u8; 3];

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

/// 从查询词中提取 trigram 集合
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

/// 可序列化的索引快照数据
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct IndexSnapshotV2 {
    pub files: HashMap<FileId, FileMeta>,
    pub path_to_id: HashMap<PathBuf, FileId>,
    pub tombstones: HashSet<FileId>,
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

/// v0.2.1 起的新快照格式：不落盘 path_to_id（可从 files 重建）
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct IndexSnapshot {
    pub files: HashMap<FileId, FileMeta>,
    pub tombstones: HashSet<FileId>,
}

impl IndexSnapshot {
    pub fn new() -> Self {
        Self {
            files: HashMap::new(),
            tombstones: HashSet::new(),
        }
    }
}

#[derive(Clone, Debug)]
enum OneOrManyFileId {
    One(FileId),
    Many(Vec<FileId>),
}

impl OneOrManyFileId {
    fn iter(&self) -> impl Iterator<Item = &FileId> {
        match self {
            OneOrManyFileId::One(fid) => std::slice::from_ref(fid).iter(),
            OneOrManyFileId::Many(v) => v.iter(),
        }
    }

    fn insert(&mut self, fid: FileId) {
        match self {
            OneOrManyFileId::One(existing) => {
                if *existing == fid {
                    return;
                }
                *self = OneOrManyFileId::Many(vec![*existing, fid]);
            }
            OneOrManyFileId::Many(v) => {
                if !v.contains(&fid) {
                    v.push(fid);
                }
            }
        }
    }

    /// 返回 true 表示变为空，需要从 map 移除
    fn remove(&mut self, fid: FileId) -> bool {
        match self {
            OneOrManyFileId::One(existing) => *existing == fid,
            OneOrManyFileId::Many(v) => {
                v.retain(|x| *x != fid);
                if v.len() == 1 {
                    let only = v[0];
                    *self = OneOrManyFileId::One(only);
                    false
                } else {
                    v.is_empty()
                }
            }
        }
    }

    fn len(&self) -> usize {
        match self {
            OneOrManyFileId::One(_) => 1,
            OneOrManyFileId::Many(v) => v.len(),
        }
    }
}

/// L2: 持久索引（内存常驻，可直接查询；trigram 倒排加速）
///
/// ## 单路径策略 (Single-Path Policy)
/// 一个 `FileId(dev, ino)` 只存储一条路径（最先发现的那个）。
/// Hardlink 的其他路径视为"不在索引中"。
/// 理由：简单、可预测、够用。如需多路径支持，需扩展为 `FileId -> Vec<PathBuf>`。
pub struct PersistentIndex {
    /// 主存储：FileId -> FileMeta（单路径：一个 inode 只存一条记录）
    files: RwLock<HashMap<FileId, FileMeta>>,
    /// 路径反查（轻量）：hash(path) -> FileId（或少量冲突列表）
    ///
    /// 目的：避免 `HashMap<PathBuf, FileId>` 导致路径在内存里存两份（files + path_to_id）。
    /// 查找时需要二次校验（通过 files 中的真实 path 比对）以保证正确性。
    path_hash_to_id: RwLock<HashMap<u64, OneOrManyFileId>>,
    /// Trigram 倒排索引：trigram -> posting_list<FileId>
    ///
    /// v0.2 优先目标：降低常驻内存占用。
    /// - `HashSet` 作为 posting list 会引入巨大的桶/指针开销；
    /// - 这里改为 `Vec<FileId>`（append-only，删除/rename 时线性移除）。
    /// 注意：为避免 Modify 等重复 upsert 造成 posting 膨胀，`upsert_inner` 对“同路径更新”会走快路径，不重复插入 trigram。
    trigram_index: RwLock<HashMap<Trigram, Vec<FileId>>>,
    /// 墓碑标记（延迟删除）
    tombstones: RwLock<HashSet<FileId>>,
    /// 脏标记（自上次快照后是否有变更）
    dirty: RwLock<bool>,
}

impl PersistentIndex {
    pub fn new() -> Self {
        Self {
            files: RwLock::new(HashMap::new()),
            path_hash_to_id: RwLock::new(HashMap::new()),
            trigram_index: RwLock::new(HashMap::new()),
            tombstones: RwLock::new(HashSet::new()),
            dirty: RwLock::new(false),
        }
    }

    /// 从快照恢复
    pub fn from_snapshot(snap: IndexSnapshot) -> Self {
        let idx = Self::new();
        {
            let mut files = idx.files.write();
            let mut path_hash_to_id = idx.path_hash_to_id.write();
            let mut trigram = idx.trigram_index.write();
            let mut tombstones = idx.tombstones.write();

            for (fid, meta) in &snap.files {
                if let Some(name) = meta.path.file_name().and_then(|n| n.to_str()) {
                    for tri in extract_trigrams(name) {
                        trigram.entry(tri).or_default().push(*fid);
                    }
                }
                let h = path_hash(meta.path.as_path());
                path_hash_to_id
                    .entry(h)
                    .and_modify(|v| v.insert(*fid))
                    .or_insert(OneOrManyFileId::One(*fid));
                files.insert(*fid, meta.clone());
            }
            *tombstones = snap.tombstones;
        }
        idx
    }

    /// 插入/更新一条文件记录
    ///
    /// ## 单路径策略 (first-seen wins)
    /// 如果该 FileId 已存在且路径不同（hardlink 场景），
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
        let fid = meta.file_id;

        {
            let files = self.files.read();
            if let Some(old) = files.get(&fid) {
                // 同路径的重复上报（Modify/Create/或重复 Rename）：只更新元数据，
                // 不触碰 trigram/posting，避免 posting 反复插入导致内存膨胀。
                if old.path == meta.path {
                    drop(files);
                    let mut files_w = self.files.write();
                    if let Some(existing) = files_w.get_mut(&fid) {
                        existing.size = meta.size;
                        existing.mtime = meta.mtime;
                    }
                    *self.dirty.write() = true;
                    return;
                }

                if old.path != meta.path {
                    // clone 旧路径后立即释放读锁，避免 use-after-drop
                    let old_path = old.path.clone();
                    drop(files);
                    if force_path_update {
                        // rename：移除旧路径的 trigram 和反查
                        self.remove_trigrams(fid, &old_path);
                        self.remove_path_hash(fid, &old_path);
                    } else {
                        // hardlink / 重复发现：保留旧路径，仅更新元数据
                        let mut files_w = self.files.write();
                        if let Some(existing) = files_w.get_mut(&fid) {
                            existing.size = meta.size;
                            existing.mtime = meta.mtime;
                        }
                        *self.dirty.write() = true;
                        return;
                    }
                }
            }
        }

        // 插入新 trigram
        if let Some(name) = meta.path.file_name().and_then(|n| n.to_str()) {
            let mut tri_idx = self.trigram_index.write();
            for tri in extract_trigrams(name) {
                tri_idx.entry(tri).or_default().push(fid);
            }
        }

        self.insert_path_hash(fid, meta.path.as_path());
        self.files.write().insert(fid, meta);
        self.tombstones.write().remove(&fid);
        *self.dirty.write() = true;
    }

    /// 标记删除（tombstone）
    pub fn mark_deleted(&self, fid: FileId) {
        let path = self.files.read().get(&fid).map(|m| m.path.clone());
        if let Some(p) = path {
            self.remove_trigrams(fid, &p);
            self.remove_path_hash(fid, &p);
        }
        self.files.write().remove(&fid);
        self.tombstones.write().insert(fid);
        *self.dirty.write() = true;
    }

    /// 按路径删除
    pub fn mark_deleted_by_path(&self, path: &std::path::Path) {
        if let Some(fid) = self.lookup_fid_by_path(path) {
            self.mark_deleted(fid);
        }
    }

    /// 查询：trigram 候选集 → 精确过滤
    pub fn query(&self, matcher: &dyn Matcher) -> Vec<FileMeta> {
        // 重要：先读取 trigram_index 计算候选集，再读取 files/tombstones。
        // 写入路径通常是先更新 trigram_index 再更新 files，如果这里反过来拿锁，
        // 在“边写边查”场景下可能形成死锁（查询持有 files.read 等待 trigram.read，
        // 写入持有 trigram.write 等待 files.write）。
        let candidates = self.trigram_candidates(matcher);
        let files = self.files.read();
        let tombstones = self.tombstones.read();

        match candidates {
            Some(fids) => {
                fids.iter()
                    .filter(|fid| !tombstones.contains(fid))
                    .filter_map(|fid| files.get(fid))
                    .filter(|meta| matcher.matches(&meta.path.to_string_lossy()))
                    .cloned()
                    .collect()
            }
            None => {
                // 无法用 trigram 加速（查询词太短），全量过滤
                files.values()
                    .filter(|meta| {
                        !tombstones.contains(&meta.file_id)
                            && matcher.matches(&meta.path.to_string_lossy())
                    })
                    .cloned()
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
                            file_id: FileId {
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
                    if let Some(fid) = self.lookup_fid_by_path(from.as_path()) {
                        let old_meta = self.files.read().get(&fid).cloned();
                        if let Some(mut m) = old_meta {
                            m.path = ev.path.clone();
                            // 更新 size/mtime（文件可能在 rename 过程中被修改）
                            if let Ok(fs_meta) = std::fs::metadata(&ev.path) {
                                m.size = fs_meta.len();
                                m.mtime = fs_meta.modified().ok();
                            }
                            self.upsert_rename(m);
                        }
                    } else {
                        // 跨 FS rename = delete + create
                        self.mark_deleted_by_path(from);
                        if let Ok(meta) = std::fs::metadata(&ev.path) {
                            self.upsert(FileMeta {
                                file_id: FileId {
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

    /// 导出快照数据
    pub fn export_snapshot(&self) -> IndexSnapshot {
        let files = self.files.read().clone();
        let tombstones = self.tombstones.read().clone();
        *self.dirty.write() = false;
        IndexSnapshot { files, tombstones }
    }

    pub fn is_dirty(&self) -> bool {
        *self.dirty.read()
    }

    pub fn file_count(&self) -> usize {
        self.files.read().len()
    }

    /// 内存占用统计
    pub fn memory_stats(&self) -> L2Stats {
        let files = self.files.read();
        let path_hash_to_id = self.path_hash_to_id.read();
        let trigram_index = self.trigram_index.read();
        let tombstones = self.tombstones.read();

        let file_count = files.len();
        let path_to_id_count: usize = path_hash_to_id.values().map(|v| v.len()).sum();
        let trigram_distinct = trigram_index.len();
        let tombstone_count = tombstones.len();

        // --- files: HashMap<FileId, FileMeta> ---
        // HashMap 桶开销: capacity * (sizeof(key) + sizeof(value) + 8 bytes 控制)
        // FileId = 16 bytes
        // FileMeta = 16(FileId) + 24(PathBuf 栈) + avg_path_heap + 8(size) + 16(Option<SystemTime>)
        //         = 64 bytes 栈 + 堆上路径
        let avg_path_len: u64 = if file_count > 0 {
            let total: u64 = files.values()
                .map(|m| m.path.as_os_str().len() as u64)
                .sum();
            total / file_count as u64
        } else {
            0
        };
        // HashMap 每个桶: key(16) + value(64 栈) + 路径堆(avg) + 控制字节(8)
        let files_bytes = file_count as u64 * (16 + 64 + avg_path_len + 8);

        // --- path_hash_to_id: HashMap<u64, FileId> ---
        // 轻量反查：每条按 key(8) + value(16) + 控制开销粗估
        let path_to_id_bytes = path_to_id_count as u64 * (8 + 16 + 16);

        // --- trigram_index: HashMap<[u8;3], Vec<FileId>> ---
        // posting list 改为 Vec 后，常驻内存通常显著低于 HashSet（少桶/少指针）。
        // 估算：每个 posting entry 约 FileId(16)；每个 trigram 桶约 64（含 Vec/HashMap 控制开销的粗估）。
        let mut trigram_postings_total: usize = 0;
        for posting in trigram_index.values() {
            trigram_postings_total += posting.len();
        }
        let trigram_bytes = trigram_distinct as u64 * 64
            + trigram_postings_total as u64 * 16;

        let estimated_bytes = files_bytes + path_to_id_bytes + trigram_bytes
            + tombstone_count as u64 * 24; // HashSet<FileId> 每条 ~24

        L2Stats {
            file_count,
            path_to_id_count,
            trigram_distinct,
            trigram_postings_total,
            tombstone_count,
            files_bytes,
            path_to_id_bytes,
            trigram_bytes,
            estimated_bytes,
        }
    }

    /// Compaction：清理墓碑
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
        self.files.write().clear();
        self.tombstones.write().clear();
        *self.dirty.write() = true;
    }

    // ── 内部方法 ──

    fn remove_trigrams(&self, fid: FileId, path: &std::path::Path) {
        if let Some(name) = path.file_name().and_then(|n| n.to_str()) {
            let mut tri_idx = self.trigram_index.write();
            for tri in extract_trigrams(name) {
                if let Some(posting) = tri_idx.get_mut(&tri) {
                    // Vec posting：线性移除（rename/delete 频率通常远低于 query）
                    posting.retain(|x| *x != fid);
                    if posting.is_empty() {
                        tri_idx.remove(&tri);
                    }
                }
            }
        }
    }

    fn insert_path_hash(&self, fid: FileId, path: &std::path::Path) {
        let h = path_hash(path);
        let mut map = self.path_hash_to_id.write();
        map.entry(h)
            .and_modify(|v| v.insert(fid))
            .or_insert(OneOrManyFileId::One(fid));
    }

    fn remove_path_hash(&self, fid: FileId, path: &std::path::Path) {
        let h = path_hash(path);
        let mut map = self.path_hash_to_id.write();
        if let Some(v) = map.get_mut(&h) {
            let empty = v.remove(fid);
            if empty {
                map.remove(&h);
            }
        }
    }

    fn lookup_fid_by_path(&self, path: &std::path::Path) -> Option<FileId> {
        let h = path_hash(path);

        // 先复制候选 FileId（避免同时持有 path_hash_to_id 与 files 的锁）
        let candidates: Vec<FileId> = {
            let map = self.path_hash_to_id.read();
            let v = map.get(&h)?;
            v.iter().copied().collect()
        };

        if candidates.is_empty() {
            return None;
        }

        let files = self.files.read();
        candidates
            .into_iter()
            .find(|fid| files.get(fid).map(|m| m.path.as_path() == path).unwrap_or(false))
    }

    fn trigram_candidates(&self, matcher: &dyn Matcher) -> Option<Vec<FileId>> {
        let prefix = matcher.prefix()?;
        let tris = query_trigrams(prefix);
        if tris.is_empty() {
            return None;
        }

        let tri_idx = self.trigram_index.read();
        // 低成本候选集策略：取 posting 最短的 trigram 作为候选集。
        // - 避免对大 posting 做多次交集/分配导致抖动
        // - 仍保持正确性（候选集为“可能命中”的超集，最终由 matcher 精确过滤）
        let mut best: Option<&Vec<FileId>> = None;
        for tri in &tris {
            let posting = match tri_idx.get(tri) {
                Some(p) => p,
                None => return Some(Vec::new()), // 任一 trigram 缺失则必不命中
            };
            best = match best {
                Some(cur) if cur.len() <= posting.len() => Some(cur),
                _ => Some(posting),
            };
        }
        Some(best.cloned().unwrap_or_default())
    }
}

fn path_hash(path: &std::path::Path) -> u64 {
    let mut hasher = DefaultHasher::new();
    path.as_os_str().hash(&mut hasher);
    hasher.finish()
}
