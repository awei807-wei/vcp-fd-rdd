use std::path::Path;
use dashmap::DashMap;
use crate::core::{FileId, FileMeta};
use crate::query::matcher::{Matcher, GlobMode};
use crate::stats::L1Stats;

/// L1: 查询结果热缓存（有界 DashMap，LRU 淘汰）
///
/// ## 语义说明
/// L1 是**查询结果缓存**，不是 L2 主索引的子集。
/// 它缓存最近被查询命中的 `FileMeta` 条目，以加速重复查询。
/// 主键为 `FileId`（与 L2 一致），辅以 `path_index` 反查。
///
/// L1 不参与索引构建流程，仅在 `TieredIndex::query()` 命中时回填。
/// 容量有限，超出时按 LRU 策略淘汰。
pub struct L1Cache {
    /// 主存储：FileId -> FileMeta（查询缓存，非主索引）
    pub inner: DashMap<FileId, FileMeta>,
    /// 路径反查：path -> FileId（用于按路径失效）
    pub path_index: DashMap<std::path::PathBuf, FileId>,
    pub capacity: usize,
    access_count: DashMap<FileId, u64>,
}

impl L1Cache {
    pub fn with_capacity(cap: usize) -> Self {
        Self {
            inner: DashMap::with_capacity(cap),
            path_index: DashMap::new(),
            capacity: cap,
            access_count: DashMap::new(),
        }
    }

    pub fn query(&self, matcher: &dyn Matcher) -> Option<Vec<FileMeta>> {
        let is_segment = matcher.glob_mode() == Some(GlobMode::Segment);
        let prefix = matcher.prefix();

        let results: Vec<FileMeta> = self.inner
            .iter()
            .filter(|e| {
                let meta = e.value();
                // 前缀启发式过滤
                if let Some(p) = prefix {
                    if is_segment {
                        let basename = meta.path.file_name()
                            .map(|n| n.to_string_lossy())
                            .unwrap_or_default();
                        if !basename.contains(p) {
                            return false;
                        }
                    } else {
                        let path_str = meta.path.to_string_lossy();
                        if !path_str.contains(p) {
                            return false;
                        }
                    }
                }
                matcher.matches(&meta.path.to_string_lossy())
            })
            .map(|e| {
                let fid = *e.key();
                self.access_count.entry(fid)
                    .and_modify(|v| *v += 1)
                    .or_insert(1);
                e.value().clone()
            })
            .collect();

        if results.is_empty() { None } else { Some(results) }
    }

    pub fn insert(&self, meta: FileMeta) {
        if self.inner.len() >= self.capacity {
            // LRU 淘汰
            let lru_key = self.access_count
                .iter()
                .min_by_key(|e| *e.value())
                .map(|e| *e.key());

            if let Some(key) = lru_key {
                if let Some((_, old)) = self.inner.remove(&key) {
                    self.path_index.remove(&old.path);
                }
                self.access_count.remove(&key);
            }
        }

        let fid = meta.file_id;
        self.path_index.insert(meta.path.clone(), fid);
        self.inner.insert(fid, meta);
    }

    pub fn remove_by_path(&self, path: &Path) {
        if let Some((_, fid)) = self.path_index.remove(path) {
            self.inner.remove(&fid);
            self.access_count.remove(&fid);
        }
    }

    pub fn remove(&self, fid: &FileId) {
        if let Some((_, meta)) = self.inner.remove(fid) {
            self.path_index.remove(&meta.path);
        }
        self.access_count.remove(fid);
    }

    pub fn clear(&self) {
        self.inner.clear();
        self.path_index.clear();
        self.access_count.clear();
    }

    /// 内存占用统计
    pub fn memory_stats(&self) -> L1Stats {
        let entry_count = self.inner.len();
        let path_index_count = self.path_index.len();
        let access_count_entries = self.access_count.len();

        // DashMap 每条 entry 开销 ≈ 数据 + ~64 bytes 控制结构
        let avg_path_len: u64 = if entry_count > 0 {
            let total: u64 = self.inner.iter()
                .map(|e| e.value().path.as_os_str().len() as u64)
                .sum();
            total / entry_count as u64
        } else {
            0
        };

        let inner_bytes = entry_count as u64 * (16 + 64 + avg_path_len + 64);
        let path_index_bytes = path_index_count as u64 * (24 + avg_path_len + 16 + 64);
        let access_bytes = access_count_entries as u64 * (16 + 8 + 64);

        L1Stats {
            entry_count,
            path_index_count,
            access_count_entries,
            estimated_bytes: inner_bytes + path_index_bytes + access_bytes,
        }
    }
}
