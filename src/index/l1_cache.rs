use crate::core::{FileKey, FileMeta};
use crate::query::matcher::{GlobMode, Matcher};
use crate::stats::L1Stats;
use parking_lot::{Mutex, RwLock};
use std::collections::HashMap;
use std::path::Path;

#[derive(Clone, Copy, Debug, Default)]
struct LruLinks {
    prev: Option<FileKey>,
    next: Option<FileKey>,
}

#[derive(Debug, Default)]
struct LruState {
    head: Option<FileKey>,
    tail: Option<FileKey>,
    links: HashMap<FileKey, LruLinks>,
}

impl LruState {
    fn len(&self) -> usize {
        self.links.len()
    }

    fn touch(&mut self, key: FileKey) {
        if self.tail == Some(key) || !self.links.contains_key(&key) {
            return;
        }

        self.detach(key);
        self.attach_tail(key);
    }

    fn insert(&mut self, key: FileKey, capacity: usize) -> Option<FileKey> {
        if self.links.contains_key(&key) {
            self.touch(key);
            return None;
        }

        self.links.insert(key, LruLinks::default());
        self.attach_tail(key);

        if capacity > 0 && self.links.len() > capacity {
            return self.pop_lru();
        }

        None
    }

    fn remove(&mut self, key: FileKey) -> bool {
        let existed = self.links.contains_key(&key);
        if existed {
            self.detach(key);
            self.links.remove(&key);
        }
        existed
    }

    fn clear(&mut self) {
        self.head = None;
        self.tail = None;
        self.links.clear();
    }

    fn pop_lru(&mut self) -> Option<FileKey> {
        let key = self.head?;
        self.remove(key);
        Some(key)
    }

    fn detach(&mut self, key: FileKey) {
        let Some(links) = self.links.get(&key).copied() else {
            return;
        };

        match links.prev {
            Some(prev) => {
                if let Some(prev_links) = self.links.get_mut(&prev) {
                    prev_links.next = links.next;
                }
            }
            None => self.head = links.next,
        }

        match links.next {
            Some(next) => {
                if let Some(next_links) = self.links.get_mut(&next) {
                    next_links.prev = links.prev;
                }
            }
            None => self.tail = links.prev,
        }

        if let Some(slot) = self.links.get_mut(&key) {
            slot.prev = None;
            slot.next = None;
        }
    }

    fn attach_tail(&mut self, key: FileKey) {
        let old_tail = self.tail;

        if let Some(slot) = self.links.get_mut(&key) {
            slot.prev = old_tail;
            slot.next = None;
        }

        match old_tail {
            Some(tail) => {
                if let Some(tail_links) = self.links.get_mut(&tail) {
                    tail_links.next = Some(key);
                }
            }
            None => self.head = Some(key),
        }

        self.tail = Some(key);
    }
}

/// L1 查询结果缓存。
///
/// 数据使用 `RwLock<HashMap>` 保存，淘汰顺序由一个单独的 O(1) LRU 链表维护。
pub struct L1Cache {
    /// 主存储：FileKey -> FileMeta
    pub inner: RwLock<HashMap<FileKey, FileMeta>>,
    /// 路径反查：path -> FileKey
    pub path_index: RwLock<HashMap<std::path::PathBuf, FileKey>>,
    pub capacity: usize,
    lru: Mutex<LruState>,
}

impl L1Cache {
    pub fn with_capacity(cap: usize) -> Self {
        Self {
            inner: RwLock::new(HashMap::with_capacity(cap)),
            path_index: RwLock::new(HashMap::with_capacity(cap)),
            capacity: cap,
            lru: Mutex::new(LruState::default()),
        }
    }

    pub fn query(&self, matcher: &dyn Matcher) -> Option<Vec<FileMeta>> {
        let is_segment = matcher.glob_mode() == Some(GlobMode::Segment);
        let prefix = matcher.prefix();
        let case_sensitive = matcher.case_sensitive();
        let mut touched: Vec<FileKey> = Vec::new();
        let inner = self.inner.read();

        let results: Vec<FileMeta> = inner
            .iter()
            .filter(|(_, meta)| {
                if let Some(p) = prefix {
                    if case_sensitive {
                        if is_segment {
                            let basename = meta
                                .path
                                .file_name()
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
                }
                matcher.matches(&meta.path.to_string_lossy())
            })
            .map(|(fid, meta)| {
                touched.push(*fid);
                meta.clone()
            })
            .collect();
        drop(inner);

        if !touched.is_empty() {
            let mut lru = self.lru.lock();
            for fid in touched {
                lru.touch(fid);
            }
        }

        if results.is_empty() {
            None
        } else {
            Some(results)
        }
    }

    pub fn insert(&self, meta: FileMeta) {
        if self.capacity == 0 {
            return;
        }

        let fid = meta.file_key;
        if let Some(old_path) = self.inner.read().get(&fid).map(|e| e.path.clone()) {
            if old_path != meta.path {
                self.path_index.write().remove(&old_path);
            }
        }

        if let Some(evicted) = self.lru.lock().insert(fid, self.capacity) {
            if let Some(old) = self.inner.write().remove(&evicted) {
                self.path_index.write().remove(&old.path);
            }
        }

        self.path_index.write().insert(meta.path.clone(), fid);
        self.inner.write().insert(fid, meta);
    }

    pub fn remove_by_path(&self, path: &Path) {
        if let Some(fid) = self.path_index.write().remove(path) {
            self.inner.write().remove(&fid);
            self.lru.lock().remove(fid);
        }
    }

    pub fn remove(&self, fid: &FileKey) {
        if let Some(meta) = self.inner.write().remove(fid) {
            self.path_index.write().remove(&meta.path);
        }
        self.lru.lock().remove(*fid);
    }

    pub fn clear(&self) {
        self.inner.write().clear();
        self.path_index.write().clear();
        self.lru.lock().clear();
    }

    pub fn memory_stats(&self) -> L1Stats {
        use std::mem::size_of;

        let inner = self.inner.read();
        let path_index = self.path_index.read();
        let entry_count = inner.len();
        let path_index_count = path_index.len();
        let lru_entries = self.lru.lock().len();

        let avg_path_len: u64 = if entry_count > 0 {
            let total: u64 = inner
                .values()
                .map(|meta| meta.path.as_os_str().len() as u64)
                .sum();
            total / entry_count as u64
        } else {
            0
        };
        drop(path_index);
        drop(inner);

        let inner_bytes = entry_count as u64 * (16 + 64 + avg_path_len + 64);
        let path_index_bytes = path_index_count as u64 * (24 + avg_path_len + 16 + 64);
        let lru_bytes = lru_entries as u64 * (size_of::<(FileKey, LruLinks)>() as u64 + 32);

        L1Stats {
            entry_count,
            path_index_count,
            lru_entries,
            estimated_bytes: inner_bytes + path_index_bytes + lru_bytes,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::query::matcher::create_matcher;
    use std::path::PathBuf;

    fn meta(ino: u64, name: &str) -> FileMeta {
        FileMeta {
            file_key: FileKey { dev: 1, ino, generation: 0 },
            path: PathBuf::from(format!("/tmp/{name}")),
            size: ino,
            mtime: None,
            ctime: None,
            atime: None,
        }
    }

    #[test]
    fn evicts_true_lru_entry() {
        let cache = L1Cache::with_capacity(2);
        cache.insert(meta(1, "alpha.txt"));
        cache.insert(meta(2, "beta.txt"));

        let matcher = create_matcher("alpha", true);
        let hit = cache.query(matcher.as_ref()).unwrap();
        assert_eq!(hit.len(), 1);
        assert_eq!(hit[0].file_key.ino, 1);

        cache.insert(meta(3, "gamma.txt"));

        let inner = cache.inner.read();
        assert!(inner.contains_key(&FileKey { dev: 1, ino: 1, generation: 0 }));
        assert!(!inner.contains_key(&FileKey { dev: 1, ino: 2, generation: 0 }));
        assert!(inner.contains_key(&FileKey { dev: 1, ino: 3, generation: 0 }));
    }
}
