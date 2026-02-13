use dashmap::DashMap;
use crate::core::rdd::FileEntry;

/// L1: 内存热缓存（DashMap实现）
pub struct L1Cache {
    pub inner: DashMap<std::path::PathBuf, FileEntry>,
    pub capacity: usize,
    pub access_count: DashMap<std::path::PathBuf, u64>,  // LRU辅助
}

impl L1Cache {
    pub fn with_capacity(cap: usize) -> Self {
        Self {
            inner: DashMap::with_capacity(cap),
            capacity: cap,
            access_count: DashMap::new(),
        }
    }
    
    pub fn query(&self, keyword: &str) -> Option<Vec<FileEntry>> {
        let results: Vec<FileEntry> = self.inner
            .iter()
            .filter(|e| e.key().to_string_lossy().contains(keyword))
            .map(|e| {
                self.access_count.insert(e.key().clone(), 
                    self.access_count.get(e.key()).map(|v| *v + 1).unwrap_or(1));
                e.value().clone()
            })
            .collect();
        
        if results.is_empty() {
            None
        } else {
            Some(results)
        }
    }
    
    pub fn insert(&self, path: std::path::PathBuf, entry: FileEntry) {
        if self.inner.len() >= self.capacity {
            // LRU淘汰
            let lru_key = self.access_count
                .iter()
                .min_by_key(|e| *e.value())
                .map(|e| e.key().clone());
            
            if let Some(key) = lru_key {
                self.inner.remove(&key);
                self.access_count.remove(&key);
            }
        }
        
        self.inner.insert(path, entry);
    }
    
    pub fn remove(&self, path: &std::path::Path) {
        self.inner.remove(path);
        self.access_count.remove(path);
    }
    
    pub fn warm_from(&self, entries: &[(std::path::PathBuf, FileEntry)]) {
        for (p, e) in entries.iter().take(self.capacity) {
            self.inner.insert(p.clone(), e.clone());
        }
    }
    
    pub fn snapshot(&self) -> Vec<(std::path::PathBuf, FileEntry)> {
        self.inner.iter()
            .map(|e| (e.key().clone(), e.value().clone()))
            .collect()
    }
}