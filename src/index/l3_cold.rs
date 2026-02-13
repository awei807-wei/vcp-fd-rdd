use crate::core::adaptive::AdaptiveScheduler;
use crate::core::rdd::FileEntry;
use ignore::WalkBuilder;

/// L3: 冷全量（弹性扫描）
pub struct L3Cold {
    pub adaptive: AdaptiveScheduler,
}

impl L3Cold {
    pub fn adaptive() -> Self {
        Self {
            adaptive: AdaptiveScheduler::new(),
        }
    }
    
    pub async fn scan(&self, keyword: &str, roots: &[std::path::PathBuf]) -> Vec<FileEntry> {
        let mut results = Vec::new();
        for root in roots {
            let walker = WalkBuilder::new(root)
                .hidden(true)
                .ignore(true)
                .git_ignore(true)
                .build();
            
            for entry in walker.filter_map(|e| e.ok()) {
                if entry.file_type().map(|ft| ft.is_file()).unwrap_or(false) {
                    let path = entry.path();
                    if path.to_string_lossy().contains(keyword) {
                        let metadata = entry.metadata().ok();
                        results.push(FileEntry {
                            path: path.to_path_buf(),
                            size: metadata.as_ref().map(|m| m.len()).unwrap_or(0),
                            modified: metadata.and_then(|m| m.modified().ok()),
                        });
                    }
                }
            }
        }
        results
    }
    
    pub async fn parallel_build(&self, shards: usize, streaming: bool) -> anyhow::Result<()> {
        tracing::info!("L3 parallel build: {} shards, streaming={}", shards, streaming);
        // 实际实现中这里会调用 ignore 库进行并行扫描
        Ok(())
    }
}