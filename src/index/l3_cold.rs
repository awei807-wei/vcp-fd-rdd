use std::path::PathBuf;
use crate::core::{FsScanRDD, BuildRDD, FileMeta};
use crate::index::l2_partition::PersistentIndex;

/// L3: IndexBuilder — 仅用于启动全扫/补扫/重建，不进入查询链路
pub struct IndexBuilder {
    pub roots: Vec<PathBuf>,
}

impl IndexBuilder {
    pub fn new(roots: Vec<PathBuf>) -> Self {
        Self { roots }
    }

    /// 全量构建：扫描所有 roots，流式灌入 PersistentIndex
    pub fn full_build(&self, index: &PersistentIndex) {
        let rdd = FsScanRDD::from_roots(self.roots.clone());
        let mut count = 0usize;

        rdd.for_each(|meta: FileMeta| {
            index.upsert(meta);
            count += 1;
            if count % 10000 == 0 {
                tracing::info!("IndexBuilder: scanned {} files...", count);
            }
        });

        tracing::info!("IndexBuilder: full build complete, {} files indexed", count);
    }

    /// 增量补扫：扫描指定目录，补充缺失条目
    pub fn incremental_scan(&self, index: &PersistentIndex, dirs: Vec<PathBuf>) {
        let rdd = FsScanRDD::from_roots(dirs);
        let mut count = 0usize;

        rdd.for_each(|meta: FileMeta| {
            index.upsert(meta);
            count += 1;
        });

        tracing::info!("IndexBuilder: incremental scan complete, {} files updated", count);
    }
}