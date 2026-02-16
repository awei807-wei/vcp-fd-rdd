use crate::core::{BuildRDD, ExecutionStrategy, FileMeta, FsScanRDD};
use crate::index::l2_partition::PersistentIndex;
use std::path::PathBuf;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

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

    /// 全量构建（带执行策略）：用于 rebuild/full_build 的“弹性计算”。
    pub fn full_build_with_strategy(
        &self,
        index: &Arc<PersistentIndex>,
        strategy: ExecutionStrategy,
    ) {
        let mut parallelism = match strategy {
            ExecutionStrategy::Serial => 1,
            ExecutionStrategy::Parallel { shards, .. } => shards.max(1),
        };
        // 兜底：避免过高并发导致系统抖动；scheduler 可能会放大 shards。
        let max_threads = num_cpus::get().saturating_mul(2).max(1);
        parallelism = parallelism.clamp(1, max_threads);

        let rdd = FsScanRDD::from_roots(self.roots.clone()).with_parallelism(parallelism);
        let count = Arc::new(AtomicUsize::new(0));
        let idx = index.clone();
        let c = count.clone();

        rdd.for_each_meta(move |meta: FileMeta| {
            idx.upsert(meta);
            let n = c.fetch_add(1, Ordering::Relaxed) + 1;
            if n % 10000 == 0 {
                tracing::info!("IndexBuilder: scanned {} files...", n);
            }
        });

        let total = count.load(Ordering::Relaxed);
        tracing::info!(
            "IndexBuilder: full build complete, {} files indexed (parallelism={}, strategy={:?})",
            total,
            parallelism,
            strategy
        );
    }

    /// 增量补扫：扫描指定目录，补充缺失条目
    pub fn incremental_scan(&self, index: &PersistentIndex, dirs: Vec<PathBuf>) {
        let rdd = FsScanRDD::from_roots(dirs);
        let mut count = 0usize;

        rdd.for_each(|meta: FileMeta| {
            index.upsert(meta);
            count += 1;
        });

        tracing::info!(
            "IndexBuilder: incremental scan complete, {} files updated",
            count
        );
    }
}
