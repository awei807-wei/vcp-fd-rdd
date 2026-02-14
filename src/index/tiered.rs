use std::sync::Arc;
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

use crate::core::{FileMeta, EventRecord, EventType};
use crate::index::l1_cache::L1Cache;
use crate::index::l2_partition::PersistentIndex;
use crate::index::l3_cold::IndexBuilder;
use crate::query::matcher::create_matcher;
use crate::storage::snapshot::SnapshotStore;
use crate::stats::{MemoryReport, EventPipelineStats};

/// 三级索引：L1 热缓存 → L2 持久索引（内存常驻）→ L3 构建器（不在查询链路）
pub struct TieredIndex {
    pub l1: L1Cache,
    pub l2: PersistentIndex,
    pub l3: IndexBuilder,
    pub event_seq: AtomicU64,
    rebuild_in_progress: AtomicBool,
    pub roots: Vec<PathBuf>,
}

impl TieredIndex {
    pub fn new(l1: L1Cache, l2: PersistentIndex, l3: IndexBuilder, roots: Vec<PathBuf>) -> Self {
        Self {
            l1,
            l2,
            l3,
            event_seq: AtomicU64::new(0),
            rebuild_in_progress: AtomicBool::new(false),
            roots,
        }
    }

    /// 直接以空索引启动（显式忽略快照加载）
    pub fn empty(roots: Vec<PathBuf>) -> Self {
        let l1 = L1Cache::with_capacity(1000);
        let l2 = PersistentIndex::new();
        let l3 = IndexBuilder::new(roots.clone());
        Self::new(l1, l2, l3, roots)
    }

    /// 从快照加载或空索引启动
    pub async fn load_or_empty(store: &SnapshotStore, roots: Vec<PathBuf>) -> anyhow::Result<Self> {
        let l1 = L1Cache::with_capacity(1000);
        let l3 = IndexBuilder::new(roots.clone());

        let l2 = match store.load_if_valid().await {
            Ok(Some(snap)) => {
                tracing::info!("Loaded index snapshot: {} files", snap.files.len());
                PersistentIndex::from_snapshot(snap)
            }
            Ok(None) => {
                tracing::info!("No valid snapshot, starting with empty index");
                PersistentIndex::new()
            }
            Err(e) => {
                tracing::warn!("Failed to load snapshot: {}, starting empty", e);
                PersistentIndex::new()
            }
        };

        Ok(Self::new(l1, l2, l3, roots))
    }

    /// 后台全量构建
    pub fn spawn_full_build(self: &Arc<Self>) {
        if self
            .rebuild_in_progress
            .compare_exchange(false, true, Ordering::SeqCst, Ordering::SeqCst)
            .is_err()
        {
            tracing::debug!("Background build already in progress, skipping");
            return;
        }

        let idx = self.clone();
        std::thread::spawn(move || {
            tracing::info!("Starting background full build...");
            idx.l3.full_build(&idx.l2);
            idx.rebuild_in_progress.store(false, Ordering::SeqCst);
            tracing::info!("Background full build complete: {} files", idx.l2.file_count());
        });
    }

    /// overflow / watcher 异常时的兜底：清空索引并后台全量重建，避免索引长期漂移。
    pub fn spawn_rebuild(self: &Arc<Self>, reason: &'static str) {
        if self
            .rebuild_in_progress
            .compare_exchange(false, true, Ordering::SeqCst, Ordering::SeqCst)
            .is_err()
        {
            tracing::debug!("Background rebuild already in progress, skipping ({})", reason);
            return;
        }

        let idx = self.clone();
        std::thread::spawn(move || {
            tracing::warn!("Starting background rebuild: {}", reason);
            idx.l1.clear();
            idx.l2.reset();
            idx.l3.full_build(&idx.l2);
            idx.rebuild_in_progress.store(false, Ordering::SeqCst);
            tracing::warn!("Background rebuild complete: {} files", idx.l2.file_count());
        });
    }

    /// 查询入口：L1 → L2，不扫盘
    pub fn query(&self, keyword: &str) -> Vec<FileMeta> {
        let matcher = create_matcher(keyword);

        // L1: 热缓存
        if let Some(results) = self.l1.query(matcher.as_ref()) {
            tracing::debug!("L1 hit: {} results", results.len());
            return results;
        }

        // L2: 持久索引（内存常驻，trigram 加速）
        let results = self.l2.query(matcher.as_ref());

        if !results.is_empty() {
            tracing::debug!("L2 hit: {} results", results.len());
            // 回填 L1（有界）
            for meta in results.iter().take(10) {
                self.l1.insert(meta.clone());
            }
            return results;
        }

        // miss：不在查询链路扫盘，返回空
        // 可选：触发后台补扫
        self.l2.maybe_schedule_repair();
        Vec::new()
    }

    /// 批量应用事件到索引
    pub fn apply_events(&self, events: &[EventRecord]) {
        self.l2.apply_events(events);

        // 同步更新 L1
        for ev in events {
            match &ev.event_type {
                EventType::Delete => {
                    self.l1.remove_by_path(&ev.path);
                }
                EventType::Rename { from } => {
                    self.l1.remove_by_path(from);
                }
                _ => {}
            }
        }

        self.event_seq.fetch_add(events.len() as u64, Ordering::Relaxed);
    }

    /// 原子快照
    pub async fn snapshot_now(&self, store: &SnapshotStore) -> anyhow::Result<()> {
        if !self.l2.is_dirty() {
            tracing::debug!("Index not dirty, skipping snapshot");
            return Ok(());
        }
        let snap = self.l2.export_snapshot();
        store.write_atomic(&snap).await
    }

    /// 定期快照循环
    pub async fn snapshot_loop(self: Arc<Self>, store: Arc<SnapshotStore>, interval_secs: u64) {
        loop {
            tokio::time::sleep(std::time::Duration::from_secs(interval_secs)).await;
            if let Err(e) = self.snapshot_now(&store).await {
                tracing::error!("Snapshot failed: {}", e);
            }
        }
    }

    pub fn file_count(&self) -> usize {
        self.l2.file_count()
    }

    /// 生成完整内存报告
    pub fn memory_report(&self, pipeline_stats: EventPipelineStats) -> MemoryReport {
        MemoryReport {
            l1: self.l1.memory_stats(),
            l2: self.l2.memory_stats(),
            event_pipeline: pipeline_stats,process_rss_bytes: MemoryReport::read_process_rss(),}
    }

    /// 定期内存报告循环
    pub async fn memory_report_loop(
        self: Arc<Self>,
        pipeline_stats_fn: Arc<dyn Fn() -> EventPipelineStats + Send + Sync>,
        interval_secs: u64,
    ) {
        // 首次报告延迟 5 秒
        tokio::time::sleep(std::time::Duration::from_secs(5)).await;

        loop {
            let stats = pipeline_stats_fn();
            let report = self.memory_report(stats);
            tracing::info!("\n{}", report);
            tokio::time::sleep(std::time::Duration::from_secs(interval_secs)).await;
        }
    }
}
