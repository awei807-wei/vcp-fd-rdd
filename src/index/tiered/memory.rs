use crate::stats::{
    infer_heap_high_water, EventPipelineStats, MemoryReport, OverlayStats, RebuildStats,
};
use std::collections::VecDeque;
use std::sync::Arc;

use super::TieredIndex;

impl TieredIndex {
    /// 手动刷新 base 索引（当 l2 被外部直接修改后需要调用）。
    pub fn refresh_base(&self) {
        let l2 = self.l2.load_full();
        let new_base = Arc::new(l2.to_base_index_data());
        self.base.store(new_base);
    }

    pub fn file_count(&self) -> usize {
        let base = self.base.load_full().file_count();
        let overlay = {
            let db = self.delta_buffer.lock();
            db.len()
        };
        base + overlay
    }

    /// 生成完整内存报告
    pub fn memory_report(&self, pipeline_stats: EventPipelineStats) -> MemoryReport {
        let l1 = self.l1.memory_stats();
        let l2 = self.l2.load_full().memory_stats();
        let overlay = {
            let db = self.delta_buffer.lock();
            let deleted_count = db.deleted_paths().count();
            let upserted_count = db.upserted_paths().count();
            OverlayStats {
                deleted_paths: deleted_count,
                upserted_paths: upserted_count,
                deleted_bytes: 0,
                upserted_bytes: 0,
                deleted_arena_len: 0,
                deleted_arena_cap: 0,
                upserted_arena_len: 0,
                upserted_arena_cap: 0,
                deleted_map_len: deleted_count,
                deleted_map_cap: db.len(),
                upserted_map_len: upserted_count,
                upserted_map_cap: db.len(),
                estimated_bytes: db.estimated_bytes() as u64,
            }
        };

        let rebuild = {
            let st = self.rebuild_state.lock();

            RebuildStats {
                in_progress: st.in_progress,
                pending_paths: 0,
                pending_map_cap: 0,
                pending_key_bytes: 0,
                pending_from_bytes: 0,
                estimated_bytes: 0,
            }
        };

        let (
            disk_segments,
            disk_deleted_paths,
            disk_deleted_bytes,
            disk_deleted_estimated_bytes,
            disk_deleted_estimated_bytes_max,
        ) = (0, 0, 0, 0, 0);

        let index_estimated_bytes = l1.estimated_bytes
            + l2.estimated_bytes
            + disk_deleted_estimated_bytes
            + overlay.estimated_bytes
            + rebuild.estimated_bytes;
        let process_smaps_rollup = MemoryReport::read_smaps_rollup();
        let (non_index_private_dirty_bytes, heap_high_water_suspected) = process_smaps_rollup
            .as_ref()
            .map(|s| {
                let (non, suspected) =
                    infer_heap_high_water(s.private_dirty_bytes, index_estimated_bytes);
                (Some(non), suspected)
            })
            .unwrap_or((None, false));

        MemoryReport {
            l1,
            l2,
            disk_segments,
            disk_deleted_paths,
            disk_deleted_bytes,
            disk_deleted_estimated_bytes,
            disk_deleted_estimated_bytes_max,
            event_pipeline: pipeline_stats,
            overlay,
            rebuild,
            process_rss_bytes: MemoryReport::read_process_rss(),
            process_smaps_rollup,
            process_faults: MemoryReport::read_faults(),
            index_estimated_bytes,
            non_index_private_dirty_bytes,
            heap_high_water_suspected,
        }
    }

    /// 定期内存报告循环
    pub async fn memory_report_loop(
        self: Arc<Self>,
        pipeline_stats_fn: Arc<dyn Fn() -> EventPipelineStats + Send + Sync>,
        interval_secs: u64,
    ) {
        if interval_secs == 0 {
            tracing::info!("Memory reporting disabled (interval_secs=0)");
            return;
        }
        // 首次报告延迟 5 秒
        tokio::time::sleep(std::time::Duration::from_secs(5)).await;

        let interval = std::time::Duration::from_secs(interval_secs);
        let mut rss_window: VecDeque<u64> = VecDeque::with_capacity(12);

        loop {
            let stats = pipeline_stats_fn();
            let report = self.memory_report(stats);

            rss_window.push_back(report.process_rss_bytes);
            while rss_window.len() > 12 {
                rss_window.pop_front();
            }

            let trend_mb_per_min = if rss_window.len() >= 2 {
                let first = *rss_window.front().unwrap_or(&0) as f64;
                let last = *rss_window.back().unwrap_or(&0) as f64;
                let minutes = ((rss_window.len() - 1) as f64 * interval_secs as f64) / 60.0;
                if minutes > 0.0 {
                    (last - first) / (1024.0 * 1024.0) / minutes
                } else {
                    0.0
                }
            } else {
                0.0
            };

            tracing::info!(
                "\n{}\n[heap-signal] index_est_bytes={} non_index_pd_bytes={} suspected={} rss_trend_mb_per_min={:+.2}",
                report,
                report.index_estimated_bytes,
                report.non_index_private_dirty_bytes.unwrap_or(0),
                report.heap_high_water_suspected,
                trend_mb_per_min
            );
            tokio::time::sleep(interval).await;
        }
    }
}
