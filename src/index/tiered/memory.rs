use std::collections::VecDeque;
use std::sync::Arc;
use std::time::Duration;

use crate::stats::{
    infer_heap_high_water, EventPipelineStats, MemoryReport, OverlayStats, RebuildStats,
};
use crate::util::maybe_trim_rss;

use super::TieredIndex;

impl TieredIndex {
    pub fn file_count(&self) -> usize {
        let l2 = self.l2.load_full().file_count();
        if l2 > 0 {
            return l2;
        }
        self.disk_layers
            .read()
            .first()
            .map(|b| b.idx.file_count_estimate())
            .unwrap_or(0)
    }

    /// 生成完整内存报告
    pub fn memory_report(&self, pipeline_stats: EventPipelineStats) -> MemoryReport {
        let l1 = self.l1.memory_stats();
        let l2 = self.l2.load_full().memory_stats();
        let overlay = {
            let ov = self.overlay_state.lock();
            OverlayStats {
                deleted_paths: ov.deleted_paths.len_paths(),
                upserted_paths: ov.upserted_paths.len_paths(),
                deleted_bytes: ov.deleted_paths.active_bytes(),
                upserted_bytes: ov.upserted_paths.active_bytes(),
                deleted_arena_len: ov.deleted_paths.arena_len(),
                deleted_arena_cap: ov.deleted_paths.arena_cap(),
                upserted_arena_len: ov.upserted_paths.arena_len(),
                upserted_arena_cap: ov.upserted_paths.arena_cap(),
                deleted_map_len: ov.deleted_paths.map_len(),
                deleted_map_cap: ov.deleted_paths.map_cap(),
                upserted_map_len: ov.upserted_paths.map_len(),
                upserted_map_cap: ov.upserted_paths.map_cap(),
                estimated_bytes: ov.deleted_paths.estimated_bytes()
                    + ov.upserted_paths.estimated_bytes(),
            }
        };

        let rebuild = {
            use std::mem::size_of;

            use super::rebuild::PendingEvent;
            use crate::core::{EventType, FileIdentifier};

            let st = self.rebuild_state.lock();
            let mut key_bytes = 0u64;
            let mut from_bytes = 0u64;
            for (k, v) in st.pending_events.iter() {
                key_bytes += match k {
                    FileIdentifier::Path(p) => p.as_os_str().as_encoded_bytes().len() as u64,
                    FileIdentifier::Fid { .. } => 16,
                };
                if let EventType::Rename {
                    from,
                    from_path_hint,
                } = &v.event_type
                {
                    from_bytes += match from {
                        FileIdentifier::Path(p) => p.as_os_str().as_encoded_bytes().len() as u64,
                        FileIdentifier::Fid { .. } => 16,
                    };
                    if let Some(p) = from_path_hint {
                        from_bytes += p.as_os_str().as_encoded_bytes().len() as u64;
                    }
                }
                if let Some(p) = &v.path_hint {
                    key_bytes += p.as_os_str().as_encoded_bytes().len() as u64;
                }
            }
            let cap = st.pending_events.capacity();
            let entry = size_of::<(FileIdentifier, PendingEvent)>() as u64;
            let estimated = cap as u64 * (entry + 16) + key_bytes + from_bytes;

            RebuildStats {
                in_progress: st.in_progress,
                pending_paths: st.pending_events.len(),
                pending_map_cap: st.pending_events.capacity(),
                pending_key_bytes: key_bytes,
                pending_from_bytes: from_bytes,
                estimated_bytes: estimated,
            }
        };

        let (
            disk_segments,
            disk_deleted_paths,
            disk_deleted_bytes,
            disk_deleted_estimated_bytes,
            disk_deleted_estimated_bytes_max,
        ) = {
            let layers = self.disk_layers.read();
            let mut total_paths: usize = 0;
            let mut total_bytes: u64 = 0;
            let mut total_est: u64 = 0;
            let mut max_est: u64 = 0;
            for l in layers.iter() {
                total_paths = total_paths.saturating_add(l.deleted_paths_count);
                total_bytes = total_bytes.saturating_add(l.deleted_paths_bytes);
                total_est = total_est.saturating_add(l.deleted_paths_estimated_bytes);
                max_est = max_est.max(l.deleted_paths_estimated_bytes);
            }
            (layers.len(), total_paths, total_bytes, total_est, max_est)
        };

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

    /// 条件性 RSS trim 循环：
    /// - 周期检查 smaps 的 Private_Dirty
    /// - 当"非索引脏页偏高"且超过阈值时触发一次 trim
    pub async fn rss_trim_loop(self: Arc<Self>, interval_secs: u64, trim_pd_threshold_mb: u64) {
        if interval_secs == 0 || trim_pd_threshold_mb == 0 {
            tracing::info!(
                "RSS trim disabled (interval_secs={}, trim_pd_threshold_mb={})",
                interval_secs,
                trim_pd_threshold_mb
            );
            return;
        }

        let interval = Duration::from_secs(interval_secs);
        let threshold_bytes = trim_pd_threshold_mb.saturating_mul(1024 * 1024);
        let mut non_idx_pd_window: VecDeque<u64> = VecDeque::with_capacity(6);

        loop {
            tokio::time::sleep(interval).await;

            let report = self.memory_report(EventPipelineStats::default());
            let Some(smaps) = report.process_smaps_rollup.as_ref() else {
                continue;
            };
            let non_idx_pd = report
                .non_index_private_dirty_bytes
                .unwrap_or(smaps.private_dirty_bytes);
            non_idx_pd_window.push_back(non_idx_pd);
            while non_idx_pd_window.len() > 6 {
                non_idx_pd_window.pop_front();
            }

            let trend_mb_per_min = if non_idx_pd_window.len() >= 2 {
                let first = *non_idx_pd_window.front().unwrap_or(&0) as f64;
                let last = *non_idx_pd_window.back().unwrap_or(&0) as f64;
                let minutes = ((non_idx_pd_window.len() - 1) as f64 * interval_secs as f64) / 60.0;
                if minutes > 0.0 {
                    (last - first) / (1024.0 * 1024.0) / minutes
                } else {
                    0.0
                }
            } else {
                0.0
            };

            let growing = trend_mb_per_min >= 0.5;
            let very_high = smaps.private_dirty_bytes >= threshold_bytes.saturating_mul(2);
            if smaps.private_dirty_bytes < threshold_bytes
                || !report.heap_high_water_suspected
                || (!growing && !very_high)
            {
                continue;
            }

            tracing::warn!(
                "RSS trim trigger: private_dirty_bytes={} threshold_bytes={} index_est_bytes={} non_index_pd_bytes={} trend_mb_per_min={:+.2}",
                smaps.private_dirty_bytes,
                threshold_bytes,
                report.index_estimated_bytes,
                report.non_index_private_dirty_bytes.unwrap_or(0),
                trend_mb_per_min
            );
            maybe_trim_rss();

            tokio::time::sleep(Duration::from_millis(120)).await;
            let after = MemoryReport::read_smaps_rollup()
                .map(|s| s.private_dirty_bytes)
                .unwrap_or(0);
            tracing::info!(
                "RSS trim done: private_dirty_bytes={} -> {}",
                smaps.private_dirty_bytes,
                after
            );
        }
    }
}
