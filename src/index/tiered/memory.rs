use crate::stats::{
    infer_heap_high_water, EventPipelineStats, GenerationStats, MemoryReport, MemorySampleDepth,
    OverlayStats, QueryGuardStats, RebuildStats,
};
use crate::util::maybe_trim_rss;
use std::collections::VecDeque;
use std::sync::Arc;
use std::time::Instant;

use super::TieredIndex;

impl TieredIndex {
    pub(crate) fn invalidate_memory_report_cache(&self) {
        let mut cache = self.memory_report_cache.lock();
        cache.report = None;
        cache.sampled_at = None;
    }

    /// Compatibility/test boundary: rebuild the read-only base from the mutable
    /// L2 index after external test helpers or one-off compatibility code mutate
    /// L2 directly.
    ///
    /// Ordinary query, event apply, fast-sync, and watcher paths must not call
    /// this method; they should read base + overlay/L2 without full materializing.
    pub fn refresh_base(&self) {
        self.stats.record_refresh_base();
        let l2 = self.l2.load_full();
        let new_base = Arc::new(l2.to_base_index_data());
        self.base.store(new_base);
        self.delta_buffer.lock().clear();
        self.invalidate_memory_report_cache();
    }

    pub fn file_count(&self) -> usize {
        // Read the rebuild flag *before* locking the overlay to preserve the same
        // rebuild_state → delta_buffer lock order as finish_rebuild (acquiring them
        // in the opposite order would risk an AB-BA deadlock). Then capture
        // (base, overlay) under a single delta_buffer lock so the pair is consistent
        // with finish_rebuild's atomic publish; otherwise a torn read at publish time
        // can momentarily report the empty post-reset l2 count instead of the freshly
        // published base count. See execute_query_plan for the matching query-path fix.
        let rebuild_in_progress = self.rebuild_in_progress();
        let db = self.delta_buffer.lock();
        let base_count = self.base.load().file_count();
        let overlay_upserts = db.upserted_paths().count();
        drop(db);
        if base_count > 0 {
            return base_count.saturating_add(overlay_upserts);
        }
        if rebuild_in_progress {
            return overlay_upserts;
        }
        self.l2.load().file_count()
    }

    fn overlay_memory_stats(&self) -> OverlayStats {
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
    }

    fn rebuild_memory_stats(&self) -> RebuildStats {
        let st = self.rebuild_state.lock();
        RebuildStats {
            in_progress: st.in_progress,
            pending_paths: 0,
            pending_map_cap: 0,
            pending_key_bytes: 0,
            pending_from_bytes: 0,
            estimated_bytes: 0,
        }
    }

    fn query_guard_memory_stats(&self) -> QueryGuardStats {
        let stats = self.stats_report();
        QueryGuardStats {
            active_count: stats.query_guard_active_count,
            hold_count: stats.query_guard_hold_count,
            hold_avg_us: stats.query_guard_hold_avg_us,
            hold_max_us: stats.query_guard_hold_max_us,
            last_hold_us: stats.query_guard_last_hold_us,
            slow_count: stats.query_guard_slow_count,
        }
    }

    fn refresh_lightweight_memory_fields(
        &self,
        mut report: MemoryReport,
        pipeline_stats: EventPipelineStats,
        sample_depth: MemorySampleDepth,
        cache_hit: bool,
        cached_full_age_ms: u64,
    ) -> MemoryReport {
        report.sample_depth = sample_depth;
        report.cache_hit = cache_hit;
        report.cached_full_age_ms = cached_full_age_ms;
        report.event_pipeline = pipeline_stats;
        report.l1 = self.l1.memory_stats();
        report.dirty_queue = self.dirty_queue.lock().memory_stats();
        report.overlay = self.overlay_memory_stats();
        report.rebuild = self.rebuild_memory_stats();
        report.query_guard = self.query_guard_memory_stats();
        report.process_smaps_rollup = MemoryReport::read_smaps_rollup();
        report.process_rss_bytes = report
            .process_smaps_rollup
            .as_ref()
            .map(|s| s.rss_bytes)
            .unwrap_or_else(MemoryReport::read_process_rss);
        report.process_swap_bytes = MemoryReport::read_process_swap();
        report.process_faults = MemoryReport::read_faults();
        report.index_estimated_bytes = report
            .l1
            .estimated_bytes
            .saturating_add(report.base.estimated_bytes)
            .saturating_add(report.l2.estimated_bytes)
            .saturating_add(report.disk_deleted_estimated_bytes)
            .saturating_add(report.dirty_queue.estimated_bytes)
            .saturating_add(report.overlay.estimated_bytes)
            .saturating_add(report.rebuild.estimated_bytes);
        if let Some(s) = &report.process_smaps_rollup {
            let (non, suspected) =
                infer_heap_high_water(s.private_dirty_bytes, report.index_estimated_bytes);
            report.non_index_private_dirty_bytes = Some(non);
            report.heap_high_water_suspected = suspected;
        } else {
            report.non_index_private_dirty_bytes = None;
            report.heap_high_water_suspected = false;
        }
        report
    }

    fn memory_report_uncached_light(&self) -> MemoryReport {
        let base_generation = self.base.load_full();
        let base_strong_refs = Arc::strong_count(&base_generation);
        let base = base_generation.memory_stats();
        let l2_generation = self.l2.load_full();
        let l2_strong_refs = Arc::strong_count(&l2_generation);
        let l2 = l2_generation.memory_stats();

        MemoryReport {
            sample_depth: MemorySampleDepth::Light,
            cache_hit: false,
            cached_full_age_ms: 0,
            full_sample_elapsed_ms: 0,
            base,
            l2,
            generation: GenerationStats {
                base_strong_refs,
                l2_strong_refs,
            },
            ..MemoryReport::default()
        }
    }

    /// 生成完整内存报告。该路径会重算 Base/L2 统计并刷新轻量观测缓存。
    pub fn memory_report(&self, pipeline_stats: EventPipelineStats) -> MemoryReport {
        let started = Instant::now();
        let l1 = self.l1.memory_stats();
        let base_generation = self.base.load_full();
        let base_strong_refs = Arc::strong_count(&base_generation);
        let base = base_generation.memory_stats();
        let l2_generation = self.l2.load_full();
        let l2_strong_refs = Arc::strong_count(&l2_generation);
        let l2 = l2_generation.memory_stats();
        let dirty_queue = self.dirty_queue.lock().memory_stats();
        let overlay = self.overlay_memory_stats();
        let rebuild = self.rebuild_memory_stats();

        let (
            disk_segments,
            disk_deleted_paths,
            disk_deleted_bytes,
            disk_deleted_estimated_bytes,
            disk_deleted_estimated_bytes_max,
        ) = (0, 0, 0, 0, 0);
        let query_guard = self.query_guard_memory_stats();

        let index_estimated_bytes = l1
            .estimated_bytes
            .saturating_add(base.estimated_bytes)
            .saturating_add(l2.estimated_bytes)
            .saturating_add(disk_deleted_estimated_bytes)
            .saturating_add(dirty_queue.estimated_bytes)
            .saturating_add(overlay.estimated_bytes)
            .saturating_add(rebuild.estimated_bytes);
        let process_smaps_rollup = MemoryReport::read_smaps_rollup();
        let process_rss_bytes = process_smaps_rollup
            .as_ref()
            .map(|s| s.rss_bytes)
            .unwrap_or_else(MemoryReport::read_process_rss);
        let (non_index_private_dirty_bytes, heap_high_water_suspected) = process_smaps_rollup
            .as_ref()
            .map(|s| {
                let (non, suspected) =
                    infer_heap_high_water(s.private_dirty_bytes, index_estimated_bytes);
                (Some(non), suspected)
            })
            .unwrap_or((None, false));

        let full_sample_elapsed_ms = started.elapsed().as_millis() as u64;
        let report = MemoryReport {
            sample_depth: MemorySampleDepth::Full,
            cache_hit: false,
            cached_full_age_ms: 0,
            full_sample_elapsed_ms,
            l1,
            base,
            l2,
            disk_segments,
            disk_deleted_paths,
            disk_deleted_bytes,
            disk_deleted_estimated_bytes,
            disk_deleted_estimated_bytes_max,
            event_pipeline: pipeline_stats,
            dirty_queue,
            overlay,
            rebuild,
            generation: GenerationStats {
                base_strong_refs,
                l2_strong_refs,
            },
            query_guard,
            process_rss_bytes,
            process_swap_bytes: MemoryReport::read_process_swap(),
            process_smaps_rollup,
            process_faults: MemoryReport::read_faults(),
            index_estimated_bytes,
            non_index_private_dirty_bytes,
            heap_high_water_suspected,
        };
        {
            let mut cache = self.memory_report_cache.lock();
            cache.report = Some(report.clone());
            cache.sampled_at = Some(Instant::now());
        }
        report
    }

    /// 生成轻量内存报告。该路径复用最近完整报告，只刷新 RSS、pipeline、dirty/overlay/rebuild 等轻量字段。
    pub fn memory_report_light(&self, pipeline_stats: EventPipelineStats) -> MemoryReport {
        let (cached, sampled_at) = {
            let cache = self.memory_report_cache.lock();
            (cache.report.clone(), cache.sampled_at)
        };
        let cached_full_age_ms = sampled_at
            .map(|at| at.elapsed().as_millis() as u64)
            .unwrap_or(0);
        let cache_hit = sampled_at.is_some();
        self.refresh_lightweight_memory_fields(
            cached.unwrap_or_else(|| self.memory_report_uncached_light()),
            pipeline_stats,
            MemorySampleDepth::Light,
            cache_hit,
            cached_full_age_ms,
        )
    }

    fn memory_report_cache_ready(&self) -> bool {
        self.memory_report_cache.lock().report.is_some()
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
        let full_sample_every = (300 / interval_secs.max(1)).max(1);
        let mut tick_count: u64 = 0;
        let mut rss_window: VecDeque<u64> = VecDeque::with_capacity(12);

        loop {
            let stats = pipeline_stats_fn();
            tick_count = tick_count.wrapping_add(1);
            let should_refresh_full =
                !self.memory_report_cache_ready() || tick_count.is_multiple_of(full_sample_every);
            let report = if should_refresh_full {
                self.memory_report(stats)
            } else {
                self.memory_report_light(stats)
            };

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
                "\n{}\n[heap-signal] sample_depth={:?} cache_hit={} index_est_bytes={} non_index_pd_bytes={} suspected={} rss_trend_mb_per_min={:+.2}",
                report,
                report.sample_depth,
                report.cache_hit,
                report.index_estimated_bytes,
                report.non_index_private_dirty_bytes.unwrap_or(0),
                report.heap_high_water_suspected,
                trend_mb_per_min
            );
            if report.sample_depth == MemorySampleDepth::Full && report.heap_high_water_suspected {
                let before = report.process_rss_bytes;
                maybe_trim_rss();
                let after = MemoryReport::read_process_rss();
                tracing::info!(
                    "[heap-trim] suspected high-water; rss_before={} rss_after={} reclaimed={}",
                    before,
                    after,
                    before.saturating_sub(after)
                );
            }
            tokio::time::sleep(interval).await;
        }
    }
}
