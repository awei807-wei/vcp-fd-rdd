use std::fmt;

/// 内存占用统计（字节级精确）
#[derive(Clone, Debug, Default)]
pub struct MemoryReport {
    /// L1 热缓存
    pub l1: L1Stats,
    /// L2 持久索引
    pub l2: L2Stats,
    /// 磁盘只读 segments（mmap 基座 + delta 段数量）
    pub disk_segments: usize,
    /// Disk segments 的 tombstones（delete/rename-from sidecar）路径条目总数
    pub disk_deleted_paths: usize,
    /// Disk segments 的 tombstones 路径字节总量（下界估算：len 之和，不含 Vec/allocator 开销）
    pub disk_deleted_bytes: u64,
    /// Disk segments 的 tombstones 常驻堆占用估算（Vec 元数据 + bytes；粗估、偏保守）
    pub disk_deleted_estimated_bytes: u64,
    /// 单个 disk layer 的 tombstones 估算上界（用于识别异常段）
    pub disk_deleted_estimated_bytes_max: u64,
    /// 事件管道
    pub event_pipeline: EventPipelineStats,
    /// overlay（跨段 delete/upsert 屏蔽集合）
    pub overlay: OverlayStats,
    /// rebuild（pending 事件队列）
    pub rebuild: RebuildStats,
    /// 进程级 RSS（从 /proc/self/statm 读取）
    pub process_rss_bytes: u64,
    /// 进程级内存拆分（从 /proc/self/smaps_rollup 读取；Linux-only）
    pub process_smaps_rollup: Option<SmapsRollupStats>,
    /// 进程级 page faults（从 /proc/self/stat 读取；Linux-only）
    pub process_faults: Option<FaultStats>,
    /// 索引相关结构估算总和（L1 + L2 + Disk tombstones + overlay + rebuild）
    pub index_estimated_bytes: u64,
    /// 非索引 Private_Dirty（smaps.private_dirty - index_estimated；仅在 smaps 可用时有值）
    pub non_index_private_dirty_bytes: Option<u64>,
    /// 是否疑似“堆高水位常驻”（非索引脏页明显偏高）
    pub heap_high_water_suspected: bool,
}

/// Service-level observability stats (uptime, snapshot, watcher health).
#[derive(Clone, Debug, Default)]
pub struct ServiceStats {
    /// Seconds since the daemon started.
    pub uptime_secs: u64,
    /// Unix timestamp of the last successful snapshot write (0 = never).
    pub last_snapshot_time: u64,
    /// Cumulative watcher failure count (e.g. inotify overflow, re-watch errors).
    pub watch_failures: u64,
    /// Whether the watcher is currently in degraded/polling mode.
    pub watcher_degraded: bool,
}

#[derive(Clone, Debug, Default)]
pub struct SmapsRollupStats {
    pub rss_bytes: u64,
    pub pss_bytes: u64,
    pub private_clean_bytes: u64,
    pub private_dirty_bytes: u64,
}

#[derive(Clone, Debug, Default)]
pub struct FaultStats {
    pub minflt: u64,
    pub majflt: u64,
}

#[derive(Clone, Debug, Default)]
pub struct L1Stats {
    /// L1 主存储条目数
    pub entry_count: usize,
    /// path_index 条目数
    pub path_index_count: usize,
    /// LRU 条目数
    pub lru_entries: usize,
    /// 估算内存（字节）
    pub estimated_bytes: u64,
}

#[derive(Clone, Debug, Default)]
pub struct L2Stats {
    /// files 条目数（活跃文档数，不含 tombstone）
    pub file_count: usize,
    /// path_to_id（hash(path)->DocId）条目数（含冲突列表展开后的 DocId 计数）
    pub path_to_id_count: usize,
    /// trigram 倒排索引：不同 trigram 数量
    pub trigram_distinct: usize,
    /// trigram 倒排索引：所有 posting list 的 DocId 总数
    pub trigram_postings_total: usize,
    /// tombstone 数量
    pub tombstone_count: usize,
    /// metas(Vec<CompactMeta>) capacity
    pub metas_capacity: usize,
    /// filekey_to_docid(HashMap<FileKey,DocId>) capacity
    pub filekey_to_docid_capacity: usize,
    /// path_hash_to_id(HashMap<u64, OneOrManyDocId>) capacity
    pub path_hash_to_id_capacity: usize,
    /// trigram_index(HashMap<Trigram, RoaringBitmap>) capacity
    pub trigram_index_capacity: usize,
    /// arena(Vec<u8>) capacity
    pub arena_capacity: usize,

    /// metas + filekey_to_docid 估算内存（字节，不含 arena 与派生索引）
    pub core_table_bytes: u64,
    /// metas(Vec<CompactMeta>) 估算内存（字节）
    pub metas_bytes: u64,
    /// filekey_to_docid(HashMap<FileKey,DocId>) 估算内存（字节）
    pub filekey_to_docid_bytes: u64,
    /// arena 估算内存（字节）
    pub arena_bytes: u64,
    /// path_to_id 估算内存（字节）
    pub path_to_id_bytes: u64,
    /// trigram 倒排索引估算内存（字节）
    pub trigram_bytes: u64,
    /// RoaringBitmap serialized_size 总和（更接近压缩后数据体量；不等于真实 heap）
    pub roaring_serialized_bytes: u64,
    /// 总估算内存（字节）
    pub estimated_bytes: u64,
}

#[derive(Clone, Debug, Default)]
pub struct EventPipelineStats {
    /// 当前批次处理的事件数（最近一次 debounce 窗口）
    pub last_batch_size: usize,
    /// 累计处理事件数
    pub total_events_processed: u64,
    /// channel 溢出丢弃次数
    pub overflow_drops: u64,
    /// notify/inotify rescan 信号次数（表示可能漏事件，需要补扫）
    pub rescan_signals: u64,
    /// watcher 注册/运行中累计失败次数（例如目录加 watch 失败）。
    pub watch_failures: u64,
    /// watcher 当前是否处于降级状态（存在未被 watch 的目录）。
    pub watcher_degraded: bool,
    /// 当前处于降级轮询的目录数。
    pub degraded_roots: usize,
    /// raw_events(Vec<notify::Event>) capacity
    pub raw_events_capacity: usize,
    /// merged(HashMap<FileIdentifier, EventRecord>) capacity
    pub merged_map_capacity: usize,
    /// records(Vec<EventRecord>) capacity
    pub records_capacity: usize,
}

#[derive(Clone, Debug, Default)]
pub struct OverlayStats {
    pub deleted_paths: usize,
    pub upserted_paths: usize,
    pub deleted_bytes: u64,
    pub upserted_bytes: u64,
    pub deleted_arena_len: usize,
    pub deleted_arena_cap: usize,
    pub upserted_arena_len: usize,
    pub upserted_arena_cap: usize,
    pub deleted_map_len: usize,
    pub deleted_map_cap: usize,
    pub upserted_map_len: usize,
    pub upserted_map_cap: usize,
    /// overlay 估算堆占用（arena + map + collision 列表；粗估、偏保守）
    pub estimated_bytes: u64,
}

#[derive(Clone, Debug, Default)]
pub struct RebuildStats {
    pub in_progress: bool,
    pub pending_paths: usize,
    pub pending_map_cap: usize,
    /// PathBuf 路径字节总量（下界估算：len，不含容量与 allocator 开销）
    pub pending_key_bytes: u64,
    /// rename-from 路径字节总量（下界估算）
    pub pending_from_bytes: u64,
    /// rebuild pending 估算堆占用（HashMap 结构 + 下界路径字节；粗估、偏保守）
    pub estimated_bytes: u64,
}

impl MemoryReport {
    /// 从 /proc/self/statm 读取进程 RSS
    pub fn read_process_rss() -> u64 {
        std::fs::read_to_string("/proc/self/statm")
            .ok()
            .and_then(|s| {
                // statm 格式: size resident shared text lib data dt (单位: 页)
                let parts: Vec<&str> = s.split_whitespace().collect();
                parts.get(1)?.parse::<u64>().ok()
            })
            .map(|pages| pages * 4096) // x86_64 page size
            .unwrap_or(0)
    }

    /// 从 /proc/self/smaps_rollup 读取关键指标（kB → bytes）。
    pub fn read_smaps_rollup() -> Option<SmapsRollupStats> {
        let s = std::fs::read_to_string("/proc/self/smaps_rollup").ok()?;
        let mut out = SmapsRollupStats::default();
        for line in s.lines() {
            let mut it = line.split_whitespace();
            let key = it.next()?;
            let val = it.next()?;
            let unit = it.next().unwrap_or("");
            if unit != "kB" {
                continue;
            }
            let v_kb: u64 = val.parse().ok()?;
            let v = v_kb.saturating_mul(1024);
            match key {
                "Rss:" => out.rss_bytes = v,
                "Pss:" => out.pss_bytes = v,
                "Private_Clean:" => out.private_clean_bytes = v,
                "Private_Dirty:" => out.private_dirty_bytes = v,
                _ => {}
            }
        }
        Some(out)
    }

    /// 从 /proc/self/stat 读取 minflt/majflt（minor/major page faults）。
    pub fn read_faults() -> Option<FaultStats> {
        let s = std::fs::read_to_string("/proc/self/stat").ok()?;
        let rparen = s.rfind(')')?;
        // 右括号后通常是 ") "，紧跟 state 等字段。
        let after = s.get(rparen + 2..)?;
        let parts: Vec<&str> = after.split_whitespace().collect();
        if parts.len() < 10 {
            return None;
        }
        let minflt: u64 = parts.get(7)?.parse().ok()?;
        let majflt: u64 = parts.get(9)?.parse().ok()?;
        Some(FaultStats { minflt, majflt })
    }
}

/// 根据当前 Private_Dirty 与索引估算量推断“非索引堆高水位”信号。
///
/// 返回：
/// - `non_index_private_dirty_bytes`: 近似“运行时/分配器/临时分配”占用
/// - `suspected`: 是否达到高水位嫌疑阈值
pub(crate) fn infer_heap_high_water(
    private_dirty_bytes: u64,
    index_estimated_bytes: u64,
) -> (u64, bool) {
    let non_index = private_dirty_bytes.saturating_sub(index_estimated_bytes);
    // 经验阈值：non-index PD 达到 32MB 以上且显著高于“索引估算占用”时，倾向认为 allocator/临时分配高水位需要关注。
    // 注意：这是启发式信号（用于触发条件性 trim），不是泄漏判定。
    let suspected = non_index >= 32 * 1024 * 1024
        && index_estimated_bytes.saturating_mul(2) <= private_dirty_bytes;
    (non_index, suspected)
}

fn human_bytes(bytes: u64) -> String {
    const KB: u64 = 1024;
    const MB: u64 = 1024 * KB;
    const GB: u64 = 1024 * MB;
    if bytes >= GB {
        format!("{:.2} GB", bytes as f64 / GB as f64)
    } else if bytes >= MB {
        format!("{:.2} MB", bytes as f64 / MB as f64)
    } else if bytes >= KB {
        format!("{:.2} KB", bytes as f64 / KB as f64)
    } else {
        format!("{} B", bytes)
    }
}

impl fmt::Display for MemoryReport {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        writeln!(f, "╔══════════════════════════════════════════════════╗")?;
        writeln!(f, "║           fd-rdd Memory Report                  ║")?;
        writeln!(f, "╠══════════════════════════════════════════════════╣")?;
        writeln!(
            f,
            "║ Process RSS: {:>35} ║",
            human_bytes(self.process_rss_bytes)
        )?;
        if let Some(s) = &self.process_smaps_rollup {
            writeln!(
                f,
                "║ Smaps: rss={:<10} pss={:<10} pc={:<10} ║",
                human_bytes(s.rss_bytes),
                human_bytes(s.pss_bytes),
                human_bytes(s.private_clean_bytes)
            )?;
            writeln!(
                f,
                "║       pd={:<10}                               ║",
                human_bytes(s.private_dirty_bytes)
            )?;
            writeln!(
                f,
                "║ Heap: idx={:<10} non-idx-pd={:<10}      ║",
                human_bytes(self.index_estimated_bytes),
                human_bytes(self.non_index_private_dirty_bytes.unwrap_or(0)),
            )?;
            writeln!(
                f,
                "║       high-water-suspected={:<5}                 ║",
                self.heap_high_water_suspected
            )?;
        }
        if let Some(pf) = &self.process_faults {
            writeln!(
                f,
                "║ Faults: minflt={:<12} majflt={:<12} ║",
                pf.minflt, pf.majflt
            )?;
        }
        writeln!(f, "╠──────────────────────────────────────────────────╣")?;
        writeln!(f, "║ L1 Cache:                                        ║")?;
        writeln!(
            f,
            "║   entries:      {:>10}                       ║",
            self.l1.entry_count
        )?;
        writeln!(
            f,
            "║   path_index:   {:>10}                       ║",
            self.l1.path_index_count
        )?;
        writeln!(
            f,
            "║   lru_entries:  {:>10}                       ║",
            self.l1.lru_entries
        )?;
        writeln!(
            f,
            "║   estimated:    {:>10}                       ║",
            human_bytes(self.l1.estimated_bytes)
        )?;
        writeln!(f, "╠──────────────────────────────────────────────────╣")?;
        writeln!(f, "║ L2 PersistentIndex:                              ║")?;
        writeln!(
            f,
            "║   files:        {:>10}                       ║",
            self.l2.file_count,
        )?;
        writeln!(
            f,
            "║   metas cap:    {:>10}  ({:>10})          ║",
            self.l2.metas_capacity,
            human_bytes(self.l2.metas_bytes)
        )?;
        writeln!(
            f,
            "║   filekey cap:  {:>10}  ({:>10})          ║",
            self.l2.filekey_to_docid_capacity,
            human_bytes(self.l2.filekey_to_docid_bytes)
        )?;
        writeln!(
            f,
            "║   arena:        {:>10}  ({:>10})          ║",
            self.l2.arena_capacity,
            human_bytes(self.l2.arena_bytes)
        )?;
        writeln!(
            f,
            "║   path_to_id:   {:>10}  ({:>10})          ║",
            self.l2.path_to_id_count,
            human_bytes(self.l2.path_to_id_bytes)
        )?;
        writeln!(
            f,
            "║   path cap:     {:>10}                       ║",
            self.l2.path_hash_to_id_capacity
        )?;
        writeln!(
            f,
            "║   trigram keys: {:>10}                       ║",
            self.l2.trigram_distinct
        )?;
        writeln!(
            f,
            "║   trigram cap:  {:>10}                       ║",
            self.l2.trigram_index_capacity
        )?;
        writeln!(
            f,
            "║   trigram posts:{:>10}  ({:>10})          ║",
            self.l2.trigram_postings_total,
            human_bytes(self.l2.trigram_bytes)
        )?;
        writeln!(
            f,
            "║   roaring data: {:>10}                       ║",
            human_bytes(self.l2.roaring_serialized_bytes)
        )?;
        writeln!(
            f,
            "║   tombstones:   {:>10}                       ║",
            self.l2.tombstone_count
        )?;
        writeln!(
            f,
            "║   L2 total:     {:>10}                       ║",
            human_bytes(self.l2.estimated_bytes)
        )?;
        writeln!(f, "╠──────────────────────────────────────────────────╣")?;
        writeln!(f, "║ Disk Segments (mmap): {:>24} ║", self.disk_segments)?;
        writeln!(
            f,
            "║   tomb paths:  {:>10}  (logic={:>10})     ║",
            self.disk_deleted_paths,
            human_bytes(self.disk_deleted_bytes)
        )?;
        writeln!(
            f,
            "║   tomb est:    {:>10}  (max={:>10})       ║",
            human_bytes(self.disk_deleted_estimated_bytes),
            human_bytes(self.disk_deleted_estimated_bytes_max)
        )?;
        writeln!(f, "╠──────────────────────────────────────────────────╣")?;
        writeln!(f, "║ Event Pipeline:                  ║")?;
        writeln!(
            f,
            "║   last batch:   {:>10}                       ║",
            self.event_pipeline.last_batch_size
        )?;
        writeln!(
            f,
            "║   total events: {:>10}                       ║",
            self.event_pipeline.total_events_processed
        )?;
        writeln!(
            f,
            "║   overflow:     {:>10}                       ║",
            self.event_pipeline.overflow_drops
        )?;
        writeln!(
            f,
            "║   rescan:       {:>10}                       ║",
            self.event_pipeline.rescan_signals
        )?;
        writeln!(
            f,
            "║   watch_fail:   {:>10}                       ║",
            self.event_pipeline.watch_failures
        )?;
        writeln!(
            f,
            "║   degraded:     {:>10}  (roots={:>5})        ║",
            self.event_pipeline.watcher_degraded, self.event_pipeline.degraded_roots
        )?;
        writeln!(
            f,
            "║   raw cap:      {:>10}                       ║",
            self.event_pipeline.raw_events_capacity
        )?;
        writeln!(
            f,
            "║   merged cap:   {:>10}                       ║",
            self.event_pipeline.merged_map_capacity
        )?;
        writeln!(
            f,
            "║   records cap:  {:>10}                       ║",
            self.event_pipeline.records_capacity
        )?;
        writeln!(f, "╠──────────────────────────────────────────────────╣")?;
        writeln!(f, "║ Shadow Memory (Overlay/Rebuild):                 ║")?;
        writeln!(
            f,
            "║   overlay del:  {:>10}  (logic={:>10})     ║",
            self.overlay.deleted_paths,
            human_bytes(self.overlay.deleted_bytes)
        )?;
        writeln!(
            f,
            "║   overlay up:   {:>10}  (logic={:>10})     ║",
            self.overlay.upserted_paths,
            human_bytes(self.overlay.upserted_bytes)
        )?;
        let overlay_arena_len =
            (self.overlay.deleted_arena_len + self.overlay.upserted_arena_len) as u64;
        let overlay_arena_cap =
            (self.overlay.deleted_arena_cap + self.overlay.upserted_arena_cap) as u64;
        writeln!(
            f,
            "║   overlay arena:{:>10}  (cap={:>10})       ║",
            human_bytes(overlay_arena_len),
            human_bytes(overlay_arena_cap),
        )?;
        let overlay_map_len = self.overlay.deleted_map_len + self.overlay.upserted_map_len;
        let overlay_map_cap = self.overlay.deleted_map_cap + self.overlay.upserted_map_cap;
        writeln!(
            f,
            "║   overlay map:  {:>10}  (cap={:>10})       ║",
            overlay_map_len, overlay_map_cap
        )?;
        writeln!(
            f,
            "║   overlay est:  {:>10}                       ║",
            human_bytes(self.overlay.estimated_bytes)
        )?;
        writeln!(
            f,
            "║   rebuild pend: {:>10}  (cap={:>10})       ║",
            self.rebuild.pending_paths, self.rebuild.pending_map_cap
        )?;
        writeln!(
            f,
            "║   rebuild keys: {:>10}  (from={:>10})      ║",
            human_bytes(self.rebuild.pending_key_bytes),
            human_bytes(self.rebuild.pending_from_bytes),
        )?;
        writeln!(
            f,
            "║   rebuild est:  {:>10}  (in_progress={})   ║",
            human_bytes(self.rebuild.estimated_bytes),
            self.rebuild.in_progress
        )?;
        writeln!(f, "╚══════════════════════════════════════════════════╝")?;
        Ok(())
    }
}

#[derive(Debug, Default, serde::Serialize)]
pub struct StatsReport {
    pub queries_total: u64,
    pub queries_avg_us: u64,
    pub events_applied: u64,
    pub events_dropped: u64,
    pub snapshot_count: u64,
    pub fast_sync_count: u64,
}

/// Thread-safe runtime stats collector.
#[derive(Debug, Default)]
pub struct StatsCollector {
    queries_total: std::sync::atomic::AtomicU64,
    queries_total_us: std::sync::atomic::AtomicU64,
    events_applied: std::sync::atomic::AtomicU64,
    events_dropped: std::sync::atomic::AtomicU64,
    snapshot_count: std::sync::atomic::AtomicU64,
    fast_sync_count: std::sync::atomic::AtomicU64,
}

impl StatsCollector {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn record_query(&self, elapsed_us: u64) {
        self.queries_total
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        self.queries_total_us
            .fetch_add(elapsed_us, std::sync::atomic::Ordering::Relaxed);
    }

    pub fn record_events_applied(&self, count: u64) {
        self.events_applied
            .fetch_add(count, std::sync::atomic::Ordering::Relaxed);
    }

    pub fn record_events_dropped(&self, count: u64) {
        self.events_dropped
            .fetch_add(count, std::sync::atomic::Ordering::Relaxed);
    }

    pub fn record_snapshot(&self) {
        self.snapshot_count
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    }

    pub fn record_fast_sync(&self) {
        self.fast_sync_count
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    }

    pub fn report(&self) -> StatsReport {
        let total = self
            .queries_total
            .load(std::sync::atomic::Ordering::Relaxed);
        let total_us = self
            .queries_total_us
            .load(std::sync::atomic::Ordering::Relaxed);
        StatsReport {
            queries_total: total,
            queries_avg_us: if total == 0 { 0 } else { total_us / total },
            events_applied: self
                .events_applied
                .load(std::sync::atomic::Ordering::Relaxed),
            events_dropped: self
                .events_dropped
                .load(std::sync::atomic::Ordering::Relaxed),
            snapshot_count: self
                .snapshot_count
                .load(std::sync::atomic::Ordering::Relaxed),
            fast_sync_count: self
                .fast_sync_count
                .load(std::sync::atomic::Ordering::Relaxed),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::infer_heap_high_water;

    #[test]
    fn heap_high_water_detects_large_non_index_dirty() {
        let pd = 256 * 1024 * 1024;
        let idx = 8 * 1024 * 1024;
        let (non, suspected) = infer_heap_high_water(pd, idx);
        assert!(non >= 200 * 1024 * 1024);
        assert!(suspected);
    }

    #[test]
    fn heap_high_water_ignores_index_dominated_dirty() {
        let pd = 96 * 1024 * 1024;
        let idx = 80 * 1024 * 1024;
        let (_non, suspected) = infer_heap_high_water(pd, idx);
        assert!(!suspected);
    }
}
