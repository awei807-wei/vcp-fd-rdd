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
    /// DashMap<FileKey, FileMeta> 条目数
    pub entry_count: usize,
    /// path_index 条目数
    pub path_index_count: usize,
    /// access_count 条目数
    pub access_count_entries: usize,
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
            "║   access_count: {:>10}                       ║",
            self.l1.access_count_entries
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
