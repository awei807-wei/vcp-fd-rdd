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
}

#[derive(Clone, Debug, Default)]
pub struct RebuildStats {
    pub in_progress: bool,
    pub pending_paths: usize,
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
            "║   overlay del:  {:>10}  ({:>10})          ║",
            self.overlay.deleted_paths,
            human_bytes(self.overlay.deleted_bytes)
        )?;
        writeln!(
            f,
            "║   overlay up:   {:>10}  ({:>10})          ║",
            self.overlay.upserted_paths,
            human_bytes(self.overlay.upserted_bytes)
        )?;
        writeln!(
            f,
            "║   rebuild pend: {:>10}  (in_progress={})      ║",
            self.rebuild.pending_paths, self.rebuild.in_progress
        )?;
        writeln!(f, "╚══════════════════════════════════════════════════╝")?;
        Ok(())
    }
}
