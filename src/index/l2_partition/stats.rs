use std::collections::HashMap;

use roaring::RoaringBitmap;

use crate::index::file_entry_v2::FileEntry;
use crate::stats::L2Stats;

use super::types::OneOrManyDocId;
use super::{DocId, PersistentIndex, Trigram};

impl PersistentIndex {
    pub fn is_dirty(&self) -> bool {
        self.dirty.load(std::sync::atomic::Ordering::Acquire)
    }

    pub fn file_count(&self) -> usize {
        let total = self.entries.read().len();
        let tomb = self.tombstones.read().len() as usize;
        total.saturating_sub(tomb)
    }

    /// 内存占用统计（粗估）
    pub fn memory_stats(&self) -> L2Stats {
        use std::mem::size_of;

        let entries = self.entries.read();
        let paths = self.paths.read();
        let filekey_to_docid = self.filekey_to_docid.read();
        let path_hash_to_id = self.path_hash_to_id.read();
        let trigram_index = self.trigram_index.read();
        let tombstones = self.tombstones.read();
        let parent_index = self.parent_index.read();
        let parent_path_table = self.parent_path_table.read();

        let total_docs = entries.len();
        debug_assert_eq!(paths.len(), total_docs);
        let tombstone_count = tombstones.len() as usize;
        let file_count = total_docs.saturating_sub(tombstone_count);

        let path_to_id_count: usize = path_hash_to_id.values().map(|v| v.len()).sum();
        let trigram_distinct = trigram_index.len();

        let mut trigram_postings_total: usize = 0;
        let mut trigram_heap_bytes: u64 = 0;
        for posting in trigram_index.values() {
            trigram_postings_total += posting.len() as usize;
            // serialized_size 更接近 Roaring 的真实压缩存储量（但仍不等同于实际 heap bytes）
            trigram_heap_bytes += posting.serialized_size() as u64;
        }

        // ── 更贴近真实的估算策略（以 capacity 为主，避免 len 低估） ──
        //
        // 说明：
        // - 这是"近似占用"，不包含 allocator 产生的碎片/空闲块（RSS 高水位常驻的主要来源）。
        // - HashMap 的真实 bucket/ctrl 布局由 hashbrown 决定，这里按"entry + 1B ctrl"做近似。

        // entries: Vec<FileEntry>
        let metas_bytes = entries.capacity() as u64 * size_of::<FileEntry>() as u64
            + size_of::<Vec<FileEntry>>() as u64;

        // mapping: open-addressed u32 DocId buckets; FileKey lives only in entries.
        let filekey_to_docid_bytes = filekey_to_docid.allocated_bytes() as u64;

        // paths: one contiguous byte arena + one 8-byte PathRef per DocId.
        let arena_bytes = paths.allocated_bytes() as u64 + size_of_val(&*paths) as u64;

        // path hash 反查：HashMap<u64, OneOrManyDocId> + Many 的 Vec<DocId> 堆分配
        let path_entry_bytes = size_of::<(u64, OneOrManyDocId)>() as u64;
        let mut path_many_bytes: u64 = 0;
        for v in path_hash_to_id.values() {
            if let OneOrManyDocId::Many(ids) = v {
                path_many_bytes += ids.capacity() as u64 * size_of::<DocId>() as u64
                    + size_of::<Vec<DocId>>() as u64;
            }
        }
        let path_to_id_bytes = path_hash_to_id.len() as u64 * (path_entry_bytes + 1)
            + size_of::<HashMap<u64, OneOrManyDocId>>() as u64
            + path_many_bytes;

        // trigram：HashMap<Trigram, RoaringBitmap> 的 entry + Roaring 的压缩存储量（serialized_size）
        let trigram_entry_bytes = size_of::<(Trigram, RoaringBitmap)>() as u64;
        let trigram_map_bytes = trigram_index.capacity() as u64 * (trigram_entry_bytes + 1)
            + size_of::<HashMap<Trigram, RoaringBitmap>>() as u64;
        let trigram_bytes = trigram_map_bytes + trigram_heap_bytes;

        // tombstones：RoaringBitmap
        let tomb_bytes = size_of::<RoaringBitmap>() as u64 + tombstones.serialized_size() as u64;
        let roaring_serialized_bytes = trigram_heap_bytes + tombstones.serialized_size() as u64;
        let parent_index_bytes = parent_index
            .as_ref()
            .map(|index| index.allocated_bytes() as u64)
            .unwrap_or(0);
        let parent_path_lookup_bytes = parent_path_table
            .as_ref()
            .map(|lookup| lookup.allocated_bytes() as u64)
            .unwrap_or(0);

        let core_table_bytes = metas_bytes + filekey_to_docid_bytes;
        let estimated_bytes = core_table_bytes
            + arena_bytes
            + path_to_id_bytes
            + trigram_bytes
            + tomb_bytes
            + parent_index_bytes
            + parent_path_lookup_bytes;

        L2Stats {
            file_count,
            path_to_id_count,
            trigram_distinct,
            trigram_postings_total,
            tombstone_count,
            metas_capacity: entries.capacity(),
            filekey_to_docid_capacity: filekey_to_docid.capacity(),
            path_hash_to_id_capacity: path_hash_to_id.len(),
            trigram_index_capacity: trigram_index.capacity(),
            arena_capacity: paths.arena_capacity(),

            core_table_bytes,
            metas_bytes,
            filekey_to_docid_bytes,
            arena_bytes,
            path_to_id_bytes,
            trigram_bytes,
            roaring_serialized_bytes,
            parent_index_bytes,
            parent_path_lookup_bytes,
            estimated_bytes,
        }
    }

    /// Compaction：清理墓碑（阶段 A：只清 tombstone 位图，不重排 DocId）
    pub fn compact(&self) {
        self.tombstones.write().clear();
        self.dirty.store(true, std::sync::atomic::Ordering::Release);
        tracing::info!("Compaction complete, tombstones cleared");
    }

    pub fn maybe_schedule_repair(&self) {
        // 占位：检测索引健康度，触发后台补扫
    }

    /// 清空索引并标记为 dirty（用于 overflow 后的重建兜底）
    pub fn reset(&self) {
        // 统一按固定顺序清理，避免读写并发下出现锁顺序反转。
        self.trigram_index.write().clear();
        self.path_hash_to_id.write().clear();
        self.filekey_to_docid.write().clear();
        self.tombstones.write().clear();
        self.entries.write().clear();
        self.paths.write().clear();
        self.dirty.store(true, std::sync::atomic::Ordering::Release);
    }
}
