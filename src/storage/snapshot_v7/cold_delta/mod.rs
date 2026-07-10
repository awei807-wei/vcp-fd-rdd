use std::collections::{BTreeMap, BTreeSet};
use std::path::Path;

use roaring::RoaringBitmap;

use crate::core::FileMeta;
use crate::index::file_entry_v2::FileEntry;

use super::{
    validate_written_v7, RawPathTableLayout, V7SegKind, V7Snapshot, V7StreamingWriter, V7_VERSION,
};

mod index_merge;
mod path_merge;
mod prepare;
mod validate;

use index_merge::{write_merged_parent_segment, write_merged_trigram_segment};
use path_merge::{write_appended_entry_segment, write_merged_raw_path_table};
use prepare::prepare_cold_delta;

pub(in crate::storage::snapshot_v7) use path_merge::{
    encoded_shared_prefix, validate_encoded_path_lengths,
};
#[cfg(test)]
pub(in crate::storage::snapshot_v7) use validate::validate_current_parent_segment;
pub(in crate::storage::snapshot_v7) use validate::validate_posting_docids;

pub(in crate::storage::snapshot_v7) fn validate_cold_delta_source(
    snapshot: &V7Snapshot,
) -> anyhow::Result<()> {
    validate::validate_cold_delta_source(snapshot).map(|_| ())
}

/// Small, owned overlay used by the current-v7 direct snapshot path.
pub struct ColdDeltaPlan {
    deleted_paths: BTreeSet<Vec<u8>>,
    upserts: Vec<FileMeta>,
}

impl ColdDeltaPlan {
    pub fn new<D, U>(deleted_paths: D, upserts: U) -> Self
    where
        D: IntoIterator<Item = Vec<u8>>,
        U: IntoIterator<Item = FileMeta>,
    {
        let deleted_paths = deleted_paths.into_iter().collect::<BTreeSet<_>>();
        let mut upserts_by_path = BTreeMap::new();
        for meta in upserts {
            upserts_by_path.insert(meta.path.as_os_str().as_encoded_bytes().to_vec(), meta);
        }
        Self {
            deleted_paths,
            upserts: upserts_by_path.into_values().collect(),
        }
    }

    pub fn is_empty(&self) -> bool {
        self.deleted_paths.is_empty() && self.upserts.is_empty()
    }

    pub fn deleted_count(&self) -> usize {
        self.deleted_paths.len()
    }

    pub fn upsert_count(&self) -> usize {
        self.upserts.len()
    }

    pub(crate) fn into_parts(self) -> (BTreeSet<Vec<u8>>, Vec<FileMeta>) {
        (self.deleted_paths, self.upserts)
    }
}

#[derive(Clone, Copy, Debug)]
pub struct ColdDeltaLimits {
    pub max_entry_amplification_bps: u32,
    pub max_path_amplification_bps: u32,
    /// Small fixtures naturally have several ancestor paths per live file.
    /// Enforce ratio-based compaction only once the generation is large enough
    /// for the ratio to describe real long-term amplification.
    pub min_live_entries_for_amplification: usize,
}

impl Default for ColdDeltaLimits {
    fn default() -> Self {
        Self {
            max_entry_amplification_bps: 25_000,
            max_path_amplification_bps: 40_000,
            min_live_entries_for_amplification: 1_024,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ColdDeltaWriteReport {
    pub old_entries: usize,
    pub appended_entries: usize,
    pub live_entries: usize,
    pub tombstone_entries: usize,
    pub path_entries: usize,
    pub entry_amplification_bps: u32,
    pub path_amplification_bps: u32,
}

struct ValidatedColdSource<'a> {
    path_table: &'a [u8],
    path_layout: RawPathTableLayout,
    entries_by_key: &'a [u8],
    entries_by_path: &'a [u8],
    trigram_index: &'a [u8],
    parent_index: &'a [u8],
    tombstones: &'a [u8],
    old_entry_count: usize,
}

struct PathAddition {
    path_idx: u32,
    path: Vec<u8>,
}

type PathIndexByBytes = BTreeMap<Vec<u8>, u32>;

struct PreparedColdDelta {
    path_additions: Vec<PathAddition>,
    appended_entries: Vec<FileEntry>,
    trigram_delta: BTreeMap<[u8; 3], RoaringBitmap>,
    parent_delta: BTreeMap<u32, RoaringBitmap>,
    tombstones: RoaringBitmap,
    report: ColdDeltaWriteReport,
}

/// Merge a small owned delta directly into a current-v7 mmap snapshot.
///
/// Old entry records keep their DocIds and path indices byte-for-byte. New
/// facts append after the original entry count, while replaced/deleted facts
/// are filtered through the tombstone bitmap.
pub fn write_v7_cold_delta_atomic(
    path: &Path,
    source_snapshot: &V7Snapshot,
    plan: ColdDeltaPlan,
    limits: ColdDeltaLimits,
) -> anyhow::Result<ColdDeltaWriteReport> {
    let source = validate::validate_cold_delta_source(source_snapshot)?;
    let prepared = prepare_cold_delta(&source, plan, limits)?;

    crate::storage::atomic_write_validated(
        path,
        "v7.tmp",
        |file| write_prepared_cold_delta(file, &source, &prepared),
        |tmp| {
            // The source mmap was scanned while encoding. Evict those pages
            // before validating the new inode so peak RSS does not contain two
            // fully resident generations at once.
            source_snapshot.advise_dontneed();
            validate_written_v7(
                tmp,
                Some(prepared.report.live_entries),
                "direct_v7_validation_failed",
            )
        },
    )?;
    Ok(prepared.report)
}

fn write_prepared_cold_delta(
    file: &mut std::fs::File,
    source: &ValidatedColdSource<'_>,
    prepared: &PreparedColdDelta,
) -> anyhow::Result<()> {
    let mut writer = V7StreamingWriter::new(file, V7_VERSION)?;
    writer.write_segment(V7SegKind::PathTable, |sink| {
        write_merged_raw_path_table(
            sink,
            source.path_table,
            source.path_layout,
            &prepared.path_additions,
        )
    })?;
    writer.write_segment(V7SegKind::EntriesByKey, |sink| {
        write_appended_entry_segment(sink, source.entries_by_key, &prepared.appended_entries)
    })?;
    writer.write_segment(V7SegKind::EntriesByPath, |sink| {
        write_appended_entry_segment(sink, source.entries_by_path, &prepared.appended_entries)
    })?;
    writer.write_segment(V7SegKind::TrigramIndex, |sink| {
        write_merged_trigram_segment(sink, source.trigram_index, &prepared.trigram_delta)
    })?;
    writer.write_segment(V7SegKind::ParentIndex, |sink| {
        write_merged_parent_segment(sink, source.parent_index, &prepared.parent_delta)
    })?;
    writer.write_segment(V7SegKind::Tombstones, |sink| {
        prepared.tombstones.serialize_into(sink)?;
        Ok(())
    })?;
    writer.finish()
}
