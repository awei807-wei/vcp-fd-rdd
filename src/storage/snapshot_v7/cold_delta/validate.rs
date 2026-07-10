use std::collections::BTreeSet;

use roaring::RoaringBitmap;

use super::*;
use crate::storage::snapshot_v7::{
    decode_tombstones, entry_count_from_segment, file_entry_at, RawPathTableLayout, V7SegKind,
    V7Snapshot, FILE_ENTRY_REC_SIZE, RAW_PATH_TABLE_ANCHOR_INTERVAL, TRIGRAM_SENTINEL,
    V7_SEGMENT_ORDER, V7_VERSION,
};

pub(super) fn validate_cold_delta_source(
    snapshot: &V7Snapshot,
) -> anyhow::Result<ValidatedColdSource<'_>> {
    if snapshot.version != V7_VERSION {
        anyhow::bail!(
            "direct_v7_unsupported: snapshot version {} is not current v{}",
            snapshot.version,
            V7_VERSION
        );
    }
    if snapshot.segments.len() != V7_SEGMENT_ORDER.len()
        || snapshot
            .segments
            .iter()
            .map(|(kind, _)| *kind)
            .ne(V7_SEGMENT_ORDER)
    {
        anyhow::bail!("direct_v7_unsupported: snapshot does not contain the six unique segments");
    }

    let path_table = required_segment(snapshot, V7SegKind::PathTable)?;
    let path_layout = RawPathTableLayout::parse(path_table).ok_or_else(|| {
        anyhow::anyhow!("direct_v7_unsupported: legacy or malformed raw path table")
    })?;
    validate_current_raw_path_table(path_table, path_layout)?;

    let entries_by_key = required_segment(snapshot, V7SegKind::EntriesByKey)?;
    let entries_by_path = required_segment(snapshot, V7SegKind::EntriesByPath)?;
    let old_entry_count =
        validate_current_entry_segments(entries_by_key, entries_by_path, path_layout.idx_len)?;

    let trigram_index = required_segment(snapshot, V7SegKind::TrigramIndex)?;
    validate_current_trigram_segment(trigram_index, old_entry_count)?;
    let parent_index = required_segment(snapshot, V7SegKind::ParentIndex)?;
    validate_current_parent_segment(parent_index, old_entry_count, path_layout.idx_len)?;
    let tombstone_segment = required_segment(snapshot, V7SegKind::Tombstones)?;
    validate_current_tombstones(tombstone_segment, old_entry_count)?;

    Ok(ValidatedColdSource {
        path_table,
        path_layout,
        entries_by_key,
        entries_by_path,
        trigram_index,
        parent_index,
        tombstones: tombstone_segment,
        old_entry_count,
    })
}

fn required_segment(snapshot: &V7Snapshot, kind: V7SegKind) -> anyhow::Result<&[u8]> {
    snapshot
        .segment(kind)
        .ok_or_else(|| anyhow::anyhow!("direct_v7_unsupported: missing {:?} segment", kind))
}

fn validate_current_raw_path_table(bytes: &[u8], layout: RawPathTableLayout) -> anyhow::Result<()> {
    let idx_bytes = layout
        .idx_len
        .checked_mul(4)
        .ok_or_else(|| anyhow::anyhow!("direct_v7_corrupt: path index size overflow"))?;
    let total = layout
        .idx_start
        .checked_add(idx_bytes)
        .ok_or_else(|| anyhow::anyhow!("direct_v7_corrupt: path table size overflow"))?;
    if total != bytes.len() || layout.slots_len != layout.idx_len {
        anyhow::bail!("direct_v7_corrupt: path table is not dense or has trailing bytes");
    }
    if layout.idx_len >= (1usize << 31) {
        anyhow::bail!("direct_v7_unsupported: path index space is exhausted");
    }

    let mut previous = Vec::new();
    for_each_raw_sorted_path(bytes, layout, |sorted_pos, orig_idx, path| {
        if sorted_pos > 0 && previous.as_slice() >= path {
            anyhow::bail!("direct_v7_corrupt: path table is not strictly sorted");
        }
        let map_offset = layout
            .idx_start
            .checked_add(orig_idx as usize * 4)
            .ok_or_else(|| anyhow::anyhow!("direct_v7_corrupt: path map overflow"))?;
        let mapped = u32::from_le_bytes(
            bytes
                .get(map_offset..map_offset + 4)
                .ok_or_else(|| anyhow::anyhow!("direct_v7_corrupt: path map truncated"))?
                .try_into()?,
        ) as usize;
        if orig_idx as usize >= layout.idx_len || mapped != sorted_pos {
            anyhow::bail!("direct_v7_corrupt: path reverse map is inconsistent");
        }
        previous.clear();
        previous.extend_from_slice(path);
        Ok(())
    })
}

fn validate_current_entry_segments(
    entries_by_key: &[u8],
    entries_by_path: &[u8],
    path_count: usize,
) -> anyhow::Result<usize> {
    let count = exact_current_entry_count(entries_by_key)?;
    if exact_current_entry_count(entries_by_path)? != count || entries_by_key != entries_by_path {
        anyhow::bail!("direct_v7_corrupt: current entry segments are inconsistent");
    }
    for docid in 0..count {
        let entry = file_entry_at(entries_by_key, V7_VERSION, docid as u32)
            .ok_or_else(|| anyhow::anyhow!("direct_v7_corrupt: entry {} is truncated", docid))?;
        if entry.path_index() as usize >= path_count {
            anyhow::bail!(
                "direct_v7_corrupt: entry {} path index {} is out of range",
                docid,
                entry.path_index()
            );
        }
    }
    Ok(count)
}

pub(super) fn exact_current_entry_count(bytes: &[u8]) -> anyhow::Result<usize> {
    let count = entry_count_from_segment(bytes, V7_VERSION)
        .ok_or_else(|| anyhow::anyhow!("direct_v7_corrupt: invalid 32-byte entry segment"))?;
    let expected = 4usize
        .checked_add(
            count
                .checked_mul(FILE_ENTRY_REC_SIZE)
                .ok_or_else(|| anyhow::anyhow!("direct_v7_corrupt: entry segment size overflow"))?,
        )
        .ok_or_else(|| anyhow::anyhow!("direct_v7_corrupt: entry segment size overflow"))?;
    if bytes.len() != expected {
        anyhow::bail!("direct_v7_corrupt: entry segment has trailing bytes");
    }
    Ok(count)
}

fn validate_current_trigram_segment(bytes: &[u8], entry_count: usize) -> anyhow::Result<()> {
    let mut previous = None;
    let mut saw_sentinel = false;
    for_each_trigram_record(bytes, |key, posting_bytes, _| {
        if previous.is_some_and(|old| old >= key) {
            anyhow::bail!("direct_v7_corrupt: trigram keys are not sorted and unique");
        }
        let posting = decode_posting(posting_bytes, "trigram")?;
        validate_posting_docids(&posting, entry_count, "trigram")?;
        saw_sentinel |= key == TRIGRAM_SENTINEL;
        previous = Some(key);
        Ok(())
    })?;
    if !saw_sentinel {
        anyhow::bail!("direct_v7_unsupported: full-path trigram sentinel is missing");
    }
    Ok(())
}

pub(in crate::storage::snapshot_v7) fn validate_current_parent_segment(
    bytes: &[u8],
    entry_count: usize,
    path_count: usize,
) -> anyhow::Result<()> {
    let mut keys = BTreeSet::new();
    for_each_parent_record(bytes, |path_idx, posting_bytes, _| {
        if path_idx as usize >= path_count || path_idx >= (1u32 << 31) {
            anyhow::bail!("direct_v7_corrupt: parent path index is out of range");
        }
        if !keys.insert(path_idx) {
            anyhow::bail!("direct_v7_corrupt: parent segment contains a duplicate path index");
        }
        let posting = decode_posting(posting_bytes, "parent")?;
        validate_posting_docids(&posting, entry_count, "parent")
    })
}

fn validate_current_tombstones(bytes: &[u8], entry_count: usize) -> anyhow::Result<()> {
    let tombstones = decode_tombstones(bytes)?;
    validate_posting_docids(&tombstones, entry_count, "tombstone")
}

pub(in crate::storage::snapshot_v7) fn validate_posting_docids(
    posting: &RoaringBitmap,
    entry_count: usize,
    label: &str,
) -> anyhow::Result<()> {
    if posting
        .max()
        .is_some_and(|docid| docid as usize >= entry_count)
    {
        anyhow::bail!("direct_v7_corrupt: {} posting DocId is out of range", label);
    }
    Ok(())
}

pub(super) fn for_each_raw_sorted_path(
    bytes: &[u8],
    layout: RawPathTableLayout,
    mut visitor: impl FnMut(usize, u32, &[u8]) -> anyhow::Result<()>,
) -> anyhow::Result<()> {
    let mut path = Vec::new();
    for sorted_pos in 0..layout.slots_len {
        let slot = layout
            .slot(bytes, sorted_pos)
            .ok_or_else(|| anyhow::anyhow!("direct_v7_corrupt: raw path slot is invalid"))?;
        if sorted_pos % RAW_PATH_TABLE_ANCHOR_INTERVAL == 0 {
            if slot.shared_len != 0 {
                anyhow::bail!("direct_v7_corrupt: raw path anchor has a shared prefix");
            }
            path.clear();
        } else if slot.shared_len > path.len() {
            anyhow::bail!("direct_v7_corrupt: raw path shared prefix is out of range");
        } else {
            path.truncate(slot.shared_len);
        }
        path.extend_from_slice(
            layout
                .suffix(bytes, slot)
                .ok_or_else(|| anyhow::anyhow!("direct_v7_corrupt: raw path suffix is invalid"))?,
        );
        visitor(sorted_pos, slot.orig_idx, &path)?;
    }
    Ok(())
}

pub(super) fn for_each_trigram_record(
    bytes: &[u8],
    mut visitor: impl FnMut([u8; 3], &[u8], std::ops::Range<usize>) -> anyhow::Result<()>,
) -> anyhow::Result<()> {
    if bytes.len() < 4 {
        anyhow::bail!("direct_v7_corrupt: trigram segment is too small");
    }
    let count = u32::from_le_bytes(bytes[0..4].try_into()?) as usize;
    let mut offset = 4usize;
    for _ in 0..count {
        let record_start = offset;
        let header = bytes
            .get(offset..offset + 8)
            .ok_or_else(|| anyhow::anyhow!("direct_v7_corrupt: trigram record is truncated"))?;
        let key = [header[0], header[1], header[2]];
        if header[3] != 0 {
            anyhow::bail!("direct_v7_corrupt: trigram padding is non-zero");
        }
        let posting_len = u32::from_le_bytes(header[4..8].try_into()?) as usize;
        offset = offset
            .checked_add(8)
            .ok_or_else(|| anyhow::anyhow!("direct_v7_corrupt: trigram offset overflow"))?;
        let posting_end = offset
            .checked_add(posting_len)
            .ok_or_else(|| anyhow::anyhow!("direct_v7_corrupt: trigram posting overflow"))?;
        let posting = bytes
            .get(offset..posting_end)
            .ok_or_else(|| anyhow::anyhow!("direct_v7_corrupt: trigram posting is truncated"))?;
        visitor(key, posting, record_start..posting_end)?;
        offset = posting_end;
    }
    if offset != bytes.len() {
        anyhow::bail!("direct_v7_corrupt: trigram segment has trailing bytes");
    }
    Ok(())
}

pub(super) fn for_each_parent_record(
    bytes: &[u8],
    mut visitor: impl FnMut(u32, &[u8], std::ops::Range<usize>) -> anyhow::Result<()>,
) -> anyhow::Result<()> {
    if bytes.len() < 8 {
        anyhow::bail!("direct_v7_corrupt: parent segment is too small");
    }
    let count = u32::from_le_bytes(bytes[0..4].try_into()?) as usize;
    let mut offset = 4usize;
    for _ in 0..count {
        let record_start = offset;
        let header = bytes
            .get(offset..offset + 8)
            .ok_or_else(|| anyhow::anyhow!("direct_v7_corrupt: parent record is truncated"))?;
        let path_idx = u32::from_le_bytes(header[0..4].try_into()?);
        let posting_len = u32::from_le_bytes(header[4..8].try_into()?) as usize;
        offset = offset
            .checked_add(8)
            .ok_or_else(|| anyhow::anyhow!("direct_v7_corrupt: parent offset overflow"))?;
        let posting_end = offset
            .checked_add(posting_len)
            .ok_or_else(|| anyhow::anyhow!("direct_v7_corrupt: parent posting overflow"))?;
        let posting = bytes
            .get(offset..posting_end)
            .ok_or_else(|| anyhow::anyhow!("direct_v7_corrupt: parent posting is truncated"))?;
        visitor(path_idx, posting, record_start..posting_end)?;
        offset = posting_end;
    }
    let subdir_count = u32::from_le_bytes(
        bytes
            .get(offset..offset + 4)
            .ok_or_else(|| anyhow::anyhow!("direct_v7_corrupt: parent footer is truncated"))?
            .try_into()?,
    );
    offset += 4;
    if subdir_count != 0 || offset != bytes.len() {
        anyhow::bail!("direct_v7_unsupported: parent legacy subdir section is present");
    }
    Ok(())
}

pub(super) fn decode_posting(bytes: &[u8], label: &str) -> anyhow::Result<RoaringBitmap> {
    RoaringBitmap::deserialize_from(bytes)
        .map_err(|error| anyhow::anyhow!("direct_v7_corrupt: {} posting: {}", label, error))
}
