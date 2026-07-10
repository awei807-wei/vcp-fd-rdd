use std::io::Write;

use crate::index::file_entry_v2::FileEntry;
use crate::storage::snapshot_v7::{
    write_file_entry, RawPathTableLayout, RAW_PATH_TABLE_ANCHOR_INTERVAL, RAW_PATH_TABLE_MAGIC,
};

use super::validate::{exact_current_entry_count, for_each_raw_sorted_path};
use super::PathAddition;

pub(super) fn write_merged_raw_path_table(
    sink: &mut dyn Write,
    old_bytes: &[u8],
    old_layout: RawPathTableLayout,
    additions: &[PathAddition],
) -> anyhow::Result<()> {
    let (path_count, suffix_bytes, idx_count) =
        merged_path_table_shape(old_bytes, old_layout, additions)?;
    sink.write_all(RAW_PATH_TABLE_MAGIC)?;
    sink.write_all(&(path_count as u32).to_le_bytes())?;
    sink.write_all(&(suffix_bytes as u32).to_le_bytes())?;
    sink.write_all(&(idx_count as u32).to_le_bytes())?;
    sink.write_all(&0u32.to_le_bytes())?;

    let idx_to_sorted = write_merged_path_slots(sink, old_bytes, old_layout, additions, idx_count)?;
    write_merged_path_suffixes(sink, old_bytes, old_layout, additions)?;
    for sorted_pos in idx_to_sorted {
        if sorted_pos == u32::MAX {
            anyhow::bail!("direct_v7_internal: path reverse map contains a hole");
        }
        sink.write_all(&sorted_pos.to_le_bytes())?;
    }
    Ok(())
}

fn merged_path_table_shape(
    old_bytes: &[u8],
    old_layout: RawPathTableLayout,
    additions: &[PathAddition],
) -> anyhow::Result<(usize, usize, usize)> {
    let mut suffix_bytes = 0usize;
    let mut previous = Vec::new();
    for_each_merged_path(old_bytes, old_layout, additions, |sorted_pos, _, path| {
        let shared = encoded_shared_prefix(sorted_pos, &previous, path);
        validate_encoded_path_lengths(shared, path.len().saturating_sub(shared))?;
        suffix_bytes = suffix_bytes
            .checked_add(path.len() - shared)
            .ok_or_else(|| {
                anyhow::anyhow!("streaming_compaction_required: path suffix overflow")
            })?;
        previous.clear();
        previous.extend_from_slice(path);
        Ok(())
    })?;
    let path_count = old_layout.slots_len + additions.len();
    let idx_count = old_layout.idx_len + additions.len();
    if path_count > u32::MAX as usize
        || idx_count >= (1usize << 31)
        || suffix_bytes > u32::MAX as usize
    {
        anyhow::bail!("streaming_compaction_required: path table encoding limit exceeded");
    }
    Ok((path_count, suffix_bytes, idx_count))
}

fn write_merged_path_slots(
    sink: &mut dyn Write,
    old_bytes: &[u8],
    old_layout: RawPathTableLayout,
    additions: &[PathAddition],
    idx_count: usize,
) -> anyhow::Result<Vec<u32>> {
    let mut idx_to_sorted = vec![u32::MAX; idx_count];
    let mut suffix_offset = 0usize;
    let mut previous = Vec::new();
    for_each_merged_path(
        old_bytes,
        old_layout,
        additions,
        |sorted_pos, orig_idx, path| {
            let shared = encoded_shared_prefix(sorted_pos, &previous, path);
            let suffix_len = path.len() - shared;
            validate_encoded_path_lengths(shared, suffix_len)?;
            sink.write_all(&(suffix_offset as u32).to_le_bytes())?;
            sink.write_all(&(shared as u16).to_le_bytes())?;
            sink.write_all(&(suffix_len as u16).to_le_bytes())?;
            sink.write_all(&orig_idx.to_le_bytes())?;
            let slot = idx_to_sorted
                .get_mut(orig_idx as usize)
                .ok_or_else(|| anyhow::anyhow!("direct_v7_internal: path index out of range"))?;
            if *slot != u32::MAX {
                anyhow::bail!("direct_v7_internal: duplicate path index during merge");
            }
            *slot = sorted_pos as u32;
            suffix_offset += suffix_len;
            previous.clear();
            previous.extend_from_slice(path);
            Ok(())
        },
    )?;
    Ok(idx_to_sorted)
}

fn write_merged_path_suffixes(
    sink: &mut dyn Write,
    old_bytes: &[u8],
    old_layout: RawPathTableLayout,
    additions: &[PathAddition],
) -> anyhow::Result<()> {
    let mut previous = Vec::new();
    for_each_merged_path(old_bytes, old_layout, additions, |sorted_pos, _, path| {
        let shared = encoded_shared_prefix(sorted_pos, &previous, path);
        sink.write_all(&path[shared..])?;
        previous.clear();
        previous.extend_from_slice(path);
        Ok(())
    })
}

fn for_each_merged_path(
    old_bytes: &[u8],
    old_layout: RawPathTableLayout,
    additions: &[PathAddition],
    mut visitor: impl FnMut(usize, u32, &[u8]) -> anyhow::Result<()>,
) -> anyhow::Result<()> {
    let mut addition_pos = 0usize;
    let mut merged_pos = 0usize;
    for_each_raw_sorted_path(old_bytes, old_layout, |_, old_idx, old_path| {
        while addition_pos < additions.len() && additions[addition_pos].path.as_slice() < old_path {
            let addition = &additions[addition_pos];
            visitor(merged_pos, addition.path_idx, &addition.path)?;
            addition_pos += 1;
            merged_pos += 1;
        }
        if addition_pos < additions.len() && additions[addition_pos].path.as_slice() == old_path {
            anyhow::bail!("direct_v7_internal: new path duplicates an old path");
        }
        visitor(merged_pos, old_idx, old_path)?;
        merged_pos += 1;
        Ok(())
    })?;
    while addition_pos < additions.len() {
        let addition = &additions[addition_pos];
        visitor(merged_pos, addition.path_idx, &addition.path)?;
        addition_pos += 1;
        merged_pos += 1;
    }
    Ok(())
}

pub(in crate::storage::snapshot_v7) fn encoded_shared_prefix(
    sorted_pos: usize,
    previous: &[u8],
    path: &[u8],
) -> usize {
    if sorted_pos.is_multiple_of(RAW_PATH_TABLE_ANCHOR_INTERVAL) {
        0
    } else {
        previous
            .iter()
            .zip(path)
            .take_while(|(left, right)| left == right)
            .count()
    }
}

pub(in crate::storage::snapshot_v7) fn validate_encoded_path_lengths(
    shared: usize,
    suffix: usize,
) -> anyhow::Result<()> {
    if shared > u16::MAX as usize || suffix > u16::MAX as usize {
        anyhow::bail!("direct_v7_unsupported: path exceeds raw path-table record limits");
    }
    Ok(())
}

pub(super) fn write_appended_entry_segment(
    sink: &mut dyn Write,
    old_segment: &[u8],
    appended: &[FileEntry],
) -> anyhow::Result<()> {
    let old_count = exact_current_entry_count(old_segment)?;
    let total = old_count
        .checked_add(appended.len())
        .and_then(|count| u32::try_from(count).ok())
        .ok_or_else(|| anyhow::anyhow!("streaming_compaction_required: entry count overflow"))?;
    sink.write_all(&total.to_le_bytes())?;
    sink.write_all(&old_segment[4..])?;
    for entry in appended {
        write_file_entry(sink, entry)?;
    }
    Ok(())
}
