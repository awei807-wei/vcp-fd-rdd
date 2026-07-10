use std::collections::{BTreeMap, BTreeSet};

use roaring::RoaringBitmap;

use crate::core::FileMeta;
use crate::index::file_entry_v2::FileEntry;
use crate::storage::snapshot_v7::{
    decode_tombstones, file_entry_at, resolve_raw_path_into, V7_VERSION,
};

use super::validate::for_each_raw_sorted_path;
use super::{
    ColdDeltaLimits, ColdDeltaPlan, ColdDeltaWriteReport, PathAddition, PathIndexByBytes,
    PreparedColdDelta, ValidatedColdSource,
};

pub(super) fn prepare_cold_delta(
    source: &ValidatedColdSource<'_>,
    plan: ColdDeltaPlan,
    limits: ColdDeltaLimits,
) -> anyhow::Result<PreparedColdDelta> {
    let candidate_paths = collect_delta_candidate_paths(&plan.upserts);
    let (path_additions, path_indices) =
        resolve_delta_path_indices(source.path_table, source.path_layout, candidate_paths)?;
    let (tombstones, subtree_delete_matched) = merged_tombstones(source, &plan)?;
    if subtree_delete_matched && plan.upserts.iter().any(|meta| meta.kind.is_directory()) {
        anyhow::bail!(
            "direct_v7_unsupported: subtree move completeness is unproven; rebuild required"
        );
    }

    let appended_entries = build_appended_entries(source, &plan.upserts, &path_indices)?;
    let trigram_delta = build_delta_trigrams(source.old_entry_count, &plan.upserts)?;
    let parent_delta =
        build_delta_parent_postings(source.old_entry_count, &plan.upserts, &path_indices)?;
    let report = cold_delta_report(source, &path_additions, &appended_entries, &tombstones)?;
    enforce_cold_delta_limits(&report, limits)?;

    Ok(PreparedColdDelta {
        path_additions,
        appended_entries,
        trigram_delta,
        parent_delta,
        tombstones,
        report,
    })
}

fn collect_delta_candidate_paths(upserts: &[FileMeta]) -> BTreeSet<Vec<u8>> {
    let mut paths = BTreeSet::new();
    for meta in upserts {
        for ancestor in meta.path.ancestors() {
            let bytes = ancestor.as_os_str().as_encoded_bytes();
            if !bytes.is_empty() {
                paths.insert(bytes.to_vec());
            }
        }
    }
    paths
}

fn resolve_delta_path_indices(
    path_table: &[u8],
    layout: crate::storage::snapshot_v7::RawPathTableLayout,
    candidates: BTreeSet<Vec<u8>>,
) -> anyhow::Result<(Vec<PathAddition>, PathIndexByBytes)> {
    let candidates: Vec<Vec<u8>> = candidates.into_iter().collect();
    let mut additions = Vec::new();
    let mut indices = BTreeMap::new();
    let mut candidate_pos = 0usize;
    let mut next_path_idx = u32::try_from(layout.idx_len)
        .map_err(|_| anyhow::anyhow!("direct_v7_unsupported: path index space is exhausted"))?;

    for_each_raw_sorted_path(path_table, layout, |_, orig_idx, old_path| {
        while candidate_pos < candidates.len() && candidates[candidate_pos].as_slice() < old_path {
            assign_new_path_index(
                &candidates[candidate_pos],
                &mut next_path_idx,
                &mut additions,
                &mut indices,
            )?;
            candidate_pos += 1;
        }
        if candidate_pos < candidates.len() && candidates[candidate_pos].as_slice() == old_path {
            indices.insert(candidates[candidate_pos].clone(), orig_idx);
            candidate_pos += 1;
        }
        Ok(())
    })?;
    while candidate_pos < candidates.len() {
        assign_new_path_index(
            &candidates[candidate_pos],
            &mut next_path_idx,
            &mut additions,
            &mut indices,
        )?;
        candidate_pos += 1;
    }
    additions.sort_by(|left, right| left.path.cmp(&right.path));
    Ok((additions, indices))
}

fn assign_new_path_index(
    path: &[u8],
    next_path_idx: &mut u32,
    additions: &mut Vec<PathAddition>,
    indices: &mut BTreeMap<Vec<u8>, u32>,
) -> anyhow::Result<()> {
    if *next_path_idx >= (1u32 << 31) {
        anyhow::bail!("direct_v7_unsupported: path index space is exhausted");
    }
    let path_idx = *next_path_idx;
    *next_path_idx = next_path_idx
        .checked_add(1)
        .ok_or_else(|| anyhow::anyhow!("direct_v7_unsupported: path index overflow"))?;
    additions.push(PathAddition {
        path_idx,
        path: path.to_vec(),
    });
    indices.insert(path.to_vec(), path_idx);
    Ok(())
}

fn merged_tombstones(
    source: &ValidatedColdSource<'_>,
    plan: &ColdDeltaPlan,
) -> anyhow::Result<(RoaringBitmap, bool)> {
    let mut tombstones = decode_tombstones(source.tombstones)?;
    let upsert_paths: BTreeSet<&[u8]> = plan
        .upserts
        .iter()
        .map(|meta| meta.path.as_os_str().as_encoded_bytes())
        .collect();
    let mut path = Vec::new();
    let mut subtree_delete_matched = false;

    for docid in 0..source.old_entry_count {
        let docid = docid as u32;
        if tombstones.contains(docid) {
            continue;
        }
        let entry = file_entry_at(source.entries_by_key, V7_VERSION, docid)
            .ok_or_else(|| anyhow::anyhow!("direct_v7_corrupt: entry {} disappeared", docid))?;
        resolve_raw_path_into(
            source.path_table,
            source.path_layout,
            entry.path_index(),
            &mut path,
        )
        .ok_or_else(|| anyhow::anyhow!("direct_v7_corrupt: entry path did not resolve"))?;

        let deleted = deleted_prefix_for_path(&path, &plan.deleted_paths);
        if deleted.is_some() || upsert_paths.contains(path.as_slice()) {
            tombstones.insert(docid);
        }
        if let Some(prefix) = deleted {
            subtree_delete_matched |= path.as_slice() != prefix;
            subtree_delete_matched |= entry.kind().is_directory();
        }
    }
    Ok((tombstones, subtree_delete_matched))
}

fn deleted_prefix_for_path<'a>(path: &[u8], deleted: &'a BTreeSet<Vec<u8>>) -> Option<&'a [u8]> {
    if let Some(exact) = deleted.get(path) {
        return Some(exact.as_slice());
    }
    for boundary in path
        .iter()
        .enumerate()
        .rev()
        .filter_map(|(index, byte)| (*byte == b'/' && index > 0).then_some(index))
    {
        if let Some(prefix) = deleted.get(&path[..=boundary]) {
            return Some(prefix.as_slice());
        }
        if let Some(prefix) = deleted.get(&path[..boundary]) {
            return Some(prefix.as_slice());
        }
    }
    deleted.get(b"/".as_slice()).map(Vec::as_slice)
}

fn build_appended_entries(
    source: &ValidatedColdSource<'_>,
    upserts: &[FileMeta],
    path_indices: &BTreeMap<Vec<u8>, u32>,
) -> anyhow::Result<Vec<FileEntry>> {
    let total = source
        .old_entry_count
        .checked_add(upserts.len())
        .ok_or_else(|| anyhow::anyhow!("streaming_compaction_required: entry count overflow"))?;
    if total > u32::MAX as usize {
        anyhow::bail!("streaming_compaction_required: entry count exceeds DocId space");
    }
    upserts
        .iter()
        .map(|meta| {
            let path = meta.path.as_os_str().as_encoded_bytes();
            let path_idx = path_indices.get(path).copied().ok_or_else(|| {
                anyhow::anyhow!("direct_v7_internal: upsert path was not assigned an index")
            })?;
            Ok(FileEntry::from_file_key_and_kind(
                meta.file_key,
                path_idx,
                system_time_to_ns(meta.mtime),
                meta.kind,
            ))
        })
        .collect()
}

fn system_time_to_ns(time: Option<std::time::SystemTime>) -> i64 {
    time.and_then(|value| value.duration_since(std::time::UNIX_EPOCH).ok())
        .and_then(|duration| i64::try_from(duration.as_nanos()).ok())
        .unwrap_or(-1)
}

fn build_delta_trigrams(
    old_entry_count: usize,
    upserts: &[FileMeta],
) -> anyhow::Result<BTreeMap<[u8; 3], RoaringBitmap>> {
    let mut trigrams = BTreeMap::new();
    for (ordinal, meta) in upserts.iter().enumerate() {
        let docid = u32::try_from(old_entry_count + ordinal)
            .map_err(|_| anyhow::anyhow!("streaming_compaction_required: DocId overflow"))?;
        let path = meta.path.as_os_str().as_encoded_bytes();
        let lower = String::from_utf8_lossy(path).to_lowercase();
        for bytes in lower.as_bytes().windows(3) {
            trigrams
                .entry([bytes[0], bytes[1], bytes[2]])
                .or_insert_with(RoaringBitmap::new)
                .insert(docid);
        }
    }
    Ok(trigrams)
}

fn build_delta_parent_postings(
    old_entry_count: usize,
    upserts: &[FileMeta],
    path_indices: &BTreeMap<Vec<u8>, u32>,
) -> anyhow::Result<BTreeMap<u32, RoaringBitmap>> {
    let mut parents = BTreeMap::new();
    for (ordinal, meta) in upserts.iter().enumerate() {
        if meta.kind.is_directory() {
            continue;
        }
        let Some(parent) = meta.path.parent() else {
            continue;
        };
        let parent_idx = path_indices
            .get(parent.as_os_str().as_encoded_bytes())
            .copied()
            .ok_or_else(|| anyhow::anyhow!("direct_v7_internal: parent path index is missing"))?;
        let docid = u32::try_from(old_entry_count + ordinal)
            .map_err(|_| anyhow::anyhow!("streaming_compaction_required: DocId overflow"))?;
        parents
            .entry(parent_idx)
            .or_insert_with(RoaringBitmap::new)
            .insert(docid);
    }
    Ok(parents)
}

fn cold_delta_report(
    source: &ValidatedColdSource<'_>,
    path_additions: &[PathAddition],
    appended_entries: &[FileEntry],
    tombstones: &RoaringBitmap,
) -> anyhow::Result<ColdDeltaWriteReport> {
    let total_entries = source.old_entry_count + appended_entries.len();
    let tombstone_entries = tombstones.len() as usize;
    let live_entries = total_entries
        .checked_sub(tombstone_entries)
        .ok_or_else(|| {
            anyhow::anyhow!("direct_v7_corrupt: tombstones exceed the resulting entry count")
        })?;
    let path_entries = source.path_layout.slots_len + path_additions.len();
    Ok(ColdDeltaWriteReport {
        old_entries: source.old_entry_count,
        appended_entries: appended_entries.len(),
        live_entries,
        tombstone_entries,
        path_entries,
        entry_amplification_bps: amplification_bps(total_entries, live_entries),
        path_amplification_bps: amplification_bps(path_entries, live_entries),
    })
}

fn amplification_bps(total: usize, live: usize) -> u32 {
    if live == 0 {
        return if total == 0 { 0 } else { u32::MAX };
    }
    total
        .saturating_mul(10_000)
        .checked_div(live)
        .unwrap_or(usize::MAX)
        .min(u32::MAX as usize) as u32
}

fn enforce_cold_delta_limits(
    report: &ColdDeltaWriteReport,
    limits: ColdDeltaLimits,
) -> anyhow::Result<()> {
    if report.live_entries < limits.min_live_entries_for_amplification {
        return Ok(());
    }
    if report.entry_amplification_bps > limits.max_entry_amplification_bps
        || report.path_amplification_bps > limits.max_path_amplification_bps
    {
        anyhow::bail!(
            "streaming_compaction_required: entry amplification={}bps (limit={}), \
             path amplification={}bps (limit={})",
            report.entry_amplification_bps,
            limits.max_entry_amplification_bps,
            report.path_amplification_bps,
            limits.max_path_amplification_bps
        );
    }
    Ok(())
}
