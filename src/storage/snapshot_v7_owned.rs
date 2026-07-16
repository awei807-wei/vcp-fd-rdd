//! Bounded-memory writer for a consumed mutable L2 generation.

use std::fs::File;
use std::io::{BufReader, BufWriter, Write};
use std::path::{Path, PathBuf};

use roaring::RoaringBitmap;

use crate::index::l2_partition::{OwnedV7Source, PersistentIndex};

use super::*;

#[path = "snapshot_v7_owned/path_sort.rs"]
mod path_sort;
#[path = "snapshot_v7_owned/run_builder.rs"]
mod run_builder;
#[cfg(test)]
#[path = "snapshot_v7_owned/tests.rs"]
mod tests;
#[path = "snapshot_v7_owned/trigram_sort.rs"]
mod trigram_sort;

use run_builder::{PreparedRuns, RunBuilder, TrigramFact};
use trigram_sort::{merge_trigram_runs, MergedTrigramSpool};

const DEFAULT_SORT_RUN_BYTES: usize = 32 * 1024 * 1024;
const MIN_SORT_RUN_BYTES: usize = 64 * 1024;
const MAX_MERGE_RUNS: usize = 1_024;

#[derive(Clone, Copy, Debug)]
pub struct OwnedV7WriteOptions {
    pub sort_run_bytes: usize,
}

impl Default for OwnedV7WriteOptions {
    fn default() -> Self {
        Self {
            sort_run_bytes: DEFAULT_SORT_RUN_BYTES,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct OwnedV7WriteReport {
    pub raw_entries: usize,
    pub live_entries: usize,
    pub path_entries: usize,
    pub trigram_entries: usize,
    pub parent_entries: usize,
    pub path_runs: usize,
    pub trigram_runs: usize,
    pub sort_run_limit_bytes: usize,
    pub peak_sort_buffer_bytes: usize,
    pub largest_sort_record_bytes: usize,
}

struct OwnedWorkspace {
    root: PathBuf,
}

impl OwnedWorkspace {
    fn create(output: &Path) -> anyhow::Result<Self> {
        let parent = output.parent().unwrap_or_else(|| Path::new("."));
        std::fs::create_dir_all(parent)?;
        let name = output
            .file_name()
            .and_then(|name| name.to_str())
            .unwrap_or("snapshot");
        let prefix = format!(".{name}.owned-v7-");
        for entry in std::fs::read_dir(parent)?.filter_map(Result::ok) {
            let file_name = entry.file_name();
            let file_name = file_name.to_string_lossy();
            if file_name.starts_with(&prefix)
                && entry.file_type().is_ok_and(|kind| kind.is_dir())
                && workspace_owner_is_gone(&file_name, &prefix)
            {
                if let Err(error) = std::fs::remove_dir_all(entry.path()) {
                    tracing::warn!(
                        "failed to clean stale owned v7 workspace {}: {}",
                        entry.path().display(),
                        error
                    );
                }
            }
        }
        let nonce = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|duration| duration.as_nanos())
            .unwrap_or(0);
        let root = parent.join(format!(".{name}.owned-v7-{}-{nonce}", std::process::id()));
        std::fs::create_dir(&root)?;
        Ok(Self { root })
    }

    fn path(&self, name: impl AsRef<Path>) -> PathBuf {
        self.root.join(name)
    }
}

fn workspace_owner_is_gone(file_name: &str, prefix: &str) -> bool {
    let Some(owner) = file_name
        .strip_prefix(prefix)
        .and_then(|suffix| suffix.split_once('-'))
        .and_then(|(pid, _nonce)| pid.parse::<u32>().ok())
    else {
        // Unknown names may belong to another writer version. Leaving them is
        // safer than deleting a workspace that could still be active.
        return false;
    };
    #[cfg(target_os = "linux")]
    {
        !Path::new("/proc").join(owner.to_string()).exists()
    }
    #[cfg(not(target_os = "linux"))]
    {
        let _ = owner;
        false
    }
}

impl Drop for OwnedWorkspace {
    fn drop(&mut self) {
        if let Err(error) = std::fs::remove_dir_all(&self.root) {
            if error.kind() != std::io::ErrorKind::NotFound {
                tracing::warn!(
                    "failed to clean owned v7 workspace {}: {}",
                    self.root.display(),
                    error
                );
            }
        }
    }
}

struct OwnedSpools {
    entry_records: PathBuf,
    path_slots: PathBuf,
    path_suffixes: PathBuf,
    path_reverse: PathBuf,
    trigram_records: PathBuf,
    parent_records: PathBuf,
    live_entries: usize,
    path_entries: usize,
    path_suffix_bytes: usize,
    trigram_entries: usize,
    parent_entries: usize,
    path_runs: usize,
    trigram_runs: usize,
    peak_sort_buffer_bytes: usize,
    largest_sort_record_bytes: usize,
}

/// Consume a complete mutable generation, fold a small overlay into it, and
/// write a compact current-v7 snapshot without materializing `BaseIndexData`.
pub fn write_v7_owned_index_atomic(
    path: &Path,
    index: PersistentIndex,
    overlay: ColdDeltaPlan,
    options: OwnedV7WriteOptions,
) -> anyhow::Result<OwnedV7WriteReport> {
    if !overlay.structural_directory_upserts_are_proven() {
        anyhow::bail!(
            "direct_v7_unsupported: subtree move completeness is unproven; rebuild required"
        );
    }
    let (deleted_paths, mut upserts) = overlay.into_parts();
    index.tombstone_snapshot_prefixes(&deleted_paths);
    for meta in upserts.drain(..) {
        index.upsert_path_alias(meta);
    }

    let source = index.into_v7_source();
    let raw_entries = source.raw_entry_count();
    let live_entries = source.live_entry_count();
    let workspace = OwnedWorkspace::create(path)?;
    let limit = options.sort_run_bytes.max(MIN_SORT_RUN_BYTES);
    let prepared = build_owned_runs(&workspace, &source, limit)?;
    // The external runs now contain every fact needed by the merge. Release
    // the final full-generation entries/path arena before allocating reverse
    // path tables or large per-key roaring postings.
    drop(source);
    let spools = prepared.merge(&workspace, live_entries)?;

    write_owned_spools_atomic(path, &spools)?;

    Ok(OwnedV7WriteReport {
        raw_entries,
        live_entries: spools.live_entries,
        path_entries: spools.path_entries,
        trigram_entries: spools.trigram_entries,
        parent_entries: spools.parent_entries,
        path_runs: spools.path_runs,
        trigram_runs: spools.trigram_runs,
        sort_run_limit_bytes: limit,
        peak_sort_buffer_bytes: spools.peak_sort_buffer_bytes,
        largest_sort_record_bytes: spools.largest_sort_record_bytes,
    })
}

fn build_owned_runs(
    workspace: &OwnedWorkspace,
    source: &OwnedV7Source,
    run_limit: usize,
) -> anyhow::Result<PreparedRuns> {
    let mut builder = RunBuilder::new(workspace, run_limit)?;
    builder.scan_source(source)?;
    builder.finish_runs()
}

fn write_owned_spools_atomic(path: &Path, spools: &OwnedSpools) -> anyhow::Result<()> {
    crate::storage::atomic_write_validated(
        path,
        "v7.tmp",
        |file| {
            let mut writer = V7StreamingWriter::new(file, V7_VERSION)?;
            write_owned_segments(&mut writer, spools)?;
            writer.finish()
        },
        |tmp| validate_written_v7(tmp, Some(spools.live_entries), "owned_v7_validation_failed"),
    )
}

fn write_owned_segments(
    writer: &mut V7StreamingWriter<'_>,
    spools: &OwnedSpools,
) -> anyhow::Result<()> {
    writer.write_segment(V7SegKind::PathTable, |sink| {
        sink.write_all(RAW_PATH_TABLE_MAGIC)?;
        sink.write_all(&(spools.path_entries as u32).to_le_bytes())?;
        sink.write_all(&(spools.path_suffix_bytes as u32).to_le_bytes())?;
        sink.write_all(&(spools.path_entries as u32).to_le_bytes())?;
        sink.write_all(&0u32.to_le_bytes())?;
        copy_file(&spools.path_slots, sink)?;
        copy_file(&spools.path_suffixes, sink)?;
        copy_file(&spools.path_reverse, sink)
    })?;
    writer.write_segment(V7SegKind::EntriesByKey, |sink| {
        sink.write_all(&(spools.live_entries as u32).to_le_bytes())?;
        copy_file(&spools.entry_records, sink)
    })?;
    writer.write_segment(V7SegKind::EntriesByPath, |sink| {
        sink.write_all(&(spools.live_entries as u32).to_le_bytes())?;
        copy_file(&spools.entry_records, sink)
    })?;
    writer.write_segment(V7SegKind::TrigramIndex, |sink| {
        sink.write_all(&(spools.trigram_entries as u32).to_le_bytes())?;
        copy_file(&spools.trigram_records, sink)
    })?;
    writer.write_segment(V7SegKind::ParentIndex, |sink| {
        sink.write_all(&(spools.parent_entries as u32).to_le_bytes())?;
        copy_file(&spools.parent_records, sink)?;
        sink.write_all(&0u32.to_le_bytes())?;
        Ok(())
    })?;
    writer.write_segment(V7SegKind::Tombstones, |sink| {
        RoaringBitmap::new().serialize_into(sink)?;
        Ok(())
    })?;
    Ok(())
}

fn copy_file(path: &Path, sink: &mut dyn Write) -> anyhow::Result<()> {
    let mut reader = BufReader::new(File::open(path)?);
    std::io::copy(&mut reader, sink)?;
    Ok(())
}

pub(super) fn write_v7_hot_base_bounded_atomic(
    path: &Path,
    data: &BaseIndexData,
) -> anyhow::Result<()> {
    if data.has_manifest_only_segments() {
        anyhow::bail!("hot_v7_unsupported: hot writer cannot encode cold manifest segments");
    }
    let workspace = OwnedWorkspace::create(path)?;
    let trigrams = build_hot_base_trigrams(&workspace, data, DEFAULT_SORT_RUN_BYTES)?;
    crate::storage::atomic_write_validated(
        path,
        "v7.tmp",
        |file| {
            let mut writer = V7StreamingWriter::new(file, V7_VERSION)?;
            writer.write_segment(V7SegKind::PathTable, |sink| {
                data.path_table.encode_raw_to_writer(sink)?;
                Ok(())
            })?;
            writer.write_segment(V7SegKind::EntriesByKey, |sink| {
                write_file_entry_index(sink, &data.entries_by_key)
            })?;
            writer.write_segment(V7SegKind::EntriesByPath, |sink| {
                write_file_entry_index(sink, &data.entries_by_key)
            })?;
            writer.write_segment(V7SegKind::TrigramIndex, |sink| {
                sink.write_all(&(trigrams.trigram_entries as u32).to_le_bytes())?;
                copy_file(&trigrams.records, sink)
            })?;
            writer.write_segment(V7SegKind::ParentIndex, |sink| {
                write_parent_index(sink, &data.parent_index)
            })?;
            writer.write_segment(V7SegKind::Tombstones, |sink| {
                data.tombstones.serialize_into(sink)?;
                Ok(())
            })?;
            writer.finish()
        },
        |tmp| validate_written_v7(tmp, None, "hot_v7_validation_failed"),
    )
}

fn build_hot_base_trigrams(
    workspace: &OwnedWorkspace,
    data: &BaseIndexData,
    run_limit: usize,
) -> anyhow::Result<MergedTrigramSpool> {
    let fact_size = std::mem::size_of::<TrigramFact>();
    let max_facts = (run_limit / fact_size).max(1);
    let mut buffer = Vec::with_capacity(max_facts.min(64 * 1024));
    let mut runs = Vec::new();
    let mut path = Vec::new();
    for (docid, entry) in data.entries_by_key.iter().enumerate() {
        let docid = u32::try_from(docid)
            .map_err(|_| anyhow::anyhow!("streaming_compaction_required: DocId overflow"))?;
        if data.tombstones.contains(docid) {
            continue;
        }
        data.path_table
            .resolve_into(entry.path_index(), &mut path)
            .ok_or_else(|| {
                anyhow::anyhow!("hot_v7_validation_failed: entry path did not resolve")
            })?;
        push_unique_path_trigrams(&path, docid, &mut buffer);
        if buffer.len() >= max_facts {
            flush_hot_trigram_run(workspace, &mut buffer, &mut runs)?;
        }
    }
    flush_hot_trigram_run(workspace, &mut buffer, &mut runs)?;
    if runs.len() > MAX_MERGE_RUNS {
        anyhow::bail!(
            "streaming_compaction_required: hot trigram external sort produced {} runs",
            runs.len()
        );
    }
    merge_trigram_runs(workspace, &runs, data.entries_by_key.len())
}

fn push_unique_path_trigrams(path: &[u8], docid: u32, buffer: &mut Vec<TrigramFact>) {
    let lower = String::from_utf8_lossy(path).to_lowercase();
    let mut trigrams: Vec<[u8; 3]> = lower
        .as_bytes()
        .windows(3)
        .map(|bytes| [bytes[0], bytes[1], bytes[2]])
        .collect();
    trigrams.sort_unstable();
    trigrams.dedup();
    buffer.extend(
        trigrams
            .into_iter()
            .map(|trigram| TrigramFact { trigram, docid }),
    );
}

fn flush_hot_trigram_run(
    workspace: &OwnedWorkspace,
    buffer: &mut Vec<TrigramFact>,
    runs: &mut Vec<PathBuf>,
) -> anyhow::Result<()> {
    if buffer.is_empty() {
        return Ok(());
    }
    buffer.sort_unstable();
    buffer.dedup();
    let path = workspace.path(format!("hot-trigram-run-{:06}.bin", runs.len()));
    let mut writer = BufWriter::new(File::create(&path)?);
    for fact in buffer.drain(..) {
        writer.write_all(&fact.trigram)?;
        writer.write_all(&fact.docid.to_le_bytes())?;
    }
    writer.flush()?;
    runs.push(path);
    Ok(())
}
