use std::cmp::Ordering;
use std::fs::File;
use std::io::{BufWriter, Write};
use std::path::PathBuf;

use crate::core::FileKind;
use crate::index::file_entry_v2::FileEntry;
use crate::index::l2_partition::OwnedV7Source;

use super::path_sort::merge_path_runs;
use super::trigram_sort::merge_trigram_runs;
use super::{OwnedSpools, OwnedWorkspace, MAX_MERGE_RUNS};
use crate::storage::snapshot_v7::write_file_entry;

#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) struct PathFact {
    pub(super) path: Vec<u8>,
    pub(super) preferred_docid: Option<u32>,
    pub(super) child_docid: Option<u32>,
}

impl PathFact {
    fn estimated_bytes(&self) -> usize {
        std::mem::size_of::<Self>() + self.path.len()
    }
}

impl Ord for PathFact {
    fn cmp(&self, other: &Self) -> Ordering {
        self.path
            .cmp(&other.path)
            .then_with(|| self.preferred_docid.cmp(&other.preferred_docid))
            .then_with(|| self.child_docid.cmp(&other.child_docid))
    }
}

impl PartialOrd for PathFact {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd)]
pub(super) struct TrigramFact {
    pub(super) trigram: [u8; 3],
    pub(super) docid: u32,
}

pub(super) struct RunBuilder<'a> {
    workspace: &'a OwnedWorkspace,
    run_limit: usize,
    entry_records: BufWriter<File>,
    path_buffer: Vec<PathFact>,
    path_buffer_bytes: usize,
    path_runs: Vec<PathBuf>,
    trigram_buffer: Vec<TrigramFact>,
    trigram_runs: Vec<PathBuf>,
    peak_buffer_bytes: usize,
    largest_record_bytes: usize,
}

pub(super) struct PreparedRuns {
    entry_records: PathBuf,
    path_runs: Vec<PathBuf>,
    trigram_runs: Vec<PathBuf>,
    peak_buffer_bytes: usize,
    largest_record_bytes: usize,
}

impl PreparedRuns {
    pub(super) fn merge(
        self,
        workspace: &OwnedWorkspace,
        live_entries: usize,
    ) -> anyhow::Result<OwnedSpools> {
        let path_run_count = self.path_runs.len();
        let trigram_run_count = self.trigram_runs.len();
        let paths = merge_path_runs(workspace, &self.path_runs, live_entries)?;
        let trigrams = merge_trigram_runs(workspace, &self.trigram_runs, live_entries)?;
        Ok(OwnedSpools {
            entry_records: self.entry_records,
            path_slots: paths.slots,
            path_suffixes: paths.suffixes,
            path_reverse: paths.reverse,
            trigram_records: trigrams.records,
            parent_records: paths.parent_records,
            live_entries,
            path_entries: paths.path_entries,
            path_suffix_bytes: paths.suffix_bytes,
            trigram_entries: trigrams.trigram_entries,
            parent_entries: paths.parent_entries,
            path_runs: path_run_count,
            trigram_runs: trigram_run_count,
            peak_sort_buffer_bytes: self.peak_buffer_bytes,
            largest_sort_record_bytes: self.largest_record_bytes,
        })
    }
}

impl<'a> RunBuilder<'a> {
    pub(super) fn new(workspace: &'a OwnedWorkspace, run_limit: usize) -> anyhow::Result<Self> {
        let entries = workspace.path("entries.records");
        Ok(Self {
            workspace,
            run_limit,
            entry_records: BufWriter::new(File::create(entries)?),
            path_buffer: Vec::new(),
            path_buffer_bytes: 0,
            path_runs: Vec::new(),
            trigram_buffer: Vec::new(),
            trigram_runs: Vec::new(),
            peak_buffer_bytes: 0,
            largest_record_bytes: 0,
        })
    }

    pub(super) fn scan_source(&mut self, source: &OwnedV7Source) -> anyhow::Result<()> {
        source.for_each_live_entry_path(|docid, entry, path| {
            let compact_entry = FileEntry::from_file_key_and_kind(
                entry.file_key(),
                docid,
                entry.mtime_ns,
                entry.kind(),
            );
            write_file_entry(&mut self.entry_records, &compact_entry)?;
            self.push_path_fact(PathFact {
                path: path.to_vec(),
                preferred_docid: Some(docid),
                child_docid: None,
            })?;
            self.push_ancestors(path, entry.kind(), docid)?;
            self.push_path_trigrams(path, docid)?;
            Ok(())
        })
    }

    fn push_ancestors(&mut self, path: &[u8], kind: FileKind, docid: u32) -> anyhow::Result<()> {
        let mut current = immediate_parent(path);
        let mut immediate = true;
        while let Some(parent) = current {
            self.push_path_fact(PathFact {
                path: parent.to_vec(),
                preferred_docid: None,
                child_docid: (immediate && !kind.is_directory()).then_some(docid),
            })?;
            immediate = false;
            current = immediate_parent(parent);
        }
        Ok(())
    }

    fn push_path_fact(&mut self, fact: PathFact) -> anyhow::Result<()> {
        let bytes = fact.estimated_bytes();
        if !self.path_buffer.is_empty()
            && self.path_buffer_bytes.saturating_add(bytes) > self.run_limit
        {
            self.flush_path_run()?;
        }
        self.largest_record_bytes = self.largest_record_bytes.max(bytes);
        self.path_buffer_bytes = self.path_buffer_bytes.saturating_add(bytes);
        self.path_buffer.push(fact);
        self.record_peak();
        Ok(())
    }

    fn push_path_trigrams(&mut self, path: &[u8], docid: u32) -> anyhow::Result<()> {
        let lower = String::from_utf8_lossy(path).to_lowercase();
        let mut trigrams: Vec<[u8; 3]> = lower
            .as_bytes()
            .windows(3)
            .map(|bytes| [bytes[0], bytes[1], bytes[2]])
            .collect();
        trigrams.sort_unstable();
        trigrams.dedup();
        for trigram in trigrams {
            let bytes = std::mem::size_of::<TrigramFact>();
            if !self.trigram_buffer.is_empty()
                && self
                    .trigram_buffer
                    .len()
                    .saturating_mul(bytes)
                    .saturating_add(bytes)
                    > self.run_limit
            {
                self.flush_trigram_run()?;
            }
            self.largest_record_bytes = self.largest_record_bytes.max(bytes);
            self.trigram_buffer.push(TrigramFact { trigram, docid });
            self.record_peak();
        }
        Ok(())
    }

    fn record_peak(&mut self) {
        let current = self.path_buffer_bytes.saturating_add(
            self.trigram_buffer
                .len()
                .saturating_mul(std::mem::size_of::<TrigramFact>()),
        );
        self.peak_buffer_bytes = self.peak_buffer_bytes.max(current);
    }

    fn flush_path_run(&mut self) -> anyhow::Result<()> {
        if self.path_buffer.is_empty() {
            return Ok(());
        }
        self.path_buffer.sort_unstable();
        self.path_buffer.dedup();
        let path = self
            .workspace
            .path(format!("path-run-{:06}.bin", self.path_runs.len()));
        let mut writer = BufWriter::new(File::create(&path)?);
        for fact in self.path_buffer.drain(..) {
            write_path_fact(&mut writer, &fact)?;
        }
        writer.flush()?;
        self.path_buffer_bytes = 0;
        self.path_runs.push(path);
        Ok(())
    }

    fn flush_trigram_run(&mut self) -> anyhow::Result<()> {
        if self.trigram_buffer.is_empty() {
            return Ok(());
        }
        self.trigram_buffer.sort_unstable();
        self.trigram_buffer.dedup();
        let path = self
            .workspace
            .path(format!("trigram-run-{:06}.bin", self.trigram_runs.len()));
        let mut writer = BufWriter::new(File::create(&path)?);
        for fact in self.trigram_buffer.drain(..) {
            writer.write_all(&fact.trigram)?;
            writer.write_all(&fact.docid.to_le_bytes())?;
        }
        writer.flush()?;
        self.trigram_runs.push(path);
        Ok(())
    }

    pub(super) fn finish_runs(mut self) -> anyhow::Result<PreparedRuns> {
        self.flush_path_run()?;
        self.flush_trigram_run()?;
        self.entry_records.flush()?;
        if self.path_runs.len() > MAX_MERGE_RUNS || self.trigram_runs.len() > MAX_MERGE_RUNS {
            anyhow::bail!(
                "streaming_compaction_required: external sort produced too many runs ({}/{})",
                self.path_runs.len(),
                self.trigram_runs.len()
            );
        }
        Ok(PreparedRuns {
            entry_records: self.workspace.path("entries.records"),
            path_runs: self.path_runs,
            trigram_runs: self.trigram_runs,
            peak_buffer_bytes: self.peak_buffer_bytes,
            largest_record_bytes: self.largest_record_bytes,
        })
    }
}

fn immediate_parent(path: &[u8]) -> Option<&[u8]> {
    if path == b"/" || path.is_empty() {
        return None;
    }
    let trimmed = if path.len() > 1 && path.ends_with(b"/") {
        &path[..path.len() - 1]
    } else {
        path
    };
    let slash = trimmed.iter().rposition(|byte| *byte == b'/')?;
    if slash == 0 {
        Some(b"/")
    } else {
        Some(&trimmed[..slash])
    }
}

fn write_path_fact(writer: &mut dyn Write, fact: &PathFact) -> anyhow::Result<()> {
    let len = u32::try_from(fact.path.len())
        .map_err(|_| anyhow::anyhow!("owned_v7_source_corrupt: path is too long"))?;
    writer.write_all(&len.to_le_bytes())?;
    writer.write_all(&fact.path)?;
    writer.write_all(&fact.preferred_docid.unwrap_or(u32::MAX).to_le_bytes())?;
    writer.write_all(&fact.child_docid.unwrap_or(u32::MAX).to_le_bytes())?;
    Ok(())
}
