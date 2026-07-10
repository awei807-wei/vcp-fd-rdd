use std::cmp::Ordering;
use std::collections::BinaryHeap;
use std::fs::File;
use std::io::{BufReader, BufWriter, Read, Write};
use std::path::{Path, PathBuf};

use roaring::RoaringBitmap;

use super::run_builder::PathFact;
use super::OwnedWorkspace;
use crate::storage::snapshot_v7::{
    encoded_shared_prefix, validate_encoded_path_lengths, validate_posting_docids,
    write_parent_posting,
};

struct PathRunReader {
    reader: BufReader<File>,
}

impl PathRunReader {
    fn open(path: &Path) -> anyhow::Result<Self> {
        Ok(Self {
            reader: BufReader::new(File::open(path)?),
        })
    }

    fn next_fact(&mut self) -> anyhow::Result<Option<PathFact>> {
        let Some(path_len) = read_optional_u32(&mut self.reader)? else {
            return Ok(None);
        };
        let mut path = vec![0u8; path_len as usize];
        self.reader.read_exact(&mut path)?;
        let preferred = read_required_u32(&mut self.reader)?;
        let child = read_required_u32(&mut self.reader)?;
        Ok(Some(PathFact {
            path,
            preferred_docid: (preferred != u32::MAX).then_some(preferred),
            child_docid: (child != u32::MAX).then_some(child),
        }))
    }
}

#[derive(Eq, PartialEq)]
struct PathHeapItem {
    fact: PathFact,
    run: usize,
}

impl Ord for PathHeapItem {
    fn cmp(&self, other: &Self) -> Ordering {
        other
            .fact
            .cmp(&self.fact)
            .then_with(|| other.run.cmp(&self.run))
    }
}

impl PartialOrd for PathHeapItem {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

pub(super) struct MergedPathSpools {
    pub(super) slots: PathBuf,
    pub(super) suffixes: PathBuf,
    pub(super) reverse: PathBuf,
    pub(super) parent_records: PathBuf,
    pub(super) path_entries: usize,
    pub(super) suffix_bytes: usize,
    pub(super) parent_entries: usize,
}

pub(super) fn merge_path_runs(
    workspace: &OwnedWorkspace,
    run_paths: &[PathBuf],
    live_entries: usize,
) -> anyhow::Result<MergedPathSpools> {
    if live_entries >= (1usize << 31) {
        anyhow::bail!("streaming_compaction_required: live entry path index space exhausted");
    }
    let mut readers: Vec<PathRunReader> = run_paths
        .iter()
        .map(|path| PathRunReader::open(path))
        .collect::<anyhow::Result<_>>()?;
    let mut heap = BinaryHeap::new();
    for (run, reader) in readers.iter_mut().enumerate() {
        if let Some(fact) = reader.next_fact()? {
            heap.push(PathHeapItem { fact, run });
        }
    }

    let slots_path = workspace.path("path.slots");
    let suffixes_path = workspace.path("path.suffixes");
    let reverse_path = workspace.path("path.reverse");
    let parent_path = workspace.path("parent.records");
    let mut slots = BufWriter::new(File::create(&slots_path)?);
    let mut suffixes = BufWriter::new(File::create(&suffixes_path)?);
    let mut parents = BufWriter::new(File::create(&parent_path)?);
    let mut state = PathMergeState::new(live_entries);

    while let Some(first) = heap.pop() {
        let group_path = first.fact.path.clone();
        let mut preferred = None;
        let mut children = RoaringBitmap::new();
        consume_path_fact(
            first,
            &mut readers,
            &mut heap,
            &mut preferred,
            &mut children,
        )?;
        while heap.peek().is_some_and(|item| item.fact.path == group_path) {
            let item = heap.pop().expect("peeked above");
            consume_path_fact(item, &mut readers, &mut heap, &mut preferred, &mut children)?;
        }
        state.write_group(
            &group_path,
            preferred,
            &children,
            &mut slots,
            &mut suffixes,
            &mut parents,
        )?;
    }
    slots.flush()?;
    suffixes.flush()?;
    parents.flush()?;
    state.finish_reverse(&reverse_path)?;
    if state.sorted_pos > u32::MAX as usize || state.suffix_offset > u32::MAX as usize {
        anyhow::bail!("streaming_compaction_required: encoded path table exceeds v7 limits");
    }

    Ok(MergedPathSpools {
        slots: slots_path,
        suffixes: suffixes_path,
        reverse: reverse_path,
        parent_records: parent_path,
        path_entries: state.sorted_pos,
        suffix_bytes: state.suffix_offset,
        parent_entries: state.parent_entries,
    })
}

fn consume_path_fact(
    item: PathHeapItem,
    readers: &mut [PathRunReader],
    heap: &mut BinaryHeap<PathHeapItem>,
    preferred: &mut Option<u32>,
    children: &mut RoaringBitmap,
) -> anyhow::Result<()> {
    if let Some(docid) = item.fact.preferred_docid {
        if preferred.is_some_and(|existing| existing != docid) {
            anyhow::bail!("owned_v7_source_corrupt: duplicate live path has multiple DocIds");
        }
        *preferred = Some(docid);
    }
    if let Some(docid) = item.fact.child_docid {
        children.insert(docid);
    }
    if let Some(fact) = readers[item.run].next_fact()? {
        heap.push(PathHeapItem {
            fact,
            run: item.run,
        });
    }
    Ok(())
}

struct PathMergeState {
    live_entries: usize,
    next_parent_idx: u32,
    sorted_pos: usize,
    suffix_offset: usize,
    parent_entries: usize,
    previous_path: Vec<u8>,
    idx_to_sorted: Vec<u32>,
}

impl PathMergeState {
    fn new(live_entries: usize) -> Self {
        Self {
            live_entries,
            next_parent_idx: live_entries as u32,
            sorted_pos: 0,
            suffix_offset: 0,
            parent_entries: 0,
            previous_path: Vec::new(),
            idx_to_sorted: vec![u32::MAX; live_entries],
        }
    }

    fn write_group(
        &mut self,
        path: &[u8],
        preferred: Option<u32>,
        children: &RoaringBitmap,
        slots: &mut dyn Write,
        suffixes: &mut dyn Write,
        parents: &mut dyn Write,
    ) -> anyhow::Result<()> {
        let path_idx = self.assign_path_idx(preferred)?;
        let shared = encoded_shared_prefix(self.sorted_pos, &self.previous_path, path);
        let suffix = &path[shared..];
        validate_encoded_path_lengths(shared, suffix.len())?;
        if self.suffix_offset > u32::MAX as usize {
            anyhow::bail!("streaming_compaction_required: path suffix offset overflow");
        }
        slots.write_all(&(self.suffix_offset as u32).to_le_bytes())?;
        slots.write_all(&(shared as u16).to_le_bytes())?;
        slots.write_all(&(suffix.len() as u16).to_le_bytes())?;
        slots.write_all(&path_idx.to_le_bytes())?;
        suffixes.write_all(suffix)?;
        self.set_reverse(path_idx)?;
        if !children.is_empty() {
            validate_posting_docids(children, self.live_entries, "parent")?;
            write_parent_posting(parents, path_idx, children)?;
            self.parent_entries += 1;
        }
        self.suffix_offset = self
            .suffix_offset
            .checked_add(suffix.len())
            .ok_or_else(|| anyhow::anyhow!("streaming_compaction_required: suffix overflow"))?;
        self.sorted_pos += 1;
        self.previous_path.clear();
        self.previous_path.extend_from_slice(path);
        Ok(())
    }

    fn assign_path_idx(&mut self, preferred: Option<u32>) -> anyhow::Result<u32> {
        if let Some(docid) = preferred {
            if docid as usize >= self.live_entries {
                anyhow::bail!("owned_v7_source_corrupt: preferred path DocId is out of range");
            }
            return Ok(docid);
        }
        if self.next_parent_idx >= (1u32 << 31) {
            anyhow::bail!("streaming_compaction_required: path index space exhausted");
        }
        let path_idx = self.next_parent_idx;
        self.next_parent_idx += 1;
        self.idx_to_sorted.push(u32::MAX);
        Ok(path_idx)
    }

    fn set_reverse(&mut self, path_idx: u32) -> anyhow::Result<()> {
        let reverse = self
            .idx_to_sorted
            .get_mut(path_idx as usize)
            .ok_or_else(|| anyhow::anyhow!("owned_v7_source_corrupt: path index hole"))?;
        if *reverse != u32::MAX {
            anyhow::bail!("owned_v7_source_corrupt: duplicate preferred path index");
        }
        *reverse = u32::try_from(self.sorted_pos)
            .map_err(|_| anyhow::anyhow!("streaming_compaction_required: path count overflow"))?;
        Ok(())
    }

    fn finish_reverse(&self, path: &Path) -> anyhow::Result<()> {
        if self.idx_to_sorted.len() != self.sorted_pos || self.idx_to_sorted.contains(&u32::MAX) {
            anyhow::bail!("owned_v7_source_corrupt: path reverse index is not dense");
        }
        let mut writer = BufWriter::new(File::create(path)?);
        for sorted_pos in &self.idx_to_sorted {
            writer.write_all(&sorted_pos.to_le_bytes())?;
        }
        writer.flush()?;
        Ok(())
    }
}

fn read_optional_u32(reader: &mut dyn Read) -> anyhow::Result<Option<u32>> {
    let mut bytes = [0u8; 4];
    match reader.read(&mut bytes[..1])? {
        0 => Ok(None),
        1 => {
            reader.read_exact(&mut bytes[1..])?;
            Ok(Some(u32::from_le_bytes(bytes)))
        }
        _ => unreachable!("one-byte read returned more than one byte"),
    }
}

fn read_required_u32(reader: &mut dyn Read) -> anyhow::Result<u32> {
    let mut bytes = [0u8; 4];
    reader.read_exact(&mut bytes)?;
    Ok(u32::from_le_bytes(bytes))
}
