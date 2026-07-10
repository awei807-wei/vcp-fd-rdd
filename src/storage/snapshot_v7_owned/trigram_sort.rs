use std::cmp::Ordering;
use std::collections::BinaryHeap;
use std::fs::File;
use std::io::{BufReader, BufWriter, Read, Write};
use std::path::{Path, PathBuf};

use roaring::RoaringBitmap;

use super::run_builder::TrigramFact;
use super::OwnedWorkspace;
use crate::storage::snapshot_v7::{
    validate_posting_docids, write_trigram_posting, TRIGRAM_SENTINEL,
};

struct TrigramRunReader {
    reader: BufReader<File>,
}

impl TrigramRunReader {
    fn open(path: &Path) -> anyhow::Result<Self> {
        Ok(Self {
            reader: BufReader::new(File::open(path)?),
        })
    }

    fn next_fact(&mut self) -> anyhow::Result<Option<TrigramFact>> {
        let mut trigram = [0u8; 3];
        match self.reader.read(&mut trigram[..1])? {
            0 => return Ok(None),
            1 => self.reader.read_exact(&mut trigram[1..])?,
            _ => unreachable!("one-byte read returned more than one byte"),
        }
        let docid = read_required_u32(&mut self.reader)?;
        Ok(Some(TrigramFact { trigram, docid }))
    }
}

#[derive(Eq, PartialEq)]
struct TrigramHeapItem {
    fact: TrigramFact,
    run: usize,
}

impl Ord for TrigramHeapItem {
    fn cmp(&self, other: &Self) -> Ordering {
        other
            .fact
            .cmp(&self.fact)
            .then_with(|| other.run.cmp(&self.run))
    }
}

impl PartialOrd for TrigramHeapItem {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

pub(super) struct MergedTrigramSpool {
    pub(super) records: PathBuf,
    pub(super) trigram_entries: usize,
}

pub(super) fn merge_trigram_runs(
    workspace: &OwnedWorkspace,
    run_paths: &[PathBuf],
    live_entries: usize,
) -> anyhow::Result<MergedTrigramSpool> {
    let mut readers: Vec<TrigramRunReader> = run_paths
        .iter()
        .map(|path| TrigramRunReader::open(path))
        .collect::<anyhow::Result<_>>()?;
    let mut heap = BinaryHeap::new();
    for (run, reader) in readers.iter_mut().enumerate() {
        if let Some(fact) = reader.next_fact()? {
            heap.push(TrigramHeapItem { fact, run });
        }
    }

    let records = workspace.path("trigram.records");
    let mut writer = BufWriter::new(File::create(&records)?);
    write_trigram_posting(&mut writer, TRIGRAM_SENTINEL, &RoaringBitmap::new())?;
    let mut trigram_entries = 1usize;

    while let Some(first) = heap.pop() {
        let trigram = first.fact.trigram;
        if trigram == TRIGRAM_SENTINEL {
            anyhow::bail!("owned_v7_source_corrupt: path generated reserved trigram sentinel");
        }
        let mut posting = RoaringBitmap::new();
        consume_trigram_fact(first, &mut readers, &mut heap, &mut posting)?;
        while heap.peek().is_some_and(|item| item.fact.trigram == trigram) {
            let item = heap.pop().expect("peeked above");
            consume_trigram_fact(item, &mut readers, &mut heap, &mut posting)?;
        }
        validate_posting_docids(&posting, live_entries, "trigram")?;
        write_trigram_posting(&mut writer, trigram, &posting)?;
        trigram_entries += 1;
    }
    writer.flush()?;
    if trigram_entries > u32::MAX as usize {
        anyhow::bail!("streaming_compaction_required: trigram key count overflow");
    }
    Ok(MergedTrigramSpool {
        records,
        trigram_entries,
    })
}

fn consume_trigram_fact(
    item: TrigramHeapItem,
    readers: &mut [TrigramRunReader],
    heap: &mut BinaryHeap<TrigramHeapItem>,
    posting: &mut RoaringBitmap,
) -> anyhow::Result<()> {
    posting.insert(item.fact.docid);
    if let Some(fact) = readers[item.run].next_fact()? {
        heap.push(TrigramHeapItem {
            fact,
            run: item.run,
        });
    }
    Ok(())
}

fn read_required_u32(reader: &mut dyn Read) -> anyhow::Result<u32> {
    let mut bytes = [0u8; 4];
    reader.read_exact(&mut bytes)?;
    Ok(u32::from_le_bytes(bytes))
}
