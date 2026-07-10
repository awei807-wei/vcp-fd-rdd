use std::collections::{BTreeMap, BTreeSet};
use std::io::Write;

use roaring::RoaringBitmap;

use crate::storage::snapshot_v7::{write_parent_posting, write_trigram_posting};

use super::validate::{decode_posting, for_each_parent_record, for_each_trigram_record};

struct TrigramRecord<'a> {
    key: [u8; 3],
    posting: &'a [u8],
    raw: &'a [u8],
}

struct TrigramRecordIter<'a> {
    bytes: &'a [u8],
    remaining: usize,
    offset: usize,
}

impl<'a> TrigramRecordIter<'a> {
    fn new(bytes: &'a [u8]) -> anyhow::Result<Self> {
        let count = bytes
            .get(0..4)
            .ok_or_else(|| anyhow::anyhow!("direct_v7_corrupt: trigram header truncated"))?;
        Ok(Self {
            bytes,
            remaining: u32::from_le_bytes(count.try_into()?) as usize,
            offset: 4,
        })
    }

    fn next_record(&mut self) -> anyhow::Result<Option<TrigramRecord<'a>>> {
        if self.remaining == 0 {
            if self.offset != self.bytes.len() {
                anyhow::bail!("direct_v7_corrupt: trigram trailing bytes");
            }
            return Ok(None);
        }
        let start = self.offset;
        let header = self
            .bytes
            .get(start..start + 8)
            .ok_or_else(|| anyhow::anyhow!("direct_v7_corrupt: trigram record truncated"))?;
        let posting_len = u32::from_le_bytes(header[4..8].try_into()?) as usize;
        let posting_start = start + 8;
        let end = posting_start
            .checked_add(posting_len)
            .ok_or_else(|| anyhow::anyhow!("direct_v7_corrupt: trigram posting overflow"))?;
        let posting = self
            .bytes
            .get(posting_start..end)
            .ok_or_else(|| anyhow::anyhow!("direct_v7_corrupt: trigram posting truncated"))?;
        self.offset = end;
        self.remaining -= 1;
        Ok(Some(TrigramRecord {
            key: [header[0], header[1], header[2]],
            posting,
            raw: &self.bytes[start..end],
        }))
    }
}

pub(super) fn write_merged_trigram_segment(
    sink: &mut dyn Write,
    old_segment: &[u8],
    delta: &BTreeMap<[u8; 3], RoaringBitmap>,
) -> anyhow::Result<()> {
    let mut matched = 0usize;
    for_each_trigram_record(old_segment, |key, _, _| {
        matched += usize::from(delta.contains_key(&key));
        Ok(())
    })?;
    let old_count = u32::from_le_bytes(old_segment[0..4].try_into()?) as usize;
    let total = old_count + delta.len() - matched;
    sink.write_all(&(total as u32).to_le_bytes())?;

    let mut records = TrigramRecordIter::new(old_segment)?;
    let mut old = records.next_record()?;
    for (key, posting) in delta {
        while old.as_ref().is_some_and(|record| record.key < *key) {
            let record = old.take().expect("checked above");
            sink.write_all(record.raw)?;
            old = records.next_record()?;
        }
        if old.as_ref().is_some_and(|record| record.key == *key) {
            let record = old.take().expect("checked above");
            let mut merged = decode_posting(record.posting, "trigram")?;
            merged |= posting;
            write_trigram_posting(sink, *key, &merged)?;
            old = records.next_record()?;
        } else {
            write_trigram_posting(sink, *key, posting)?;
        }
    }
    while let Some(record) = old {
        sink.write_all(record.raw)?;
        old = records.next_record()?;
    }
    Ok(())
}

pub(super) fn write_merged_parent_segment(
    sink: &mut dyn Write,
    old_segment: &[u8],
    delta: &BTreeMap<u32, RoaringBitmap>,
) -> anyhow::Result<()> {
    let mut consumed = BTreeSet::new();
    for_each_parent_record(old_segment, |path_idx, _, _| {
        if delta.contains_key(&path_idx) {
            consumed.insert(path_idx);
        }
        Ok(())
    })?;
    let old_count = u32::from_le_bytes(old_segment[0..4].try_into()?) as usize;
    sink.write_all(&((old_count + delta.len() - consumed.len()) as u32).to_le_bytes())?;
    for_each_parent_record(old_segment, |path_idx, posting_bytes, raw_range| {
        if let Some(additions) = delta.get(&path_idx) {
            let mut merged = decode_posting(posting_bytes, "parent")?;
            merged |= additions;
            write_parent_posting(sink, path_idx, &merged)
        } else {
            sink.write_all(&old_segment[raw_range])?;
            Ok(())
        }
    })?;
    for (path_idx, posting) in delta {
        if !consumed.contains(path_idx) {
            write_parent_posting(sink, *path_idx, posting)?;
        }
    }
    sink.write_all(&0u32.to_le_bytes())?;
    Ok(())
}
