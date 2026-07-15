use std::io::{BufWriter, Seek, SeekFrom, Write};

use roaring::RoaringBitmap;

use crate::index::base_index::FileEntryIndex;
use crate::index::file_entry_v2::FileEntry;
use crate::index::parent_index::ParentIndex;
use crate::storage::checksum::Crc32c;
use crate::util::align_up;

use super::{
    compute_header_crc, encode_header_with_version, V7SegDesc, V7SegKind, V7Trailer, V7_HEADER_SIZE,
};

pub(super) const V7_SEGMENT_ORDER: [V7SegKind; 6] = [
    V7SegKind::PathTable,
    V7SegKind::EntriesByKey,
    V7SegKind::EntriesByPath,
    V7SegKind::TrigramIndex,
    V7SegKind::ParentIndex,
    V7SegKind::Tombstones,
];

struct SegmentChecksumWriter<'a> {
    file: &'a mut dyn Write,
    segment_crc: Crc32c,
    global_crc: &'a mut Crc32c,
    len: u64,
}

impl Write for SegmentChecksumWriter<'_> {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        let written = self.file.write(buf)?;
        let bytes = &buf[..written];
        self.segment_crc.update(bytes);
        self.global_crc.update(bytes);
        self.len = self.len.saturating_add(written as u64);
        Ok(written)
    }

    fn flush(&mut self) -> std::io::Result<()> {
        self.file.flush()
    }
}

/// Sequential v7 writer. Each producer finishes before the next segment starts,
/// so callers never need to retain all six encoded segments at once.
pub(super) struct V7StreamingWriter<'a> {
    file: &'a mut std::fs::File,
    version: u32,
    next_segment: usize,
    segment_descs: Vec<V7SegDesc>,
    global_crc: Crc32c,
    cursor: u64,
}

impl<'a> V7StreamingWriter<'a> {
    pub(super) fn new(file: &'a mut std::fs::File, version: u32) -> anyhow::Result<Self> {
        file.write_all(&encode_header_with_version(
            V7_SEGMENT_ORDER.len() as u32,
            0,
            version,
        ))?;
        Ok(Self {
            file,
            version,
            next_segment: 0,
            segment_descs: Vec::with_capacity(V7_SEGMENT_ORDER.len()),
            global_crc: Crc32c::new(),
            cursor: V7_HEADER_SIZE as u64,
        })
    }

    pub(super) fn write_segment<F>(&mut self, kind: V7SegKind, producer: F) -> anyhow::Result<()>
    where
        F: FnOnce(&mut dyn Write) -> anyhow::Result<()>,
    {
        let expected = V7_SEGMENT_ORDER.get(self.next_segment).copied();
        if expected != Some(kind) {
            anyhow::bail!(
                "v7 segment order mismatch: expected {:?}, got {:?}",
                expected,
                kind
            );
        }

        self.write_alignment_padding()?;
        let offset = self.cursor;
        let (len, crc32c) = {
            let mut buffered = BufWriter::with_capacity(64 * 1024, &mut *self.file);
            let mut sink = SegmentChecksumWriter {
                file: &mut buffered,
                segment_crc: Crc32c::new(),
                global_crc: &mut self.global_crc,
                len: 0,
            };
            producer(&mut sink)?;
            sink.flush()?;
            (sink.len, sink.segment_crc.finalize())
        };
        self.cursor = self.cursor.saturating_add(len);
        self.segment_descs.push(V7SegDesc {
            offset,
            len,
            crc32c,
        });
        self.next_segment += 1;
        Ok(())
    }

    pub(super) fn finish(mut self) -> anyhow::Result<()> {
        if self.next_segment != V7_SEGMENT_ORDER.len() {
            anyhow::bail!(
                "v7 streaming writer incomplete: wrote {} of {} segments",
                self.next_segment,
                V7_SEGMENT_ORDER.len()
            );
        }
        self.write_alignment_padding()?;
        let global_crc32c = std::mem::take(&mut self.global_crc).finalize();
        let trailer = V7Trailer {
            num_segments: self.segment_descs.len() as u32,
            global_crc32c,
            segment_offsets: self.segment_descs.iter().map(|desc| desc.offset).collect(),
            segment_lens: self.segment_descs.iter().map(|desc| desc.len).collect(),
            segment_crcs: self.segment_descs.iter().map(|desc| desc.crc32c).collect(),
        };
        self.file.write_all(&trailer.encode())?;

        let mut header =
            encode_header_with_version(self.segment_descs.len() as u32, 0, self.version);
        let header_crc = compute_header_crc(&header);
        header =
            encode_header_with_version(self.segment_descs.len() as u32, header_crc, self.version);
        self.file.seek(SeekFrom::Start(0))?;
        self.file.write_all(&header)?;
        Ok(())
    }

    fn write_alignment_padding(&mut self) -> anyhow::Result<()> {
        let aligned = align_up(self.cursor as usize, 8) as u64;
        let padding = aligned.saturating_sub(self.cursor) as usize;
        if padding > 0 {
            self.file.write_all(&[0u8; 8][..padding])?;
            self.cursor = aligned;
        }
        Ok(())
    }
}

pub(super) fn write_file_entry_index(
    sink: &mut dyn Write,
    entries: &FileEntryIndex,
) -> anyhow::Result<()> {
    sink.write_all(&(entries.len() as u32).to_le_bytes())?;
    for entry in entries.iter() {
        write_file_entry(sink, entry)?;
    }
    Ok(())
}

pub(super) fn write_file_entry(sink: &mut dyn Write, entry: &FileEntry) -> anyhow::Result<()> {
    sink.write_all(&entry.dev.to_le_bytes())?;
    sink.write_all(&entry.ino.to_le_bytes())?;
    sink.write_all(&entry.generation.to_le_bytes())?;
    sink.write_all(&entry.path_idx.to_le_bytes())?;
    sink.write_all(&entry.mtime_ns.to_le_bytes())?;
    Ok(())
}

pub(super) fn write_trigram_posting(
    sink: &mut dyn Write,
    trigram: [u8; 3],
    posting: &RoaringBitmap,
) -> anyhow::Result<()> {
    sink.write_all(&trigram)?;
    sink.write_all(&[0])?;
    sink.write_all(&(posting.serialized_size() as u32).to_le_bytes())?;
    posting.serialize_into(sink)?;
    Ok(())
}

pub(super) fn write_parent_index(sink: &mut dyn Write, index: &ParentIndex) -> anyhow::Result<()> {
    sink.write_all(&(index.dir_to_files.len() as u32).to_le_bytes())?;
    for (path_idx, docids) in &index.dir_to_files {
        let posting: RoaringBitmap = docids.iter().copied().collect();
        write_parent_posting(sink, *path_idx, &posting)?;
    }
    sink.write_all(&0u32.to_le_bytes())?;
    Ok(())
}

pub(super) fn write_parent_posting(
    sink: &mut dyn Write,
    path_idx: u32,
    posting: &RoaringBitmap,
) -> anyhow::Result<()> {
    sink.write_all(&path_idx.to_le_bytes())?;
    sink.write_all(&(posting.serialized_size() as u32).to_le_bytes())?;
    posting.serialize_into(sink)?;
    Ok(())
}
