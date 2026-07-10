//! PathTable v2: front-encoding delta-compressed path storage.
//!
//! Paths are sorted lexicographically. Every 256th entry is an anchor storing
//! the full path. Non-anchor entries store only `shared_len` + `suffix`.
//! This achieves ~75% compression for typical deep directory trees.

use std::cmp::Ordering;

use crate::util::read_u32;

/// Index into the path table.
pub type PathIdx = u32;

/// A single slot in the path table, combining entry metadata with reverse index.
#[repr(C)]
#[derive(Clone, Debug)]
struct PathTableSlot {
    /// Byte offset into `suffix_bytes` where this entry's suffix starts.
    suffix_offset: u32,
    /// For anchor entries (every 256th) this is 0.
    /// For delta entries this is the shared prefix length with the previous entry.
    shared_len: u16,
    /// Length of the suffix stored in `suffix_bytes`.
    suffix_len: u16,
    /// Original PathIdx for this sorted position.
    orig_idx: u32,
}

/// Builder used to construct a `PathTableV2` from unsorted paths.
pub struct PathTableBuilder {
    paths: Vec<(PathIdx, Vec<u8>)>,
}

impl PathTableBuilder {
    pub fn new() -> Self {
        Self { paths: Vec::new() }
    }

    pub fn with_capacity(cap: usize) -> Self {
        Self {
            paths: Vec::with_capacity(cap),
        }
    }

    pub fn push(&mut self, idx: PathIdx, path: &[u8]) {
        self.paths.push((idx, path.to_vec()));
    }

    pub fn build(mut self) -> PathTableV2 {
        // Sort by path bytes, stable on idx so equal paths are deterministic.
        self.paths.sort_by(|a, b| a.1.cmp(&b.1));
        PathTableV2::from_sorted_paths(self.paths)
    }

    pub fn len(&self) -> usize {
        self.paths.len()
    }

    pub fn is_empty(&self) -> bool {
        self.paths.is_empty()
    }
}

impl Default for PathTableBuilder {
    fn default() -> Self {
        Self::new()
    }
}

/// Fixed-size chunk of anchor indices.
const ANCHOR_INTERVAL: usize = 256;

/// Magic for the current raw layout (4×u32 header + 12-byte slots carrying
/// `orig_idx`, then suffix bytes, then `idx_to_sorted`).
const RAW_MAGIC_V2: &[u8; 8] = b"PTV2rw2\0";
/// Magic carried by snapshots written before the magic was bumped. Two distinct
/// on-disk layouts share it: the pre-slot-merge layout (5×u32 header + 8-byte
/// entries + anchors + idx_to_sorted + sorted_to_idx) and the interim layout
/// that reused the magic while already using the slot layout above. Both are
/// decoded here and re-emitted as `RAW_MAGIC_V2` on the next snapshot.
const RAW_MAGIC_LEGACY: &[u8; 8] = b"PTV2raw\0";
/// Header size of the current layout: 8-byte magic + 4×u32.
const RAW_V2_HEADER_SIZE: usize = 8 + 4 * 4;
/// Header size of the pre-slot-merge legacy layout: 8-byte magic + 5×u32.
const RAW_LEGACY_HEADER_SIZE: usize = 8 + 5 * 4;

/// Delta-compressed path table.
#[derive(Clone, Debug, Default)]
pub struct PathTableV2 {
    /// Slots in sorted order (entry metadata + original PathIdx).
    slots: Vec<PathTableSlot>,
    /// All suffix bytes concatenated (including full paths for anchors).
    suffix_bytes: Vec<u8>,
    /// Map from original PathIdx -> position in sorted `slots`.
    idx_to_sorted: Vec<u32>,
}

impl PathTableV2 {
    fn from_sorted_paths(sorted: Vec<(PathIdx, Vec<u8>)>) -> Self {
        let n = sorted.len();
        let mut slots = Vec::with_capacity(n);
        let mut suffix_bytes = Vec::new();
        let max_orig_idx = sorted.iter().map(|(idx, _)| *idx).max().unwrap_or(0);
        let mut idx_to_sorted = vec![0u32; max_orig_idx as usize + 1];

        let mut prev_path: Vec<u8> = Vec::new();

        for (sorted_pos, (orig_idx, path)) in sorted.into_iter().enumerate() {
            let sorted_pos_u32 = sorted_pos as u32;
            idx_to_sorted[orig_idx as usize] = sorted_pos_u32;

            let is_anchor = sorted_pos % ANCHOR_INTERVAL == 0;
            let shared_len = if is_anchor {
                0usize
            } else {
                common_prefix_len(&prev_path, &path)
            };
            let suffix = &path[shared_len..];

            let suffix_offset = suffix_bytes.len() as u32;
            suffix_bytes.extend_from_slice(suffix);

            slots.push(PathTableSlot {
                suffix_offset,
                shared_len: shared_len as u16,
                suffix_len: suffix.len() as u16,
                orig_idx,
            });

            prev_path = path;
        }

        slots.shrink_to_fit();
        suffix_bytes.shrink_to_fit();
        idx_to_sorted.shrink_to_fit();

        Self {
            slots,
            suffix_bytes,
            idx_to_sorted,
        }
    }

    /// Build from an iterator of `(PathIdx, path_bytes)`.
    pub fn from_path_iter<I>(iter: I) -> Self
    where
        I: IntoIterator<Item = (PathIdx, Vec<u8>)>,
    {
        let mut paths: Vec<(PathIdx, Vec<u8>)> = iter.into_iter().collect();
        paths.sort_by(|a, b| a.1.cmp(&b.1));
        Self::from_sorted_paths(paths)
    }

    /// Number of stored paths.
    pub fn len(&self) -> usize {
        self.slots.len()
    }

    pub fn is_empty(&self) -> bool {
        self.slots.is_empty()
    }

    /// Total bytes occupied by this structure (approximate).
    pub fn allocated_bytes(&self) -> usize {
        self.slots.capacity() * std::mem::size_of::<PathTableSlot>()
            + self.suffix_bytes.capacity()
            + self.idx_to_sorted.capacity() * std::mem::size_of::<u32>()
    }

    pub fn encode_raw(&self) -> Vec<u8> {
        let mut out = Vec::with_capacity(RAW_V2_HEADER_SIZE + self.allocated_bytes());
        self.encode_raw_to_writer(&mut out)
            .expect("writing a path table to Vec cannot fail");
        out
    }

    /// Stream the current raw layout without allocating a second full encoded
    /// path-table buffer.
    pub fn encode_raw_to_writer(
        &self,
        writer: &mut (impl std::io::Write + ?Sized),
    ) -> std::io::Result<()> {
        writer.write_all(RAW_MAGIC_V2)?;
        writer.write_all(&(self.slots.len() as u32).to_le_bytes())?;
        writer.write_all(&(self.suffix_bytes.len() as u32).to_le_bytes())?;
        writer.write_all(&(self.idx_to_sorted.len() as u32).to_le_bytes())?;
        // Padding for the old anchors_len field; kept for header-size stability.
        writer.write_all(&0u32.to_le_bytes())?;
        for slot in &self.slots {
            writer.write_all(&slot.suffix_offset.to_le_bytes())?;
            writer.write_all(&slot.shared_len.to_le_bytes())?;
            writer.write_all(&slot.suffix_len.to_le_bytes())?;
            writer.write_all(&slot.orig_idx.to_le_bytes())?;
        }
        writer.write_all(&self.suffix_bytes)?;
        for v in &self.idx_to_sorted {
            writer.write_all(&v.to_le_bytes())?;
        }
        Ok(())
    }

    /// Decode a raw path table, transparently handling the current layout and
    /// the two legacy layouts that share `RAW_MAGIC_LEGACY`. Any truncated,
    /// out-of-bounds, or inconsistent input returns `None` (never panics).
    pub fn decode_raw(bytes: &[u8]) -> Option<Self> {
        if bytes.len() < 8 {
            return None;
        }
        let magic = &bytes[..8];
        if magic == RAW_MAGIC_V2.as_slice() {
            return Self::decode_raw_v2(bytes);
        }
        if magic != RAW_MAGIC_LEGACY.as_slice() {
            return None;
        }
        // `RAW_MAGIC_LEGACY` is ambiguous between two layouts; the encoded byte
        // total each header implies is the discriminator. Path-table segments
        // are written without trailing slack, so an exact match is reliable.
        // Prefer the pre-slot-merge layout (the real on-disk legacy format),
        // then fall back to the interim slot layout that reused the magic.
        if raw_legacy_total(bytes) == Some(bytes.len()) {
            if let Some(table) = Self::decode_raw_legacy(bytes) {
                return Some(table);
            }
        }
        if raw_v2_total(bytes) == Some(bytes.len()) {
            if let Some(table) = Self::decode_raw_v2(bytes) {
                return Some(table);
            }
        }
        None
    }

    /// Decode the current layout. Magic is assumed already matched by the caller.
    fn decode_raw_v2(bytes: &[u8]) -> Option<Self> {
        if bytes.len() < RAW_V2_HEADER_SIZE {
            return None;
        }
        let mut off = 8usize;
        let slots_len = read_u32(bytes, &mut off)? as usize;
        let suffix_len = read_u32(bytes, &mut off)? as usize;
        let idx_len = read_u32(bytes, &mut off)? as usize;
        // Skip the old anchors_len field (4 bytes) for header-size stability.
        let _anchors_len = read_u32(bytes, &mut off)? as usize;

        let slots_bytes = slots_len.checked_mul(12)?;
        let suffix_end = off.checked_add(slots_bytes)?.checked_add(suffix_len)?;
        let idx_bytes = idx_len.checked_mul(4)?;
        let total = suffix_end.checked_add(idx_bytes)?;
        if total > bytes.len() {
            return None;
        }

        let mut slots = Vec::with_capacity(slots_len);
        for _ in 0..slots_len {
            let suffix_offset = read_u32(bytes, &mut off)?;
            let shared_len = u16::from_le_bytes(bytes.get(off..off + 2)?.try_into().ok()?);
            off += 2;
            let suffix_len = u16::from_le_bytes(bytes.get(off..off + 2)?.try_into().ok()?);
            off += 2;
            let orig_idx = read_u32(bytes, &mut off)?;
            slots.push(PathTableSlot {
                suffix_offset,
                shared_len,
                suffix_len,
                orig_idx,
            });
        }

        let suffix_bytes = bytes.get(off..off + suffix_len)?.to_vec();
        off += suffix_len;

        let mut idx_to_sorted = Vec::with_capacity(idx_len);
        for _ in 0..idx_len {
            idx_to_sorted.push(u32::from_le_bytes(
                bytes.get(off..off + 4)?.try_into().ok()?,
            ));
            off += 4;
        }

        Some(Self {
            slots,
            suffix_bytes,
            idx_to_sorted,
        })
    }

    /// Decode the pre-slot-merge legacy layout by reconstructing every
    /// `(orig_idx, full_path)` pair and rebuilding through the current code
    /// path, so downstream resolve/lookup/encode all use the new layout and the
    /// next snapshot migrates the file forward.
    fn decode_raw_legacy(bytes: &[u8]) -> Option<Self> {
        if bytes.len() < RAW_LEGACY_HEADER_SIZE {
            return None;
        }
        let mut off = 8usize;
        let entries_len = read_u32(bytes, &mut off)? as usize;
        let suffix_len = read_u32(bytes, &mut off)? as usize;
        let anchors_len = read_u32(bytes, &mut off)? as usize;
        let idx_len = read_u32(bytes, &mut off)? as usize;
        let sorted_len = read_u32(bytes, &mut off)? as usize;

        // One original index per sorted entry is required to rebuild the table.
        if sorted_len != entries_len {
            return None;
        }

        let entries_start = off;
        let entries_bytes = entries_len.checked_mul(8)?;
        let suffix_start = entries_start.checked_add(entries_bytes)?;
        let anchors_start = suffix_start.checked_add(suffix_len)?;
        let idx_start = anchors_start.checked_add(anchors_len.checked_mul(4)?)?;
        let sorted_start = idx_start.checked_add(idx_len.checked_mul(4)?)?;
        let total = sorted_start.checked_add(sorted_len.checked_mul(4)?)?;
        if total > bytes.len() {
            return None;
        }

        let suffix_section = bytes.get(suffix_start..suffix_start + suffix_len)?;

        // Sequentially front-decode each sorted path (anchor every interval) and
        // pair it with its original index from the sorted_to_idx table.
        let mut paths: Vec<(PathIdx, Vec<u8>)> = Vec::with_capacity(entries_len);
        let mut current: Vec<u8> = Vec::new();
        for pos in 0..entries_len {
            let e_off = entries_start + pos * 8;
            let suffix_offset =
                u32::from_le_bytes(bytes.get(e_off..e_off + 4)?.try_into().ok()?) as usize;
            let shared_len =
                u16::from_le_bytes(bytes.get(e_off + 4..e_off + 6)?.try_into().ok()?) as usize;
            let s_len =
                u16::from_le_bytes(bytes.get(e_off + 6..e_off + 8)?.try_into().ok()?) as usize;
            let suffix = suffix_section.get(suffix_offset..suffix_offset.checked_add(s_len)?)?;

            if pos % ANCHOR_INTERVAL == 0 {
                current.clear();
            } else {
                if shared_len > current.len() {
                    return None;
                }
                current.truncate(shared_len);
            }
            current.extend_from_slice(suffix);

            let sorted_off = sorted_start + pos * 4;
            let orig_idx =
                u32::from_le_bytes(bytes.get(sorted_off..sorted_off + 4)?.try_into().ok()?);
            if orig_idx as usize >= idx_len {
                return None;
            }
            paths.push((orig_idx, current.clone()));
        }

        Some(Self::from_path_iter(paths))
    }

    fn get_suffix(&self, pos: usize) -> &[u8] {
        let e = &self.slots[pos];
        &self.suffix_bytes
            [e.suffix_offset as usize..(e.suffix_offset as usize + e.suffix_len as usize)]
    }

    /// Resolve a sorted position to the full path bytes into a caller-owned buffer.
    fn resolve_sorted_into(&self, sorted_pos: usize, out: &mut Vec<u8>) {
        let anchor_pos = (sorted_pos / ANCHOR_INTERVAL) * ANCHOR_INTERVAL;
        out.clear();
        out.extend_from_slice(self.get_suffix(anchor_pos));
        for k in (anchor_pos + 1)..=sorted_pos {
            let e = &self.slots[k];
            out.truncate(e.shared_len as usize);
            out.extend_from_slice(self.get_suffix(k));
        }
    }

    /// Resolve a sorted position to the full path bytes.
    fn resolve_sorted(&self, sorted_pos: usize) -> Vec<u8> {
        let mut path = Vec::new();
        self.resolve_sorted_into(sorted_pos, &mut path);
        path
    }

    /// Resolve a `PathIdx` to the full path bytes.
    pub fn resolve(&self, idx: PathIdx) -> Option<Vec<u8>> {
        let sorted_pos = *self.idx_to_sorted.get(idx as usize)? as usize;
        Some(self.resolve_sorted(sorted_pos))
    }

    /// Resolve a `PathIdx` into a caller-owned buffer.
    pub fn resolve_into(&self, idx: PathIdx, out: &mut Vec<u8>) -> Option<()> {
        let sorted_pos = *self.idx_to_sorted.get(idx as usize)? as usize;
        self.resolve_sorted_into(sorted_pos, out);
        Some(())
    }

    /// Find the parent directory index for the entry at original index.
    pub fn parent_idx(&self, idx: PathIdx) -> Option<PathIdx> {
        let sorted_pos = *self.idx_to_sorted.get(idx as usize)? as usize;
        let path = self.resolve_sorted(sorted_pos);
        if path.as_slice() == b"/" {
            return None;
        }
        // Find the last '/' before the end.
        let parent_len = match path.iter().rposition(|&b| b == b'/') {
            Some(0) => 1, // root "/"
            Some(pos) => pos,
            None => return None, // no parent
        };
        let parent_path = &path[..parent_len];
        self.find_exact(parent_path)
            .map(|sorted| self.slots[sorted].orig_idx)
    }

    /// Lookup a path by its bytes, returning the original `PathIdx`.
    pub fn lookup(&self, target: &[u8]) -> Option<PathIdx> {
        self.find_exact(target)
            .map(|sorted| self.slots[sorted].orig_idx)
    }

    /// Find the exact path by binary search, returning its sorted position.
    fn find_exact(&self, target: &[u8]) -> Option<usize> {
        let mut left = 0usize;
        let mut right = self.slots.len();
        while left < right {
            let mid = (left + right) / 2;
            let mid_path = self.resolve_sorted(mid);
            match mid_path.as_slice().cmp(target) {
                Ordering::Less => left = mid + 1,
                Ordering::Greater => right = mid,
                Ordering::Equal => return Some(mid),
            }
        }
        None
    }

    /// Find the range of entries whose paths start with `prefix`.
    /// Returns `(start_sorted, end_sorted)` where `end_sorted` is exclusive.
    pub fn find_prefix_range(&self, prefix: &[u8]) -> Option<(usize, usize)> {
        if self.slots.is_empty() {
            return None;
        }

        // Binary search for the first entry >= prefix.
        let mut left = 0usize;
        let mut right = self.slots.len();
        while left < right {
            let mid = (left + right) / 2;
            let mid_path = self.resolve_sorted(mid);
            if mid_path.as_slice() < prefix {
                left = mid + 1;
            } else {
                right = mid;
            }
        }
        let start = left;
        if start >= self.slots.len() {
            return None;
        }
        let start_path = self.resolve_sorted(start);
        if !start_path.starts_with(prefix) {
            return None;
        }

        // Find the upper bound: first entry that does NOT start with prefix.
        let mut left = start;
        let mut right = self.slots.len();
        while left < right {
            let mid = (left + right) / 2;
            let mid_path = self.resolve_sorted(mid);
            if mid_path.starts_with(prefix) {
                left = mid + 1;
            } else {
                right = mid;
            }
        }
        let end = left;
        Some((start, end))
    }
}

fn common_prefix_len(a: &[u8], b: &[u8]) -> usize {
    a.iter().zip(b.iter()).take_while(|(x, y)| x == y).count()
}

/// Encoded byte total implied by the current-layout header, or `None` on
/// overflow / too-short input.
fn raw_v2_total(bytes: &[u8]) -> Option<usize> {
    if bytes.len() < RAW_V2_HEADER_SIZE {
        return None;
    }
    let mut off = 8usize;
    let slots_len = read_u32(bytes, &mut off)? as usize;
    let suffix_len = read_u32(bytes, &mut off)? as usize;
    let idx_len = read_u32(bytes, &mut off)? as usize;
    RAW_V2_HEADER_SIZE
        .checked_add(slots_len.checked_mul(12)?)?
        .checked_add(suffix_len)?
        .checked_add(idx_len.checked_mul(4)?)
}

/// Encoded byte total implied by the pre-slot-merge legacy header, or `None` on
/// overflow / too-short input.
fn raw_legacy_total(bytes: &[u8]) -> Option<usize> {
    if bytes.len() < RAW_LEGACY_HEADER_SIZE {
        return None;
    }
    let mut off = 8usize;
    let entries_len = read_u32(bytes, &mut off)? as usize;
    let suffix_len = read_u32(bytes, &mut off)? as usize;
    let anchors_len = read_u32(bytes, &mut off)? as usize;
    let idx_len = read_u32(bytes, &mut off)? as usize;
    let sorted_len = read_u32(bytes, &mut off)? as usize;
    RAW_LEGACY_HEADER_SIZE
        .checked_add(entries_len.checked_mul(8)?)?
        .checked_add(suffix_len)?
        .checked_add(anchors_len.checked_mul(4)?)?
        .checked_add(idx_len.checked_mul(4)?)?
        .checked_add(sorted_len.checked_mul(4)?)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_table(paths: &[&str]) -> PathTableV2 {
        let mut builder = PathTableBuilder::new();
        for (i, p) in paths.iter().enumerate() {
            builder.push(i as PathIdx, p.as_bytes());
        }
        builder.build()
    }

    #[test]
    fn test_basic_resolve() {
        let paths = vec![
            "/home/user/a.txt",
            "/home/user/b.txt",
            "/home/user/c.txt",
            "/var/log/syslog",
        ];
        let table = make_table(&paths);
        assert_eq!(table.len(), 4);

        for (i, expected) in paths.iter().enumerate() {
            let resolved = table.resolve(i as PathIdx).unwrap();
            assert_eq!(std::str::from_utf8(&resolved).unwrap(), *expected);
        }
    }

    #[test]
    fn test_anchor_interval() {
        let mut paths: Vec<String> = (0..600)
            .map(|i| format!("/home/user/dir{}/file{}.txt", i % 10, i))
            .collect();
        paths.sort();
        let mut builder = PathTableBuilder::with_capacity(paths.len());
        for (i, p) in paths.iter().enumerate() {
            builder.push(i as PathIdx, p.as_bytes());
        }
        let table = builder.build();
        assert_eq!(table.len(), 600);
        assert_eq!(table.slots.len(), 600);

        for (i, expected) in paths.iter().enumerate() {
            let resolved = table.resolve(i as PathIdx).unwrap();
            assert_eq!(std::str::from_utf8(&resolved).unwrap(), expected.as_str());
        }
    }

    #[test]
    fn test_parent_idx() {
        let paths = vec!["/home/user/a.txt", "/home/user/b.txt", "/home"];
        let table = make_table(&paths);
        // Original 0 -> /home/user/a.txt -> parent /home/user (not in table)
        // Original 2 -> /home -> no parent (or root)
        // Original 1 -> /home/user/b.txt -> parent /home/user (not in table)
        assert!(table.parent_idx(0).is_none());
        assert!(table.parent_idx(1).is_none());
        assert!(table.parent_idx(2).is_none());
    }

    #[test]
    fn test_parent_idx_found() {
        let paths = vec!["/a", "/a/b", "/a/b/c"];
        let table = make_table(&paths);
        // sorted order: /a, /a/b, /a/b/c
        // original 0 (/a) -> parent None (or /)
        // original 1 (/a/b) -> parent /a -> original 0
        // original 2 (/a/b/c) -> parent /a/b -> original 1
        assert!(table.parent_idx(0).is_none());
        assert_eq!(table.parent_idx(1), Some(0));
        assert_eq!(table.parent_idx(2), Some(1));
    }

    #[test]
    fn test_find_prefix_range() {
        let paths = vec![
            "/home/user/a.txt",
            "/home/user/b.txt",
            "/home/user/c.txt",
            "/var/log/a.log",
            "/var/log/b.log",
        ];
        let table = make_table(&paths);
        let (start, end) = table.find_prefix_range(b"/home/user/").unwrap();
        assert_eq!(end - start, 3);

        let (start, end) = table.find_prefix_range(b"/var/log/").unwrap();
        assert_eq!(end - start, 2);

        assert!(table.find_prefix_range(b"/nonexistent/").is_none());
    }

    #[test]
    fn raw_roundtrip_preserves_lookup_and_resolution() {
        let paths = vec![
            "/home/user/a.txt",
            "/home/user/project/src/main.rs",
            "/home/user/project/src/lib.rs",
            "/var/log/syslog",
        ];
        let table = make_table(&paths);
        let encoded = table.encode_raw();
        // The current layout must be written with the bumped magic.
        assert_eq!(&encoded[..8], RAW_MAGIC_V2.as_slice());
        let decoded = PathTableV2::decode_raw(&encoded).expect("raw decode");

        assert_eq!(decoded.len(), table.len());
        for (idx, expected) in paths.iter().enumerate() {
            let resolved = decoded.resolve(idx as PathIdx).unwrap();
            assert_eq!(std::str::from_utf8(&resolved).unwrap(), *expected);
            assert_eq!(decoded.lookup(expected.as_bytes()), Some(idx as PathIdx));
        }
    }

    #[test]
    fn raw_stream_encoding_matches_vec_encoding() {
        let table = make_table(&["/tmp/a", "/tmp/deep/b", "/var/log/c"]);
        let expected = table.encode_raw();
        let mut streamed = Vec::new();
        table.encode_raw_to_writer(&mut streamed).unwrap();
        assert_eq!(streamed, expected);
    }

    /// Encode `paths` (idx == position in the slice) in the pre-slot-merge
    /// legacy on-disk layout: `MAGIC + 5×u32 header + 8-byte entries + suffix +
    /// anchors + idx_to_sorted + sorted_to_idx`. Mirrors the encoder that wrote
    /// the real `stable.v7` files this compat path must read.
    fn encode_legacy_raw(paths: &[&str]) -> Vec<u8> {
        let mut indexed: Vec<(u32, Vec<u8>)> = paths
            .iter()
            .enumerate()
            .map(|(i, p)| (i as u32, p.as_bytes().to_vec()))
            .collect();
        indexed.sort_by(|a, b| a.1.cmp(&b.1));

        let n = indexed.len();
        let max_orig = indexed.iter().map(|(i, _)| *i).max().unwrap_or(0) as usize;
        let mut idx_to_sorted = vec![0u32; max_orig + 1];
        let mut sorted_to_idx = Vec::with_capacity(n);
        let mut anchors: Vec<u32> = Vec::new();
        let mut suffix_bytes: Vec<u8> = Vec::new();
        // entry = (suffix_offset, shared_len, suffix_len)
        let mut entries: Vec<(u32, u16, u16)> = Vec::with_capacity(n);

        let mut prev: Vec<u8> = Vec::new();
        for (sorted_pos, (orig_idx, path)) in indexed.iter().enumerate() {
            idx_to_sorted[*orig_idx as usize] = sorted_pos as u32;
            sorted_to_idx.push(*orig_idx);
            let is_anchor = sorted_pos % ANCHOR_INTERVAL == 0;
            let shared_len = if is_anchor {
                0
            } else {
                common_prefix_len(&prev, path)
            };
            let suffix = &path[shared_len..];
            let suffix_offset = suffix_bytes.len() as u32;
            suffix_bytes.extend_from_slice(suffix);
            if is_anchor {
                anchors.push(sorted_pos as u32);
            }
            entries.push((suffix_offset, shared_len as u16, suffix.len() as u16));
            prev = path.clone();
        }

        let mut out = Vec::new();
        out.extend_from_slice(RAW_MAGIC_LEGACY);
        out.extend_from_slice(&(entries.len() as u32).to_le_bytes());
        out.extend_from_slice(&(suffix_bytes.len() as u32).to_le_bytes());
        out.extend_from_slice(&(anchors.len() as u32).to_le_bytes());
        out.extend_from_slice(&(idx_to_sorted.len() as u32).to_le_bytes());
        out.extend_from_slice(&(sorted_to_idx.len() as u32).to_le_bytes());
        for (suffix_offset, shared_len, suffix_len) in &entries {
            out.extend_from_slice(&suffix_offset.to_le_bytes());
            out.extend_from_slice(&shared_len.to_le_bytes());
            out.extend_from_slice(&suffix_len.to_le_bytes());
        }
        out.extend_from_slice(&suffix_bytes);
        for v in &anchors {
            out.extend_from_slice(&v.to_le_bytes());
        }
        for v in &idx_to_sorted {
            out.extend_from_slice(&v.to_le_bytes());
        }
        for v in &sorted_to_idx {
            out.extend_from_slice(&v.to_le_bytes());
        }
        out
    }

    #[test]
    fn legacy_raw_decodes_and_resolves_every_path() {
        // Span more than one anchor interval so the front-decode walk is exercised.
        let mut owned: Vec<String> = (0..700)
            .map(|i| {
                format!(
                    "/home/user/project{}/src/dir{}/file{:05}.rs",
                    i % 7,
                    i % 13,
                    i
                )
            })
            .collect();
        owned.push("/var/log/syslog".to_string());
        owned.push("/".to_string());
        let paths: Vec<&str> = owned.iter().map(|s| s.as_str()).collect();

        let legacy_bytes = encode_legacy_raw(&paths);
        assert_eq!(&legacy_bytes[..8], RAW_MAGIC_LEGACY.as_slice());

        let decoded = PathTableV2::decode_raw(&legacy_bytes).expect("legacy decode");
        assert_eq!(decoded.len(), paths.len());
        for (idx, expected) in paths.iter().enumerate() {
            let resolved = decoded.resolve(idx as PathIdx).expect("resolve");
            assert_eq!(std::str::from_utf8(&resolved).unwrap(), *expected);
            assert_eq!(decoded.lookup(expected.as_bytes()), Some(idx as PathIdx));
        }

        // Re-encoding migrates the table to the current magic, decodable again.
        let reencoded = decoded.encode_raw();
        assert_eq!(&reencoded[..8], RAW_MAGIC_V2.as_slice());
        let migrated = PathTableV2::decode_raw(&reencoded).expect("migrated decode");
        for (idx, expected) in paths.iter().enumerate() {
            assert_eq!(
                std::str::from_utf8(&migrated.resolve(idx as PathIdx).unwrap()).unwrap(),
                *expected
            );
        }
    }

    #[test]
    fn corrupt_and_truncated_inputs_return_none_without_panic() {
        // Unknown magic.
        assert!(PathTableV2::decode_raw(b"BOGUS\0\0\0and some bytes").is_none());
        // Too short to even hold a magic.
        assert!(PathTableV2::decode_raw(b"PTV").is_none());

        // Current-layout header claiming far more slots than bytes present.
        let mut v2 = Vec::new();
        v2.extend_from_slice(RAW_MAGIC_V2);
        v2.extend_from_slice(&1_000_000u32.to_le_bytes()); // slots_len
        v2.extend_from_slice(&0u32.to_le_bytes()); // suffix_len
        v2.extend_from_slice(&0u32.to_le_bytes()); // idx_len
        v2.extend_from_slice(&0u32.to_le_bytes()); // padding
        assert!(PathTableV2::decode_raw(&v2).is_none());

        // A valid legacy buffer truncated mid-body must not panic or misdecode.
        let legacy = encode_legacy_raw(&["/a/b", "/a/b/c", "/a/d"]);
        for cut in [9usize, RAW_LEGACY_HEADER_SIZE, legacy.len() - 1] {
            assert!(
                PathTableV2::decode_raw(&legacy[..cut]).is_none(),
                "truncation at {cut} should reject"
            );
        }

        // Legacy header with overflowing length fields must return None.
        let mut overflow = Vec::new();
        overflow.extend_from_slice(RAW_MAGIC_LEGACY);
        for _ in 0..5 {
            overflow.extend_from_slice(&u32::MAX.to_le_bytes());
        }
        assert!(PathTableV2::decode_raw(&overflow).is_none());
    }

    #[test]
    fn path_table_compression_ratio() {
        // Generate 100_000 realistic paths with deep shared prefixes.
        let mut paths: Vec<String> = Vec::with_capacity(100_000);
        for d1 in 0..10 {
            for d2 in 0..10 {
                for d3 in 0..10 {
                    for f in 0..100 {
                        paths.push(format!(
                            "/home/user/project{}/src/module{}/sub{}/file{:04}.rs",
                            d1, d2, d3, f
                        ));
                    }
                }
            }
        }
        paths.sort();

        let mut builder = PathTableBuilder::with_capacity(paths.len());
        for (i, p) in paths.iter().enumerate() {
            builder.push(i as PathIdx, p.as_bytes());
        }
        let table = builder.build();

        // Compute naive storage size (sum of all path bytes + Vec overhead ~24B each).
        let naive_bytes: usize = paths.iter().map(|p| p.len() + 24).sum();
        let compressed = table.allocated_bytes();
        let ratio = compressed as f64 / naive_bytes as f64;
        assert!(
            ratio < 0.40,
            "compression ratio should be < 40%, got {:.1}%",
            ratio * 100.0
        );
    }
}
