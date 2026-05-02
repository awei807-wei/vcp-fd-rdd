//! PathTable v2: front-encoding delta-compressed path storage.
//!
//! Paths are sorted lexicographically. Every 256th entry is an anchor storing
//! the full path. Non-anchor entries store only `shared_len` + `suffix`.
//! This achieves ~75% compression for typical deep directory trees.

use std::cmp::Ordering;

/// Index into the path table.
pub type PathIdx = u32;

/// A single encoded entry in the path table.
#[derive(Clone, Debug)]
struct EncodedEntry {
    /// Byte offset into `suffix_bytes` where this entry's suffix starts.
    suffix_offset: u32,
    /// For anchor entries (every 256th) this is 0.
    /// For delta entries this is the shared prefix length with the previous entry.
    shared_len: u16,
    /// Length of the suffix stored in `suffix_bytes`.
    suffix_len: u16,
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

/// Delta-compressed path table.
#[derive(Clone, Debug, Default)]
pub struct PathTableV2 {
    /// Encoded entries in sorted order.
    entries: Vec<EncodedEntry>,
    /// All suffix bytes concatenated (including full paths for anchors).
    suffix_bytes: Vec<u8>,
    /// Anchor entry indices (every ANCHOR_INTERVAL entries).
    anchors: Vec<u32>,
    /// Map from original PathIdx -> position in sorted `entries`.
    idx_to_sorted: Vec<u32>,
    /// Map from sorted position -> original PathIdx.
    sorted_to_idx: Vec<u32>,
}

impl PathTableV2 {
    fn from_sorted_paths(sorted: Vec<(PathIdx, Vec<u8>)>) -> Self {
        let n = sorted.len();
        let mut entries = Vec::with_capacity(n);
        let mut suffix_bytes = Vec::new();
        let mut anchors = Vec::with_capacity(n / ANCHOR_INTERVAL + 1);
        let max_orig_idx = sorted.iter().map(|(idx, _)| *idx).max().unwrap_or(0);
        let mut idx_to_sorted = vec![0u32; max_orig_idx as usize + 1];
        let mut sorted_to_idx = Vec::with_capacity(n);

        let mut prev_path: Vec<u8> = Vec::new();

        for (sorted_pos, (orig_idx, path)) in sorted.into_iter().enumerate() {
            let sorted_pos_u32 = sorted_pos as u32;
            idx_to_sorted[orig_idx as usize] = sorted_pos_u32;
            sorted_to_idx.push(orig_idx);

            let is_anchor = sorted_pos % ANCHOR_INTERVAL == 0;
            let shared_len = if is_anchor {
                0usize
            } else {
                common_prefix_len(&prev_path, &path)
            };
            let suffix = &path[shared_len..];

            let suffix_offset = suffix_bytes.len() as u32;
            suffix_bytes.extend_from_slice(suffix);

            if is_anchor {
                anchors.push(sorted_pos_u32);
            }

            entries.push(EncodedEntry {
                shared_len: shared_len as u16,
                suffix_offset,
                suffix_len: suffix.len() as u16,
            });

            prev_path = path;
        }

        entries.shrink_to_fit();
        suffix_bytes.shrink_to_fit();
        anchors.shrink_to_fit();
        idx_to_sorted.shrink_to_fit();
        sorted_to_idx.shrink_to_fit();

        Self {
            entries,
            suffix_bytes,
            anchors,
            idx_to_sorted,
            sorted_to_idx,
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
        self.entries.len()
    }

    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    /// Total bytes occupied by this structure (approximate).
    pub fn allocated_bytes(&self) -> usize {
        self.entries.capacity() * std::mem::size_of::<EncodedEntry>()
            + self.suffix_bytes.capacity()
            + self.anchors.capacity() * std::mem::size_of::<u32>()
            + self.idx_to_sorted.capacity() * std::mem::size_of::<u32>()
            + self.sorted_to_idx.capacity() * std::mem::size_of::<u32>()
    }

    pub fn encode_raw(&self) -> Vec<u8> {
        const MAGIC: &[u8; 8] = b"PTV2raw\0";
        let mut out = Vec::with_capacity(8 + 5 * 4 + self.allocated_bytes());
        out.extend_from_slice(MAGIC);
        out.extend_from_slice(&(self.entries.len() as u32).to_le_bytes());
        out.extend_from_slice(&(self.suffix_bytes.len() as u32).to_le_bytes());
        out.extend_from_slice(&(self.anchors.len() as u32).to_le_bytes());
        out.extend_from_slice(&(self.idx_to_sorted.len() as u32).to_le_bytes());
        out.extend_from_slice(&(self.sorted_to_idx.len() as u32).to_le_bytes());
        for entry in &self.entries {
            out.extend_from_slice(&entry.suffix_offset.to_le_bytes());
            out.extend_from_slice(&entry.shared_len.to_le_bytes());
            out.extend_from_slice(&entry.suffix_len.to_le_bytes());
        }
        out.extend_from_slice(&self.suffix_bytes);
        for v in &self.anchors {
            out.extend_from_slice(&v.to_le_bytes());
        }
        for v in &self.idx_to_sorted {
            out.extend_from_slice(&v.to_le_bytes());
        }
        for v in &self.sorted_to_idx {
            out.extend_from_slice(&v.to_le_bytes());
        }
        out
    }

    pub fn decode_raw(bytes: &[u8]) -> Option<Self> {
        const MAGIC: &[u8; 8] = b"PTV2raw\0";
        if bytes.len() < 8 + 5 * 4 || &bytes[..8] != MAGIC {
            return None;
        }
        let mut off = 8usize;
        let read_u32 = |bytes: &[u8], off: &mut usize| -> Option<u32> {
            let v = u32::from_le_bytes(bytes.get(*off..*off + 4)?.try_into().ok()?);
            *off += 4;
            Some(v)
        };
        let entries_len = read_u32(bytes, &mut off)? as usize;
        let suffix_len = read_u32(bytes, &mut off)? as usize;
        let anchors_len = read_u32(bytes, &mut off)? as usize;
        let idx_len = read_u32(bytes, &mut off)? as usize;
        let sorted_len = read_u32(bytes, &mut off)? as usize;

        let entries_bytes = entries_len.checked_mul(8)?;
        let suffix_end = off.checked_add(entries_bytes)?.checked_add(suffix_len)?;
        let anchors_bytes = anchors_len.checked_mul(4)?;
        let idx_bytes = idx_len.checked_mul(4)?;
        let sorted_bytes = sorted_len.checked_mul(4)?;
        let total = suffix_end
            .checked_add(anchors_bytes)?
            .checked_add(idx_bytes)?
            .checked_add(sorted_bytes)?;
        if total > bytes.len() {
            return None;
        }

        let mut entries = Vec::with_capacity(entries_len);
        for _ in 0..entries_len {
            let suffix_offset = read_u32(bytes, &mut off)?;
            let shared_len = u16::from_le_bytes(bytes.get(off..off + 2)?.try_into().ok()?);
            off += 2;
            let suffix_len = u16::from_le_bytes(bytes.get(off..off + 2)?.try_into().ok()?);
            off += 2;
            entries.push(EncodedEntry {
                suffix_offset,
                shared_len,
                suffix_len,
            });
        }

        let suffix_bytes = bytes.get(off..off + suffix_len)?.to_vec();
        off += suffix_len;

        let read_u32_vec = |bytes: &[u8], off: &mut usize, len: usize| -> Option<Vec<u32>> {
            let mut out = Vec::with_capacity(len);
            for _ in 0..len {
                out.push(u32::from_le_bytes(
                    bytes.get(*off..*off + 4)?.try_into().ok()?,
                ));
                *off += 4;
            }
            Some(out)
        };
        let anchors = read_u32_vec(bytes, &mut off, anchors_len)?;
        let idx_to_sorted = read_u32_vec(bytes, &mut off, idx_len)?;
        let sorted_to_idx = read_u32_vec(bytes, &mut off, sorted_len)?;

        Some(Self {
            entries,
            suffix_bytes,
            anchors,
            idx_to_sorted,
            sorted_to_idx,
        })
    }

    fn get_suffix(&self, pos: usize) -> &[u8] {
        let e = &self.entries[pos];
        &self.suffix_bytes
            [e.suffix_offset as usize..(e.suffix_offset as usize + e.suffix_len as usize)]
    }

    /// Resolve a sorted position to the full path bytes.
    fn resolve_sorted(&self, sorted_pos: usize) -> Vec<u8> {
        let anchor_pos = (sorted_pos / ANCHOR_INTERVAL) * ANCHOR_INTERVAL;
        let mut path = self.get_suffix(anchor_pos).to_vec();
        for k in (anchor_pos + 1)..=sorted_pos {
            let e = &self.entries[k];
            path.truncate(e.shared_len as usize);
            path.extend_from_slice(self.get_suffix(k));
        }
        path
    }

    /// Resolve a `PathIdx` to the full path bytes.
    pub fn resolve(&self, idx: PathIdx) -> Option<Vec<u8>> {
        let sorted_pos = *self.idx_to_sorted.get(idx as usize)? as usize;
        Some(self.resolve_sorted(sorted_pos))
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
            .map(|sorted| self.sorted_to_idx[sorted])
    }

    /// Lookup a path by its bytes, returning the original `PathIdx`.
    pub fn lookup(&self, target: &[u8]) -> Option<PathIdx> {
        self.find_exact(target)
            .map(|sorted| self.sorted_to_idx[sorted])
    }

    /// Find the exact path by binary search, returning its sorted position.
    fn find_exact(&self, target: &[u8]) -> Option<usize> {
        let mut left = 0usize;
        let mut right = self.entries.len();
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
        if self.entries.is_empty() {
            return None;
        }

        // Binary search for the first entry >= prefix.
        let mut left = 0usize;
        let mut right = self.entries.len();
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
        if start >= self.entries.len() {
            return None;
        }
        let start_path = self.resolve_sorted(start);
        if !start_path.starts_with(prefix) {
            return None;
        }

        // Find the upper bound: first entry that does NOT start with prefix.
        let mut left = start;
        let mut right = self.entries.len();
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
        assert_eq!(table.anchors.len(), 3); // 0, 256, 512

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
        let decoded = PathTableV2::decode_raw(&table.encode_raw()).expect("raw decode");

        assert_eq!(decoded.len(), table.len());
        for (idx, expected) in paths.iter().enumerate() {
            let resolved = decoded.resolve(idx as PathIdx).unwrap();
            assert_eq!(std::str::from_utf8(&resolved).unwrap(), *expected);
            assert_eq!(decoded.lookup(expected.as_bytes()), Some(idx as PathIdx));
        }
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
