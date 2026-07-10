use std::collections::BTreeSet;

use roaring::RoaringBitmap;

use crate::index::file_entry_v2::FileEntry;

use super::path_store::PathStore;
use super::{DocId, PersistentIndex};

/// Minimal owned state retained after a mutable L2 generation is handed to the
/// v7 writer. Derived query maps are dropped before any snapshot spools are
/// allocated.
pub struct OwnedV7Source {
    entries: Vec<FileEntry>,
    paths: PathStore,
    tombstones: RoaringBitmap,
}

impl PersistentIndex {
    /// Consume a mutable generation and release every query-only derived index.
    pub fn into_v7_source(self) -> OwnedV7Source {
        let PersistentIndex {
            roots,
            roots_bytes,
            entries,
            paths,
            filekey_to_docid,
            path_hash_to_id,
            trigram_index,
            tombstones,
            dirty: _,
            parent_index,
            parent_path_table,
        } = self;

        drop(roots);
        drop(roots_bytes);
        drop(filekey_to_docid.into_inner());
        drop(path_hash_to_id.into_inner());
        drop(trigram_index.into_inner());
        drop(parent_index.into_inner());
        drop(parent_path_table.into_inner());

        OwnedV7Source {
            entries: entries.into_inner(),
            paths: paths.into_inner(),
            tombstones: tombstones.into_inner(),
        }
    }

    /// Return whether a delete prefix touches descendants rather than only an
    /// exact path. Callers use this to reject unproven directory renames before
    /// mutating the generation.
    pub fn snapshot_prefixes_match_descendants(&self, prefixes: &BTreeSet<Vec<u8>>) -> bool {
        // Canonical PersistentIndex lock order: entries → paths → tombstones.
        let entries = self.entries.read();
        let paths = self.paths.read();
        let tombstones = self.tombstones.read();
        (0..entries.len()).any(|docid| {
            let docid = docid as DocId;
            if tombstones.contains(docid) {
                return false;
            }
            paths.get_bytes(docid).is_some_and(|path| {
                matching_prefix(path, prefixes).is_some_and(|prefix| path != prefix)
            })
        })
    }

    /// Apply exact/subtree tombstones without rebuilding query maps. This is
    /// only for an exclusively owned generation immediately before consumption.
    pub fn tombstone_snapshot_prefixes(&self, prefixes: &BTreeSet<Vec<u8>>) -> usize {
        if prefixes.is_empty() {
            return 0;
        }
        let entries = self.entries.read();
        let paths = self.paths.read();
        let mut tombstones = self.tombstones.write();
        let before = tombstones.len();
        for docid in 0..entries.len() {
            let docid = docid as DocId;
            if tombstones.contains(docid) {
                continue;
            }
            let Some(path) = paths.get_bytes(docid) else {
                continue;
            };
            if matching_prefix(path, prefixes).is_some() {
                tombstones.insert(docid);
            }
        }
        (tombstones.len() - before) as usize
    }
}

impl OwnedV7Source {
    pub fn validate(&self) -> anyhow::Result<()> {
        if self.paths.len() != self.entries.len() {
            anyhow::bail!(
                "owned_v7_source_corrupt: {} entries but {} paths",
                self.entries.len(),
                self.paths.len()
            );
        }
        if self
            .tombstones
            .max()
            .is_some_and(|docid| docid as usize >= self.entries.len())
        {
            anyhow::bail!("owned_v7_source_corrupt: tombstone DocId is out of range");
        }
        Ok(())
    }

    pub fn raw_entry_count(&self) -> usize {
        self.entries.len()
    }

    pub fn live_entry_count(&self) -> usize {
        self.entries
            .len()
            .saturating_sub(self.tombstones.len() as usize)
    }

    pub fn for_each_live_entry_path(
        &self,
        mut visitor: impl FnMut(u32, &FileEntry, &[u8]) -> anyhow::Result<()>,
    ) -> anyhow::Result<()> {
        self.validate()?;
        let mut compact_docid = 0u32;
        for (raw_docid, entry) in self.entries.iter().enumerate() {
            let raw_docid = u32::try_from(raw_docid)
                .map_err(|_| anyhow::anyhow!("owned_v7_source_corrupt: DocId overflow"))?;
            if self.tombstones.contains(raw_docid) {
                continue;
            }
            let path = self.paths.get_bytes(raw_docid).ok_or_else(|| {
                anyhow::anyhow!("owned_v7_source_corrupt: path {} is missing", raw_docid)
            })?;
            visitor(compact_docid, entry, path)?;
            compact_docid = compact_docid
                .checked_add(1)
                .ok_or_else(|| anyhow::anyhow!("owned_v7_source_corrupt: DocId overflow"))?;
        }
        Ok(())
    }
}

fn matching_prefix<'a>(path: &[u8], prefixes: &'a BTreeSet<Vec<u8>>) -> Option<&'a [u8]> {
    if let Some(exact) = prefixes.get(path) {
        return Some(exact.as_slice());
    }
    for boundary in path
        .iter()
        .enumerate()
        .rev()
        .filter_map(|(index, byte)| (*byte == b'/' && index > 0).then_some(index))
    {
        if let Some(prefix) = prefixes.get(&path[..=boundary]) {
            return Some(prefix.as_slice());
        }
        if let Some(prefix) = prefixes.get(&path[..boundary]) {
            return Some(prefix.as_slice());
        }
    }
    prefixes.get(b"/".as_slice()).map(Vec::as_slice)
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;

    use crate::core::{FileKey, FileKind, FileMeta};

    use super::*;

    fn meta(path: &str, ino: u64) -> FileMeta {
        FileMeta {
            file_key: FileKey {
                dev: 1,
                ino,
                generation: 0,
            },
            path: PathBuf::from(path),
            size: 0,
            mtime: None,
            ctime: None,
            atime: None,
            kind: FileKind::File,
        }
    }

    #[test]
    fn owned_source_compacts_docids_and_keeps_hardlink_paths() {
        let index = PersistentIndex::new_with_roots(vec![PathBuf::from("/tmp")]);
        let shared = FileKey {
            dev: 1,
            ino: 7,
            generation: 0,
        };
        index.upsert_path_alias(meta("/tmp/deleted", 1));
        index.upsert_path_alias(FileMeta {
            file_key: shared,
            ..meta("/tmp/a", 7)
        });
        index.upsert_path_alias(FileMeta {
            file_key: shared,
            ..meta("/tmp/b", 7)
        });
        index.mark_deleted_by_path(PathBuf::from("/tmp/deleted").as_path());

        let source = index.into_v7_source();
        assert_eq!(source.raw_entry_count(), 3);
        assert_eq!(source.live_entry_count(), 2);
        let mut seen = Vec::new();
        source
            .for_each_live_entry_path(|docid, entry, path| {
                seen.push((docid, entry.file_key(), path.to_vec()));
                Ok(())
            })
            .unwrap();
        assert_eq!(seen[0].0, 0);
        assert_eq!(seen[1].0, 1);
        assert_eq!(seen[0].1, shared);
        assert_eq!(seen[1].1, shared);
        assert_eq!(seen[0].2, b"/tmp/a");
        assert_eq!(seen[1].2, b"/tmp/b");
    }

    #[test]
    fn snapshot_prefix_tombstones_exact_and_descendants_only() {
        let index = PersistentIndex::new_with_roots(vec![PathBuf::from("/tmp")]);
        index.upsert_path_alias(meta("/tmp/tree", 1));
        index.upsert_path_alias(meta("/tmp/tree/child", 2));
        index.upsert_path_alias(meta("/tmp/treehouse", 3));
        let prefixes = BTreeSet::from([b"/tmp/tree".to_vec()]);

        assert!(index.snapshot_prefixes_match_descendants(&prefixes));
        assert_eq!(index.tombstone_snapshot_prefixes(&prefixes), 2);
        let source = index.into_v7_source();
        let mut paths = Vec::new();
        source
            .for_each_live_entry_path(|_, _, path| {
                paths.push(path.to_vec());
                Ok(())
            })
            .unwrap();
        assert_eq!(paths, vec![b"/tmp/treehouse".to_vec()]);
    }

    #[test]
    fn snapshot_prefix_with_trailing_separator_matches_descendants() {
        let index = PersistentIndex::new_with_roots(vec![PathBuf::from("/tmp")]);
        index.upsert_path_alias(meta("/tmp/tree/child", 1));
        index.upsert_path_alias(meta("/tmp/treehouse/child", 2));
        let prefixes = BTreeSet::from([b"/tmp/tree/".to_vec()]);

        assert!(index.snapshot_prefixes_match_descendants(&prefixes));
        assert_eq!(index.tombstone_snapshot_prefixes(&prefixes), 1);
        let source = index.into_v7_source();
        let mut paths = Vec::new();
        source
            .for_each_live_entry_path(|_, _, path| {
                paths.push(path.to_vec());
                Ok(())
            })
            .unwrap();
        assert_eq!(paths, vec![b"/tmp/treehouse/child".to_vec()]);
    }
}
