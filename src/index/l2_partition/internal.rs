use std::path::{Path, PathBuf};

use crate::core::FileKind;
use crate::util::pathbuf_from_encoded_vec;

use super::helpers::{for_each_basename_trigram, path_hash_bytes};
use super::types::OneOrManyDocId;
use super::{DocId, PersistentIndex};

impl PersistentIndex {
    pub(super) fn path_buf_for_docid(&self, docid: DocId) -> Option<PathBuf> {
        let paths = self.paths.read();
        paths
            .get(docid as usize)
            .map(|bytes| pathbuf_from_encoded_vec(bytes.clone()))
    }

    pub(super) fn entry_mtime(&self, docid: DocId) -> Option<i64> {
        let entries = self.entries.read();
        let entry = entries.get(docid as usize)?;
        Some(entry.mtime_ns)
    }

    pub(super) fn update_entry_metadata(
        &self,
        docid: DocId,
        mtime_ns: i64,
        kind: FileKind,
    ) -> bool {
        let mut entries = self.entries.write();
        let Some(entry) = entries.get_mut(docid as usize) else {
            return false;
        };
        let path_idx = entry.path_index();
        entry.set_path_index_and_kind(path_idx, kind);
        entry.mtime_ns = mtime_ns;
        true
    }

    pub(super) fn update_entry_path(
        &self,
        docid: DocId,
        abs_path_bytes: &[u8],
        mtime_ns: i64,
        kind: FileKind,
    ) -> bool {
        {
            let mut entries = self.entries.write();
            let Some(entry) = entries.get_mut(docid as usize) else {
                return false;
            };
            let path_idx = match docid.try_into() {
                Ok(path_idx) => path_idx,
                Err(_) => return false,
            };
            entry.set_path_index_and_kind(path_idx, kind);
            entry.mtime_ns = mtime_ns;
        }
        let mut paths = self.paths.write();
        let Some(path) = paths.get_mut(docid as usize) else {
            return false;
        };
        *path = abs_path_bytes.to_vec();
        true
    }

    pub(super) fn remove_trigrams(&self, docid: DocId, path: &Path) {
        let mut tri_idx = self.trigram_index.write();
        for_each_basename_trigram(path, |tri| {
            if let Some(posting) = tri_idx.get_mut(&tri) {
                posting.remove(docid);
                if posting.is_empty() {
                    tri_idx.remove(&tri);
                }
            }
        });
    }

    pub(super) fn insert_trigrams(&self, docid: DocId, path: &Path) {
        let mut tri_idx = self.trigram_index.write();
        for_each_basename_trigram(path, |tri| {
            tri_idx.entry(tri).or_default().insert(docid);
        });
    }

    pub(super) fn insert_path_hash(&self, docid: DocId, path: &Path) {
        let bytes = path.as_os_str().as_encoded_bytes();
        let h = path_hash_bytes(bytes);
        let mut map = self.path_hash_to_id.write();
        map.entry(h)
            .and_modify(|v| v.insert(docid))
            .or_insert(OneOrManyDocId::One(docid));
    }

    pub(super) fn remove_path_hash(&self, docid: DocId, path: &Path) {
        let bytes = path.as_os_str().as_encoded_bytes();
        let h = path_hash_bytes(bytes);
        let mut map = self.path_hash_to_id.write();
        if let Some(v) = map.get_mut(&h) {
            let empty = v.remove(docid);
            if empty {
                map.remove(&h);
            }
        }
    }

    pub(super) fn lookup_docid_by_path(&self, path: &Path) -> Option<DocId> {
        let bytes = path.as_os_str().as_encoded_bytes();
        let h = path_hash_bytes(bytes);

        // 先复制候选 DocId（避免同时持有 path_hash_to_id 与 paths 的锁）
        let candidates: Vec<DocId> = {
            let map = self.path_hash_to_id.read();
            let v = map.get(&h)?;
            v.iter().copied().collect()
        };

        if candidates.is_empty() {
            return None;
        }

        let paths = self.paths.read();
        candidates.into_iter().find(|docid| {
            paths
                .get(*docid as usize)
                .map(|path_bytes| path_bytes.as_slice() == bytes)
                .unwrap_or(false)
        })
    }
}
