use std::path::{Path, PathBuf};

use super::helpers::{for_each_basename_trigram, path_hash_bytes};
use super::types::OneOrManyDocId;
use super::{DocId, PersistentIndex};
use crate::core::{FileKey, FileKind};

impl PersistentIndex {
    pub(super) fn filekey_representative(&self, key: FileKey) -> Option<DocId> {
        let entries = self.entries.read();
        let index = self.filekey_to_docid.read();
        index.get(key, &entries)
    }

    pub(super) fn insert_filekey_if_absent(&self, key: FileKey, docid: DocId) -> Option<DocId> {
        let entries = self.entries.read();
        let mut index = self.filekey_to_docid.write();
        index.insert_if_absent(key, docid, &entries)
    }

    pub(super) fn replace_filekey_representative(
        &self,
        key: FileKey,
        docid: DocId,
    ) -> Option<DocId> {
        let entries = self.entries.read();
        let mut index = self.filekey_to_docid.write();
        index.insert_or_replace(key, docid, &entries)
    }

    pub(super) fn remove_filekey_representative(&self, key: FileKey) -> Option<DocId> {
        let entries = self.entries.read();
        let mut index = self.filekey_to_docid.write();
        index.remove(key, &entries)
    }

    pub(super) fn filekey_pairs(&self) -> Vec<(FileKey, DocId)> {
        let entries = self.entries.read();
        let index = self.filekey_to_docid.read();
        index.iter(&entries).collect()
    }

    pub(super) fn path_buf_for_docid(&self, docid: DocId) -> Option<PathBuf> {
        self.paths.read().get_path_buf(docid)
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
            let mut paths = self.paths.write();
            if paths.update(docid, abs_path_bytes).is_err() {
                return false;
            }
            entry.set_path_index_and_kind(docid, kind);
            entry.mtime_ns = mtime_ns;
        }
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
                .get_bytes(*docid)
                .map(|path_bytes| path_bytes == bytes)
                .unwrap_or(false)
        })
    }
}
