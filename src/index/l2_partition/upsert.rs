use std::path::Path;

use crate::core::{FileKey, FileKind, FileMeta};
use crate::index::file_entry_v2::FileEntry;
use crate::index::PathFreshness;
use crate::util::pathbuf_from_encoded_vec;

use super::helpers::{for_each_basename_trigram, mtime_to_ns, path_hash_bytes};
use super::types::OneOrManyDocId;
use super::{DocId, PersistentIndex};

impl PersistentIndex {
    pub(super) fn rebuild_derived_indexes(&self) {
        let entries = self.entries.read();
        let paths = self.paths.read();
        let tomb = self.tombstones.read();

        let mut filekey_to_docid = super::filekey_index::CompactFileKeyIndex::new();
        let mut path_hash_to_id: std::collections::HashMap<u64, OneOrManyDocId> =
            std::collections::HashMap::new();
        let mut trigram_index: std::collections::HashMap<super::Trigram, roaring::RoaringBitmap> =
            std::collections::HashMap::new();

        for (docid_usize, entry) in entries.iter().enumerate() {
            let docid: DocId = docid_usize as DocId;

            if tomb.contains(docid) {
                continue;
            }

            filekey_to_docid.insert_if_absent(entry.file_key(), docid, &entries);

            let Some(abs_bytes) = paths.get_bytes(docid) else {
                continue;
            };
            if !abs_bytes.is_empty() {
                let h = path_hash_bytes(abs_bytes);
                path_hash_to_id
                    .entry(h)
                    .and_modify(|v| v.insert(docid))
                    .or_insert(OneOrManyDocId::One(docid));

                let abs_path = pathbuf_from_encoded_vec(abs_bytes.to_vec());
                for_each_basename_trigram(abs_path.as_path(), |tri| {
                    trigram_index.entry(tri).or_default().insert(docid);
                });
            }
        }

        drop(tomb);
        drop(paths);
        drop(entries);

        *self.filekey_to_docid.write() = filekey_to_docid;
        *self.path_hash_to_id.write() = path_hash_to_id;
        *self.trigram_index.write() = trigram_index;
    }

    /// 插入/更新一条文件记录。
    ///
    /// 搜索主键是 path/docid，不是 FileKey。相同 FileKey 的不同路径
    /// 会作为 hardlink aliases 分别入库；只有旧路径已消失的 reconcile
    /// 或显式 rename 才会移动现有 docid。
    pub fn upsert(&self, meta: FileMeta) {
        self.upsert_inner(meta, false);
    }

    /// rename 专用：强制更新路径
    pub fn upsert_rename(&self, meta: FileMeta) {
        self.upsert_inner(meta, true);
    }

    /// Insert a path as an independent search entry even if another live path
    /// has the same FileKey. Used when materializing already-resolved visible
    /// metas where path alias preservation is mandatory.
    pub fn upsert_path_alias(&self, mut meta: FileMeta) {
        meta.path = crate::index::tiered::normalize_path(&meta.path);
        let fkey = meta.file_key;
        let mtime_ns = mtime_to_ns(meta.mtime);
        if let Some(docid) = self.lookup_docid_by_path(meta.path.as_path()) {
            self.update_existing_docid(docid, fkey, mtime_ns, meta.kind);
            self.tombstones.write().remove(docid);
            self.dirty.store(true, std::sync::atomic::Ordering::Release);
            return;
        }
        let bytes = meta.path.as_os_str().as_encoded_bytes().to_vec();
        let Some(docid) = self.alloc_docid(fkey, &bytes, mtime_ns, meta.kind) else {
            return;
        };
        self.insert_trigrams(docid, meta.path.as_path());
        self.insert_path_hash(docid, meta.path.as_path());
        self.dirty.store(true, std::sync::atomic::Ordering::Release);
    }

    fn upsert_inner(&self, mut meta: FileMeta, force_path_update: bool) {
        meta.path = crate::index::tiered::normalize_path(&meta.path);
        let fkey = meta.file_key;
        let new_abs_bytes = meta.path.as_os_str().as_encoded_bytes().to_vec();
        let new_mtime_ns = mtime_to_ns(meta.mtime);

        if let Some(docid) = self.lookup_docid_by_path(meta.path.as_path()) {
            self.update_existing_docid(docid, fkey, new_mtime_ns, meta.kind);
            self.tombstones.write().remove(docid);
            self.dirty.store(true, std::sync::atomic::Ordering::Release);
            return;
        }

        // 先查代表 docid（只持有 mapping 的读锁）。这只用于 rename/reconcile，
        // 不能阻止 hardlink alias 以新 path 入库。
        let existing_docid = self.filekey_representative(fkey);

        if let Some(docid) = existing_docid {
            // 读旧路径 bytes（不持有 trigram/path_hash 锁）
            let old_path_bytes = { self.paths.read().get_bytes(docid).map(<[u8]>::to_vec) };

            let old_path_missing = if force_path_update {
                false
            } else {
                old_path_bytes
                    .as_ref()
                    .map(|old| pathbuf_from_encoded_vec(old.clone()))
                    .map(|old_path| match std::fs::symlink_metadata(&old_path) {
                        Ok(_) => false,
                        Err(err) if err.kind() == std::io::ErrorKind::NotFound => true,
                        Err(_) => false,
                    })
                    .unwrap_or(false)
            };

            // 路径不同且旧路径仍存在：这是 hardlink alias，追加新 docid。
            if !force_path_update && !old_path_missing {
                let Some(docid_new) =
                    self.alloc_docid(fkey, &new_abs_bytes, new_mtime_ns, meta.kind)
                else {
                    return;
                };
                self.insert_trigrams(docid_new, meta.path.as_path());
                self.insert_path_hash(docid_new, meta.path.as_path());
                self.dirty.store(true, std::sync::atomic::Ordering::Release);
                return;
            }

            // rename：先移除旧路径关联
            if let Some(old_path_bytes) = old_path_bytes {
                let old_path = pathbuf_from_encoded_vec(old_path_bytes);
                self.remove_trigrams(docid, &old_path);
                self.remove_path_hash(docid, &old_path);
            };

            // posting/path_hash 先写（与 query 锁顺序一致：trigram -> entries/paths）
            self.insert_trigrams(docid, meta.path.as_path());
            self.insert_path_hash(docid, meta.path.as_path());

            if !self.update_entry_path(docid, &new_abs_bytes, new_mtime_ns, meta.kind) {
                // 极端情况：docid 槽位不存在，降级为 append
                if let Some(docid_new) =
                    self.alloc_docid(fkey, &new_abs_bytes, new_mtime_ns, meta.kind)
                {
                    self.insert_trigrams(docid_new, meta.path.as_path());
                    self.insert_path_hash(docid_new, meta.path.as_path());
                }
            }

            // rename/reconcile 视为"存在且活跃"
            self.tombstones.write().remove(docid);
            self.replace_filekey_representative(fkey, docid);
            self.dirty.store(true, std::sync::atomic::Ordering::Release);
            return;
        }

        // 新文件：分配 docid 并写入
        let Some(docid) = self.alloc_docid(fkey, &new_abs_bytes, new_mtime_ns, meta.kind) else {
            return;
        };
        self.insert_trigrams(docid, meta.path.as_path());
        self.insert_path_hash(docid, meta.path.as_path());
        self.dirty.store(true, std::sync::atomic::Ordering::Release);
    }

    fn alloc_docid(
        &self,
        file_key: FileKey,
        abs_path_bytes: &[u8],
        mtime_ns: i64,
        kind: FileKind,
    ) -> Option<DocId> {
        let docid = {
            let mut entries = self.entries.write();
            if entries.len() > super::filekey_index::MAX_DOC_ID as usize {
                return None;
            }
            let docid = DocId::try_from(entries.len()).ok()?;
            let mut paths = self.paths.write();
            let stored_docid = paths.push(abs_path_bytes).ok()?;
            debug_assert_eq!(stored_docid, docid);
            entries.push(FileEntry::from_file_key_and_kind(
                file_key, docid, mtime_ns, kind,
            ));
            docid
        };

        self.insert_filekey_if_absent(file_key, docid);
        self.tombstones.write().remove(docid);
        Some(docid)
    }

    /// 标记删除（tombstone）
    pub fn mark_deleted(&self, file_key: FileKey) {
        let docids = self.docids_for_filekey(file_key);
        for docid in docids {
            self.mark_docid_deleted(docid);
        }
        self.rebuild_filekey_representative(file_key);
    }

    /// 按路径删除
    pub fn mark_deleted_by_path(&self, path: &Path) {
        if let Some(docid) = self.lookup_docid_by_path(path) {
            let file_key = {
                let entries = self.entries.read();
                entries.get(docid as usize).map(|e| e.file_key())
            };
            self.mark_docid_deleted(docid);
            if let Some(k) = file_key {
                self.rebuild_filekey_representative(k);
            }
        }
    }

    fn docids_for_filekey(&self, file_key: FileKey) -> Vec<DocId> {
        let entries = self.entries.read();
        entries
            .iter()
            .enumerate()
            .filter_map(|(docid, entry)| {
                if entry.file_key() == file_key {
                    Some(docid as DocId)
                } else {
                    None
                }
            })
            .collect()
    }

    fn update_existing_docid(&self, docid: DocId, new_key: FileKey, mtime_ns: i64, kind: FileKind) {
        let old_key = self
            .entries
            .read()
            .get(docid as usize)
            .map(|entry| entry.file_key());
        let Some(old_key) = old_key else {
            return;
        };

        if old_key == new_key {
            self.update_entry_metadata(docid, mtime_ns, kind);
            self.insert_filekey_if_absent(new_key, docid);
            return;
        }

        self.remove_filekey_representative(old_key);
        {
            let mut entries = self.entries.write();
            let Some(entry) = entries.get_mut(docid as usize) else {
                return;
            };
            let path_idx = entry.path_index();
            *entry = FileEntry::from_file_key_and_kind(new_key, path_idx, mtime_ns, kind);
        }
        self.rebuild_filekey_representative(old_key);
        self.insert_filekey_if_absent(new_key, docid);
    }

    fn rebuild_filekey_representative(&self, file_key: FileKey) {
        let entries = self.entries.read();
        let tombstones = self.tombstones.read();
        let next = entries.iter().enumerate().find_map(|(docid, entry)| {
            let docid = docid as DocId;
            if entry.file_key() == file_key && !tombstones.contains(docid) {
                Some(docid)
            } else {
                None
            }
        });
        drop(tombstones);
        drop(entries);
        match next {
            Some(docid) => {
                self.replace_filekey_representative(file_key, docid);
            }
            None => {
                self.remove_filekey_representative(file_key);
            }
        }
    }

    fn mark_docid_deleted(&self, docid: DocId) {
        let path = { self.path_buf_for_docid(docid) };

        self.tombstones.write().insert(docid);
        self.dirty.store(true, std::sync::atomic::Ordering::Release);

        if let Some(p) = path {
            self.remove_trigrams(docid, &p);
            self.remove_path_hash(docid, &p);
        }
    }

    pub fn path_freshness(&self, path: &Path, mtime_ns: i64) -> PathFreshness {
        let Some(docid) = self.lookup_docid_by_path(path) else {
            return PathFreshness::Missing;
        };
        if self.tombstones.read().contains(docid) {
            return PathFreshness::Missing;
        }
        let Some(old_mtime_ns) = self.entry_mtime(docid) else {
            return PathFreshness::Changed;
        };
        if old_mtime_ns == mtime_ns {
            PathFreshness::Unchanged
        } else {
            PathFreshness::Changed
        }
    }
}
