use std::hash::{Hash, Hasher};
use std::path::PathBuf;

use crate::core::FileKind;
use crate::index::file_entry_v2::FileEntry;
use crate::util::{compose_abs_path_bytes, root_bytes_for_id};

use super::helpers::{mtime_to_ns, normalize_roots_with_fallback};
use super::path_store::PathStore;
use super::snapshot_format::{IndexSnapshotV2, IndexSnapshotV3, IndexSnapshotV4, IndexSnapshotV5};
use super::PersistentIndex;

impl PersistentIndex {
    pub fn new() -> Self {
        Self::new_with_roots(Vec::new())
    }

    pub fn new_with_roots(roots: Vec<PathBuf>) -> Self {
        let roots = normalize_roots_with_fallback(roots);
        let roots_bytes = roots
            .iter()
            .map(|p| p.as_os_str().as_encoded_bytes().to_vec())
            .collect::<Vec<_>>();

        Self {
            roots,
            roots_bytes,
            entries: parking_lot::RwLock::new(Vec::new()),
            paths: parking_lot::RwLock::new(PathStore::new()),
            filekey_to_docid: parking_lot::RwLock::new(Default::default()),
            path_hash_to_id: parking_lot::RwLock::new(std::collections::HashMap::new()),
            trigram_index: parking_lot::RwLock::new(std::collections::HashMap::new()),
            tombstones: parking_lot::RwLock::new(roaring::RoaringBitmap::new()),
            dirty: std::sync::atomic::AtomicBool::new(false),
            parent_index: parking_lot::RwLock::new(None),
            parent_path_table: parking_lot::RwLock::new(None),
        }
    }

    pub fn from_snapshot_v5(snap: IndexSnapshotV5, roots: Vec<PathBuf>) -> Self {
        let idx = Self::new_with_roots(roots);

        if snap.roots_hash != idx.roots_hash() {
            tracing::warn!(
                "Snapshot roots_hash mismatch, ignoring snapshot ({} != {})",
                snap.roots_hash,
                idx.roots_hash()
            );
            return idx;
        }

        {
            let mut entries = Vec::with_capacity(snap.metas.len());
            let mut paths = PathStore::with_capacity(snap.metas.len(), snap.arena.data.len());
            for (docid_usize, meta) in snap.metas.iter().enumerate() {
                let Ok(docid) = u32::try_from(docid_usize) else {
                    tracing::warn!("Snapshot has too many documents for 32-bit DocId; ignoring");
                    return idx;
                };
                let abs_bytes = snap
                    .arena
                    .get_bytes(meta.path_off, meta.path_len)
                    .map(|rel| {
                        compose_abs_path_bytes(
                            root_bytes_for_id(&idx.roots_bytes, meta.root_id),
                            rel,
                        )
                    })
                    .unwrap_or_default();
                let Ok(stored_docid) = paths.push(&abs_bytes) else {
                    tracing::warn!("Snapshot path arena exceeds runtime capacity; ignoring");
                    return idx;
                };
                debug_assert_eq!(stored_docid, docid);
                entries.push(FileEntry::from_file_key_and_kind(
                    meta.file_key,
                    docid,
                    meta.mtime_ns,
                    FileKind::File,
                ));
            }
            *idx.entries.write() = entries;
            *idx.paths.write() = paths;
            *idx.tombstones.write() = snap.tombstones.into_iter().collect();
            idx.dirty.store(false, std::sync::atomic::Ordering::Release);
        }

        // rebuild derived indexes
        idx.rebuild_derived_indexes();
        idx
    }

    pub fn from_snapshot_v4(snap: IndexSnapshotV4, roots: Vec<PathBuf>) -> Self {
        // v4 arena 里存的是绝对路径字节；这里迁移为 v5（root-relative + root_id）
        let idx = Self::new_with_roots(roots);

        let IndexSnapshotV4 {
            arena: old_arena,
            metas: old_metas,
            tombstones,
        } = snap;

        let mut entries: Vec<FileEntry> = Vec::with_capacity(old_metas.len());
        let mut paths = PathStore::with_capacity(old_metas.len(), old_arena.data.len());

        for m in old_metas {
            let abs_path = old_arena.get_path_buf(m.path_off, m.path_len);
            let Some(abs_path) = abs_path else {
                continue;
            };
            let Ok(docid) = u32::try_from(entries.len()) else {
                tracing::warn!("Snapshot has too many documents for 32-bit DocId; ignoring");
                return idx;
            };
            let mtime_ns = mtime_to_ns(m.mtime);
            let abs_bytes = abs_path.as_os_str().as_encoded_bytes();
            let Ok(stored_docid) = paths.push(abs_bytes) else {
                tracing::warn!("Snapshot path arena exceeds runtime capacity; ignoring");
                return idx;
            };
            debug_assert_eq!(stored_docid, docid);
            entries.push(FileEntry::from_file_key_and_kind(
                m.file_key,
                docid,
                mtime_ns,
                FileKind::File,
            ));
        }

        {
            *idx.entries.write() = entries;
            *idx.paths.write() = paths;
            *idx.tombstones.write() = tombstones.into_iter().collect();
            idx.dirty.store(false, std::sync::atomic::Ordering::Release);
        }

        idx.rebuild_derived_indexes();
        idx
    }

    pub fn from_snapshot_v3(snap: IndexSnapshotV3, roots: Vec<PathBuf>) -> Self {
        // v3 的 tombstones 不携带对应文档记录；阶段 A 的 DocId tombstone 以"保留 doc 槽位"实现，
        // 因此这里仅重建 files，本质上等价于"干净加载"。
        let idx = Self::new_with_roots(roots);
        for (_k, meta) in snap.files {
            idx.upsert(meta);
        }
        idx
    }

    pub fn from_snapshot_v2(snap: IndexSnapshotV2, roots: Vec<PathBuf>) -> Self {
        let idx = Self::new_with_roots(roots);
        for (_k, meta) in snap.files {
            idx.upsert(meta);
        }
        idx
    }

    pub(super) fn roots_hash(&self) -> u64 {
        // 稳定哈希：按 root bytes 顺序（含 "/" 兜底 + 其余 root 的排序规则）拼接后 hash。
        // 目的：避免 root 顺序变化导致 root_id 解释错位。
        let mut hasher = std::collections::hash_map::DefaultHasher::new();
        for b in &self.roots_bytes {
            b.hash(&mut hasher);
            0xff_u8.hash(&mut hasher); // 分隔符，避免 ["ab","c"] 与 ["a","bc"] 冲突
        }
        hasher.finish()
    }
}
