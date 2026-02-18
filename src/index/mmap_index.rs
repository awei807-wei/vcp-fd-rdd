use std::io::Cursor;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use roaring::RoaringBitmap;

#[cfg(feature = "rkyv")]
use crate::core::FileKeyEntry;
use crate::core::{FileKey, FileMeta};
use crate::index::IndexLayer;
use crate::query::matcher::Matcher;
use crate::storage::snapshot::MmapSnapshotV6;

#[cfg(feature = "rkyv")]
use std::sync::OnceLock;

// MetaRecordV6：与 PersistentIndex::export_segments_v6 的编码保持一致（LE，40B）
const META_REC_SIZE: usize = 40;
// TrigramEntryV6：3 + 1 + 4 + 4 = 12B
const TRI_REC_SIZE: usize = 12;
// FileKeyMap：dev(8) + ino(8) + docid(4) = 20B
const FILEKEY_MAP_REC_SIZE: usize = 20;

const FKM_MAGIC: [u8; 4] = *b"FKM\0";
const FKM_HDR_SIZE: usize = 8;
const FKM_FLAG_LEGACY: u16 = 0;
const FKM_FLAG_RKYV: u16 = 1;

pub struct MmapIndex {
    snap: Arc<MmapSnapshotV6>,
    tomb_cache: parking_lot::Mutex<Option<RoaringBitmap>>,
    // 兼容旧段：若缺少 mmap 内的 file_key_map 段，则按需构建一次排序 map（以 bytes 形式存储，便于二分查找）。
    filekey_map_cache: parking_lot::Mutex<Option<Arc<Vec<u8>>>>,
    #[cfg(feature = "rkyv")]
    validated_rkyv: OnceLock<anyhow::Result<()>>,
}

impl MmapIndex {
    pub fn new(snap: MmapSnapshotV6) -> Self {
        Self {
            snap: Arc::new(snap),
            tomb_cache: parking_lot::Mutex::new(None),
            filekey_map_cache: parking_lot::Mutex::new(None),
            #[cfg(feature = "rkyv")]
            validated_rkyv: OnceLock::new(),
        }
    }

    pub fn snap(&self) -> &MmapSnapshotV6 {
        &self.snap
    }

    pub fn file_count_estimate(&self) -> usize {
        self.snap.metas_bytes().len() / META_REC_SIZE
    }

    fn tombstones(&self) -> RoaringBitmap {
        let mut g = self.tomb_cache.lock();
        if let Some(b) = g.as_ref() {
            return b.clone();
        }
        let bytes = self.snap.tombstones_bytes();
        let mut cur = Cursor::new(bytes);
        let b = RoaringBitmap::deserialize_from(&mut cur).unwrap_or_else(|_| RoaringBitmap::new());
        *g = Some(b.clone());
        b
    }

    fn compose_abs_path_bytes(&self, root_id: u16, rel_bytes: &[u8]) -> Vec<u8> {
        let root = self
            .snap
            .roots
            .get(root_id as usize)
            .map(|v| v.as_slice())
            .unwrap_or(b"/");
        let mut out = Vec::with_capacity(root.len() + 1 + rel_bytes.len());
        out.extend_from_slice(root);
        if !out.ends_with(b"/") {
            out.push(b'/');
        }
        out.extend_from_slice(rel_bytes);
        out
    }

    fn compose_abs_path(&self, root_id: u16, rel_bytes: &[u8]) -> PathBuf {
        use std::os::unix::ffi::OsStringExt;
        let root = self
            .snap
            .roots
            .get(root_id as usize)
            .map(|v| v.as_slice())
            .unwrap_or(b"/");
        let mut out = Vec::with_capacity(root.len() + 1 + rel_bytes.len());
        out.extend_from_slice(root);
        if !out.ends_with(b"/") {
            out.push(b'/');
        }
        out.extend_from_slice(rel_bytes);
        PathBuf::from(std::ffi::OsString::from_vec(out))
    }

    fn meta_at(
        &self,
        docid: u32,
    ) -> Option<(FileKey, u16, u32, u16, u64, Option<std::time::SystemTime>)> {
        let bytes = self.snap.metas_bytes();
        let i = (docid as usize).checked_mul(META_REC_SIZE)?;
        let rec = bytes.get(i..i + META_REC_SIZE)?;

        let dev = u64::from_le_bytes(rec[0..8].try_into().ok()?);
        let ino = u64::from_le_bytes(rec[8..16].try_into().ok()?);
        let root_id = u16::from_le_bytes(rec[16..18].try_into().ok()?);
        let path_off = u32::from_le_bytes(rec[18..22].try_into().ok()?);
        let path_len = u16::from_le_bytes(rec[22..24].try_into().ok()?);
        let size = u64::from_le_bytes(rec[24..32].try_into().ok()?);
        let mtime_ns = i64::from_le_bytes(rec[32..40].try_into().ok()?);

        let mtime = if mtime_ns < 0 {
            None
        } else {
            let dur = std::time::Duration::from_nanos(mtime_ns as u64);
            Some(std::time::UNIX_EPOCH + dur)
        };

        Some((
            FileKey { dev, ino },
            root_id,
            path_off,
            path_len,
            size,
            mtime,
        ))
    }

    fn lookup_docid_in_legacy_map(bytes: &[u8], key: FileKey) -> Option<u32> {
        let n = bytes.len() / FILEKEY_MAP_REC_SIZE;
        if n == 0 {
            return None;
        }

        let mut lo = 0usize;
        let mut hi = n;
        while lo < hi {
            let mid = (lo + hi) / 2;
            let off = mid * FILEKEY_MAP_REC_SIZE;
            let rec = &bytes[off..off + FILEKEY_MAP_REC_SIZE];
            let dev = u64::from_le_bytes(rec[0..8].try_into().ok()?);
            let ino = u64::from_le_bytes(rec[8..16].try_into().ok()?);
            if (dev, ino) < (key.dev, key.ino) {
                lo = mid + 1;
            } else {
                hi = mid;
            }
        }
        if lo >= n {
            return None;
        }
        let off = lo * FILEKEY_MAP_REC_SIZE;
        let rec = &bytes[off..off + FILEKEY_MAP_REC_SIZE];
        let dev = u64::from_le_bytes(rec[0..8].try_into().ok()?);
        let ino = u64::from_le_bytes(rec[8..16].try_into().ok()?);
        if (dev, ino) != (key.dev, key.ino) {
            return None;
        }
        let docid = u32::from_le_bytes(rec[16..20].try_into().ok()?);
        Some(docid)
    }

    #[cfg(feature = "rkyv")]
    fn lookup_docid_in_rkyv_map(&self, payload: &[u8], key: FileKey) -> Option<u32> {
        let res = self.validated_rkyv.get_or_init(|| {
            // 只在首次访问校验一次（O(N)），后续查询走 archived_root + O(logN) 二分。
            rkyv::check_archived_root::<Vec<FileKeyEntry>>(payload)
                .map(|_| ())
                .map_err(|e| anyhow::anyhow!("rkyv check_archived_root failed: {e}"))
        });
        if res.is_err() {
            return None;
        }

        let archived = unsafe { rkyv::archived_root::<Vec<FileKeyEntry>>(payload) };
        let slice = archived.as_slice();
        let mut lo = 0usize;
        let mut hi = slice.len();
        while lo < hi {
            let mid = (lo + hi) / 2;
            let e = &slice[mid];
            let dev = e.key.dev;
            let ino = e.key.ino;
            if (dev, ino) < (key.dev, key.ino) {
                lo = mid + 1;
            } else {
                hi = mid;
            }
        }
        if lo >= slice.len() {
            return None;
        }
        let e = &slice[lo];
        if (e.key.dev, e.key.ino) != (key.dev, key.ino) {
            return None;
        }
        Some(e.doc_id)
    }

    fn lookup_docid_by_filekey(&self, key: FileKey) -> Option<u32> {
        if let Some(bytes) = self.snap.file_key_map_bytes() {
            // 兼容：测试/旧数据可能出现空 range（0..0）。空表等价于“缺失”，走 fallback。
            if !bytes.is_empty() {
                // 新格式：magic + header
                if bytes.len() >= FKM_HDR_SIZE && bytes[0..4] == FKM_MAGIC {
                    let ver = u16::from_le_bytes(bytes[4..6].try_into().ok()?);
                    let flags = u16::from_le_bytes(bytes[6..8].try_into().ok()?);
                    let payload = &bytes[FKM_HDR_SIZE..];
                    if ver != 1 {
                        tracing::warn!(
                            "MmapIndex: unsupported file_key_map version {}, falling back",
                            ver
                        );
                    } else {
                        match flags {
                            FKM_FLAG_LEGACY => {
                                if payload.len() % FILEKEY_MAP_REC_SIZE != 0 {
                                    tracing::warn!(
                                        "MmapIndex: legacy file_key_map length not aligned, falling back"
                                    );
                                } else {
                                    return Self::lookup_docid_in_legacy_map(payload, key);
                                }
                            }
                            FKM_FLAG_RKYV => {
                                #[cfg(feature = "rkyv")]
                                {
                                    return self.lookup_docid_in_rkyv_map(payload, key);
                                }
                                #[cfg(not(feature = "rkyv"))]
                                {
                                    tracing::info!(
                                        "MmapIndex: rkyv file_key_map found but rkyv feature disabled; falling back"
                                    );
                                }
                            }
                            _ => {
                                tracing::warn!(
                                    "MmapIndex: unknown file_key_map flags {}, falling back",
                                    flags
                                );
                            }
                        }
                    }

                    // magic 命中但无法解析：走 fallback（不要尝试当 legacy 裸表解析，避免误读）。
                } else if bytes.len() % FILEKEY_MAP_REC_SIZE != 0 {
                    tracing::warn!(
                        "MmapIndex: legacy file_key_map length not aligned, falling back"
                    );
                } else {
                    // 旧段：裸 legacy 表
                    return Self::lookup_docid_in_legacy_map(bytes, key);
                }
            }
        }

        // 旧段兼容：按需构建一次 bytes map（live keys only）
        let cached = { self.filekey_map_cache.lock().clone() };
        if let Some(arc) = cached {
            return Self::lookup_docid_in_legacy_map(arc.as_slice(), key);
        }

        tracing::info!("MmapIndex: missing/empty file_key_map, building fallback cache");

        let tomb = self.tombstones();
        let n = (self.snap.metas_bytes().len() / META_REC_SIZE) as u32;
        let mut pairs: Vec<(FileKey, u32)> = Vec::with_capacity(n as usize);
        for docid in 0..n {
            if tomb.contains(docid) {
                continue;
            }
            let Some((file_key, ..)) = self.meta_at(docid) else {
                continue;
            };
            pairs.push((file_key, docid));
        }
        pairs.sort_unstable_by_key(|(k, _)| (k.dev, k.ino));
        let mut bytes = Vec::with_capacity(pairs.len() * FILEKEY_MAP_REC_SIZE);
        for (k, docid) in pairs {
            bytes.extend_from_slice(&k.dev.to_le_bytes());
            bytes.extend_from_slice(&k.ino.to_le_bytes());
            bytes.extend_from_slice(&docid.to_le_bytes());
        }
        let arc = Arc::new(bytes);
        *self.filekey_map_cache.lock() = Some(arc.clone());
        Self::lookup_docid_in_legacy_map(arc.as_slice(), key)
    }

    fn posting_for_trigram(&self, tri: [u8; 3]) -> Option<RoaringBitmap> {
        let table = self.snap.trigram_table_bytes();
        let n = table.len() / TRI_REC_SIZE;
        if n == 0 {
            return Some(RoaringBitmap::new());
        }

        let mut lo = 0usize;
        let mut hi = n;
        while lo < hi {
            let mid = (lo + hi) / 2;
            let off = mid * TRI_REC_SIZE;
            let rec = &table[off..off + TRI_REC_SIZE];
            let key = [rec[0], rec[1], rec[2]];
            if key < tri {
                lo = mid + 1;
            } else {
                hi = mid;
            }
        }
        if lo >= n {
            return Some(RoaringBitmap::new());
        }
        let off = lo * TRI_REC_SIZE;
        let rec = &table[off..off + TRI_REC_SIZE];
        let key = [rec[0], rec[1], rec[2]];
        if key != tri {
            return Some(RoaringBitmap::new());
        }

        let posting_off = u32::from_le_bytes(rec[4..8].try_into().ok()?) as usize;
        let posting_len = u32::from_le_bytes(rec[8..12].try_into().ok()?) as usize;
        let blob = self.snap.postings_blob_bytes();
        let bytes = blob.get(posting_off..posting_off + posting_len)?;
        let mut cur = Cursor::new(bytes);
        RoaringBitmap::deserialize_from(&mut cur).ok()
    }

    fn trigram_key_exists(&self, tri: [u8; 3]) -> bool {
        let table = self.snap.trigram_table_bytes();
        let n = table.len() / TRI_REC_SIZE;
        if n == 0 {
            return false;
        }

        let mut lo = 0usize;
        let mut hi = n;
        while lo < hi {
            let mid = (lo + hi) / 2;
            let off = mid * TRI_REC_SIZE;
            let rec = &table[off..off + TRI_REC_SIZE];
            let key = [rec[0], rec[1], rec[2]];
            if key < tri {
                lo = mid + 1;
            } else {
                hi = mid;
            }
        }
        if lo >= n {
            return false;
        }
        let off = lo * TRI_REC_SIZE;
        let rec = &table[off..off + TRI_REC_SIZE];
        let key = [rec[0], rec[1], rec[2]];
        key == tri
    }

    fn has_trigram_sentinel(&self) -> bool {
        // 哨兵 key：b"\0\0\0"
        self.trigram_key_exists([0, 0, 0])
    }

    fn hint_trigrams(hint: &[u8]) -> Vec<[u8; 3]> {
        let lower = String::from_utf8_lossy(hint).to_lowercase();
        let bytes = lower.as_bytes();
        if bytes.len() < 3 {
            return Vec::new();
        }
        bytes.windows(3).map(|w| [w[0], w[1], w[2]]).collect()
    }

    fn trigram_candidates(&self, matcher: &dyn Matcher) -> Option<RoaringBitmap> {
        let hint = matcher.literal_hint()?;
        let tris = Self::hint_trigrams(hint);
        if tris.is_empty() {
            return None;
        }

        let mut bitmaps: Vec<RoaringBitmap> = Vec::with_capacity(tris.len());
        for tri in tris {
            bitmaps.push(self.posting_for_trigram(tri)?);
        }
        bitmaps.sort_by_key(|b| b.len());
        let mut iter = bitmaps.into_iter();
        let mut acc = iter.next().unwrap_or_else(RoaringBitmap::new);
        for b in iter {
            acc &= &b;
            if acc.is_empty() {
                break;
            }
        }
        Some(acc)
    }

    pub fn query_keys(&self, matcher: &dyn Matcher) -> Vec<FileKey> {
        // 能力感知：无哨兵视为旧段（trigram 不覆盖目录组件），禁用预过滤避免假阴性。
        let candidates = if self.has_trigram_sentinel() {
            self.trigram_candidates(matcher)
        } else {
            None
        };

        let tomb = self.tombstones();
        let arena = self.snap.path_arena_bytes();

        let mut out = Vec::new();

        if let Some(bm) = candidates {
            for docid in bm.iter() {
                if tomb.contains(docid) {
                    continue;
                }
                let Some((file_key, root_id, path_off, path_len, ..)) = self.meta_at(docid) else {
                    continue;
                };
                let start = path_off as usize;
                let end = start.saturating_add(path_len as usize);
                let Some(rel) = arena.get(start..end) else {
                    continue;
                };
                let abs = self.compose_abs_path_bytes(root_id, rel);
                let s = std::str::from_utf8(&abs)
                    .map(std::borrow::Cow::Borrowed)
                    .unwrap_or_else(|_| String::from_utf8_lossy(&abs));
                if matcher.matches(&s) {
                    out.push(file_key);
                }
            }
            return out;
        }

        let n = (self.snap.metas_bytes().len() / META_REC_SIZE) as u32;
        for docid in 0..n {
            if tomb.contains(docid) {
                continue;
            }
            let Some((file_key, root_id, path_off, path_len, ..)) = self.meta_at(docid) else {
                continue;
            };
            let start = path_off as usize;
            let end = start.saturating_add(path_len as usize);
            let Some(rel) = arena.get(start..end) else {
                continue;
            };
            let abs = self.compose_abs_path_bytes(root_id, rel);
            let s = std::str::from_utf8(&abs)
                .map(std::borrow::Cow::Borrowed)
                .unwrap_or_else(|_| String::from_utf8_lossy(&abs));
            if matcher.matches(&s) {
                out.push(file_key);
            }
        }

        out
    }

    pub fn get_meta_by_key(&self, key: FileKey) -> Option<FileMeta> {
        let docid = self.lookup_docid_by_filekey(key)?;
        let tomb = self.tombstones();
        if tomb.contains(docid) {
            return None;
        }

        let arena = self.snap.path_arena_bytes();
        let Some((file_key, root_id, path_off, path_len, size, mtime)) = self.meta_at(docid) else {
            return None;
        };
        let start = path_off as usize;
        let end = start.saturating_add(path_len as usize);
        let rel = arena.get(start..end)?;
        let path = self.compose_abs_path(root_id, rel);
        Some(FileMeta {
            file_key,
            path,
            size,
            mtime,
        })
    }

    pub fn query(&self, matcher: &dyn Matcher) -> Vec<FileMeta> {
        // 能力感知：无哨兵视为旧段（trigram 不覆盖目录组件），禁用预过滤避免假阴性。
        let candidates = if self.has_trigram_sentinel() {
            self.trigram_candidates(matcher)
        } else {
            None
        };

        let tomb = self.tombstones();
        let arena = self.snap.path_arena_bytes();

        let mut out = Vec::new();

        if let Some(bm) = candidates {
            for docid in bm.iter() {
                if tomb.contains(docid) {
                    continue;
                }
                let Some((file_key, root_id, path_off, path_len, size, mtime)) =
                    self.meta_at(docid)
                else {
                    continue;
                };

                let start = path_off as usize;
                let end = start.saturating_add(path_len as usize);
                let Some(rel) = arena.get(start..end) else {
                    continue;
                };

                let path = self.compose_abs_path(root_id, rel);
                let s = path.to_string_lossy();
                if !matcher.matches(&s) {
                    continue;
                }

                out.push(FileMeta {
                    file_key,
                    path,
                    size,
                    mtime,
                });
            }
            return out;
        }

        let n = (self.snap.metas_bytes().len() / META_REC_SIZE) as u32;
        for docid in 0..n {
            if tomb.contains(docid) {
                continue;
            }
            let Some((file_key, root_id, path_off, path_len, size, mtime)) = self.meta_at(docid)
            else {
                continue;
            };

            let start = path_off as usize;
            let end = start.saturating_add(path_len as usize);
            let Some(rel) = arena.get(start..end) else {
                continue;
            };

            let path = self.compose_abs_path(root_id, rel);
            let s = path.to_string_lossy();
            if !matcher.matches(&s) {
                continue;
            }

            out.push(FileMeta {
                file_key,
                path,
                size,
                mtime,
            });
        }

        out
    }

    pub fn for_each_live_meta(&self, mut f: impl FnMut(FileMeta)) {
        let tomb = self.tombstones();
        let arena = self.snap.path_arena_bytes();
        let n = (self.snap.metas_bytes().len() / META_REC_SIZE) as u32;
        for docid in 0..n {
            if tomb.contains(docid) {
                continue;
            }
            let Some((file_key, root_id, path_off, path_len, size, mtime)) = self.meta_at(docid)
            else {
                continue;
            };
            let start = path_off as usize;
            let end = start.saturating_add(path_len as usize);
            let Some(rel) = arena.get(start..end) else {
                continue;
            };
            let path = self.compose_abs_path(root_id, rel);
            f(FileMeta {
                file_key,
                path,
                size,
                mtime,
            });
        }
    }

    pub fn roots_match(&self, expected_roots: &[PathBuf]) -> bool {
        use std::os::unix::ffi::OsStrExt;
        let encoded = {
            let mut roots = expected_roots.to_vec();
            roots.sort_by(|a, b| a.as_os_str().as_bytes().cmp(b.as_os_str().as_bytes()));
            roots.dedup();
            roots.retain(|p| p != Path::new("/"));
            roots.insert(0, PathBuf::from("/"));
            roots
                .into_iter()
                .map(|p| p.as_os_str().as_bytes().to_vec())
                .collect::<Vec<_>>()
        };
        self.snap.roots == encoded
    }
}

impl IndexLayer for MmapIndex {
    fn query_keys(&self, matcher: &dyn Matcher) -> Vec<FileKey> {
        self.query_keys(matcher)
    }

    fn get_meta(&self, key: FileKey) -> Option<FileMeta> {
        self.get_meta_by_key(key)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::index::PersistentIndex;
    use crate::query::matcher::create_matcher;
    use crate::storage::snapshot::SnapshotStore;
    use std::collections::HashMap;

    fn unique_tmp_dir(tag: &str) -> PathBuf {
        let nanos = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        std::env::temp_dir().join(format!("fd-rdd-mmap-{}-{}", tag, nanos))
    }

    #[tokio::test]
    async fn missing_file_key_map_range_falls_back_and_returns_correct_meta() {
        let root = unique_tmp_dir("no-filekey-map");
        std::fs::create_dir_all(&root).unwrap();

        let idx = PersistentIndex::new_with_roots(vec![root.clone()]);
        let p = root.join("alpha_test.txt");
        std::fs::write(&p, b"a").unwrap();
        idx.upsert(FileMeta {
            file_key: FileKey { dev: 1, ino: 1 },
            path: p.clone(),
            size: 1,
            mtime: None,
        });

        let store = SnapshotStore::new(root.join("index.db"));
        let segs = idx.export_segments_v6();
        store.write_atomic_v6(&segs).await.unwrap();

        let mut snap = store
            .load_v6_mmap_if_valid(&[root.clone()])
            .unwrap()
            .expect("load v6");

        // 人工制造“缺失 FileKeyMap”的段：强行把 range 设为 0..0。
        snap.file_key_map = Some(0..0);

        let mmap_idx = MmapIndex::new(snap);

        // 预期：触发 lookup_docid_by_filekey 的 fallback 路径，并返回正确 meta。
        let meta = mmap_idx
            .get_meta_by_key(FileKey { dev: 1, ino: 1 })
            .expect("get_meta_by_key");
        assert!(meta.path.to_string_lossy().contains("alpha_test"));
    }

    fn build_basename_only_trigram_segments(docs: &[(u32, &PathBuf)]) -> (Vec<u8>, Vec<u8>) {
        let mut tri_idx: HashMap<[u8; 3], RoaringBitmap> = HashMap::new();

        for (docid, path) in docs {
            let name = path
                .file_name()
                .map(|s| s.to_string_lossy().to_string())
                .unwrap_or_else(|| path.to_string_lossy().to_string());
            let lower = name.to_lowercase();
            let bytes = lower.as_bytes();
            if bytes.len() < 3 {
                continue;
            }
            for w in bytes.windows(3) {
                let tri = [w[0], w[1], w[2]];
                tri_idx
                    .entry(tri)
                    .or_insert_with(RoaringBitmap::new)
                    .insert(*docid);
            }
        }

        let mut postings_blob_bytes: Vec<u8> = Vec::new();
        let mut entries: Vec<([u8; 3], u32, u32)> = Vec::with_capacity(tri_idx.len());
        for (tri, posting) in tri_idx.iter() {
            let mut buf = Vec::new();
            posting.serialize_into(&mut buf).expect("write to vec");
            let off: u32 = postings_blob_bytes.len().try_into().unwrap_or(u32::MAX);
            let len: u32 = buf.len().try_into().unwrap_or(u32::MAX);
            postings_blob_bytes.extend_from_slice(&buf);
            entries.push((*tri, off, len));
        }
        entries.sort_by_key(|(tri, _, _)| *tri);

        let mut trigram_table_bytes = Vec::with_capacity(entries.len() * TRI_REC_SIZE);
        for (tri, off, len) in entries {
            trigram_table_bytes.extend_from_slice(&tri);
            trigram_table_bytes.push(0); // pad
            trigram_table_bytes.extend_from_slice(&off.to_le_bytes());
            trigram_table_bytes.extend_from_slice(&len.to_le_bytes());
        }

        (trigram_table_bytes, postings_blob_bytes)
    }

    #[tokio::test]
    async fn sentinel_absent_forces_full_scan_to_avoid_false_negative() {
        let root = unique_tmp_dir("sentinel-old");
        std::fs::create_dir_all(&root).unwrap();

        let dir_src = root.join("src");
        std::fs::create_dir_all(&dir_src).unwrap();

        let p_dir_hit = dir_src.join("a.txt");
        let p_base_hit = root.join("src_lib.rs");
        std::fs::write(&p_dir_hit, b"a").unwrap();
        std::fs::write(&p_base_hit, b"b").unwrap();

        // 构建索引：两个 doc，docid 依插入顺序为 0/1。
        let idx = PersistentIndex::new_with_roots(vec![root.clone()]);
        idx.upsert(FileMeta {
            file_key: FileKey { dev: 1, ino: 1 },
            path: p_dir_hit.clone(),
            size: 1,
            mtime: None,
        });
        idx.upsert(FileMeta {
            file_key: FileKey { dev: 1, ino: 2 },
            path: p_base_hit.clone(),
            size: 1,
            mtime: None,
        });

        // 模拟“旧段”：仅 basename 建 trigram，且无哨兵 key。
        let mut segs = idx.export_segments_v6();
        let (tri_table, postings_blob) =
            build_basename_only_trigram_segments(&[(0, &p_dir_hit), (1, &p_base_hit)]);
        segs.trigram_table_bytes = tri_table;
        segs.postings_blob_bytes = postings_blob;

        let store = SnapshotStore::new(root.join("index.db"));
        store.write_atomic_v6(&segs).await.unwrap();

        let snap = store
            .load_v6_mmap_if_valid(&[root.clone()])
            .unwrap()
            .expect("load v6");
        let mmap_idx = MmapIndex::new(snap);

        // 查询 "src"：若错误走候选集（basename-only），会漏掉 /src/a.txt（目录段命中）。
        let m = create_matcher("src");
        let r = mmap_idx.query(m.as_ref());
        assert_eq!(r.len(), 2);
        let s0 = r[0].path.to_string_lossy();
        let s1 = r[1].path.to_string_lossy();
        assert!(
            s0.contains("/src/a.txt") || s1.contains("/src/a.txt"),
            "must include dir-segment hit"
        );
        assert!(
            s0.ends_with("src_lib.rs") || s1.ends_with("src_lib.rs"),
            "must include basename hit"
        );
    }
}
