use std::io::Cursor;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use roaring::RoaringBitmap;

use crate::core::{FileKey, FileMeta};
use crate::query::matcher::Matcher;
use crate::storage::snapshot::MmapSnapshotV6;

// MetaRecordV6：与 PersistentIndex::export_segments_v6 的编码保持一致（LE，40B）
const META_REC_SIZE: usize = 40;
// TrigramEntryV6：3 + 1 + 4 + 4 = 12B
const TRI_REC_SIZE: usize = 12;

pub struct MmapIndex {
    snap: Arc<MmapSnapshotV6>,
    tomb_cache: parking_lot::Mutex<Option<RoaringBitmap>>,
}

impl MmapIndex {
    pub fn new(snap: MmapSnapshotV6) -> Self {
        Self {
            snap: Arc::new(snap),
            tomb_cache: parking_lot::Mutex::new(None),
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

    fn basename_trigrams(s: &str) -> Vec<[u8; 3]> {
        let lower = s.to_lowercase();
        let bytes = lower.as_bytes();
        if bytes.len() < 3 {
            return Vec::new();
        }
        bytes.windows(3).map(|w| [w[0], w[1], w[2]]).collect()
    }

    pub fn query(&self, matcher: &dyn Matcher) -> Vec<FileMeta> {
        let candidates = matcher.prefix().and_then(|p| {
            let tris = Self::basename_trigrams(p);
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
        });

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
