use crate::storage::checksum::{crc32c_checksum, simple_checksum};
use crate::storage::snapshot::SnapshotStore;
use crate::storage::snapshot_v6::MmapSnapshotV6;
use std::collections::HashSet;
use std::io::{Read, Write};
use std::path::{Path, PathBuf};

// ─────────────────────────────────────────────────────────────────────────────
// LSM directory layout (Manifest + segments)
// ─────────────────────────────────────────────────────────────────────────────

const LSM_MANIFEST_MAGIC: u32 = 0x314D_534C; // "LSM1" little-endian
const LSM_MANIFEST_VERSION: u32 = 4;
const LSM_MANIFEST_HEADER_SIZE: usize = 4 + 4 + 4 + 4; // magic + ver + body_len + checksum

// Safety guards: these values are read from disk; cap to avoid OOM on corrupted files.
const MAX_LSM_MANIFEST_BODY_BYTES: usize = 16 * 1024 * 1024; // 16 MiB
const MAX_LSM_DELETED_PATHS: usize = 500_000;
const MAX_LSM_DELETED_TOTAL_BYTES: usize = 256 * 1024 * 1024; // 256 MiB

#[derive(Clone, Debug, Default)]
pub struct LsmManifest {
    pub next_id: u64,
    pub base_id: u64,
    pub delta_ids: Vec<u64>,
    pub wal_seal_id: u64,
    /// 上次认为"索引与磁盘现实一致"的时间戳（Unix epoch nanos）。
    ///
    /// 用途：冷启动时用于检测停机期间的离线变更（目录 mtime crawl）。
    pub last_build_ns: u64,
}

#[derive(Clone, Debug)]
pub struct LsmSegmentLoaded {
    pub id: u64,
    pub snap: MmapSnapshotV6,
    pub deleted_paths: Vec<Vec<u8>>,
}

#[derive(Clone, Debug)]
pub struct LsmLoadedLayers {
    pub base: Option<LsmSegmentLoaded>,
    pub deltas: Vec<LsmSegmentLoaded>,
    pub wal_seal_id: u64,
}

pub fn lsm_encode_manifest_body(m: &LsmManifest) -> Vec<u8> {
    let mut out = Vec::with_capacity(8 + 8 + 4 + m.delta_ids.len() * 8 + 8 + 8);
    out.extend_from_slice(&m.next_id.to_le_bytes());
    out.extend_from_slice(&m.base_id.to_le_bytes());
    let n: u32 = m.delta_ids.len().try_into().unwrap_or(u32::MAX);
    out.extend_from_slice(&n.to_le_bytes());
    for &id in m.delta_ids.iter().take(n as usize) {
        out.extend_from_slice(&id.to_le_bytes());
    }
    out.extend_from_slice(&m.wal_seal_id.to_le_bytes());
    out.extend_from_slice(&m.last_build_ns.to_le_bytes());
    out
}

pub fn lsm_decode_manifest_body(body: &[u8]) -> anyhow::Result<LsmManifest> {
    if body.len() < 8 + 8 + 4 {
        anyhow::bail!("LSM manifest body too small");
    }
    let next_id = u64::from_le_bytes(body[0..8].try_into()?);
    let base_id = u64::from_le_bytes(body[8..16].try_into()?);
    let n = u32::from_le_bytes(body[16..20].try_into()?) as usize;
    let max_n = body.len().saturating_sub(20) / 8;
    if n > max_n {
        anyhow::bail!("LSM manifest body truncated");
    }
    let mut delta_ids = Vec::with_capacity(n);
    let mut off = 20;
    for _ in 0..n {
        if off + 8 > body.len() {
            anyhow::bail!("LSM manifest body truncated");
        }
        let id = u64::from_le_bytes(body[off..off + 8].try_into()?);
        delta_ids.push(id);
        off += 8;
    }
    // v2: trailing wal_seal_id；v1: missing -> 0
    let wal_seal_id = if off + 8 <= body.len() {
        let v = u64::from_le_bytes(body[off..off + 8].try_into()?);
        off += 8;
        v
    } else {
        0
    };
    // v3: trailing last_build_ns；v2/v1: missing -> 0
    let last_build_ns = if off + 8 <= body.len() {
        u64::from_le_bytes(body[off..off + 8].try_into()?)
    } else {
        0
    };
    Ok(LsmManifest {
        next_id,
        base_id,
        delta_ids,
        wal_seal_id,
        last_build_ns,
    })
}

pub fn lsm_read_manifest(path: &Path) -> anyhow::Result<LsmManifest> {
    let mut f = std::fs::File::open(path)?;
    let mut hdr = [0u8; LSM_MANIFEST_HEADER_SIZE];
    f.read_exact(&mut hdr)?;
    let magic = u32::from_le_bytes(hdr[0..4].try_into()?);
    let ver = u32::from_le_bytes(hdr[4..8].try_into()?);
    let body_len = u32::from_le_bytes(hdr[8..12].try_into()?) as usize;
    let checksum = u32::from_le_bytes(hdr[12..16].try_into()?);
    if magic != LSM_MANIFEST_MAGIC
        || !(ver == 1 || ver == 2 || ver == 3 || ver == LSM_MANIFEST_VERSION)
    {
        anyhow::bail!("LSM manifest magic/version mismatch");
    }
    if body_len > MAX_LSM_MANIFEST_BODY_BYTES {
        anyhow::bail!("LSM manifest body too large: {}", body_len);
    }
    let file_len = f.metadata().map(|m| m.len() as usize).unwrap_or(usize::MAX);
    let remaining = file_len.saturating_sub(LSM_MANIFEST_HEADER_SIZE);
    if body_len > remaining {
        anyhow::bail!("LSM manifest body truncated");
    }
    let mut body = vec![0u8; body_len];
    f.read_exact(&mut body)?;

    // v4+ uses CRC32C; v1-v3 use legacy SimpleChecksum
    let checksum_ok = if ver >= 4 {
        crc32c_checksum(&body) == checksum
    } else if simple_checksum(&body) == checksum {
        tracing::warn!(
            "Loading legacy LSM manifest v{} from {}; consider upgrading to v{} (CRC32C)",
            ver,
            path.display(),
            LSM_MANIFEST_VERSION
        );
        true
    } else {
        false
    };
    if !checksum_ok {
        anyhow::bail!("LSM manifest checksum mismatch");
    }
    lsm_decode_manifest_body(&body)
}

fn now_unix_nanos() -> u64 {
    use std::time::{SystemTime, UNIX_EPOCH};
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_nanos() as u64)
        .unwrap_or(0)
}

pub fn lsm_write_manifest_atomic(path: &Path, m: &LsmManifest) -> anyhow::Result<()> {
    let body = lsm_encode_manifest_body(m);
    let body_len: u32 = body.len().try_into().unwrap_or(u32::MAX);
    let checksum = crc32c_checksum(&body);

    let tmp = path.with_extension("bin.tmp");
    let mut f = std::fs::File::create(&tmp)?;
    f.write_all(&LSM_MANIFEST_MAGIC.to_le_bytes())?;
    f.write_all(&LSM_MANIFEST_VERSION.to_le_bytes())?;
    f.write_all(&body_len.to_le_bytes())?;
    f.write_all(&checksum.to_le_bytes())?;
    f.write_all(&body)?;
    f.sync_all()?;
    std::fs::rename(&tmp, path)?;
    if let Some(parent) = path.parent() {
        if let Ok(dir) = std::fs::File::open(parent) {
            let _ = dir.sync_all();
        }
    }
    Ok(())
}

/// 解析 LSM segment 文件名中的 id（hex）。
///
/// 支持的形式（含崩溃残留的临时文件）：
/// - `seg-<hex>.db`
/// - `seg-<hex>.del`
/// - `seg-<hex>.db.tmp`
/// - `seg-<hex>.del.tmp`
pub fn parse_lsm_seg_id(name: &str) -> Option<u64> {
    let s = name.strip_prefix("seg-")?;
    let s = s.strip_suffix(".tmp").unwrap_or(s);
    let s = s.strip_suffix(".db").or_else(|| s.strip_suffix(".del"))?;
    if s.is_empty() || s.len() > 16 {
        return None;
    }
    u64::from_str_radix(s, 16).ok()
}

const LSM_DEL_MAGIC: u32 = 0x314C_4544; // "DEL1"
const LSM_DEL_VERSION: u32 = 1;

pub fn lsm_write_deleted_paths_atomic(
    path: &Path,
    deleted_paths: &[Vec<u8>],
) -> anyhow::Result<()> {
    let tmp = path.with_extension("del.tmp");
    let mut f = std::fs::File::create(&tmp)?;
    f.write_all(&LSM_DEL_MAGIC.to_le_bytes())?;
    f.write_all(&LSM_DEL_VERSION.to_le_bytes())?;
    let count: u32 = deleted_paths.len().try_into().unwrap_or(u32::MAX);
    f.write_all(&count.to_le_bytes())?;
    for p in deleted_paths.iter().take(count as usize) {
        let len: u16 = p.len().try_into().unwrap_or(u16::MAX);
        f.write_all(&len.to_le_bytes())?;
        f.write_all(&p[..len as usize])?;
    }
    f.sync_all()?;
    std::fs::rename(&tmp, path)?;
    if let Some(parent) = path.parent() {
        if let Ok(dir) = std::fs::File::open(parent) {
            let _ = dir.sync_all();
        }
    }
    Ok(())
}

pub fn lsm_read_deleted_paths(path: &Path) -> anyhow::Result<Vec<Vec<u8>>> {
    if !path.exists() {
        return Ok(Vec::new());
    }
    let mut f = std::fs::File::open(path)?;
    let mut hdr = [0u8; 12];
    f.read_exact(&mut hdr)?;
    let magic = u32::from_le_bytes(hdr[0..4].try_into()?);
    let ver = u32::from_le_bytes(hdr[4..8].try_into()?);
    let count = u32::from_le_bytes(hdr[8..12].try_into()?) as usize;
    if magic != LSM_DEL_MAGIC || ver != LSM_DEL_VERSION {
        anyhow::bail!("LSM del magic/version mismatch");
    }
    if count > MAX_LSM_DELETED_PATHS {
        anyhow::bail!("LSM del too many paths: {}", count);
    }
    let mut total_bytes = 0usize;
    let mut out = Vec::with_capacity(count);
    for _ in 0..count {
        let mut lenb = [0u8; 2];
        if f.read_exact(&mut lenb).is_err() {
            break;
        }
        let len = u16::from_le_bytes(lenb) as usize;
        total_bytes = total_bytes.saturating_add(len);
        if total_bytes > MAX_LSM_DELETED_TOTAL_BYTES {
            anyhow::bail!("LSM del too large (total bytes exceeded)");
        }
        let mut buf = vec![0u8; len];
        f.read_exact(&mut buf)?;
        out.push(buf);
    }
    Ok(out)
}

impl SnapshotStore {
    fn lsm_manifest_path(&self) -> PathBuf {
        self.lsm_dir_path().join("MANIFEST.bin")
    }

    fn lsm_seg_db_path(&self, id: u64) -> PathBuf {
        self.lsm_dir_path().join(format!("seg-{id:016x}.db"))
    }

    fn lsm_seg_del_path(&self, id: u64) -> PathBuf {
        self.lsm_dir_path().join(format!("seg-{id:016x}.del"))
    }

    /// 读取 LSM manifest 的 last_build_ns（用于冷启动离线变更检测）。
    pub fn lsm_last_build_ns(&self) -> anyhow::Result<Option<u64>> {
        let p = self.lsm_manifest_path();
        if !p.exists() {
            return Ok(None);
        }
        let m = lsm_read_manifest(&p)?;
        Ok(Some(m.last_build_ns))
    }

    /// 读取当前 LSM manifest 的 wal_seal_id（用于 compaction 时保持 checkpoint 不回退）。
    pub fn lsm_manifest_wal_seal_id(&self) -> anyhow::Result<u64> {
        let p = self.lsm_manifest_path();
        if !p.exists() {
            return Ok(0);
        }
        Ok(lsm_read_manifest(&p)?.wal_seal_id)
    }

    /// Compaction 完成后，清理不再被 manifest 引用的旧 segment 文件。
    ///
    /// 说明:
    /// - 只会删除 LSM 目录下形如 `seg-{id:016x}.db` / `seg-{id:016x}.del` 的文件。
    /// - 删除单个文件失败不会中断（避免 compaction 因清理失败而失败）。
    pub fn gc_stale_segments(&self) -> anyhow::Result<usize> {
        let manifest = lsm_read_manifest(&self.lsm_manifest_path())?;
        let live_ids: HashSet<u64> = std::iter::once(manifest.base_id)
            .chain(manifest.delta_ids.iter().copied())
            .collect();

        let mut removed = 0usize;
        for entry in std::fs::read_dir(self.lsm_dir_path())? {
            let entry = entry?;
            let name = entry.file_name();
            let name = name.to_string_lossy();
            let is_tmp = name.ends_with(".tmp");
            if let Some(id) = parse_lsm_seg_id(&name) {
                // `.tmp` 属于崩溃/异常残留，始终视为孤儿文件（即使 id 仍在 manifest 中也应清理）。
                if is_tmp || !live_ids.contains(&id) {
                    let path = entry.path();
                    match std::fs::remove_file(&path) {
                        Ok(()) => removed += 1,
                        Err(e) => {
                            // 删除失败不应阻断 compaction；保守地记录并继续。
                            tracing::warn!("LSM gc stale segment remove failed: {:?}: {}", path, e);
                        }
                    }
                }
            }
        }

        Ok(removed)
    }

    /// LSM：加载目录化 segments（base + delta）。
    pub fn load_lsm_if_valid(
        &self,
        expected_roots: &[PathBuf],
    ) -> anyhow::Result<Option<LsmLoadedLayers>> {
        let mpath = self.lsm_manifest_path();
        if !mpath.exists() {
            return Ok(None);
        }

        let manifest = lsm_read_manifest(&mpath)?;
        let mut base = None;
        if manifest.base_id != 0 {
            let id = manifest.base_id;
            let snap =
                Self::load_v6_mmap_from_path_if_valid(&self.lsm_seg_db_path(id), expected_roots)?;
            let Some(snap) = snap else {
                tracing::warn!(
                    "LSM base segment corrupted or invalid, rejecting LSM: id={}",
                    id
                );
                return Ok(None);
            };
            let deleted_paths = match lsm_read_deleted_paths(&self.lsm_seg_del_path(id)) {
                Ok(v) => v,
                Err(e) => {
                    tracing::warn!(
                        "LSM base tombstone sidecar invalid, rejecting LSM: id={} err={}",
                        id,
                        e
                    );
                    return Ok(None);
                }
            };
            base = Some(LsmSegmentLoaded {
                id,
                snap,
                deleted_paths,
            });
        }

        let mut deltas = Vec::with_capacity(manifest.delta_ids.len());
        for &id in &manifest.delta_ids {
            let snap =
                Self::load_v6_mmap_from_path_if_valid(&self.lsm_seg_db_path(id), expected_roots)?;
            let Some(snap) = snap else {
                tracing::warn!(
                    "LSM delta segment corrupted or invalid, rejecting LSM: id={}",
                    id
                );
                return Ok(None);
            };
            let deleted_paths = match lsm_read_deleted_paths(&self.lsm_seg_del_path(id)) {
                Ok(v) => v,
                Err(e) => {
                    tracing::warn!(
                        "LSM delta tombstone sidecar invalid, rejecting LSM: id={} err={}",
                        id,
                        e
                    );
                    return Ok(None);
                }
            };
            deltas.push(LsmSegmentLoaded {
                id,
                snap,
                deleted_paths,
            });
        }

        Ok(Some(LsmLoadedLayers {
            base,
            deltas,
            wal_seal_id: manifest.wal_seal_id,
        }))
    }

    /// LSM：追加一个 delta segment（v6 + sidecar .del），并更新 manifest。
    pub async fn lsm_append_delta_v6(
        &self,
        segs: &crate::index::l2_partition::V6Segments,
        deleted_paths: &[Vec<u8>],
        expected_roots: &[PathBuf],
        wal_seal_id: u64,
    ) -> anyhow::Result<LsmSegmentLoaded> {
        let dir = self.lsm_dir_path();
        tokio::fs::create_dir_all(&dir).await?;

        // 读取/初始化 manifest
        let mpath = self.lsm_manifest_path();
        let mut manifest = if mpath.exists() {
            lsm_read_manifest(&mpath)?
        } else {
            LsmManifest {
                next_id: 1,
                base_id: 0,
                delta_ids: Vec::new(),
                wal_seal_id: 0,
                last_build_ns: 0,
            }
        };

        let id = manifest.next_id.max(1);
        manifest.next_id = id.saturating_add(1);
        manifest.delta_ids.push(id);
        manifest.wal_seal_id = wal_seal_id;
        manifest.last_build_ns = now_unix_nanos();

        // 先写 segment 与 sidecar；manifest 最后写入（崩溃时最多留下孤儿段）。
        let seg_path = self.lsm_seg_db_path(id);
        SnapshotStore::new(seg_path.clone())
            .write_atomic_v6(segs)
            .await?;
        lsm_write_deleted_paths_atomic(&self.lsm_seg_del_path(id), deleted_paths)?;
        lsm_write_manifest_atomic(&mpath, &manifest)?;

        let snap = Self::load_v6_mmap_from_path_if_valid(&seg_path, expected_roots)?
            .ok_or_else(|| anyhow::anyhow!("LSM: failed to load freshly written segment"))?;
        Ok(LsmSegmentLoaded {
            id,
            snap,
            deleted_paths: deleted_paths.to_vec(),
        })
    }

    /// LSM：用新的 base segment 替换当前"base + 一段 delta 前缀"。
    ///
    /// `expected_prev` 表示本轮将被 compact 掉的层前缀：
    /// - `(base_id, delta_ids_prefix)`
    /// - 当前 manifest 必须仍以该前缀开头
    /// - 未参与本轮 compaction 的 suffix delta 会被原样保留
    pub async fn lsm_replace_base_v6(
        &self,
        segs: &crate::index::l2_partition::V6Segments,
        expected_prev: Option<(u64, Vec<u64>)>,
        expected_roots: &[PathBuf],
        wal_seal_id: u64,
    ) -> anyhow::Result<LsmSegmentLoaded> {
        let dir = self.lsm_dir_path();
        tokio::fs::create_dir_all(&dir).await?;

        let mpath = self.lsm_manifest_path();
        let mut manifest = if mpath.exists() {
            lsm_read_manifest(&mpath)?
        } else {
            LsmManifest {
                next_id: 1,
                base_id: 0,
                delta_ids: Vec::new(),
                wal_seal_id: 0,
                last_build_ns: 0,
            }
        };

        let preserved_suffix = if let Some((base_id, delta_ids_prefix)) = expected_prev {
            let prefix_matches = manifest.base_id == base_id
                && manifest.delta_ids.len() >= delta_ids_prefix.len()
                && manifest.delta_ids[..delta_ids_prefix.len()] == delta_ids_prefix[..];
            if !prefix_matches {
                anyhow::bail!("LSM manifest changed, aborting compaction");
            }
            manifest.delta_ids[delta_ids_prefix.len()..].to_vec()
        } else {
            Vec::new()
        };

        let id = manifest.next_id.max(1);
        manifest.next_id = id.saturating_add(1);
        manifest.base_id = id;
        manifest.delta_ids = preserved_suffix;
        manifest.wal_seal_id = manifest.wal_seal_id.max(wal_seal_id);
        manifest.last_build_ns = now_unix_nanos();

        let seg_path = self.lsm_seg_db_path(id);
        SnapshotStore::new(seg_path.clone())
            .write_atomic_v6(segs)
            .await?;
        lsm_write_deleted_paths_atomic(&self.lsm_seg_del_path(id), &[])?;
        lsm_write_manifest_atomic(&mpath, &manifest)?;

        let snap = Self::load_v6_mmap_from_path_if_valid(&seg_path, expected_roots)?
            .ok_or_else(|| anyhow::anyhow!("LSM: failed to load freshly written base"))?;
        Ok(LsmSegmentLoaded {
            id,
            snap,
            deleted_paths: Vec::new(),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::{FileKey, FileMeta};
    use crate::index::PersistentIndex;
    use crate::test_util::unique_tmp_dir;

    #[tokio::test]
    async fn load_lsm_rejects_partial_delta_set() {
        let root = unique_tmp_dir("lsm-partial");
        std::fs::create_dir_all(&root).unwrap();
        let store = SnapshotStore::new(root.join("index.db"));

        let idx = PersistentIndex::new_with_roots(vec![root.clone()]);
        let p1 = root.join("a.txt");
        std::fs::write(&p1, b"a").unwrap();
        idx.upsert(FileMeta {
            file_key: FileKey { dev: 1, ino: 1 },
            path: p1,
            size: 1,
            mtime: None,
            ctime: None,
            atime: None,
        });
        let base = idx.export_segments_v6();
        store
            .lsm_replace_base_v6(&base, None, std::slice::from_ref(&root), 0)
            .await
            .unwrap();

        let delta_idx = PersistentIndex::new_with_roots(vec![root.clone()]);
        let p2 = root.join("b.txt");
        std::fs::write(&p2, b"b").unwrap();
        delta_idx.upsert(FileMeta {
            file_key: FileKey { dev: 1, ino: 2 },
            path: p2,
            size: 1,
            mtime: None,
            ctime: None,
            atime: None,
        });
        let delta = delta_idx.export_segments_v6();
        let appended = store
            .lsm_append_delta_v6(&delta, &[], std::slice::from_ref(&root), 0)
            .await
            .unwrap();

        std::fs::remove_file(store.lsm_seg_db_path(appended.id)).unwrap();
        let loaded = store
            .load_lsm_if_valid(std::slice::from_ref(&root))
            .unwrap();
        assert!(loaded.is_none());
    }

    #[tokio::test]
    async fn load_lsm_rejects_invalid_del_sidecar() {
        let root = unique_tmp_dir("lsm-del-invalid");
        std::fs::create_dir_all(&root).unwrap();
        let store = SnapshotStore::new(root.join("index.db"));

        let idx = PersistentIndex::new_with_roots(vec![root.clone()]);
        let p1 = root.join("a.txt");
        std::fs::write(&p1, b"a").unwrap();
        idx.upsert(FileMeta {
            file_key: FileKey { dev: 1, ino: 1 },
            path: p1,
            size: 1,
            mtime: None,
            ctime: None,
            atime: None,
        });
        let segs = idx.export_segments_v6();
        let appended = store
            .lsm_append_delta_v6(&segs, &[], std::slice::from_ref(&root), 0)
            .await
            .unwrap();

        std::fs::write(store.lsm_seg_del_path(appended.id), b"bad-sidecar").unwrap();
        let loaded = store
            .load_lsm_if_valid(std::slice::from_ref(&root))
            .unwrap();
        assert!(loaded.is_none());
    }

    #[test]
    fn gc_stale_segments_removes_unreferenced_files() {
        let root = unique_tmp_dir("lsm-gc");
        std::fs::create_dir_all(&root).unwrap();

        let store = SnapshotStore::new(root.join("index.db"));
        std::fs::create_dir_all(store.lsm_dir_path()).unwrap();

        let manifest = LsmManifest {
            next_id: 5,
            base_id: 1,
            delta_ids: vec![2],
            wal_seal_id: 0,
            last_build_ns: 1,
        };
        lsm_write_manifest_atomic(&store.lsm_manifest_path(), &manifest).unwrap();

        // live: 1,2; stale: 3,4
        for id in [1u64, 2, 3, 4] {
            std::fs::write(store.lsm_seg_db_path(id), b"db").unwrap();
            std::fs::write(store.lsm_seg_del_path(id), b"del").unwrap();
        }
        // 崩溃/异常残留的临时文件：即使 id 仍在 manifest 中也应被清理。
        std::fs::write(
            store.lsm_dir_path().join("seg-0000000000000001.db.tmp"),
            b"tmp",
        )
        .unwrap();
        std::fs::write(
            store.lsm_dir_path().join("seg-0000000000000002.del.tmp"),
            b"tmp",
        )
        .unwrap();
        std::fs::write(store.lsm_dir_path().join("unrelated.tmp"), b"x").unwrap();

        let removed = store.gc_stale_segments().unwrap();
        assert_eq!(removed, 6);

        assert!(store.lsm_seg_db_path(1).exists());
        assert!(store.lsm_seg_del_path(1).exists());
        assert!(store.lsm_seg_db_path(2).exists());
        assert!(store.lsm_seg_del_path(2).exists());

        assert!(!store.lsm_seg_db_path(3).exists());
        assert!(!store.lsm_seg_del_path(3).exists());
        assert!(!store.lsm_seg_db_path(4).exists());
        assert!(!store.lsm_seg_del_path(4).exists());

        assert!(store.lsm_dir_path().join("unrelated.tmp").exists());
        assert!(!store
            .lsm_dir_path()
            .join("seg-0000000000000001.db.tmp")
            .exists());
        assert!(!store
            .lsm_dir_path()
            .join("seg-0000000000000002.del.tmp")
            .exists());
    }
}
