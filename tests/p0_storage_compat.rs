//! P0-1.4 — Storage version compatibility tests.
//!
//! Validates that `SnapshotStore` and `WalStore` correctly handle different
//! on-disk format versions (snapshot v2–v7, WAL v1/v2).
//!
//! NOTE: These tests require a real filesystem (temp dirs).  `cargo check --tests`
//! must pass; `cargo test` may be run separately.

use std::io::Write;
use std::path::PathBuf;

use fd_rdd::storage::snapshot::SnapshotStore;
use fd_rdd::storage::wal::WalStore;

// Re-declare format constants (private in the crate) for test construction.
const SNAPSHOT_MAGIC: u32 = 0xFDDD_0002;
const STATE_COMMITTED: u32 = 0x0000_0001;
const STATE_INCOMPLETE: u32 = 0xFFFF_FFFF;
const HEADER_SIZE: usize = 20; // magic + version + state + data_len + checksum

const WAL_MAGIC: u32 = 0x314C_4157;

fn tmp_dir(tag: &str) -> PathBuf {
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    std::env::temp_dir().join(format!("fd-rdd-compat-{}-{}", tag, nanos))
}

/// Simple checksum matching the crate-internal `SimpleChecksum` algorithm.
fn simple_checksum(data: &[u8]) -> u32 {
    let mut hash: u32 = 0;
    let mut pending = [0u8; 4];
    let mut pending_len = 0usize;
    let mut pos = 0usize;

    // Fill pending from previous iteration
    while pos < data.len() {
        if pending_len < 4 {
            pending[pending_len] = data[pos];
            pending_len += 1;
            pos += 1;
            if pending_len == 4 {
                hash = hash.wrapping_add(u32::from_le_bytes(pending));
                hash = hash.rotate_left(7);
                pending = [0u8; 4];
                pending_len = 0;
            }
        }
    }
    // Finalize remaining
    if pending_len > 0 {
        let mut buf = [0u8; 4];
        buf[..pending_len].copy_from_slice(&pending[..pending_len]);
        hash = hash.wrapping_add(u32::from_le_bytes(buf));
        hash = hash.rotate_left(7);
    }
    hash
}

/// WAL-style simple checksum (different algorithm from snapshot).
fn wal_crc32_simple(data: &[u8]) -> u32 {
    let mut s: u32 = 0;
    for &b in data {
        s = s.wrapping_add(b as u32);
        s = s.rotate_left(3);
    }
    s
}

/// Write a raw snapshot file with the given version and body bytes.
fn write_raw_snapshot(path: &std::path::Path, version: u32, body: &[u8]) {
    let mut f = std::fs::File::create(path).unwrap();
    let checksum = simple_checksum(body);
    let data_len = body.len() as u32;

    let mut header = [0u8; HEADER_SIZE];
    header[0..4].copy_from_slice(&SNAPSHOT_MAGIC.to_le_bytes());
    header[4..8].copy_from_slice(&version.to_le_bytes());
    header[8..12].copy_from_slice(&STATE_COMMITTED.to_le_bytes());
    header[12..16].copy_from_slice(&data_len.to_le_bytes());
    header[16..20].copy_from_slice(&checksum.to_le_bytes());
    f.write_all(&header).unwrap();
    f.write_all(body).unwrap();
    f.sync_all().unwrap();
}

/// Write a raw snapshot file with INCOMPLETE state (should be rejected on load).
fn write_incomplete_snapshot(path: &std::path::Path, version: u32, body: &[u8]) {
    let mut f = std::fs::File::create(path).unwrap();
    let checksum = simple_checksum(body);
    let data_len = body.len() as u32;

    let mut header = [0u8; HEADER_SIZE];
    header[0..4].copy_from_slice(&SNAPSHOT_MAGIC.to_le_bytes());
    header[4..8].copy_from_slice(&version.to_le_bytes());
    header[8..12].copy_from_slice(&STATE_INCOMPLETE.to_le_bytes());
    header[12..16].copy_from_slice(&data_len.to_le_bytes());
    header[16..20].copy_from_slice(&checksum.to_le_bytes());
    f.write_all(&header).unwrap();
    f.write_all(body).unwrap();
    f.sync_all().unwrap();
}

// =========================================================================
// Snapshot version compat tests
// =========================================================================

/// A missing snapshot file should return Ok(None), not an error.
#[tokio::test]
async fn snapshot_missing_file_returns_none() {
    let dir = tmp_dir("snap-missing");
    std::fs::create_dir_all(&dir).unwrap();
    let store = SnapshotStore::new(dir.join("index.db"));
    let result = store.load_if_valid().await.unwrap();
    assert!(result.is_none());
}

/// A snapshot with unknown version should be rejected (Ok(None)).
#[tokio::test]
async fn snapshot_unknown_version_returns_none() {
    let dir = tmp_dir("snap-unknown-ver");
    std::fs::create_dir_all(&dir).unwrap();
    let db_path = dir.join("index.db");
    // version 99 is not recognized
    write_raw_snapshot(&db_path, 99, b"dummy-body-data");
    let store = SnapshotStore::new(db_path);
    let result = store.load_if_valid().await.unwrap();
    assert!(result.is_none());
}

/// A snapshot with INCOMPLETE state should be rejected.
#[tokio::test]
async fn snapshot_incomplete_state_returns_none() {
    let dir = tmp_dir("snap-incomplete");
    std::fs::create_dir_all(&dir).unwrap();
    let db_path = dir.join("index.db");
    write_incomplete_snapshot(&db_path, 5, b"some-body");
    let store = SnapshotStore::new(db_path);
    let result = store.load_if_valid().await.unwrap();
    assert!(result.is_none());
}

/// A snapshot with wrong magic should be rejected.
#[tokio::test]
async fn snapshot_wrong_magic_returns_none() {
    let dir = tmp_dir("snap-bad-magic");
    std::fs::create_dir_all(&dir).unwrap();
    let db_path = dir.join("index.db");

    let body = b"test-body";
    let mut f = std::fs::File::create(&db_path).unwrap();
    let mut header = [0u8; HEADER_SIZE];
    header[0..4].copy_from_slice(&0xDEAD_BEEFu32.to_le_bytes()); // wrong magic
    header[4..8].copy_from_slice(&5u32.to_le_bytes());
    header[8..12].copy_from_slice(&STATE_COMMITTED.to_le_bytes());
    header[12..16].copy_from_slice(&(body.len() as u32).to_le_bytes());
    header[16..20].copy_from_slice(&simple_checksum(body).to_le_bytes());
    f.write_all(&header).unwrap();
    f.write_all(body).unwrap();
    drop(f);

    let store = SnapshotStore::new(db_path);
    let result = store.load_if_valid().await.unwrap();
    assert!(result.is_none());
}

/// A snapshot with corrupted checksum should be rejected.
#[tokio::test]
async fn snapshot_bad_checksum_returns_none() {
    let dir = tmp_dir("snap-bad-crc");
    std::fs::create_dir_all(&dir).unwrap();
    let db_path = dir.join("index.db");

    let body = b"test-body-data";
    let mut f = std::fs::File::create(&db_path).unwrap();
    let mut header = [0u8; HEADER_SIZE];
    header[0..4].copy_from_slice(&SNAPSHOT_MAGIC.to_le_bytes());
    header[4..8].copy_from_slice(&5u32.to_le_bytes());
    header[8..12].copy_from_slice(&STATE_COMMITTED.to_le_bytes());
    header[12..16].copy_from_slice(&(body.len() as u32).to_le_bytes());
    header[16..20].copy_from_slice(&0xDEAD_BEEFu32.to_le_bytes()); // wrong checksum
    f.write_all(&header).unwrap();
    f.write_all(body).unwrap();
    drop(f);

    let store = SnapshotStore::new(db_path);
    let result = store.load_if_valid().await.unwrap();
    assert!(result.is_none());
}

/// A truncated snapshot (header says more data than file contains) should be rejected.
#[tokio::test]
async fn snapshot_truncated_body_returns_none() {
    let dir = tmp_dir("snap-truncated");
    std::fs::create_dir_all(&dir).unwrap();
    let db_path = dir.join("index.db");

    let body = b"short";
    let mut f = std::fs::File::create(&db_path).unwrap();
    let mut header = [0u8; HEADER_SIZE];
    header[0..4].copy_from_slice(&SNAPSHOT_MAGIC.to_le_bytes());
    header[4..8].copy_from_slice(&5u32.to_le_bytes());
    header[8..12].copy_from_slice(&STATE_COMMITTED.to_le_bytes());
    header[12..16].copy_from_slice(&9999u32.to_le_bytes()); // claims 9999 bytes
    header[16..20].copy_from_slice(&simple_checksum(body).to_le_bytes());
    f.write_all(&header).unwrap();
    f.write_all(body).unwrap(); // only 5 bytes
    drop(f);

    let store = SnapshotStore::new(db_path);
    let result = store.load_if_valid().await.unwrap();
    assert!(result.is_none());
}

/// v6/v7 snapshots should NOT be loaded via load_if_valid (they use the mmap path).
#[tokio::test]
async fn snapshot_v6_v7_skipped_by_load_if_valid() {
    let dir = tmp_dir("snap-v6v7-skip");
    std::fs::create_dir_all(&dir).unwrap();

    for ver in [6u32, 7] {
        let db_path = dir.join(format!("index-v{}.db", ver));
        write_raw_snapshot(&db_path, ver, b"v6-or-v7-body");
        let store = SnapshotStore::new(db_path);
        let result = store.load_if_valid().await.unwrap();
        assert!(result.is_none(), "v{} should be skipped by load_if_valid", ver);
    }
}

/// v6 mmap load with no file should return Ok(None).
#[test]
fn snapshot_v6_mmap_missing_returns_none() {
    let dir = tmp_dir("snap-v6-missing");
    std::fs::create_dir_all(&dir).unwrap();
    let store = SnapshotStore::new(dir.join("index.db"));
    let result = store.load_v6_mmap_if_valid(&[dir]).unwrap();
    assert!(result.is_none());
}

// =========================================================================
// WAL version compat tests
// =========================================================================

/// A fresh WAL directory should replay zero events.
#[test]
fn wal_fresh_dir_replays_empty() {
    let dir = tmp_dir("wal-fresh");
    let wal = WalStore::open_in_dir(dir).unwrap();
    let r = wal.replay_since_seal(0).unwrap();
    assert!(r.events.is_empty());
    assert_eq!(r.sealed_used, 0);
    assert_eq!(r.truncated_tail_records, 0);
}

/// A WAL file with garbage content should be handled gracefully (truncated).
#[test]
fn wal_garbage_file_handled_gracefully() {
    let dir = tmp_dir("wal-garbage");
    std::fs::create_dir_all(&dir).unwrap();

    // Write a valid header but garbage records
    let wal_path = dir.join("events.wal");
    {
        let mut f = std::fs::File::create(&wal_path).unwrap();
        f.write_all(&WAL_MAGIC.to_le_bytes()).unwrap();
        f.write_all(&2u32.to_le_bytes()).unwrap(); // v2
        f.write_all(b"this-is-not-a-valid-record-at-all").unwrap();
        f.flush().unwrap();
    }

    let wal = WalStore::open_in_dir(dir).unwrap();
    let r = wal.replay_since_seal(0).unwrap();
    // Should not panic; may have 0 events + some truncated
    assert!(r.events.is_empty() || r.truncated_tail_records > 0);
}

/// A WAL file with wrong magic should be re-initialized (no crash).
#[test]
fn wal_wrong_magic_reinitialized() {
    let dir = tmp_dir("wal-bad-magic");
    std::fs::create_dir_all(&dir).unwrap();

    let wal_path = dir.join("events.wal");
    std::fs::write(&wal_path, b"NOT-A-WAL-FILE-AT-ALL").unwrap();

    // open_in_dir should handle this gracefully (reinit)
    let wal = WalStore::open_in_dir(dir).unwrap();
    let r = wal.replay_since_seal(0).unwrap();
    assert!(r.events.is_empty());
}

/// Construct a v1 WAL file manually, open WalStore (triggers v1→v2 upgrade),
/// and verify replay can read the sealed v1 events.
#[test]
fn wal_v1_to_v2_upgrade_preserves_events() {
    let dir = tmp_dir("wal-v1-upgrade");
    std::fs::create_dir_all(&dir).unwrap();

    let wal_path = dir.join("events.wal");
    {
        let mut f = std::fs::File::create(&wal_path).unwrap();
        // v1 header
        f.write_all(&WAL_MAGIC.to_le_bytes()).unwrap();
        f.write_all(&1u32.to_le_bytes()).unwrap();

        // Construct a minimal v1 event record:
        // kind(1) + secs(8) + nanos(4) + path_len(4) + path + from_len(4)
        let mut payload = Vec::new();
        payload.push(1u8); // Create
        payload.extend_from_slice(&100u64.to_le_bytes()); // secs
        payload.extend_from_slice(&0u32.to_le_bytes()); // nanos
        let path_bytes = b"/tmp/test.txt";
        payload.extend_from_slice(&(path_bytes.len() as u32).to_le_bytes());
        payload.extend_from_slice(path_bytes);
        payload.extend_from_slice(&0u32.to_le_bytes()); // from_len = 0

        let len = payload.len() as u32;
        let crc = wal_crc32_simple(&payload);
        f.write_all(&len.to_le_bytes()).unwrap();
        f.write_all(&crc.to_le_bytes()).unwrap();
        f.write_all(&payload).unwrap();
        f.flush().unwrap();
    }

    // Opening should trigger v1→v2 upgrade (rename to sealed)
    let wal = WalStore::open_in_dir(dir.clone()).unwrap();

    // Verify a sealed v1 file was created
    let has_sealed_v1 = std::fs::read_dir(&dir)
        .unwrap()
        .filter_map(|e| e.ok())
        .any(|e| {
            e.path()
                .file_name()
                .and_then(|n| n.to_str())
                .map(|s| s.starts_with("events.wal.seal-") && s.contains(".v1"))
                .unwrap_or(false)
        });
    assert!(has_sealed_v1, "v1 WAL should be sealed during upgrade");

    // Replay should recover the v1 event
    let r = wal.replay_since_seal(0).unwrap();
    assert_eq!(r.events.len(), 1, "should replay 1 event from v1 WAL");
}

/// Seal + cleanup: sealed WALs with id ≤ checkpoint should be cleaned up.
#[test]
fn wal_seal_and_cleanup() {
    let dir = tmp_dir("wal-seal-cleanup");
    let wal = WalStore::open_in_dir(dir).unwrap();

    let seal1 = wal.seal().unwrap();
    assert!(seal1 > 0);

    let seal2 = wal.seal().unwrap();
    assert!(seal2 > seal1);

    // Cleanup up to seal1 — seal2 should survive
    wal.cleanup_sealed_up_to(seal1).unwrap();

    let r = wal.replay_since_seal(0).unwrap();
    // seal1 was cleaned, only seal2 (empty) + current (empty) remain
    assert!(r.events.is_empty());
}

// =========================================================================
// Trait object tests — verify traits are object-safe
// =========================================================================

use fd_rdd::storage::traits::{SegmentStore, WriteAheadLog};

/// SegmentStore should be usable as a trait object.
#[test]
fn segment_store_trait_object_compiles() {
    let dir = tmp_dir("trait-seg");
    std::fs::create_dir_all(&dir).unwrap();
    let store = SnapshotStore::new(dir.join("index.db"));
    let dyn_store: &dyn SegmentStore = &store;
    // Just verify the trait methods are callable
    let _ = dyn_store.path();
    let _ = dyn_store.derived_lsm_dir_path();
}

/// WriteAheadLog should be usable as a trait object.
#[test]
fn write_ahead_log_trait_object_compiles() {
    let dir = tmp_dir("trait-wal");
    let wal = WalStore::open_in_dir(dir).unwrap();
    let dyn_wal: &dyn WriteAheadLog = &wal;
    let _ = dyn_wal.dir();
    let r = dyn_wal.replay_since_seal(0).unwrap();
    assert!(r.events.is_empty());
}
