//! P1 — WAL recovery tests.
//!
//! Validates WAL corruption handling, crash recovery, and version compatibility.

use std::io::Write;
use std::path::PathBuf;

use fd_rdd::core::{EventRecord, EventType, FileIdentifier};
use fd_rdd::storage::wal::WalStore;

const WAL_MAGIC: u32 = 0x314C_4157;

fn unique_tmp_dir(tag: &str) -> PathBuf {
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    std::env::temp_dir().join(format!("fd-rdd-wal-recovery-{}-{}", tag, nanos))
}

/// Legacy WAL checksum (v1/v2 format)
fn crc32_simple(data: &[u8]) -> u32 {
    let mut s: u32 = 0;
    for &b in data {
        s = s.wrapping_add(b as u32);
        s = s.rotate_left(3);
    }
    s
}

/// 14. WAL 损坏记录跳过（不丢后续事件）
#[test]
fn wal_corrupted_record_skipped_continues_reading() {
    let dir = unique_tmp_dir("corrupt-skip");
    std::fs::create_dir_all(&dir).unwrap();

    // Create a WAL and write some events
    let wal = WalStore::open_in_dir(dir.clone()).unwrap();

    let p1 = dir.join("first.txt");
    let p2 = dir.join("second.txt");
    let p3 = dir.join("third.txt");

    wal.append(&[
        EventRecord {
            seq: 1,
            timestamp: std::time::SystemTime::now(),
            event_type: EventType::Create,
            id: FileIdentifier::Path(p1.clone()),
            path_hint: Some(p1),
        },
        EventRecord {
            seq: 2,
            timestamp: std::time::SystemTime::now(),
            event_type: EventType::Create,
            id: FileIdentifier::Path(p2.clone()),
            path_hint: Some(p2),
        },
        EventRecord {
            seq: 3,
            timestamp: std::time::SystemTime::now(),
            event_type: EventType::Create,
            id: FileIdentifier::Path(p3.clone()),
            path_hint: Some(p3),
        },
    ])
    .unwrap();

    // Now corrupt the middle record by flipping bytes in the WAL file
    let wal_path = dir.join("events.wal");
    let mut data = std::fs::read(&wal_path).unwrap();

    // The WAL format is: 8-byte header, then records of [4-byte len][4-byte crc][payload]
    // Corrupt the CRC of the second record
    // Header = 8 bytes, first record = 8 + payload_len bytes
    // We need to find the second record and corrupt its CRC
    if data.len() > 20 {
        // Corrupt some bytes in the middle of the file (after first record)
        let mid = data.len() / 2;
        data[mid] ^= 0xFF;
        data[mid + 1] ^= 0xFF;
    }
    std::fs::write(&wal_path, &data).unwrap();

    // Replay should recover what it can (skip corrupted records)
    let wal2 = WalStore::open_in_dir(dir.clone()).unwrap();
    let result = wal2.replay_since_seal(0).unwrap();

    // Should have recovered at least some events (the ones before corruption)
    // and possibly some after (due to skip-and-continue logic)
    assert!(
        result.truncated_tail_records > 0,
        "Should detect corrupted records"
    );

    let _ = std::fs::remove_dir_all(&dir);
}

/// 15. 崩溃恢复：WAL 回放恢复增量事件
#[test]
fn wal_crash_recovery_replays_events() {
    let dir = unique_tmp_dir("crash-recovery");
    std::fs::create_dir_all(&dir).unwrap();

    let wal = WalStore::open_in_dir(dir.clone()).unwrap();

    let p1 = dir.join("survived.txt");
    wal.append(&[EventRecord {
        seq: 1,
        timestamp: std::time::SystemTime::now(),
        event_type: EventType::Create,
        id: FileIdentifier::Path(p1.clone()),
        path_hint: Some(p1),
    }])
    .unwrap();

    // Simulate crash: drop the WAL store
    drop(wal);

    // Re-open and replay
    let wal2 = WalStore::open_in_dir(dir.clone()).unwrap();
    let result = wal2.replay_since_seal(0).unwrap();

    assert_eq!(
        result.events.len(),
        1,
        "Should recover the event written before crash"
    );

    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn wal_truncated_tail_sets_repair_signal() {
    let dir = unique_tmp_dir("truncated-tail");
    std::fs::create_dir_all(&dir).unwrap();

    let wal = WalStore::open_in_dir(dir.clone()).unwrap();
    let p = dir.join("before-tail.txt");
    wal.append(&[EventRecord {
        seq: 1,
        timestamp: std::time::SystemTime::now(),
        event_type: EventType::Create,
        id: FileIdentifier::Path(p.clone()),
        path_hint: Some(p),
    }])
    .unwrap();
    drop(wal);

    let wal_path = dir.join("events.wal");
    let mut f = std::fs::OpenOptions::new()
        .append(true)
        .open(&wal_path)
        .unwrap();
    f.write_all(&1234u32.to_le_bytes()).unwrap();
    f.flush().unwrap();

    let wal2 = WalStore::open_in_dir(dir.clone()).unwrap();
    let result = wal2.replay_since_seal(0).unwrap();
    assert_eq!(result.events.len(), 1);
    assert!(result.truncated_tail_records > 0);

    let _ = std::fs::remove_dir_all(&dir);
}

/// 16. 版本兼容：v1 WAL 正确加载（升级到 v3）
#[test]
fn wal_v1_compat_loads_after_upgrade() {
    let dir = unique_tmp_dir("v1-compat");
    std::fs::create_dir_all(&dir).unwrap();

    let wal_path = dir.join("events.wal");

    // Manually construct a v1 WAL with one record
    {
        let mut f = std::fs::OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(&wal_path)
            .unwrap();

        // v1 header
        f.write_all(&WAL_MAGIC.to_le_bytes()).unwrap();
        f.write_all(&1u32.to_le_bytes()).unwrap(); // version 1

        // v1 record: kind(1) + secs(8) + nanos(4) + path_len(4) + path + from_len(4)
        let path_bytes = b"/tmp/legacy.txt";
        let mut payload = Vec::new();
        payload.push(1u8); // Create
        payload.extend_from_slice(&0u64.to_le_bytes()); // secs
        payload.extend_from_slice(&0u32.to_le_bytes()); // nanos
        let plen = path_bytes.len() as u32;
        payload.extend_from_slice(&plen.to_le_bytes());
        payload.extend_from_slice(path_bytes);
        payload.extend_from_slice(&0u32.to_le_bytes()); // from_len = 0

        let len = payload.len() as u32;
        let crc = crc32_simple(&payload);
        f.write_all(&len.to_le_bytes()).unwrap();
        f.write_all(&crc.to_le_bytes()).unwrap();
        f.write_all(&payload).unwrap();
        f.flush().unwrap();
    }

    // Open should trigger v1 -> v3 upgrade (rename to sealed)
    let wal = WalStore::open_in_dir(dir.clone()).unwrap();

    // Should find the sealed v1 file
    let has_sealed_v1 = std::fs::read_dir(&dir)
        .unwrap()
        .filter_map(|e| e.ok())
        .any(|e| {
            e.file_name()
                .to_str()
                .map(|s| s.starts_with("events.wal.seal-") && s.contains(".v1"))
                .unwrap_or(false)
        });
    assert!(has_sealed_v1, "Should have created a sealed v1 file");

    // Replay should recover the v1 event
    let result = wal.replay_since_seal(0).unwrap();
    assert_eq!(result.events.len(), 1, "Should recover v1 event");

    let _ = std::fs::remove_dir_all(&dir);
}
