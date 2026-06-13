//! P1 — WAL durability configuration tests.

#[allow(dead_code)]
mod common;

use common::{create_event, unique_tmp_dir};
use fd_rdd::config::Config;
use fd_rdd::storage::wal::{WalDurability, WalStore};

#[test]
fn wal_default_durability_is_flush_only() {
    let dir = unique_tmp_dir("default");
    std::fs::create_dir_all(&dir).unwrap();

    let wal = WalStore::open_in_dir(dir.clone()).unwrap();
    assert_eq!(wal.durability(), WalDurability::FlushOnly);
    assert_eq!(wal.durability().label(), "flush-only");

    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn wal_sync_interval_durability_is_observable_and_replays() {
    let dir = unique_tmp_dir("interval");
    std::fs::create_dir_all(&dir).unwrap();

    let wal = WalStore::open_in_dir(dir.clone()).unwrap();
    wal.set_durability(WalDurability::sync_interval(5, 1));
    assert_eq!(wal.durability().label(), "sync-interval");
    assert_eq!(wal.durability().sync_interval_ms(), 5);
    assert_eq!(wal.durability().sync_batch_records(), 1);

    let p = dir.join("synced.txt");
    std::fs::write(&p, b"synced").unwrap();
    wal.append(&[create_event(p)]).unwrap();
    let replay = wal.replay_since_seal(0).unwrap();
    assert_eq!(replay.events_replayed, 1);

    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn config_accepts_wal_durability_settings() {
    let cfg: Config = toml::from_str(
        r#"
roots = ["/tmp"]
wal_durability = "sync-interval"
wal_sync_interval_ms = 250
wal_sync_batch_records = 64
"#,
    )
    .expect("config should parse wal durability settings");

    assert_eq!(cfg.wal_durability, "sync-interval");
    assert_eq!(cfg.wal_sync_interval_ms, 250);
    assert_eq!(cfg.wal_sync_batch_records, 64);
}
