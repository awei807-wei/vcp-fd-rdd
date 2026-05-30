//! P1 — recovery ledger audit tests.

use std::path::{Path, PathBuf};

use fd_rdd::core::{EventRecord, EventType, FileIdentifier, FileKey, FileMeta};
use fd_rdd::index::l2_partition::PersistentIndex;
use fd_rdd::index::TieredIndex;
use fd_rdd::storage::recovery_audit::audit_recovery_ledger;
use fd_rdd::storage::snapshot::{
    stable_prev_v7_path_for, stable_v7_path_for, write_recovery_runtime_state,
    write_stable_v7_atomic, RecoveryRuntimeState, SnapshotStore,
};
use fd_rdd::storage::snapshot_v7::write_v7_snapshot_atomic;
use fd_rdd::storage::wal::WalStore;

const WAL_MAGIC: u32 = 0x314C_4157;

fn unique_tmp_dir(tag: &str) -> PathBuf {
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    std::env::temp_dir().join(format!("fd-rdd-recovery-audit-{}-{}", tag, nanos))
}

fn one_file_base(root: &Path, name: &str) -> fd_rdd::index::base_index::BaseIndexData {
    let path = root.join(name);
    std::fs::write(&path, b"indexed").unwrap();
    let meta = std::fs::metadata(&path).unwrap();
    let file_key = FileKey::from_path_and_metadata(&path, &meta).unwrap();

    let idx = PersistentIndex::new_with_roots(vec![root.to_path_buf()]);
    idx.upsert(FileMeta {
        file_key,
        path,
        size: meta.len(),
        mtime: meta.modified().ok(),
        ctime: meta.created().ok(),
        atime: meta.accessed().ok(),
        kind: Default::default(),
    });
    idx.to_base_index_data()
}

fn empty_base(root: &Path) -> fd_rdd::index::base_index::BaseIndexData {
    PersistentIndex::new_with_roots(vec![root.to_path_buf()]).to_base_index_data()
}

fn create_event(path: PathBuf) -> EventRecord {
    EventRecord {
        seq: 1,
        timestamp: std::time::SystemTime::now(),
        event_type: EventType::Create,
        id: FileIdentifier::Path(path.clone()),
        path_hint: Some(path),
    }
}

fn delete_event(path: PathBuf) -> EventRecord {
    EventRecord {
        seq: 1,
        timestamp: std::time::SystemTime::now(),
        event_type: EventType::Delete,
        id: FileIdentifier::Path(path.clone()),
        path_hint: Some(path),
    }
}

#[tokio::test]
async fn audit_marks_bad_stable_and_loads_good_prev() {
    let root = unique_tmp_dir("bad-stable-good-prev");
    std::fs::create_dir_all(&root).unwrap();
    let snap_path = root.join("index.db");
    let store = SnapshotStore::new(snap_path.clone());

    let prev = one_file_base(&root, "prev.txt");
    std::fs::create_dir_all(snap_path.with_extension("d")).unwrap();
    write_v7_snapshot_atomic(&stable_prev_v7_path_for(&snap_path), &prev).unwrap();
    std::fs::write(stable_v7_path_for(&snap_path), b"bad stable").unwrap();

    let runtime = RecoveryRuntimeState {
        last_clean_shutdown: true,
        last_snapshot_unix_secs: 10,
        last_wal_seal_id: 9,
        last_startup_source: "stable".to_string(),
        last_recovery_mode: "clean-shutdown".to_string(),
        root_case_policies: Vec::new(),
    };
    write_recovery_runtime_state(&snap_path, &runtime).unwrap();

    let audit = audit_recovery_ledger(&snap_path, std::slice::from_ref(&root), &runtime);
    assert!(!audit.stable_ok);
    assert!(audit.stable_prev_ok);
    assert!(audit.requires_repair);
    assert!(audit.reasons.iter().any(|r| r == "bad_stable_snapshot"));

    let loaded = TieredIndex::load_or_empty(&store, vec![root.clone()])
        .await
        .unwrap();
    let recovery = loaded.recovery_status();
    assert_eq!(recovery.report.snapshot_source, "stable-prev");
    assert_eq!(recovery.report.wal_checkpoint_used, 0);
    assert!(recovery
        .report
        .reasons
        .iter()
        .any(|r| r == "bad_stable_snapshot"));

    let _ = std::fs::remove_dir_all(&root);
}

#[tokio::test]
async fn audit_missing_lsm_segment_requires_rebuild() {
    let root = unique_tmp_dir("missing-segment");
    std::fs::create_dir_all(&root).unwrap();
    let snap_path = root.join("index.db");
    let store = SnapshotStore::new(snap_path.clone());

    let idx = PersistentIndex::new_with_roots(vec![root.clone()]);
    let path = root.join("seg-backed.txt");
    std::fs::write(&path, b"segment").unwrap();
    let meta = std::fs::metadata(&path).unwrap();
    idx.upsert(FileMeta {
        file_key: FileKey::from_path_and_metadata(&path, &meta).unwrap(),
        path,
        size: meta.len(),
        mtime: meta.modified().ok(),
        ctime: None,
        atime: None,
        kind: Default::default(),
    });
    let segment = store
        .lsm_append_delta_v6(
            &idx.export_segments_v6(),
            &[],
            std::slice::from_ref(&root),
            3,
        )
        .await
        .unwrap();
    let segment_path = store
        .derived_lsm_dir_path()
        .join(format!("seg-{:016x}.db", segment.id));
    std::fs::remove_file(segment_path).unwrap();

    let runtime = RecoveryRuntimeState::default();
    let audit = audit_recovery_ledger(&snap_path, std::slice::from_ref(&root), &runtime);
    assert!(audit.manifest_ok);
    assert_eq!(audit.missing_segment_count, 1);
    assert!(audit.requires_rebuild);
    assert!(audit.reasons.iter().any(|r| r == "missing_segment"));

    let loaded = TieredIndex::load_or_empty(&store, vec![root.clone()])
        .await
        .unwrap();
    let recovery = loaded.recovery_status();
    assert!(recovery.report.requires_rebuild);
    assert!(recovery.report.hard_rebuild_needed);
    assert!(recovery
        .report
        .hard_reasons
        .iter()
        .any(|r| r == "missing_segment"));

    let _ = std::fs::remove_dir_all(&root);
}

#[tokio::test]
async fn audit_bad_manifest_is_hard_rebuild_evidence() {
    let root = unique_tmp_dir("bad-manifest");
    std::fs::create_dir_all(&root).unwrap();
    let snap_path = root.join("index.db");
    let store = SnapshotStore::new(snap_path.clone());

    std::fs::create_dir_all(store.derived_lsm_dir_path()).unwrap();
    std::fs::write(store.derived_lsm_dir_path().join("MANIFEST.bin"), b"bad").unwrap();

    let runtime = RecoveryRuntimeState::default();
    let audit = audit_recovery_ledger(&snap_path, std::slice::from_ref(&root), &runtime);
    assert!(audit.requires_rebuild);
    assert!(audit.reasons.iter().any(|r| r.starts_with("bad_manifest:")));

    let loaded = TieredIndex::load_or_empty(&store, vec![root.clone()])
        .await
        .unwrap();
    let recovery = loaded.recovery_status();
    assert!(recovery.report.hard_rebuild_needed);
    assert!(recovery
        .report
        .hard_reasons
        .iter()
        .any(|r| r.starts_with("bad_manifest:")));

    let _ = std::fs::remove_dir_all(&root);
}

#[tokio::test]
async fn audit_wal_gap_is_hard_rebuild_evidence() {
    let root = unique_tmp_dir("wal-gap");
    std::fs::create_dir_all(&root).unwrap();
    let snap_path = root.join("index.db");
    let store = SnapshotStore::new(snap_path.clone());

    std::fs::create_dir_all(store.derived_lsm_dir_path()).unwrap();
    let header = {
        let mut data = Vec::new();
        data.extend_from_slice(&WAL_MAGIC.to_le_bytes());
        data.extend_from_slice(&3u32.to_le_bytes());
        data
    };
    std::fs::write(
        store
            .derived_lsm_dir_path()
            .join("events.wal.seal-0000000000000001"),
        &header,
    )
    .unwrap();
    std::fs::write(
        store
            .derived_lsm_dir_path()
            .join("events.wal.seal-0000000000000003"),
        &header,
    )
    .unwrap();

    let runtime = RecoveryRuntimeState::default();
    let audit = audit_recovery_ledger(&snap_path, std::slice::from_ref(&root), &runtime);
    assert!(audit.wal_gap_detected);
    assert!(audit.requires_repair);
    assert!(audit.reasons.iter().any(|r| r == "wal_gap"));

    let loaded = TieredIndex::load_or_empty(&store, vec![root.clone()])
        .await
        .unwrap();
    let recovery = loaded.recovery_status();
    assert!(recovery.report.hard_rebuild_needed);
    assert!(recovery.report.hard_reasons.iter().any(|r| r == "wal_gap"));

    let _ = std::fs::remove_dir_all(&root);
}

#[tokio::test]
async fn audit_bad_current_wal_is_hard_rebuild_evidence() {
    let root = unique_tmp_dir("bad-current-wal");
    std::fs::create_dir_all(&root).unwrap();
    let snap_path = root.join("index.db");
    let store = SnapshotStore::new(snap_path.clone());

    std::fs::create_dir_all(store.derived_lsm_dir_path()).unwrap();
    std::fs::write(store.derived_lsm_dir_path().join("events.wal"), b"bad").unwrap();

    let runtime = RecoveryRuntimeState::default();
    let audit = audit_recovery_ledger(&snap_path, std::slice::from_ref(&root), &runtime);
    assert!(!audit.current_wal_ok);
    assert!(audit.requires_repair);
    assert!(audit.reasons.iter().any(|r| r == "bad_current_wal"));

    let loaded = TieredIndex::load_or_empty(&store, vec![root.clone()])
        .await
        .unwrap();
    let recovery = loaded.recovery_status();
    assert!(recovery.report.hard_rebuild_needed);
    assert!(recovery
        .report
        .hard_reasons
        .iter()
        .any(|r| r == "bad_current_wal"));

    let _ = std::fs::remove_dir_all(&root);
}

#[tokio::test]
async fn audit_current_wal_written_by_store_is_not_bad_current_wal() {
    let root = unique_tmp_dir("current-wal-version");
    std::fs::create_dir_all(&root).unwrap();
    let snap_path = root.join("index.db");
    let store = SnapshotStore::new(snap_path.clone());

    let wal = WalStore::open_in_dir(store.derived_lsm_dir_path()).unwrap();
    drop(wal);

    let runtime = RecoveryRuntimeState::default();
    let audit = audit_recovery_ledger(&snap_path, std::slice::from_ref(&root), &runtime);
    assert!(audit.current_wal_ok);
    assert!(!audit.reasons.iter().any(|r| r == "bad_current_wal"));

    let loaded = TieredIndex::load_or_empty(&store, vec![root.clone()])
        .await
        .unwrap();
    let recovery = loaded.recovery_status();
    assert!(!recovery
        .report
        .hard_reasons
        .iter()
        .any(|r| r == "bad_current_wal"));

    let _ = std::fs::remove_dir_all(&root);
}

#[tokio::test]
async fn audit_counts_orphan_and_tmp_without_expanding_gc_scope() {
    let root = unique_tmp_dir("orphan-tmp");
    std::fs::create_dir_all(&root).unwrap();
    let snap_path = root.join("index.db");
    let store = SnapshotStore::new(snap_path.clone());

    let idx = PersistentIndex::new_with_roots(vec![root.clone()]);
    let live = store
        .lsm_append_delta_v6(
            &idx.export_segments_v6(),
            &[],
            std::slice::from_ref(&root),
            0,
        )
        .await
        .unwrap();
    let dir = store.derived_lsm_dir_path();
    let live_path = dir.join(format!("seg-{:016x}.db", live.id));
    let orphan_path = dir.join("seg-00000000000000ff.db");
    let tmp_path = dir.join("seg-00000000000000aa.db.tmp");
    std::fs::write(&orphan_path, b"orphan").unwrap();
    std::fs::write(&tmp_path, b"tmp").unwrap();

    let runtime = RecoveryRuntimeState::default();
    let audit = audit_recovery_ledger(&snap_path, std::slice::from_ref(&root), &runtime);
    assert_eq!(audit.orphan_segment_count, 2);
    assert_eq!(audit.tmp_file_count, 1);

    let removed = store.gc_stale_segments().unwrap();
    assert_eq!(removed, 2);
    assert!(live_path.exists());
    assert!(!orphan_path.exists());
    assert!(!tmp_path.exists());

    let _ = std::fs::remove_dir_all(&root);
}

#[tokio::test]
async fn load_uses_runtime_checkpoint_to_skip_old_sealed_wal() {
    let root = unique_tmp_dir("checkpoint");
    std::fs::create_dir_all(&root).unwrap();
    let snap_path = root.join("index.db");
    let store = SnapshotStore::new(snap_path.clone());

    write_stable_v7_atomic(&snap_path, &empty_base(&root)).unwrap();

    let old_path = root.join("old-sealed.txt");
    let new_path = root.join("new-current.txt");
    std::fs::write(&old_path, b"old").unwrap();
    std::fs::write(&new_path, b"new").unwrap();

    let wal = WalStore::open_in_dir(store.derived_lsm_dir_path()).unwrap();
    wal.append(&[create_event(old_path.clone())]).unwrap();
    let checkpoint = wal.seal().unwrap();
    wal.append(&[create_event(new_path.clone())]).unwrap();
    drop(wal);

    write_recovery_runtime_state(
        &snap_path,
        &RecoveryRuntimeState {
            last_clean_shutdown: true,
            last_snapshot_unix_secs: 11,
            last_wal_seal_id: checkpoint,
            last_startup_source: "stable".to_string(),
            last_recovery_mode: "snapshot".to_string(),
            root_case_policies: Vec::new(),
        },
    )
    .unwrap();

    let loaded = TieredIndex::load_or_empty(&store, vec![root.clone()])
        .await
        .unwrap();
    let recovery = loaded.recovery_status();
    assert_eq!(recovery.report.snapshot_source, "stable");
    assert_eq!(recovery.report.wal_checkpoint_used, checkpoint);
    assert_eq!(recovery.report.wal_sealed_used, 0);
    assert_eq!(recovery.report.wal_events_replayed, 1);
    assert_eq!(loaded.file_count(), 1);

    let _ = std::fs::remove_dir_all(&root);
}

#[tokio::test]
async fn delete_wal_replay_hides_stable_snapshot_path() {
    let root = unique_tmp_dir("delete-replay");
    std::fs::create_dir_all(&root).unwrap();
    let snap_path = root.join("index.db");
    let store = SnapshotStore::new(snap_path.clone());

    let path = root.join("delete-me.txt");
    let base = one_file_base(&root, "delete-me.txt");
    write_stable_v7_atomic(&snap_path, &base).unwrap();
    std::fs::remove_file(&path).unwrap();

    let wal = WalStore::open_in_dir(store.derived_lsm_dir_path()).unwrap();
    wal.append(&[delete_event(path.clone())]).unwrap();
    drop(wal);

    write_recovery_runtime_state(
        &snap_path,
        &RecoveryRuntimeState {
            last_clean_shutdown: true,
            last_snapshot_unix_secs: 12,
            last_wal_seal_id: 0,
            last_startup_source: "stable".to_string(),
            last_recovery_mode: "snapshot".to_string(),
            root_case_policies: Vec::new(),
        },
    )
    .unwrap();

    let loaded = TieredIndex::load_or_empty(&store, vec![root.clone()])
        .await
        .unwrap();
    assert!(loaded.query("delete-me").is_empty());
    assert_eq!(loaded.recovery_status().report.wal_events_replayed, 1);

    let _ = std::fs::remove_dir_all(&root);
}

#[tokio::test]
async fn startup_repair_delete_alignment_removes_missing_stable_path() {
    let root = unique_tmp_dir("repair-delete-align");
    std::fs::create_dir_all(&root).unwrap();
    let snap_path = root.join("index.db");
    let store = SnapshotStore::new(snap_path.clone());

    let path = root.join("missing-after-crash.txt");
    let base = one_file_base(&root, "missing-after-crash.txt");
    write_stable_v7_atomic(&snap_path, &base).unwrap();
    std::fs::remove_file(&path).unwrap();

    write_recovery_runtime_state(
        &snap_path,
        &RecoveryRuntimeState {
            last_clean_shutdown: false,
            last_snapshot_unix_secs: 13,
            last_wal_seal_id: 0,
            last_startup_source: "stable".to_string(),
            last_recovery_mode: "running".to_string(),
            root_case_policies: Vec::new(),
        },
    )
    .unwrap();

    let loaded = TieredIndex::load_or_empty(&store, vec![root.clone()])
        .await
        .unwrap();
    let stats = loaded.startup_repair_if_needed(true, "dirty-only", 4, 10_000, 1.0);
    assert!(stats.ran);
    assert!(stats.changed >= 1);
    assert!(loaded.query("missing-after-crash").is_empty());

    let _ = std::fs::remove_dir_all(&root);
}

#[tokio::test]
async fn startup_repair_reconciles_renamed_subtree_after_crash() {
    let root = unique_tmp_dir("repair-rename-subtree");
    let old_project = root.join("old_project");
    let old_deep = old_project.join("deps_tree/libA/out_js");
    std::fs::create_dir_all(&old_deep).unwrap();
    let old_file = old_deep.join("bundle_1.js");
    std::fs::write(&old_file, b"bundle").unwrap();

    let snap_path = root.join("index.db");
    let store = SnapshotStore::new(snap_path.clone());
    let base = one_file_base(&root, "old_project/deps_tree/libA/out_js/bundle_1.js");
    write_stable_v7_atomic(&snap_path, &base).unwrap();

    let new_project = root.join("new_project");
    std::fs::rename(&old_project, &new_project).unwrap();
    write_recovery_runtime_state(
        &snap_path,
        &RecoveryRuntimeState {
            last_clean_shutdown: false,
            last_snapshot_unix_secs: 14,
            last_wal_seal_id: 0,
            last_startup_source: "stable".to_string(),
            last_recovery_mode: "running".to_string(),
            root_case_policies: Vec::new(),
        },
    )
    .unwrap();

    let loaded = TieredIndex::load_or_empty(&store, vec![root.clone()])
        .await
        .unwrap();
    let stats = loaded.startup_repair_if_needed(true, "dirty-only", 8, 10_000, 1.0);
    assert!(stats.ran);

    let results = loaded.query("bundle_1");
    assert!(
        results.iter().any(|meta| meta
            .path
            .to_string_lossy()
            .contains("new_project/deps_tree/libA/out_js/bundle_1.js")),
        "renamed subtree file should be visible after startup repair: {results:?}"
    );
    assert!(
        results
            .iter()
            .all(|meta| !meta.path.to_string_lossy().contains("old_project/")),
        "old subtree paths should be hidden after startup repair: {results:?}"
    );

    let _ = std::fs::remove_dir_all(&root);
}
