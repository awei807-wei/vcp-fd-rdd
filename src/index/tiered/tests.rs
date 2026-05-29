use super::*;
use crate::config::{ContentIndexConfig, RuntimeProfile};
use crate::core::{EventRecord, EventType, FileIdentifier};
use crate::diagnostics::{DiagnosticReport, DiagnosticSource};
use crate::event::sync::{DirtyReason, DirtyScope};
use crate::fs_policy::{FsPolicyDecision, MountTable};
use crate::index::tiered::events::event_record_estimated_bytes;
use crate::index::tiered::sync::RebuildAdmission;
use crate::io_governor::{BackoffPolicy, IoGovernor, IoPressure};
use crate::stats::EventPipelineStats;
use crate::storage::quarantine::{
    FreezeGate, MountIdentity, QuarantineRoot, QuarantineRootState, QuarantineSidecar,
    RootStateKind, RootStateRecord,
};
use crate::storage::snapshot::{quarantine_sidecar_path_for, SnapshotStore};
use crate::storage::traits::WalFactory;
use std::path::PathBuf;
use std::sync::Arc;

fn mk_event(seq: u64, event_type: EventType, path: PathBuf) -> EventRecord {
    EventRecord {
        seq,
        timestamp: std::time::SystemTime::now(),
        event_type,
        id: FileIdentifier::Path(path.clone()),
        path_hint: Some(path),
    }
}

fn unique_tmp_dir(tag: &str) -> PathBuf {
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    std::env::temp_dir().join(format!("fd-rdd-{}-{}", tag, nanos))
}

fn unix_secs_for_test() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
}

fn test_mount_identity() -> MountIdentity {
    MountIdentity {
        mount_id: 7,
        major_minor: "8:1".to_string(),
        fs_uuid: Some("uuid-a".to_string()),
        source: "/dev/sda1".to_string(),
        fstype: "ext4".to_string(),
    }
}

fn quarantine_sidecar(root: PathBuf, affected_prefixes: Vec<PathBuf>) -> QuarantineSidecar {
    QuarantineSidecar {
        version: crate::storage::quarantine::QUARANTINE_SIDECAR_VERSION,
        roots: vec![QuarantineRoot {
            root_path: root,
            identity: MountIdentity {
                fs_uuid: None,
                ..test_mount_identity()
            },
            affected_prefixes,
            state: QuarantineRootState::Offline,
            reason: Some("probe_timeout".to_string()),
            last_observed_unix_ns: 1,
        }],
    }
}

fn mount_table_for(root: &std::path::Path, major_minor: &str, source: &str) -> MountTable {
    MountTable::parse(&format!(
        "42 1 {major_minor} / {} rw,relatime - ext4 {source} rw\n",
        root.display()
    ))
}

#[test]
fn tiered_diagnostics_include_shared_mount_policy_counters() {
    let root = unique_tmp_dir("mount-policy-diag");
    std::fs::create_dir_all(&root).unwrap();
    let idx = TieredIndex::empty(vec![root.clone()]);

    let counters = idx.mount_policy_counters();
    counters.record_decision(&FsPolicyDecision::Deny {
        reason: "deny_fstype:fuse.rclone".to_string(),
    });
    counters.record_decision(&FsPolicyDecision::Deny {
        reason: "one_file_system".to_string(),
    });
    counters.record_fuse_probe_timeout();
    counters.record_allowed_override();

    let mut report = DiagnosticReport::default();
    idx.collect(&mut report);

    assert_eq!(report.watchers.denied_mount_count, 2);
    assert_eq!(report.watchers.fstype_blocked_count, 1);
    assert_eq!(report.watchers.network_fs_ignored_count, 1);
    assert_eq!(report.watchers.one_file_system_boundary_count, 1);
    assert_eq!(report.watchers.fuse_probe_timeout_count, 1);
    assert_eq!(report.watchers.allowed_override_count, 1);

    let _ = std::fs::remove_dir_all(&root);
}

#[test]
fn tiered_diagnostics_include_ioprio_status() {
    let root = unique_tmp_dir("ioprio-diag");
    std::fs::create_dir_all(&root).unwrap();
    let idx = TieredIndex::empty(vec![root.clone()]);

    let mut report = DiagnosticReport::default();
    idx.collect(&mut report);
    assert_eq!(report.io.ioprio_class, "unset");
    assert!(!report.io.ioprio_set_failed);

    idx.record_idle_io_priority_result(Err(std::io::Error::new(
        std::io::ErrorKind::PermissionDenied,
        "denied",
    )));
    let mut report = DiagnosticReport::default();
    idx.collect(&mut report);
    assert_eq!(report.io.ioprio_class, "unavailable");
    assert!(report.io.ioprio_set_failed);

    idx.record_idle_io_priority_result(Ok(()));
    let mut report = DiagnosticReport::default();
    idx.collect(&mut report);
    assert_eq!(report.io.ioprio_class, "idle");
    assert!(report.io.ioprio_set_failed);

    let _ = std::fs::remove_dir_all(&root);
}

#[test]
fn tiered_diagnostics_include_io_governor_counters() {
    let root = unique_tmp_dir("io-governor-diag");
    std::fs::create_dir_all(&root).unwrap();

    let l1 = L1Cache::with_capacity(1000);
    let l2 = Arc::new(PersistentIndex::new_with_roots(vec![root.clone()]));
    let l3 = IndexBuilder::new(vec![root.clone()]);
    let governor = Arc::new(IoGovernor::new(true, 1_000_000));
    let idx = TieredIndex::new_with_base_and_io_governor(
        l1,
        l2,
        l3,
        vec![root.clone()],
        false,
        true,
        false,
        Vec::new(),
        None,
        governor,
    );

    idx.io_governor.observe_pressure(
        IoPressure {
            some_avg10: 20.0,
            full_avg10: 1.5,
        },
        BackoffPolicy {
            some_threshold: 10.0,
            full_threshold: 5.0,
            base_delay: std::time::Duration::from_millis(1),
            max_delay: std::time::Duration::from_millis(1),
        },
    );

    let mut report = DiagnosticReport::default();
    idx.collect(&mut report);
    assert_eq!(report.io.psi_some_avg10, Some(20.0));
    assert_eq!(report.io.psi_full_avg10, Some(1.5));
    assert_eq!(report.io.backoff_count, 1);
    assert_eq!(report.io.token_bucket_limited_count, 0);

    let _ = std::fs::remove_dir_all(&root);
}

#[test]
fn tiered_diagnostics_include_hardlink_physical_stats() {
    let root = unique_tmp_dir("hardlink-diag");
    std::fs::create_dir_all(&root).unwrap();
    let original = root.join("original.txt");
    let alias_a = root.join("alias-a.txt");
    let alias_b = root.join("alias-b.txt");
    let copy = root.join("copy.txt");
    std::fs::write(&original, b"same").unwrap();
    std::fs::hard_link(&original, &alias_a).unwrap();
    std::fs::hard_link(&original, &alias_b).unwrap();
    std::fs::write(&copy, b"same").unwrap();

    let idx = TieredIndex::empty(vec![root.clone()]);
    idx.apply_events(&[
        mk_event(1, EventType::Create, original),
        mk_event(2, EventType::Create, alias_a),
        mk_event(3, EventType::Create, alias_b),
        mk_event(4, EventType::Create, copy),
    ]);

    let mut report = DiagnosticReport::default();
    idx.collect(&mut report);
    assert_eq!(report.storage.hardlink_group_count, 1);
    assert_eq!(report.storage.hardlink_max_group_size, 3);

    let _ = std::fs::remove_dir_all(&root);
}

#[test]
fn freeze_gate_blocks_destructive_events_before_delta_buffer() {
    let root = unique_tmp_dir("freeze-delta");
    std::fs::create_dir_all(&root).unwrap();
    let idx = TieredIndex::empty(vec![root.clone()]);
    idx.install_freeze_gate(FreezeGate::from_roots(vec![root.clone()]));

    let p = root.join("blocked.txt");
    let old = root.join("old.txt");
    let rename = EventRecord {
        seq: 3,
        timestamp: std::time::SystemTime::now(),
        event_type: EventType::Rename {
            from: FileIdentifier::Path(old.clone()),
            from_path_hint: Some(old),
        },
        id: FileIdentifier::Path(root.join("new.txt")),
        path_hint: Some(root.join("new.txt")),
    };
    idx.apply_events(&[
        mk_event(1, EventType::Modify, p.clone()),
        mk_event(2, EventType::Delete, p),
        rename,
    ]);

    assert_eq!(idx.delta_buffer.lock().len(), 0);
    assert_eq!(idx.freeze_gate.lock().blocked_events(), 3);
}

#[test]
fn frozen_root_query_results_are_hidden_by_default() {
    let root = unique_tmp_dir("freeze-query");
    std::fs::create_dir_all(&root).unwrap();
    let path = root.join("visible_before_freeze.txt");
    std::fs::write(&path, b"data").unwrap();
    let idx = TieredIndex::empty(vec![root.clone()]);

    idx.apply_events(&[mk_event(1, EventType::Create, path)]);
    assert!(!idx.query("visible_before_freeze").is_empty());

    idx.install_freeze_gate(FreezeGate::from_roots(vec![root]));
    assert!(idx.query("visible_before_freeze").is_empty());
}

#[test]
fn online_root_record_unfreezes_and_queues_prefix_reconciliation() {
    let root = unique_tmp_dir("freeze-online");
    std::fs::create_dir_all(&root).unwrap();
    let idx = TieredIndex::empty(vec![root.clone()]);
    let prefix = root.join("project");

    idx.apply_root_state_record(RootStateRecord::offline(
        1,
        root.clone(),
        test_mount_identity(),
        vec![prefix.clone()],
        Some("probe_timeout".to_string()),
    ));
    assert!(idx.path_is_frozen(&prefix));

    idx.apply_root_state_record(RootStateRecord::online(
        2,
        root,
        test_mount_identity(),
        vec![prefix],
    ));

    assert_eq!(idx.freeze_gate.lock().frozen_root_count(), 0);
    assert_eq!(idx.dirty_queue_len(), 1);
}

#[tokio::test]
async fn quarantine_sidecar_restore_installs_freeze_gate_before_events() -> anyhow::Result<()> {
    let root = unique_tmp_dir("quarantine-restore");
    let content_root = root.join("content");
    let state_root = root.join("state");
    let prefix = content_root.join("project");
    let restored_file = prefix.join("restored_after_online.txt");
    std::fs::create_dir_all(&prefix)?;
    std::fs::write(&restored_file, b"restored")?;
    std::fs::create_dir_all(&state_root)?;

    let store = SnapshotStore::new(state_root.join("index.db"));
    let sidecar_path = quarantine_sidecar_path_for(store.path());
    quarantine_sidecar(content_root.clone(), vec![prefix.clone()]).write_to(&sidecar_path)?;

    let idx = TieredIndex::load_or_empty(&store, vec![content_root.clone()]).await?;

    assert!(idx.path_is_frozen(&prefix));
    assert_eq!(idx.quarantine_verify_pending.load(Ordering::Relaxed), 1);

    let deleted = prefix.join("blocked_delete.txt");
    idx.apply_events(&[mk_event(1, EventType::Delete, deleted)]);
    assert_eq!(idx.delta_buffer.lock().len(), 0);
    assert_eq!(idx.freeze_gate.lock().blocked_events(), 1);

    let _ = std::fs::remove_dir_all(&root);
    Ok(())
}

#[test]
fn quarantine_verify_online_appends_wal_unfreezes_and_queues_scan() -> anyhow::Result<()> {
    let root = unique_tmp_dir("quarantine-verify-online");
    let content_root = root.join("content");
    let state_root = root.join("state");
    let prefix = content_root.join("project");
    let restored_file = prefix.join("restored_after_online.txt");
    std::fs::create_dir_all(&prefix)?;
    std::fs::write(&restored_file, b"restored")?;
    std::fs::create_dir_all(&state_root)?;

    let store = SnapshotStore::new(state_root.join("index.db"));
    let sidecar_path = quarantine_sidecar_path_for(store.path());
    let sidecar = quarantine_sidecar(content_root.clone(), vec![prefix.clone()]);
    sidecar.write_to(&sidecar_path)?;

    let idx = TieredIndex::empty(vec![content_root.clone()]);
    idx.attach_wal(&store)?;
    idx.restore_quarantine_from_sidecar(sidecar.clone());
    assert!(idx.path_is_frozen(&prefix));

    let table = mount_table_for(&content_root, "8:1", "/dev/sda1");
    idx.verify_quarantine_sidecar_once_with_mount_table(sidecar_path.clone(), &table, sidecar);

    assert!(!idx.path_is_frozen(&prefix));
    assert_eq!(idx.dirty_queue_len(), 1);
    assert_eq!(idx.quarantine_verify_pending.load(Ordering::Relaxed), 0);
    assert_eq!(idx.quarantine_verified_roots.load(Ordering::Relaxed), 1);

    let dirty_entry = idx.dirty_queue.lock().pop_ready(u64::MAX, 1).pop().unwrap();
    let report = idx.process_dirty_entry(dirty_entry, &[]);
    assert!(!report.failed);
    assert!(!idx.query("restored_after_online").is_empty());

    let updated = QuarantineSidecar::read_from(&sidecar_path)?;
    assert_eq!(updated.active_roots().count(), 0);

    let replay = store.open_wal()?.replay_since_seal(0)?;
    assert_eq!(replay.root_events_replayed, 1);
    assert_eq!(replay.root_events[0].kind, RootStateKind::OnlineRoot);
    assert_eq!(replay.root_events[0].root_path, content_root);
    assert_eq!(replay.root_events[0].affected_prefixes, vec![prefix]);

    let _ = std::fs::remove_dir_all(&root);
    Ok(())
}

#[test]
fn quarantine_verify_identity_mismatch_keeps_freeze() -> anyhow::Result<()> {
    let root = unique_tmp_dir("quarantine-verify-mismatch");
    let content_root = root.join("content");
    let state_root = root.join("state");
    let prefix = content_root.join("project");
    std::fs::create_dir_all(&prefix)?;
    std::fs::create_dir_all(&state_root)?;

    let store = SnapshotStore::new(state_root.join("index.db"));
    let sidecar_path = quarantine_sidecar_path_for(store.path());
    let sidecar = quarantine_sidecar(content_root.clone(), vec![prefix.clone()]);
    sidecar.write_to(&sidecar_path)?;

    let idx = TieredIndex::empty(vec![content_root.clone()]);
    idx.attach_wal(&store)?;
    idx.restore_quarantine_from_sidecar(sidecar.clone());

    let table = mount_table_for(&content_root, "9:9", "/dev/other");
    idx.verify_quarantine_sidecar_once_with_mount_table(sidecar_path.clone(), &table, sidecar);

    assert!(idx.path_is_frozen(&prefix));
    assert_eq!(idx.dirty_queue_len(), 0);
    assert_eq!(idx.quarantine_verify_pending.load(Ordering::Relaxed), 1);
    assert_eq!(idx.quarantine_verified_roots.load(Ordering::Relaxed), 0);
    assert_eq!(
        QuarantineSidecar::read_from(&sidecar_path)?
            .active_roots()
            .count(),
        1
    );
    assert_eq!(
        store.open_wal()?.replay_since_seal(0)?.root_events_replayed,
        0
    );

    let _ = std::fs::remove_dir_all(&root);
    Ok(())
}

#[tokio::test]
async fn wal_online_root_replay_unfreezes_before_following_file_events() -> anyhow::Result<()> {
    let root = unique_tmp_dir("quarantine-wal-order");
    let content_root = root.join("content");
    let state_root = root.join("state");
    let prefix = content_root.join("project");
    let restored_file = prefix.join("wal_restored_after_online.txt");
    std::fs::create_dir_all(&prefix)?;
    std::fs::write(&restored_file, b"restored")?;
    std::fs::create_dir_all(&state_root)?;

    let store = SnapshotStore::new(state_root.join("index.db"));
    quarantine_sidecar(content_root.clone(), vec![prefix.clone()])
        .write_to(&quarantine_sidecar_path_for(store.path()))?;
    let wal = store.open_wal()?;
    wal.append_root_events(&[RootStateRecord::online(
        1,
        content_root.clone(),
        MountIdentity {
            fs_uuid: None,
            ..test_mount_identity()
        },
        vec![prefix.clone()],
    )])?;
    wal.append(&[mk_event(2, EventType::Create, restored_file.clone())])?;
    drop(wal);

    let idx = TieredIndex::load_or_empty(&store, vec![content_root.clone()]).await?;

    assert!(!idx.path_is_frozen(&prefix));
    assert!(!idx.query("wal_restored_after_online").is_empty());
    assert_eq!(idx.freeze_gate.lock().blocked_events(), 0);

    let _ = std::fs::remove_dir_all(&root);
    Ok(())
}

#[test]
fn rebuild_with_pending_events_no_loss() {
    let root = unique_tmp_dir("rebuild");
    std::fs::create_dir_all(&root).unwrap();

    let old_path = root.join("old_aaa.txt");
    std::fs::write(&old_path, b"old").unwrap();

    let idx = Arc::new(TieredIndex::empty(vec![root.clone()]));

    // 先让旧索引里有内容（模拟在线服务已有数据）。
    idx.apply_events(&[mk_event(1, EventType::Create, old_path.clone())]);
    assert!(!idx.query("old_aaa").is_empty());

    // 开始 rebuild：此时 apply_events 会进入 pending 缓冲。
    assert!(idx.try_start_rebuild_force());
    assert!(idx.rebuild_in_progress());

    // new L2：模拟 full_build 的结果（这里直接应用一次 Create）。
    let new_l2 = Arc::new(PersistentIndex::new_with_roots(vec![root.clone()]));
    new_l2.apply_events(&[mk_event(2, EventType::Create, old_path.clone())]);

    // rebuild 期间新增文件：必须在切换后仍可查询到。
    let new_path = root.join("new_bbb.txt");
    std::fs::write(&new_path, b"new").unwrap();
    idx.apply_events(&[mk_event(3, EventType::Create, new_path.clone())]);

    // 完成：回放 pending -> 原子切换
    idx.finish_rebuild(new_l2);
    assert!(!idx.rebuild_in_progress());

    assert!(!idx.query("old_aaa").is_empty());
    assert!(!idx.query("new_bbb").is_empty());
}

#[test]
fn rebuild_in_progress_does_not_publish_partial_l2_as_ready() {
    let root = unique_tmp_dir("rebuild-partial-visibility");
    std::fs::create_dir_all(&root).unwrap();

    let partial_path = root.join("partial_ready.txt");
    std::fs::write(&partial_path, b"partial").unwrap();

    let idx = Arc::new(TieredIndex::empty(vec![root.clone()]));
    assert!(idx.try_start_rebuild_force());

    let partial_l2 = Arc::new(PersistentIndex::new_with_roots(vec![root.clone()]));
    partial_l2.apply_events(&[mk_event(1, EventType::Create, partial_path.clone())]);
    idx.l2.store(partial_l2.clone());

    assert_eq!(
        idx.file_count(),
        0,
        "status must not report rebuild scratch L2 as query-visible"
    );
    assert!(
        idx.query("partial_ready").is_empty(),
        "query must not materialize a partial rebuild L2 into base"
    );

    idx.finish_rebuild(partial_l2);
    assert_eq!(idx.file_count(), 1);
    assert!(!idx.query("partial_ready").is_empty());

    let _ = std::fs::remove_dir_all(&root);
}

#[test]
fn rebuild_request_coalesces_while_rebuild_in_progress() {
    let root = unique_tmp_dir("rebuild-coalesce-in-progress");
    std::fs::create_dir_all(&root).unwrap();

    let idx = Arc::new(TieredIndex::empty(vec![root.clone()]));
    assert!(idx.try_start_rebuild_force());

    assert_eq!(
        idx.reserve_rebuild_with_cooldown("test full build"),
        RebuildAdmission::Coalesced
    );
    assert!(idx.rebuild_in_progress());

    let st = idx.rebuild_state.lock();
    assert!(st.requested);
    assert!(!st.scheduled);

    let _ = std::fs::remove_dir_all(&root);
}

#[test]
fn rebuild_cooldown_coalesces_duplicate_requests() {
    let root = unique_tmp_dir("rebuild-coalesce-cooldown");
    std::fs::create_dir_all(&root).unwrap();

    let idx = Arc::new(TieredIndex::empty(vec![root.clone()]));
    assert!(idx.try_start_rebuild_force());
    let new_l2 = Arc::new(PersistentIndex::new_with_roots(vec![root.clone()]));
    assert!(!idx.finish_rebuild(new_l2));
    assert!(!idx.rebuild_in_progress());

    let first = idx.reserve_rebuild_with_cooldown("test full build");
    assert!(matches!(first, RebuildAdmission::Scheduled(wait) if wait <= REBUILD_COOLDOWN));

    let second = idx.reserve_rebuild_with_cooldown("test rebuild");
    assert_eq!(second, RebuildAdmission::Coalesced);

    let st = idx.rebuild_state.lock();
    assert!(st.requested);
    assert!(st.scheduled);
    assert!(!st.in_progress);

    let _ = std::fs::remove_dir_all(&root);
}

#[test]
fn memory_light_profile_applies_runtime_flush_and_rebuild_controls() {
    let root = unique_tmp_dir("memory-light-settings");
    std::fs::create_dir_all(&root).unwrap();

    let idx = TieredIndex::empty(vec![root.clone()]);
    let settings = RuntimeProfile::MemoryLight.settings();
    idx.apply_runtime_profile_settings(settings);

    assert_eq!(
        idx.auto_flush_overlay_paths
            .load(std::sync::atomic::Ordering::Relaxed),
        settings.auto_flush_overlay_paths
    );
    assert_eq!(
        idx.auto_flush_overlay_bytes
            .load(std::sync::atomic::Ordering::Relaxed),
        settings.auto_flush_overlay_bytes
    );
    assert_eq!(
        idx.periodic_flush_min_events
            .load(std::sync::atomic::Ordering::Relaxed),
        settings.periodic_flush_min_events
    );
    assert_eq!(
        idx.periodic_flush_min_bytes
            .load(std::sync::atomic::Ordering::Relaxed),
        settings.periodic_flush_min_bytes
    );
    assert_eq!(
        idx.periodic_flush_max_staleness_secs
            .load(std::sync::atomic::Ordering::Relaxed),
        settings.periodic_flush_max_staleness_secs
    );
    assert_eq!(
        idx.rebuild_cooldown_secs
            .load(std::sync::atomic::Ordering::Relaxed),
        settings.rebuild_cooldown_secs
    );
    assert_eq!(
        idx.wal_seal_bytes
            .load(std::sync::atomic::Ordering::Relaxed),
        settings.wal_seal_bytes
    );

    let _ = std::fs::remove_dir_all(&root);
}

#[test]
fn default_profile_settings_preserve_existing_flush_controls() {
    let defaults = RuntimeProfile::Default.settings();

    assert_eq!(defaults.auto_flush_overlay_paths, 250_000);
    assert_eq!(defaults.auto_flush_overlay_bytes, 64 * 1024 * 1024);
    assert_eq!(defaults.periodic_flush_min_events, 4_096);
    assert_eq!(defaults.periodic_flush_min_bytes, 4 * 1024 * 1024);
    assert_eq!(defaults.periodic_flush_max_staleness_secs, 0);
    assert_eq!(defaults.rebuild_cooldown_secs, REBUILD_COOLDOWN.as_secs());
    assert_eq!(defaults.wal_seal_bytes, 0);
}

#[test]
fn memory_light_rebuild_cooldown_is_shorter_than_default() {
    let root = unique_tmp_dir("memory-light-rebuild-cooldown");
    std::fs::create_dir_all(&root).unwrap();

    let idx = Arc::new(TieredIndex::empty(vec![root.clone()]));
    idx.apply_runtime_profile_settings(RuntimeProfile::MemoryLight.settings());
    assert!(idx.try_start_rebuild_force());
    let new_l2 = Arc::new(PersistentIndex::new_with_roots(vec![root.clone()]));
    assert!(!idx.finish_rebuild(new_l2));

    let admission = idx.reserve_rebuild_with_cooldown("memory light test");
    assert!(matches!(
        admission,
        RebuildAdmission::Scheduled(wait) if wait <= std::time::Duration::from_secs(15)
    ));

    let _ = std::fs::remove_dir_all(&root);
}

#[test]
fn periodic_flush_max_staleness_can_force_ready_batch() {
    let root = unique_tmp_dir("periodic-staleness");
    std::fs::create_dir_all(&root).unwrap();

    let idx = TieredIndex::empty(vec![root.clone()]);
    idx.set_auto_flush_limits(0, 0);
    idx.set_periodic_flush_batch_limits(10_000, 10_000_000);
    idx.periodic_flush_max_staleness_secs
        .store(30, std::sync::atomic::Ordering::Relaxed);

    let p = root.join("stale.txt");
    std::fs::write(&p, b"stale").unwrap();
    idx.apply_events(&[mk_event(1, EventType::Create, p)]);
    assert!(!idx.periodic_flush_batch_ready());

    idx.pending_flush_since_unix_secs.store(
        unix_secs_for_test().saturating_sub(31),
        std::sync::atomic::Ordering::Relaxed,
    );
    assert!(idx.periodic_flush_batch_ready());

    let _ = std::fs::remove_dir_all(&root);
}

#[tokio::test]
async fn wal_size_threshold_requests_snapshot_seal() -> anyhow::Result<()> {
    let root = unique_tmp_dir("wal-size-threshold");
    let content_root = root.join("content");
    let state_root = root.join("state");
    std::fs::create_dir_all(&content_root)?;
    std::fs::create_dir_all(&state_root)?;

    let store = Arc::new(SnapshotStore::new(state_root.join("index.db")));
    let idx = Arc::new(TieredIndex::empty(vec![content_root.clone()]));
    idx.attach_wal(store.as_ref())?;
    idx.set_auto_flush_limits(0, 0);
    idx.set_periodic_flush_batch_limits(10_000, 10_000_000);
    idx.wal_seal_bytes
        .store(1, std::sync::atomic::Ordering::Relaxed);

    let p = content_root.join("wal-trigger.txt");
    std::fs::write(&p, b"wal")?;
    idx.apply_events(&[mk_event(1, EventType::Create, p.clone())]);
    assert!(
        idx.flush_requested
            .load(std::sync::atomic::Ordering::Acquire),
        "WAL size threshold should request a snapshot boundary"
    );
    assert!(
        !idx.query("wal-trigger").is_empty(),
        "WAL-triggered flush request must not block normal event visibility"
    );

    idx.snapshot_now(store.clone()).await?;
    assert!(
        !idx.flush_requested
            .load(std::sync::atomic::Ordering::Acquire),
        "snapshot should clear the WAL-triggered flush request"
    );
    assert!(store.path().with_extension("v7").exists());

    let loaded = TieredIndex::load_or_empty(&*store, vec![content_root.clone()]).await?;
    assert!(!loaded.query("wal-trigger").is_empty());

    let _ = std::fs::remove_dir_all(&root);
    Ok(())
}

#[test]
fn event_apply_does_not_materialize_base_hot_path() {
    let root = unique_tmp_dir("hot-path-base");
    std::fs::create_dir_all(&root).unwrap();

    let idx = TieredIndex::empty(vec![root.clone()]);
    let first = root.join("first_hot_path.txt");
    std::fs::write(&first, b"first").unwrap();
    idx.apply_events(&[mk_event(1, EventType::Create, first.clone())]);

    assert_eq!(
        idx.base.load().file_count(),
        0,
        "ordinary event application must not rebuild BaseIndex"
    );
    assert!(!idx.query("first_hot_path").is_empty());
    assert!(
        idx.base.load().file_count() > 0,
        "query may lazily materialize an initially empty test index"
    );

    let second = root.join("second_hot_path.txt");
    std::fs::write(&second, b"second").unwrap();
    let before = idx.base.load().file_count();
    idx.apply_events(&[mk_event(2, EventType::Create, second.clone())]);
    assert_eq!(
        idx.base.load().file_count(),
        before,
        "event application after base exists must still avoid full materialization"
    );
    assert!(!idx.query("second_hot_path").is_empty());

    let _ = std::fs::remove_dir_all(&root);
}

#[test]
fn overlay_delete_then_recreate_cancels_deleted() {
    let root = unique_tmp_dir("overlay-cancel");
    std::fs::create_dir_all(&root).unwrap();

    let p = root.join("x.txt");
    std::fs::write(&p, b"x").unwrap();

    let idx = TieredIndex::empty(vec![root.clone()]);
    idx.apply_events(&[mk_event(1, EventType::Delete, p.clone())]);
    idx.apply_events(&[mk_event(2, EventType::Create, p.clone())]);

    let r = idx.memory_report(EventPipelineStats::default());
    assert_eq!(r.overlay.deleted_paths, 0);
    assert_eq!(r.overlay.upserted_paths, 1);
}

#[test]
fn memory_report_tracks_generation_strong_refs() {
    let root = unique_tmp_dir("generation-refs");
    std::fs::create_dir_all(&root).unwrap();
    let idx = TieredIndex::empty(vec![root.clone()]);

    let baseline = idx.memory_report(EventPipelineStats::default()).generation;
    let _base_ref = idx.base.load_full();
    let _l2_ref = idx.l2.load_full();
    let with_refs = idx.memory_report(EventPipelineStats::default()).generation;

    assert!(
        with_refs.base_strong_refs > baseline.base_strong_refs,
        "base strong refs should include externally held Arc generations: baseline={baseline:?} with_refs={with_refs:?}"
    );
    assert!(
        with_refs.l2_strong_refs > baseline.l2_strong_refs,
        "l2 strong refs should include externally held Arc generations: baseline={baseline:?} with_refs={with_refs:?}"
    );

    let _ = std::fs::remove_dir_all(&root);
}

#[test]
fn memory_report_tracks_dirty_queue_pending_scopes() {
    let root = unique_tmp_dir("dirty-memory");
    let dirty_dir = root.join("project");
    std::fs::create_dir_all(&dirty_dir).unwrap();
    let idx = TieredIndex::empty(vec![root.clone()]);

    idx.enqueue_dirty_dirs(vec![dirty_dir.clone()], DirtyReason::QueryMiss);
    let report = idx.memory_report(EventPipelineStats::default());

    assert_eq!(report.dirty_queue.pending_scopes, 1);
    assert_eq!(report.dirty_queue.pending_dirs, 1);
    assert!(!report.dirty_queue.all_scope_pending);
    assert!(
        report.dirty_queue.pending_path_bytes
            >= dirty_dir.as_os_str().as_encoded_bytes().len() as u64
    );
    assert!(report.dirty_queue.estimated_bytes > 0);

    let _ = std::fs::remove_dir_all(&root);
}

#[test]
fn memory_report_tracks_query_guard_holds() {
    let root = unique_tmp_dir("query-guard");
    std::fs::create_dir_all(&root).unwrap();
    let path = root.join("guard_probe.txt");
    std::fs::write(&path, b"guard").unwrap();

    let idx = TieredIndex::empty(vec![root.clone()]);
    idx.apply_events(&[mk_event(1, EventType::Create, path.clone())]);

    let before = idx.memory_report(EventPipelineStats::default()).query_guard;
    assert!(!idx.query("guard_probe").is_empty());
    let after = idx.memory_report(EventPipelineStats::default()).query_guard;

    assert_eq!(after.active_count, 0);
    assert!(after.hold_count > before.hold_count);
    assert!(after.last_hold_us <= after.hold_max_us);

    let _ = std::fs::remove_dir_all(&root);
}

#[test]
fn overlay_rename_tracks_from_as_delete_and_to_as_upsert() {
    let root = unique_tmp_dir("overlay-rename");
    std::fs::create_dir_all(&root).unwrap();

    let from = root.join("old_aaa.txt");
    std::fs::write(&from, b"old").unwrap();
    let to = root.join("new_bbb.txt");
    std::fs::rename(&from, &to).unwrap();

    let idx = TieredIndex::empty(vec![root.clone()]);
    idx.apply_events(&[mk_event(
        1,
        EventType::Rename {
            from: FileIdentifier::Path(from.clone()),
            from_path_hint: Some(from.clone()),
        },
        to.clone(),
    )]);

    let r = idx.memory_report(EventPipelineStats::default());
    assert_eq!(r.overlay.deleted_paths, 1);
    assert_eq!(r.overlay.upserted_paths, 1);
}

#[test]
fn rebuild_pending_rename_applied_after_switch() {
    let root = unique_tmp_dir("rebuild-rename");
    std::fs::create_dir_all(&root).unwrap();

    let old_path = root.join("old_aaa.txt");
    std::fs::write(&old_path, b"old").unwrap();

    let idx = Arc::new(TieredIndex::empty(vec![root.clone()]));
    idx.apply_events(&[mk_event(1, EventType::Create, old_path.clone())]);
    assert!(!idx.query("old_aaa").is_empty());

    assert!(idx.try_start_rebuild_force());
    let new_l2 = Arc::new(PersistentIndex::new_with_roots(vec![root.clone()]));
    new_l2.apply_events(&[mk_event(2, EventType::Create, old_path.clone())]);

    let new_path = root.join("new_bbb.txt");
    std::fs::rename(&old_path, &new_path).unwrap();
    idx.apply_events(&[mk_event(
        3,
        EventType::Rename {
            from: FileIdentifier::Path(old_path.clone()),
            from_path_hint: Some(old_path.clone()),
        },
        new_path.clone(),
    )]);

    idx.finish_rebuild(new_l2);
    assert!(idx.query("old_aaa").is_empty());
    assert!(!idx.query("new_bbb").is_empty());
}

#[test]
fn fast_sync_reconciles_add_and_delete() {
    let root = unique_tmp_dir("fast-sync");
    std::fs::create_dir_all(&root).unwrap();

    let a = root.join("a_match.txt");
    let b = root.join("b_match.txt");
    std::fs::write(&a, b"a").unwrap();
    std::fs::write(&b, b"b").unwrap();

    let idx = TieredIndex::empty(vec![root.clone()]);
    idx.apply_events(&[
        mk_event(1, EventType::Create, a.clone()),
        mk_event(2, EventType::Create, b.clone()),
    ]);
    assert!(!idx.query("a_match").is_empty());
    assert!(!idx.query("b_match").is_empty());

    // 离线变更：不经过事件管道直接修改文件系统
    std::fs::remove_file(&b).unwrap();
    let c = root.join("c_match.txt");
    std::fs::write(&c, b"c").unwrap();

    let r = idx.fast_sync(
        DirtyScope::Dirs {
            cutoff_ns: 0,
            dirs: vec![root.clone()],
        },
        &[],
    );
    assert!(r.dirs_scanned >= 1);
    assert!(r.upsert_events >= 1);
    // Linux 上删除后立即新建文件时可能复用 inode，当前实现会把它视为
    // same-FileKey reconcile；此时最终状态正确，但 delete_events 可能为 0。
    assert!(
        r.delete_events >= 1 || r.upsert_events >= 2,
        "expected a delete event or a same-FileKey reconcile: {r:?}"
    );

    assert!(!idx.query("a_match").is_empty());
    assert!(idx.query("b_match").is_empty());

    // fast_sync 后底层索引可能有极短异步窗口，poll 等待 c_match 出现
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
    let mut c_found = false;
    while std::time::Instant::now() < deadline {
        if !idx.query("c_match").is_empty() {
            c_found = true;
            break;
        }
        std::thread::sleep(std::time::Duration::from_millis(50));
    }
    assert!(c_found, "c_match should appear after fast_sync");
    assert!(!idx.query("c_match").is_empty());
}

#[test]
fn fast_sync_and_immediate_scan_consume_io_governor() {
    let root = unique_tmp_dir("io-governor-sync");
    std::fs::create_dir_all(&root).unwrap();
    let first = root.join("governor_first.txt");
    std::fs::write(&first, b"first").unwrap();

    let l1 = L1Cache::with_capacity(1000);
    let l2 = Arc::new(PersistentIndex::new_with_roots(vec![root.clone()]));
    let l3 = IndexBuilder::new(vec![root.clone()]);
    let governor = Arc::new(IoGovernor::new(true, 1_000_000));
    let idx = TieredIndex::new_with_base_and_io_governor(
        l1,
        l2,
        l3,
        vec![root.clone()],
        false,
        true,
        false,
        Vec::new(),
        None,
        governor,
    );

    let (scanned, _) = idx.scan_dirs_immediate(std::slice::from_ref(&root));
    assert_eq!(scanned, 1);
    let after_scan = idx.io_governor.operations();
    assert!(after_scan >= 1);

    let second = root.join("governor_second.txt");
    std::fs::write(&second, b"second").unwrap();
    let report = idx.fast_sync(
        DirtyScope::Dirs {
            cutoff_ns: 0,
            dirs: vec![root.clone()],
        },
        &[],
    );
    assert!(report.dirs_scanned >= 1);
    assert!(idx.io_governor.operations() > after_scan);

    let _ = std::fs::remove_dir_all(&root);
}

#[test]
fn fast_sync_untrusted_clock_forces_full_crawl_despite_future_cutoff() {
    let root = unique_tmp_dir("fast-sync-clock");
    std::fs::create_dir_all(&root).unwrap();

    let idx = TieredIndex::empty(vec![root.clone()]);
    let unseen = root.join("clock_reconcile_new.txt");
    std::fs::write(&unseen, b"new").unwrap();

    {
        let mut clock = idx.clock_skew.lock();
        let base_wall = std::time::SystemTime::now();
        let base_mono = std::time::Instant::now();
        assert!(!clock.observe(base_wall, base_mono));
        assert!(clock.observe(
            base_wall - std::time::Duration::from_secs(3),
            base_mono + std::time::Duration::from_secs(3),
        ));
        assert!(!clock.cutoff_trusted());
    }

    let future_cutoff_ns =
        crate::event::sync::now_ns().saturating_add(24 * 60 * 60 * 1_000_000_000);
    let report = idx.fast_sync(
        DirtyScope::Dirs {
            cutoff_ns: future_cutoff_ns,
            dirs: vec![root.clone()],
        },
        &[],
    );

    assert!(report.dirs_scanned >= 1);
    assert!(report.upsert_events >= 1);
    assert!(!idx.query("clock_reconcile_new").is_empty());
    assert!(idx.clock_skew.lock().cutoff_trusted());

    let _ = std::fs::remove_dir_all(&root);
}

#[test]
fn nfd_query_matches_nfc_normalized_path() {
    let root = unique_tmp_dir("unicode-query");
    std::fs::create_dir_all(&root).unwrap();

    let nfc_path = root.join("café_nfc.txt");
    std::fs::write(&nfc_path, b"nfc").unwrap();

    let idx = TieredIndex::empty(vec![root.clone()]);
    let (scanned, _) = idx.scan_dirs_immediate_deep(std::slice::from_ref(&root));
    assert!(scanned >= 1);

    let results = idx.query("cafe\u{301}");
    assert!(
        results
            .iter()
            .any(|meta| meta.path.to_string_lossy().contains("café_nfc.txt")),
        "nfd query should match nfc-normalized indexed path: {results:?}"
    );

    let _ = std::fs::remove_dir_all(&root);
}

#[cfg(unix)]
#[test]
fn deep_scan_reconciles_directory_rename_when_old_path_is_gone() {
    let root = unique_tmp_dir("deep-rename-reconcile");
    let old_project = root.join("old_project");
    let deep_dir = old_project.join("deps_tree/libA/out_js");
    std::fs::create_dir_all(&deep_dir).unwrap();

    let old_file = deep_dir.join("bundle_1.js");
    std::fs::write(&old_file, b"bundle").unwrap();

    let idx = TieredIndex::empty(vec![root.clone()]);
    let (scanned, _) = idx.scan_dirs_immediate_deep(std::slice::from_ref(&root));
    assert!(scanned >= 1);

    let new_project = root.join("new_project");
    std::fs::rename(&old_project, &new_project).unwrap();

    let (rescanned, _) = idx.scan_dirs_immediate_deep(std::slice::from_ref(&new_project));
    assert!(rescanned >= 1);

    let results = idx.query("bundle_1");
    assert!(
        results.iter().any(|meta| meta
            .path
            .to_string_lossy()
            .contains("new_project/deps_tree/libA/out_js/bundle_1.js")),
        "renamed deep path should be visible after reconcile: {results:?}"
    );
    assert!(
        results
            .iter()
            .all(|meta| !meta.path.to_string_lossy().contains("old_project/")),
        "old renamed path should be removed after reconcile: {results:?}"
    );
    let old_results = idx.query("old_project");
    assert!(
        old_results.is_empty(),
        "renamed old subtree should be shadowed even when query only matches old path: {old_results:?}"
    );

    let _ = std::fs::remove_dir_all(&root);
}

#[tokio::test]
async fn auto_flush_overlay_wakes_snapshot_loop() {
    let root = unique_tmp_dir("auto-flush");
    std::fs::create_dir_all(&root).unwrap();

    let store = Arc::new(SnapshotStore::new(root.join("index.db")));
    let idx = Arc::new(TieredIndex::empty(vec![root.clone()]));
    // 低阈值：1 条路径/1 字节即可触发
    idx.set_auto_flush_limits(1, 1);

    let h = tokio::spawn(idx.clone().snapshot_loop(store.clone(), 3600));

    let p = root.join("a.txt");
    std::fs::write(&p, b"a").unwrap();
    idx.apply_events(&[mk_event(1, EventType::Create, p.clone())]);

    let v7_path = store.path().with_extension("v7");
    let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(5);
    while !v7_path.exists() {
        if tokio::time::Instant::now() >= deadline {
            panic!("auto flush did not produce a v7 snapshot in time");
        }
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    }

    h.abort();
}

#[tokio::test]
async fn removed_size_filter_is_not_text_matched() -> anyhow::Result<()> {
    let root = unique_tmp_dir("query-dsl-no-leak");
    std::fs::create_dir_all(&root)?;

    let store = Arc::new(SnapshotStore::new(root.join("index.db")));
    let idx = Arc::new(TieredIndex::empty(vec![root.clone()]));

    let p = root.join("x.txt");
    std::fs::write(&p, b"x")?;
    idx.apply_events(&[mk_event(1, EventType::Create, p.clone())]);

    // flush: 让旧元数据进入 v7 快照
    idx.snapshot_now(store.clone()).await?;

    // 修改文件：新元数据进入 L2。`size:` 已移除，查询入口不能把它当普通文本匹配。
    std::fs::write(&p, vec![b'a'; 128])?;
    idx.apply_events(&[mk_event(2, EventType::Modify, p.clone())]);

    let r = idx.query_limit("size:<10b", 100);
    assert!(r.is_empty(), "removed size filter must not text-match");
    assert!(idx.query_limit_detailed_strict("size:<10b", 100).is_err());

    let _ = std::fs::remove_dir_all(&root);
    Ok(())
}

#[test]
fn content_filter_is_explicitly_unsupported_until_index_enabled() -> anyhow::Result<()> {
    let root = unique_tmp_dir("query-content-disabled");
    std::fs::create_dir_all(&root)?;

    let p = root.join("content_probe.txt");
    std::fs::write(&p, b"needle in body only")?;
    let idx = Arc::new(TieredIndex::empty(vec![root.clone()]));
    idx.apply_events(&[mk_event(1, EventType::Create, p.clone())]);

    let err = idx
        .query_limit_detailed_strict("content:needle", 100)
        .unwrap_err()
        .to_string();
    assert!(err.contains("content index is disabled"));
    assert!(
        idx.query_limit("content:needle", 100).is_empty(),
        "legacy query path must not text-match disabled content filters"
    );

    let text_err = idx
        .query_limit_detailed_strict("text:needle", 100)
        .unwrap_err()
        .to_string();
    assert!(text_err.contains("content index is disabled"));

    let _ = std::fs::remove_dir_all(&root);
    Ok(())
}

#[test]
fn enabled_content_index_supports_content_and_text_filters() -> anyhow::Result<()> {
    let root = unique_tmp_dir("query-content-enabled");
    std::fs::create_dir_all(&root)?;

    let hit = root.join("body_only.txt");
    let md_hit = root.join("body_only.md");
    let wrong_ext = root.join("body_only.log");
    let too_large = root.join("oversized.txt");
    std::fs::write(&hit, b"needle appears only inside this file")?;
    std::fs::write(&md_hit, b"another unique needle payload")?;
    std::fs::write(&wrong_ext, b"needle should not be indexed")?;
    std::fs::write(&too_large, vec![b'n'; 80])?;

    let idx = Arc::new(TieredIndex::empty(vec![root.clone()]));
    let l2 = idx.l2.load_full();
    IndexBuilder::new(vec![root.clone()]).full_build(l2.as_ref());
    idx.refresh_base();
    idx.apply_content_index_config(ContentIndexConfig {
        enable: true,
        max_file_size: 40,
        include_ext: vec!["txt".to_string(), ".md".to_string()],
        exclude_ext: vec!["log".to_string()],
    });

    let report = idx.rebuild_content_index_now();
    assert_eq!(report.indexed_paths, 2);
    assert!(report.indexed_bytes > 0);

    let content_results = idx.query_limit_detailed_strict("content:needle", 10)?;
    let paths = content_results
        .iter()
        .map(|result| result.meta.path.clone())
        .collect::<Vec<_>>();
    assert!(paths.contains(&hit));
    assert!(paths.contains(&md_hit));
    assert!(!paths.contains(&wrong_ext));
    assert!(!paths.contains(&too_large));

    let text_results = idx.query_limit_detailed_strict("text:another", 10)?;
    assert_eq!(text_results.len(), 1);
    assert_eq!(text_results[0].meta.path, md_hit);

    let or_results = idx.query_limit_detailed_strict("content:missing | text:another", 10)?;
    assert_eq!(or_results.len(), 1);
    assert_eq!(or_results[0].meta.path, md_hit);

    let exclude_results = idx.query_limit_detailed_strict("content:needle !text:another", 10)?;
    assert_eq!(exclude_results.len(), 1);
    assert_eq!(exclude_results[0].meta.path, hit);

    let filename_results = idx.query_limit_detailed_strict("content:needle body_only", 10)?;
    assert!(filename_results
        .iter()
        .any(|result| result.meta.path == hit));
    assert!(filename_results
        .iter()
        .any(|result| result.meta.path == md_hit));

    let mut diagnostics = DiagnosticReport::default();
    idx.collect(&mut diagnostics);
    assert!(diagnostics.storage.content_index_enabled);
    assert_eq!(diagnostics.storage.content_indexed_paths, 2);
    assert_eq!(
        diagnostics.storage.content_indexed_bytes,
        report.indexed_bytes
    );

    let _ = std::fs::remove_dir_all(&root);
    Ok(())
}

#[test]
fn strict_dsl_preserves_path_initials_with_filename_tail() {
    let root = unique_tmp_dir("query-path-initials-tail");
    let dir = root.join("segment/client/user/search");
    std::fs::create_dir_all(&dir).unwrap();

    let p = dir.join("initials_probe_123.txt");
    std::fs::write(&p, b"initials").unwrap();
    let idx = Arc::new(TieredIndex::empty(vec![root.clone()]));
    idx.apply_events(&[mk_event(1, EventType::Create, p.clone())]);

    let results = idx
        .query_limit_detailed_strict("c/u/s/initials_probe_123", 100)
        .unwrap();
    assert!(
        results.iter().any(|r| r.meta.path == p),
        "strict DSL query should preserve path-initials matching for smoke-style filename tails"
    );

    let _ = std::fs::remove_dir_all(&root);
}

#[tokio::test]
async fn v7_load_mounts_base_without_l2_hydration_and_preserves_next_snapshot() -> anyhow::Result<()>
{
    let root = unique_tmp_dir("v7-direct-load");
    let content_root = root.join("content");
    let state_root = root.join("state");
    std::fs::create_dir_all(&content_root)?;
    std::fs::create_dir_all(&state_root)?;

    let store = Arc::new(SnapshotStore::new(state_root.join("index.db")));
    let idx = Arc::new(TieredIndex::empty(vec![content_root.clone()]));

    let alpha = content_root.join("alpha_direct_v7.txt");
    std::fs::write(&alpha, b"alpha")?;
    idx.apply_events(&[mk_event(1, EventType::Create, alpha.clone())]);
    idx.snapshot_now(store.clone()).await?;

    let loaded = Arc::new(TieredIndex::load_or_empty(&*store, vec![content_root.clone()]).await?);
    assert_eq!(
        loaded.l2.load().file_count(),
        0,
        "v7 load should mount BaseIndexData directly instead of hydrating L2"
    );
    assert_eq!(loaded.file_count(), 1);
    assert!(!loaded.query("alpha_direct_v7").is_empty());

    let beta = content_root.join("beta_after_direct_load.txt");
    std::fs::write(&beta, b"beta")?;
    loaded.apply_events(&[mk_event(2, EventType::Create, beta.clone())]);
    assert!(!loaded.query("beta_after_direct_load").is_empty());
    loaded.snapshot_now(store.clone()).await?;

    let reloaded = TieredIndex::load_or_empty(&*store, vec![content_root.clone()]).await?;
    assert_eq!(
        reloaded.l2.load().file_count(),
        0,
        "reloaded v7 snapshot should still avoid L2 hydration"
    );
    assert_eq!(reloaded.file_count(), 2);
    assert!(!reloaded.query("alpha_direct_v7").is_empty());
    assert!(!reloaded.query("beta_after_direct_load").is_empty());

    let _ = std::fs::remove_dir_all(&root);
    Ok(())
}

#[tokio::test]
async fn v7_load_mounts_manifest_only_cold_segment_and_queries_on_demand() -> anyhow::Result<()> {
    let root = unique_tmp_dir("v7-cold-manifest");
    let content_root = root.join("content");
    let archive = content_root.join("archive");
    let state_root = root.join("state");
    std::fs::create_dir_all(&archive)?;
    std::fs::create_dir_all(&state_root)?;

    let store = Arc::new(SnapshotStore::new(state_root.join("index.db")));
    let idx = Arc::new(TieredIndex::empty(vec![content_root.clone()]));

    for i in 0..128u32 {
        let path = archive.join(format!("cold_file_{i:03}.txt"));
        std::fs::write(&path, format!("cold-{i}"))?;
        idx.apply_events(&[mk_event(u64::from(i) + 1, EventType::Create, path)]);
    }
    idx.refresh_base();
    let hot_report = idx.memory_report(EventPipelineStats::default()).base;
    assert_eq!(hot_report.hot_memory_entries, 128);
    assert_eq!(hot_report.manifest_only_entries, 0);
    idx.snapshot_now(store.clone()).await?;
    let after_snapshot_report = idx.memory_report(EventPipelineStats::default()).base;
    assert_eq!(idx.l2.load().file_count(), 0);
    assert_eq!(after_snapshot_report.hot_memory_entries, 0);
    assert_eq!(after_snapshot_report.manifest_only_entries, 128);
    assert_eq!(after_snapshot_report.cold_segment_count, 1);
    assert!(after_snapshot_report.cold_mmap_bytes > 0);
    assert!(
        after_snapshot_report.estimated_bytes < hot_report.estimated_bytes,
        "snapshot should remount base as manifest-only cold segment: hot={hot_report:?} after={after_snapshot_report:?}"
    );
    let after_snapshot_results = idx.query_limit_detailed("cold_file_042", 10);
    assert_eq!(after_snapshot_results.len(), 1);
    assert_eq!(
        after_snapshot_results[0].index_tier,
        QueryResultIndexTier::FrozenManifestOnly
    );

    let loaded = Arc::new(TieredIndex::load_or_empty(&*store, vec![content_root.clone()]).await?);
    let cold_report = loaded.memory_report(EventPipelineStats::default()).base;
    assert_eq!(loaded.l2.load().file_count(), 0);
    assert_eq!(cold_report.hot_memory_entries, 0);
    assert_eq!(cold_report.manifest_only_entries, 128);
    assert_eq!(cold_report.cold_segment_count, 1);
    assert!(cold_report.cold_mmap_bytes > 0);
    assert!(
        cold_report.estimated_bytes < hot_report.estimated_bytes,
        "manifest-only residency should be smaller than hydrated base: hot={hot_report:?} cold={cold_report:?}"
    );

    let results = loaded.query_limit_detailed("cold_file_042", 10);
    assert_eq!(results.len(), 1);
    assert_eq!(
        results[0].index_tier,
        QueryResultIndexTier::FrozenManifestOnly
    );
    assert!(results[0].validated);

    let after_query = loaded.memory_report(EventPipelineStats::default()).base;
    assert_eq!(after_query.hot_memory_entries, 0);
    assert_eq!(after_query.manifest_only_entries, 128);

    let _ = std::fs::remove_dir_all(&root);
    Ok(())
}

#[tokio::test]
async fn periodic_flush_batch_threshold_skips_then_flushes() {
    let root = unique_tmp_dir("periodic-batch-events");
    std::fs::create_dir_all(&root).unwrap();

    let store = Arc::new(SnapshotStore::new(root.join("index.db")));
    let idx = Arc::new(TieredIndex::empty(vec![root.clone()]));
    idx.set_auto_flush_limits(0, 0);
    idx.set_periodic_flush_batch_limits(2, 0);

    let h = tokio::spawn(idx.clone().snapshot_loop(store.clone(), 1));

    let v7_path = store.path().with_extension("v7");

    let p1 = root.join("a.txt");
    std::fs::write(&p1, b"a").unwrap();
    idx.apply_events(&[mk_event(1, EventType::Create, p1.clone())]);

    tokio::time::sleep(std::time::Duration::from_millis(1300)).await;
    assert!(!v7_path.exists());

    let p2 = root.join("b.txt");
    std::fs::write(&p2, b"b").unwrap();
    idx.apply_events(&[mk_event(2, EventType::Create, p2.clone())]);

    let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(5);
    while !v7_path.exists() {
        if tokio::time::Instant::now() >= deadline {
            panic!("periodic flush did not flush after event threshold was met");
        }
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    }

    h.abort();
}

#[tokio::test]
async fn periodic_flush_batch_byte_threshold_skips_then_flushes() {
    let root = unique_tmp_dir("periodic-batch-bytes");
    std::fs::create_dir_all(&root).unwrap();

    let store = Arc::new(SnapshotStore::new(root.join("index.db")));
    let idx = Arc::new(TieredIndex::empty(vec![root.clone()]));
    idx.set_auto_flush_limits(0, 0);

    let p1 = root.join("alpha-long-name.txt");
    let ev1 = mk_event(1, EventType::Create, p1.clone());
    let threshold = event_record_estimated_bytes(&ev1).saturating_add(1);
    idx.set_periodic_flush_batch_limits(0, threshold);

    let h = tokio::spawn(idx.clone().snapshot_loop(store.clone(), 1));

    let v7_path = store.path().with_extension("v7");

    std::fs::write(&p1, b"a").unwrap();
    idx.apply_events(&[ev1]);

    tokio::time::sleep(std::time::Duration::from_millis(1300)).await;
    assert!(!v7_path.exists());

    let p2 = root.join("beta-long-name.txt");
    std::fs::write(&p2, b"b").unwrap();
    idx.apply_events(&[mk_event(2, EventType::Create, p2.clone())]);

    let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(5);
    while !v7_path.exists() {
        if tokio::time::Instant::now() >= deadline {
            panic!("periodic flush did not flush after byte threshold was met");
        }
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    }

    h.abort();
}

#[test]
fn base_delta_delete_blocks_base() {
    let root = unique_tmp_dir("lsm-del");
    std::fs::create_dir_all(&root).unwrap();

    let alpha = root.join("alpha_test.txt");
    let gamma = root.join("gamma_test.txt");
    std::fs::write(&alpha, b"alpha").unwrap();

    let idx = TieredIndex::empty(vec![root.clone()]);
    idx.apply_events(&[mk_event(1, EventType::Create, alpha.clone())]);
    assert_eq!(idx.query("alpha").len(), 1);

    std::fs::remove_file(&alpha).unwrap();
    std::fs::write(&gamma, b"gamma").unwrap();
    idx.apply_events(&[
        mk_event(2, EventType::Delete, alpha.clone()),
        mk_event(3, EventType::Create, gamma.clone()),
    ]);

    assert!(idx.query("alpha").is_empty());
    assert_eq!(idx.query("gamma").len(), 1);
}

#[test]
fn base_delta_delete_then_recreate_prefers_newest() {
    let root = unique_tmp_dir("lsm-recreate");
    std::fs::create_dir_all(&root).unwrap();

    let alpha = root.join("alpha_test.txt");
    std::fs::write(&alpha, b"a").unwrap();

    let idx = TieredIndex::empty(vec![root.clone()]);
    idx.apply_events(&[mk_event(1, EventType::Create, alpha.clone())]);
    assert_eq!(idx.query("alpha").len(), 1);

    std::fs::remove_file(&alpha).unwrap();
    idx.apply_events(&[mk_event(2, EventType::Delete, alpha.clone())]);
    assert!(idx.query("alpha").is_empty());

    std::fs::write(&alpha, b"bb").unwrap();
    idx.apply_events(&[mk_event(3, EventType::Create, alpha.clone())]);

    let r = idx.query("alpha");
    assert_eq!(r.len(), 1);
    assert_eq!(r[0].size, 2);
}

#[test]
fn query_same_path_different_filekey_prefers_delta_overlay() {
    use crate::core::{FileKey, FileMeta};

    let root = unique_tmp_dir("q-samepath-newest");
    std::fs::create_dir_all(&root).unwrap();

    let a = root.join("a.txt");
    std::fs::write(&a, b"x").unwrap();

    let idx = TieredIndex::empty(vec![root.clone()]);
    let l2 = idx.l2.load_full();
    l2.upsert(FileMeta {
        file_key: FileKey {
            dev: 1,
            ino: 100,
            generation: 0,
        },
        path: a.clone(),
        size: 1,
        mtime: None,
        ctime: None,
        atime: None,
        kind: Default::default(),
    });
    idx.refresh_base();
    idx.apply_events(&[mk_event(2, EventType::Create, a.clone())]);

    let r = idx.query("a.txt");
    assert_eq!(r.len(), 1);
    assert_ne!(r[0].file_key.ino, 100);
}

#[test]
fn query_rename_from_tombstone_blocks_old_path() {
    let root = unique_tmp_dir("q-rename-tombstone");
    std::fs::create_dir_all(&root).unwrap();

    let old = root.join("old.txt");
    let newp = root.join("new.txt");
    std::fs::write(&old, b"old").unwrap();
    std::fs::write(&newp, b"new").unwrap();

    let idx = TieredIndex::empty(vec![root.clone()]);
    idx.apply_events(&[mk_event(1, EventType::Create, old.clone())]);
    assert_eq!(idx.query("old.txt").len(), 1);

    idx.apply_events(&[
        mk_event(2, EventType::Delete, old.clone()),
        mk_event(3, EventType::Create, newp.clone()),
    ]);

    assert!(idx.query("old.txt").is_empty());
    assert_eq!(idx.query("new.txt").len(), 1);

    // Query wide pattern that would match both paths: old must remain blocked.
    let stale_hits_before = idx.stats_report().query_stale_hit_count;
    let all = idx.query(".txt");
    assert_eq!(all.len(), 1);
    assert!(all[0].path.to_string_lossy().ends_with("new.txt"));
    assert!(idx.stats_report().query_stale_hit_count > stale_hits_before);
}

#[test]
fn query_same_filekey_multiple_paths_only_returns_newest_path() {
    let root = unique_tmp_dir("q-samekey-newestpath");
    std::fs::create_dir_all(&root).unwrap();

    let p1 = root.join("ghost_v1.txt");
    let p2 = root.join("ghost_v2.txt");
    let p3 = root.join("ghost_v3.txt");
    std::fs::write(&p1, b"1").unwrap();
    std::fs::write(&p2, b"2").unwrap();
    std::fs::write(&p3, b"3").unwrap();

    let idx = TieredIndex::empty(vec![root.clone()]);
    idx.apply_events(&[mk_event(1, EventType::Create, p1.clone())]);
    assert_eq!(idx.query("ghost_").len(), 1);
    idx.apply_events(&[mk_event(
        2,
        EventType::Rename {
            from: FileIdentifier::Path(p1.clone()),
            from_path_hint: Some(p1.clone()),
        },
        p2.clone(),
    )]);
    idx.apply_events(&[mk_event(
        3,
        EventType::Rename {
            from: FileIdentifier::Path(p2.clone()),
            from_path_hint: Some(p2.clone()),
        },
        p3.clone(),
    )]);

    let r = idx.query("ghost_");
    assert_eq!(r.len(), 1);
    assert!(r[0].path.to_string_lossy().ends_with("ghost_v3.txt"));
}

#[tokio::test]
async fn lsm_offline_dir_mtime_change_skips_disk_segments() {
    use crate::core::{FileKey, FileMeta};
    use crate::index::PersistentIndex;

    let base = unique_tmp_dir("lsm-offline-mtime");
    let content_root = base.join("content");
    let state_root = base.join("state");
    std::fs::create_dir_all(&content_root).unwrap();
    std::fs::create_dir_all(&state_root).unwrap();

    // 预先创建深层目录（用于验证"深层变更不冒泡到 root"的场景）。
    let deep = content_root.join("deep");
    std::fs::create_dir_all(&deep).unwrap();

    let alpha = deep.join("alpha_test.txt");
    std::fs::write(&alpha, b"a").unwrap();

    let store = SnapshotStore::new(state_root.join("index.db"));

    // base: alpha（写入 LSM，生成 last_build_ns）
    let base_idx = PersistentIndex::new_with_roots(vec![content_root.clone()]);
    base_idx.upsert(FileMeta {
        file_key: FileKey {
            dev: 1,
            ino: 1,
            generation: 0,
        },
        path: alpha.clone(),
        size: 1,
        mtime: None,
        ctime: None,
        atime: None,
        kind: Default::default(),
    });
    store
        .lsm_replace_base_v6(
            &base_idx.export_segments_v6(),
            None,
            std::slice::from_ref(&content_root),
            0,
        )
        .await
        .unwrap();
    store.gc_stale_segments().unwrap();

    // 离线变更：在 deep 下新增文件（只会更新 deep 的 mtime，不会更新 content_root 的 mtime）
    std::thread::sleep(std::time::Duration::from_millis(20));
    std::fs::write(deep.join("offline_new.txt"), b"x").unwrap();

    // 重新启动加载：应判定快照不可信，不挂载 disk segments。
    let idx = TieredIndex::load_or_empty(&store, vec![content_root.clone()])
        .await
        .unwrap();
    assert_eq!(idx.file_count(), 0);
}

#[test]
fn cold_query_validates_unchanged_base_result() {
    let root = unique_tmp_dir("cold-query-unchanged");
    std::fs::create_dir_all(&root).unwrap();

    let path = root.join("stable_cold_hit.txt");
    std::fs::write(&path, b"stable").unwrap();

    let idx = TieredIndex::empty(vec![root.clone()]);
    idx.apply_events(&[mk_event(1, EventType::Create, path.clone())]);
    idx.refresh_base();

    let results = idx.query_limit_detailed("stable_cold_hit", 10);
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].freshness, QueryResultFreshness::StaleChecked);
    assert_eq!(results[0].index_tier, QueryResultIndexTier::ColdMmap);
    assert!(results[0].validated);

    let stats = idx.stats_report();
    assert_eq!(stats.cold_validate_count, 1);
    assert_eq!(stats.query_stale_hit_count, 0);

    let _ = std::fs::remove_dir_all(&root);
}

#[test]
fn cold_query_suppresses_deleted_base_result() {
    let root = unique_tmp_dir("cold-query-deleted");
    std::fs::create_dir_all(&root).unwrap();

    let path = root.join("deleted_cold_hit.txt");
    std::fs::write(&path, b"gone").unwrap();

    let idx = TieredIndex::empty(vec![root.clone()]);
    idx.apply_events(&[mk_event(1, EventType::Create, path.clone())]);
    idx.refresh_base();

    std::fs::remove_file(&path).unwrap();

    let results = idx.query_limit_detailed("deleted_cold_hit", 10);
    assert!(results.is_empty());

    let stats = idx.stats_report();
    assert_eq!(stats.cold_validate_count, 1);
    assert_eq!(stats.query_stale_hit_count, 1);
    assert!(
        idx.query("deleted_cold_hit").is_empty(),
        "missing cold hit should be tombstoned by query validation"
    );

    let _ = std::fs::remove_dir_all(&root);
}

#[test]
fn cold_query_changed_result_enqueues_parent_rescan() {
    let root = unique_tmp_dir("cold-query-changed");
    std::fs::create_dir_all(&root).unwrap();

    let path = root.join("changed_cold_hit.txt");
    std::fs::write(&path, b"old").unwrap();

    let idx = TieredIndex::empty(vec![root.clone()]);
    idx.apply_events(&[mk_event(1, EventType::Create, path.clone())]);
    idx.refresh_base();

    std::thread::sleep(std::time::Duration::from_millis(25));
    std::fs::write(&path, b"new-content").unwrap();

    let results = idx.query_limit_detailed("changed_cold_hit", 10);
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].freshness, QueryResultFreshness::Changed);
    assert!(results[0].validated);

    let stats = idx.stats_report();
    assert_eq!(stats.cold_validate_count, 1);
    assert_eq!(stats.query_stale_hit_count, 1);
    assert_eq!(idx.dirty_queue_len(), 1);

    let dirty_entry = idx.dirty_queue.lock().pop_ready(u64::MAX, 1).pop().unwrap();
    assert_eq!(dirty_entry.reason, DirtyReason::QueryHitStale);
    let report = idx.process_dirty_entry(dirty_entry, &[]);
    assert!(!report.failed);
    assert_eq!(report.dirs_scanned, 1);

    let hot_results = idx.query_limit_detailed("changed_cold_hit", 10);
    assert_eq!(hot_results.len(), 1);
    assert_eq!(hot_results[0].freshness, QueryResultFreshness::Fresh);
    assert!(!hot_results[0].validated);

    let _ = std::fs::remove_dir_all(&root);
}

#[test]
fn query_miss_path_enqueues_dirty_parent() {
    let root = unique_tmp_dir("query-miss-dirty");
    std::fs::create_dir_all(root.join("missing_dir")).unwrap();

    let idx = TieredIndex::empty(vec![root.clone()]);
    let results = idx.query_limit_detailed("missing_dir/missing_file.txt", 10);
    assert!(results.is_empty());
    assert_eq!(idx.dirty_queue_len(), 1);

    let dirty_entry = idx.dirty_queue.lock().pop_ready(u64::MAX, 1).pop().unwrap();
    assert_eq!(dirty_entry.reason, DirtyReason::QueryMiss);
    assert_eq!(dirty_entry.scope.dir_paths(), &[root.join("missing_dir")]);

    let _ = std::fs::remove_dir_all(&root);
}

#[test]
fn dirty_queue_missing_leaf_retries_parent_scope() {
    let root = unique_tmp_dir("dirty-retry-parent");
    std::fs::create_dir_all(root.join("parent")).unwrap();
    let missing_leaf = root.join("parent").join("gone");

    let idx = TieredIndex::empty(vec![root.clone()]);
    idx.enqueue_dirty_dirs(vec![missing_leaf], DirtyReason::QueryHitStale);

    let entry = idx.dirty_queue.lock().pop_ready(u64::MAX, 1).pop().unwrap();
    let report = idx.process_dirty_entry(entry.clone(), &[]);
    assert!(report.failed);
    assert!(idx.retry_dirty_entry(entry));

    let retry = idx.dirty_queue.lock().pop_ready(u64::MAX, 1).pop().unwrap();
    assert_eq!(retry.scope.dir_paths(), &[root.join("parent")]);
    let retry_report = idx.process_dirty_entry(retry, &[]);
    assert!(!retry_report.failed);
    assert_eq!(retry_report.dirs_scanned, 1);

    let _ = std::fs::remove_dir_all(&root);
}

#[test]
fn dirty_scan_collects_project_marker_roots() {
    let root = unique_tmp_dir("project-marker-scan");
    let project = root.join("project");
    std::fs::create_dir_all(project.join(".git")).unwrap();
    std::fs::write(
        project.join("Cargo.toml"),
        b"[package]\nname = \"marker-test\"\n",
    )
    .unwrap();

    let idx = TieredIndex::empty(vec![root.clone()]);
    idx.enqueue_dirty_dirs(vec![project.clone()], DirtyReason::QueryMiss);

    let entry = idx.dirty_queue.lock().pop_ready(u64::MAX, 1).pop().unwrap();
    let markers = vec!["Cargo.toml".to_string(), ".git".to_string()];
    let report = idx.process_dirty_entry_with_project_markers(entry, &[], &markers);

    assert!(!report.failed);
    assert_eq!(report.dirs_scanned, 1);
    assert_eq!(report.outcomes.len(), 1);
    assert_eq!(report.outcomes[0].outcome.project_roots, vec![project]);

    let _ = std::fs::remove_dir_all(&root);
}
