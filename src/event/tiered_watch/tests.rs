//! Inline tests for the tiered watch subsystem.

use std::path::{Path, PathBuf};
use std::sync::atomic::Ordering;

use crate::config::{L3ScanPolicy, TieredWatchConfig};
use crate::index::tiered::ScanOutcome;

use super::types::*;
use super::*;

fn runtime() -> TieredWatchRuntime {
    TieredWatchRuntime::new(
        vec![(PathBuf::from("/tmp/hot"), 2)],
        vec![(PathBuf::from("/tmp/warm"), 3)],
        5,
        5_000,
        20,
    )
}

fn temp_root(tag: &str) -> PathBuf {
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    std::env::temp_dir().join(format!(
        "fd-rdd-tiered-watch-{tag}-{}-{nanos}",
        std::process::id()
    ))
}

fn mount_table_for(path: &Path, fstype: &str) -> crate::fs_policy::MountTable {
    crate::fs_policy::MountTable::parse(&format!(
        "1 0 0:1 / {} rw - {} source rw\n",
        path.display(),
        fstype
    ))
}

#[test]
fn fast_scan_mount_classification_is_conservative() {
    assert_eq!(
        classify_fast_scan_fstype("ext4"),
        FastScanMountClass::LocalTrusted
    );
    assert_eq!(
        classify_fast_scan_fstype("nfs4"),
        FastScanMountClass::NetworkUntrusted
    );
    assert_eq!(
        classify_fast_scan_fstype("fuse.sshfs"),
        FastScanMountClass::FuseUntrusted
    );
    assert_eq!(
        classify_fast_scan_fstype("sshfs"),
        FastScanMountClass::FuseUntrusted
    );
    assert_eq!(
        classify_fast_scan_fstype("rclone"),
        FastScanMountClass::FuseUntrusted
    );
    assert_eq!(
        classify_fast_scan_fstype("mysteryfs"),
        FastScanMountClass::Unknown
    );
}

#[test]
fn fast_scan_sentinel_detects_local_directory_entry_change() {
    let root = temp_root("fast-scan-change");
    std::fs::create_dir_all(&root).unwrap();
    let table = mount_table_for(root.as_path(), "ext4");
    let rt = TieredWatchRuntime::new(Vec::new(), vec![(root.clone(), 1)], 16, 5_000, 20);
    assert!(rt.grant_fast_scan_lease(root.clone(), FastScanLeaseKind::Explicit, None, 1));
    assert_eq!(
        rt.bootstrap_fast_scan_dirs(vec![root.clone()], &table, 8),
        1
    );

    let cfg = FastScanTickConfig {
        target_secs: 0,
        local_stat_budget_per_tick: 2_000,
        local_readdir_budget_per_tick: 8,
        ..FastScanTickConfig::default()
    };
    let initial = rt.fast_scan_tick(&table, cfg);
    assert_eq!(initial.checked_dirs, 1);
    assert_eq!(initial.initial_dirs, vec![root.clone()]);
    assert!(initial.changed_dirs.is_empty());

    let settled = rt.fast_scan_tick(&table, cfg);
    assert_eq!(settled.checked_dirs, 1);
    assert!(settled.changed_dirs.is_empty());

    std::thread::sleep(std::time::Duration::from_millis(5));
    std::fs::write(root.join("created.txt"), b"new").unwrap();

    let changed = rt.fast_scan_tick(&table, cfg);
    assert_eq!(changed.checked_dirs, 1);
    assert_eq!(changed.changed_dirs, vec![root.clone()]);
    let report = rt.report();
    assert_eq!(report.fast_scan_known_dirs, 1);
    assert_eq!(report.fast_scan_local_trusted_dirs, 1);
    assert_eq!(report.fast_scan_changed_dirs, 2);
    assert!(report.fast_scan_local_strict_ok);

    let _ = std::fs::remove_dir_all(root);
}

#[test]
fn fast_scan_bootstrap_is_driven_by_uncovered_hotset_leases() {
    let root = temp_root("fast-scan-bootstrap-limit");
    let hot = root.join("hot");
    let warm = root.join("warm");
    std::fs::create_dir_all(&hot).unwrap();
    std::fs::create_dir_all(&warm).unwrap();
    let table = mount_table_for(root.as_path(), "ext4");
    let rt = TieredWatchRuntime::new(
        vec![(hot.clone(), 1)],
        vec![(warm.clone(), 1)],
        16,
        5_000,
        20,
    );

    assert!(rt.grant_fast_scan_lease(warm.clone(), FastScanLeaseKind::Explicit, None, 1));
    assert!(rt.should_bootstrap_fast_scan_dirs(1));
    assert_eq!(
        rt.bootstrap_fast_scan_dirs(vec![warm.clone()], &table, 1),
        1
    );
    assert!(!rt.should_bootstrap_fast_scan_dirs(1));

    let drained = rt.fast_scan_tick(
        &table,
        FastScanTickConfig {
            target_secs: 0,
            local_stat_budget_per_tick: 8,
            local_readdir_budget_per_tick: 8,
            ..FastScanTickConfig::default()
        },
    );
    assert_eq!(drained.initial_dirs, vec![warm.clone()]);
    assert!(drained.changed_dirs.is_empty());
    let second = root.join("second");
    std::fs::create_dir_all(&second).unwrap();
    assert!(rt.grant_fast_scan_lease(second.clone(), FastScanLeaseKind::Query, None, 1));
    assert!(rt.should_bootstrap_fast_scan_dirs(1));
    assert_eq!(rt.fast_scan_l0_roots(), vec![hot.clone()]);
    let excluded = rt.fast_scan_bootstrap_excluded_roots();
    assert_eq!(excluded.len(), 2);
    assert!(excluded.contains(&hot));
    assert!(excluded.contains(&warm));

    let l0_only = TieredWatchRuntime::new(vec![(hot.clone(), 1)], Vec::new(), 16, 5_000, 20);
    assert!(!l0_only.should_bootstrap_fast_scan_dirs(2_048));

    let _ = std::fs::remove_dir_all(root);
}

#[test]
fn fast_scan_bootstrap_exhaustion_adds_retry_cooldown() {
    let warm = PathBuf::from("/tmp/warm");
    let rt = TieredWatchRuntime::new(Vec::new(), vec![(warm, 1)], 16, 5_000, 20);

    assert!(rt.grant_fast_scan_lease(
        PathBuf::from("/tmp/warm"),
        FastScanLeaseKind::Query,
        None,
        1
    ));
    assert!(rt.should_bootstrap_fast_scan_dirs_at(2_048, 10_000));
    rt.record_fast_scan_bootstrap_result_at(0, 0, 2_048, 60_000, 10_000);

    assert!(!rt.should_bootstrap_fast_scan_dirs_at(2_048, 69_999));
    rt.record_fast_scan_bootstrap_result_at(1, 1, 2_048, 60_000, 20_000);
    assert!(rt.should_bootstrap_fast_scan_dirs_at(2_048, 20_000));
    rt.record_fast_scan_bootstrap_result_at(0, 0, 2_048, 60_000, 10_000);
    assert!(rt.should_bootstrap_fast_scan_dirs_at(2_048, 70_000));
}

#[test]
fn fast_scan_uncovered_or_backfill_pending_lease_blocks_strict_ok() {
    let root = temp_root("fast-scan-pending-strict");
    std::fs::create_dir_all(&root).unwrap();
    let table = mount_table_for(root.as_path(), "ext4");
    let rt = TieredWatchRuntime::new(Vec::new(), vec![(root.clone(), 1)], 16, 5_000, 20);

    assert!(rt.grant_fast_scan_lease(root.clone(), FastScanLeaseKind::Query, None, 1));
    let uncovered = rt.report();
    assert_eq!(uncovered.fast_scan_hotset_lease_count, 1);
    assert_eq!(uncovered.fast_scan_hotset_sentinel_count, 0);
    assert!(!uncovered.fast_scan_local_strict_ok);

    assert_eq!(
        rt.bootstrap_fast_scan_dirs(vec![root.clone()], &table, 8),
        1
    );
    let backfill_pending = rt.report();
    assert_eq!(backfill_pending.fast_scan_hotset_sentinel_count, 1);
    assert_eq!(backfill_pending.fast_scan_initial_backfill_pending, 1);
    assert!(!backfill_pending.fast_scan_local_strict_ok);

    let warmed = rt.fast_scan_tick(
        &table,
        FastScanTickConfig {
            target_secs: 0,
            local_stat_budget_per_tick: 8,
            initial_backfill_budget_per_tick: 8,
            ..FastScanTickConfig::default()
        },
    );
    assert_eq!(warmed.initial_dirs, vec![root.clone()]);
    assert!(rt.report().fast_scan_local_strict_ok);

    let _ = std::fs::remove_dir_all(root);
}

#[test]
fn fast_scan_record_scan_does_not_create_implicit_lease() {
    let root = temp_root("fast-scan-no-scan-lease");
    std::fs::create_dir_all(&root).unwrap();
    let rt = TieredWatchRuntime::new(Vec::new(), vec![(root.clone(), 1)], 16, 5_000, 20);

    rt.record_scan(
        root.as_path(),
        ScanOutcome {
            scanned: 1,
            changed: 1,
            elapsed_ms: 0,
            project_roots: Vec::new(),
        },
    );

    let report = rt.report();
    assert_eq!(report.fast_scan_hotset_lease_count, 0);
    assert_eq!(report.fast_scan_hotset_sentinel_count, 0);

    let _ = std::fs::remove_dir_all(root);
}

#[test]
fn fast_scan_registry_restore_trust_gate_controls_backfill() {
    let root = temp_root("fast-scan-registry");
    let hot = root.join("hot");
    let snapshot = root.join("snapshot.v7");
    std::fs::create_dir_all(&hot).unwrap();
    let table = mount_table_for(root.as_path(), "ext4");
    let cfg = TieredWatchConfig::default();

    let rt = TieredWatchRuntime::new(Vec::new(), vec![(hot.clone(), 1)], 16, 5_000, 20);
    rt.apply_fast_scan_config(&cfg);
    assert!(rt.grant_fast_scan_lease(hot.clone(), FastScanLeaseKind::Query, None, 1));
    assert_eq!(rt.bootstrap_fast_scan_dirs(vec![hot.clone()], &table, 8), 1);
    let warm = rt.fast_scan_tick(
        &table,
        FastScanTickConfig {
            target_secs: 0,
            local_stat_budget_per_tick: 8,
            initial_backfill_budget_per_tick: 8,
            ..FastScanTickConfig::default()
        },
    );
    assert_eq!(warm.initial_dirs, vec![hot.clone()]);
    assert_eq!(
        rt.persist_fast_scan_registry(&snapshot, &cfg, "stable", 42, true)
            .unwrap(),
        1
    );

    let trusted = TieredWatchRuntime::new(Vec::new(), vec![(hot.clone(), 1)], 16, 5_000, 20);
    trusted.apply_fast_scan_config(&cfg);
    let trusted_restore =
        trusted.restore_fast_scan_registry(&snapshot, &cfg, true, "stable", 42, &table);
    assert!(trusted_restore.trusted);
    assert_eq!(trusted_restore.restored_active, 1);
    assert_eq!(trusted_restore.restored_unknown, 0);
    let trusted_report = trusted.report();
    assert_eq!(trusted_report.fast_scan_hotset_sentinel_count, 1);
    assert_eq!(trusted_report.fast_scan_initial_backfill_pending, 0);
    assert!(trusted_report.fast_scan_local_strict_ok);

    let untrusted = TieredWatchRuntime::new(Vec::new(), vec![(hot.clone(), 1)], 16, 5_000, 20);
    untrusted.apply_fast_scan_config(&cfg);
    let untrusted_restore =
        untrusted.restore_fast_scan_registry(&snapshot, &cfg, false, "stable", 42, &table);
    assert!(!untrusted_restore.trusted);
    assert_eq!(untrusted_restore.reason, "unclean_shutdown");
    assert_eq!(untrusted_restore.restored_active, 0);
    assert_eq!(untrusted_restore.restored_unknown, 1);
    let untrusted_report = untrusted.report();
    assert_eq!(untrusted_report.fast_scan_hotset_sentinel_count, 1);
    assert_eq!(untrusted_report.fast_scan_initial_backfill_pending, 1);
    assert!(!untrusted_report.fast_scan_local_strict_ok);

    let _ = std::fs::remove_dir_all(root);
}

#[test]
fn fast_scan_reports_budget_degraded_for_local_sla_shortfall() {
    let root = temp_root("fast-scan-budget");
    let first = root.join("a");
    let second = root.join("b");
    std::fs::create_dir_all(&first).unwrap();
    std::fs::create_dir_all(&second).unwrap();
    let table = mount_table_for(root.as_path(), "ext4");
    let rt = TieredWatchRuntime::new(Vec::new(), vec![(root.clone(), 1)], 16, 5_000, 20);
    assert!(rt.grant_fast_scan_lease(first.clone(), FastScanLeaseKind::Query, None, 1));
    assert!(rt.grant_fast_scan_lease(second.clone(), FastScanLeaseKind::Query, None, 1));
    assert_eq!(
        rt.bootstrap_fast_scan_dirs(vec![first, second], &table, 8),
        2
    );
    let warmup = rt.fast_scan_tick(
        &table,
        FastScanTickConfig {
            target_secs: 0,
            local_stat_budget_per_tick: 2,
            initial_backfill_budget_per_tick: 2,
            ..FastScanTickConfig::default()
        },
    );
    assert_eq!(warmup.initial_dirs.len(), 2);

    let tick = rt.fast_scan_tick(
        &table,
        FastScanTickConfig {
            target_secs: 1,
            tick_ms: 1_000,
            local_stat_budget_per_tick: 1,
            ..FastScanTickConfig::default()
        },
    );

    assert!(tick.budget_degraded);
    let report = rt.report();
    assert!(report.fast_scan_budget_degraded);
    assert!(!report.fast_scan_local_strict_ok);
    assert!(report
        .fast_scan_last_degraded_reason
        .contains("below required"));

    let _ = std::fs::remove_dir_all(root);
}

#[test]
fn fast_scan_does_not_report_strict_sla_for_untrusted_mounts() {
    let root = temp_root("fast-scan-nfs");
    std::fs::create_dir_all(&root).unwrap();
    let table = mount_table_for(root.as_path(), "nfs4");
    let rt = TieredWatchRuntime::new(Vec::new(), vec![(root.clone(), 1)], 16, 5_000, 20);
    assert!(rt.grant_fast_scan_lease(root.clone(), FastScanLeaseKind::Explicit, None, 1));
    assert_eq!(
        rt.bootstrap_fast_scan_dirs(vec![root.clone()], &table, 8),
        1
    );

    let report = rt.report();
    assert_eq!(report.fast_scan_untrusted_dirs, 1);
    assert!(!report.fast_scan_sla_ok);
    assert_eq!(report.fast_scan_mode, "best_effort");

    let _ = std::fs::remove_dir_all(root);
}

#[test]
fn proc_sampler_report_is_exposed_in_watch_state() {
    let rt = runtime();
    rt.set_proc_sampler_enabled(true);
    rt.record_proc_sampler_report(
        ProcSamplerReport {
            duration_ms: 12,
            pids_seen: 30,
            pids_scanned: 20,
            pids_denied: 10,
            fdinfo_read_count: 40,
            readlink_count: 5,
            write_fd_count: 6,
            sampled_dirs: 7,
            budget_exhausted: true,
            unavailable: false,
        },
        3,
    );

    let report = rt.report();
    assert!(report.proc_sampler_enabled);
    assert_eq!(report.proc_sampler_last_duration_ms, 12);
    assert_eq!(report.proc_sampler_pids_seen, 30);
    assert_eq!(report.proc_sampler_pids_scanned, 20);
    assert_eq!(report.proc_sampler_pids_denied, 10);
    assert_eq!(report.proc_sampler_fdinfo_read_count, 40);
    assert_eq!(report.proc_sampler_readlink_count, 5);
    assert_eq!(report.proc_sampler_write_fd_count, 6);
    assert_eq!(report.proc_sampler_sampled_dirs, 7);
    assert_eq!(report.proc_sampler_triggered_watches, 3);
    assert!(report.proc_sampler_budget_exhausted);
    assert!(!report.proc_sampler_unavailable);
    assert!(report
        .notes
        .iter()
        .any(|note| note.contains("proc sampler write-fd dirs=7")));
}

#[test]
fn l0_event_refresh_blocks_idle_expiry() {
    let rt = runtime();
    rt.record_event_paths([&PathBuf::from("/tmp/hot/file.txt")]);
    assert!(rt.expired_l0(60).is_empty());
}

#[test]
fn l1_scan_changed_resets_empty_count() {
    let rt = runtime();
    let warm = PathBuf::from("/tmp/warm");

    rt.record_scan(
        warm.as_path(),
        ScanOutcome {
            scanned: 1,
            changed: 0,
            elapsed_ms: 1,
            project_roots: Vec::new(),
        },
    );
    rt.record_scan(
        warm.as_path(),
        ScanOutcome {
            scanned: 1,
            changed: 1,
            elapsed_ms: 1,
            project_roots: Vec::new(),
        },
    );

    let report = rt.report();
    assert_eq!(report.l1_dirs, 1);
    assert_eq!(report.scan_backlog, 1);
}

#[test]
fn promotion_reserves_budget_and_rollback_releases_it() {
    let rt = runtime();
    let warm = PathBuf::from("/tmp/warm");

    assert_eq!(
        rt.try_reserve_promotion(warm.as_path()),
        PromotionDecision::SendAdd
    );
    assert_eq!(rt.report().watched_dirs_estimated, 5);

    rt.rollback_promote(warm.as_path());
    assert_eq!(rt.report().watched_dirs_estimated, 2);
    assert_eq!(rt.report().l1_dirs, 1);
}

#[test]
fn confirm_demoted_releases_watch_budget() {
    let rt = runtime();
    let hot = PathBuf::from("/tmp/hot");

    assert!(rt.mark_demotion_pending(hot.as_path()));
    rt.confirm_demoted(hot.as_path());

    let report = rt.report();
    assert_eq!(report.l0_dirs, 0);
    assert_eq!(report.l1_dirs, 2);
    assert_eq!(report.watched_dirs_estimated, 0);
    assert_eq!(report.demotions, 1);
}

#[test]
fn dynamic_candidate_reserves_budget_and_promotes() {
    let rt = runtime();
    let dynamic = PathBuf::from("/tmp/hot/new-child");

    assert_eq!(
        rt.register_dynamic_candidate(dynamic.clone(), 1),
        PromotionDecision::SendAdd
    );
    let reserved = rt.report();
    assert_eq!(reserved.watched_dirs_estimated, 3);
    assert_eq!(reserved.l1_dirs, 2);

    rt.confirm_promoted(dynamic.as_path());
    let promoted = rt.report();
    assert_eq!(promoted.l0_dirs, 2);
    assert_eq!(promoted.l1_dirs, 1);
    assert_eq!(promoted.promotions, 1);
}

#[test]
fn project_marker_candidate_reserves_budget_and_marks_high_priority() {
    let rt = runtime();
    let project = PathBuf::from("/tmp/projects/app");

    assert_eq!(
        rt.register_project_marker_candidate(project.clone(), 1),
        PromotionDecision::SendAdd
    );
    let reserved = rt.report();
    assert_eq!(reserved.watched_dirs_estimated, 3);
    assert_eq!(reserved.l1_dirs, 2);

    let dump = rt.debug_dump(Some("/tmp/projects/app"));
    let dir = dump
        .dirs
        .first()
        .expect("project marker candidate should be tracked");
    assert!(dir.high_priority_scan);
    assert!(dir.event_score >= 96);

    rt.confirm_promoted(project.as_path());
    let promoted = rt.report();
    assert_eq!(promoted.l0_dirs, 2);
    assert_eq!(promoted.l1_dirs, 1);
}

#[test]
fn acceptance_project_marker_candidate_can_promote_or_request_lease() {
    let promoted_rt = TieredWatchRuntime::new(
        vec![(PathBuf::from("/tmp/hot"), 1)],
        Vec::new(),
        2,
        5_000,
        20,
    );
    let project = PathBuf::from("/tmp/projects/new-app");

    assert_eq!(
        promoted_rt.register_project_marker_candidate(project.clone(), 1),
        PromotionDecision::SendAdd
    );
    promoted_rt.confirm_promoted(project.as_path());

    let promoted = promoted_rt.report();
    assert_eq!(promoted.l0_dirs, 2);
    assert_eq!(promoted.promotions, 1);
    let promoted_dump = promoted_rt.debug_dump(Some("/tmp/projects/new-app"));
    assert_eq!(promoted_dump.dirs[0].watch_tier, "L0");
    assert!(promoted_dump.dirs[0].high_priority_scan);

    let leased_rt = TieredWatchRuntime::new_with_ephemeral(
        vec![(PathBuf::from("/tmp/hot"), 2)],
        Vec::new(),
        2,
        5_000,
        20,
        2,
    );
    leased_rt.record_scan(
        Path::new("/tmp/hot"),
        ScanOutcome {
            scanned: 1,
            changed: 30,
            elapsed_ms: 1,
            project_roots: Vec::new(),
        },
    );
    let blocked_project = PathBuf::from("/tmp/projects/leased-app");
    assert_eq!(
        leased_rt.register_project_marker_candidate(blocked_project.clone(), 1),
        PromotionDecision::BudgetBlocked
    );

    let lease_cfg = EphemeralWatchConfig {
        budget: 2,
        repeat_threshold: 1,
        max_cost_per_root: 1,
        ..EphemeralWatchConfig::default()
    };
    assert_eq!(
        leased_rt.note_dirty_scope_at(blocked_project.clone(), 1, &[], &lease_cfg, 100, 1),
        EphemeralWatchDecision::Add(blocked_project.clone())
    );
    leased_rt.confirm_ephemeral_added(blocked_project.as_path());

    let leased = leased_rt.report();
    assert_eq!(leased.promotion_budget_blocked, 1);
    assert_eq!(leased.ephemeral_watch_dirs, 1);
    assert_eq!(leased.ephemeral_watch_created, 1);
    assert!(leased
        .notes
        .iter()
        .any(|note| note.contains("last budget rejection")));
}

#[test]
fn acceptance_project_l0_can_demote_after_idle_ttl() {
    let rt = TieredWatchRuntime::new(Vec::new(), Vec::new(), 2, 5_000, 20);
    let project = PathBuf::from("/tmp/projects/idle-app");

    assert_eq!(
        rt.register_project_marker_candidate(project.clone(), 1),
        PromotionDecision::SendAdd
    );
    rt.confirm_promoted(project.as_path());

    let state = rt
        .state(project.as_path())
        .expect("promoted project should be tracked");
    state
        .last_event_unix_secs
        .store(unix_secs().saturating_sub(120), Ordering::Relaxed);

    let expired = rt.expired_l0(60);
    assert_eq!(expired, vec![project.clone()]);
    assert!(rt.mark_demotion_pending(project.as_path()));
    rt.confirm_demoted(project.as_path());

    let report = rt.report();
    assert_eq!(report.l0_dirs, 0);
    assert_eq!(report.l1_dirs, 1);
    assert_eq!(report.demotions, 1);
}

#[test]
fn dynamic_candidate_stays_l1_when_budget_blocked() {
    let rt = runtime();
    let dynamic = PathBuf::from("/tmp/hot/too-large-child");

    assert_eq!(
        rt.register_dynamic_candidate(dynamic, 6),
        PromotionDecision::BudgetBlocked
    );
    let report = rt.report();
    assert_eq!(report.watched_dirs_estimated, 2);
    assert_eq!(report.l0_dirs, 1);
    assert_eq!(report.l1_dirs, 2);
    assert!(report
        .notes
        .iter()
        .any(|note| note.contains("blocked by watch budget")));
}

#[test]
fn project_marker_candidate_gets_priority_when_budget_blocked() {
    let rt = TieredWatchRuntime::new(vec![(PathBuf::from("/tmp/hot"), 2)], vec![], 2, 5_000, 20);
    let hot = PathBuf::from("/tmp/hot");
    rt.record_scan(
        hot.as_path(),
        ScanOutcome {
            scanned: 1,
            changed: 30,
            elapsed_ms: 1,
            project_roots: Vec::new(),
        },
    );

    let normal = PathBuf::from("/tmp/normal");
    let project = PathBuf::from("/tmp/project");

    assert_eq!(
        rt.register_dynamic_candidate(normal.clone(), 1),
        PromotionDecision::BudgetBlocked
    );
    assert_eq!(
        rt.register_project_marker_candidate(project.clone(), 1),
        PromotionDecision::BudgetBlocked
    );

    let batch = rt.scan_batch(10);
    let project_pos = batch.iter().position(|p| p == &project);
    let normal_pos = batch.iter().position(|p| p == &normal);
    assert!(
        project_pos.is_some() && normal_pos.is_some(),
        "project and normal candidates should both stay scannable"
    );
    assert!(
        project_pos.unwrap() < normal_pos.unwrap(),
        "project marker candidate should be scanned before normal candidate"
    );
}

#[test]
fn empty_scans_demote_l1_to_l2_and_l3() {
    let rt = runtime();
    let warm = PathBuf::from("/tmp/warm");

    rt.record_scan(
        warm.as_path(),
        ScanOutcome {
            scanned: 1,
            changed: 0,
            elapsed_ms: 1,
            project_roots: Vec::new(),
        },
    );
    rt.apply_scan_policy(warm.as_path(), 1, 2, L3ScanPolicy::Interval, 4, 1, 1);
    let l2 = rt.report();
    assert_eq!(l2.l1_dirs, 0);
    assert_eq!(l2.l2_dirs, 1);
    assert!(l2.next_scan_unix_secs > 0);

    rt.record_scan(
        warm.as_path(),
        ScanOutcome {
            scanned: 1,
            changed: 0,
            elapsed_ms: 1,
            project_roots: Vec::new(),
        },
    );
    rt.apply_scan_policy(warm.as_path(), 1, 2, L3ScanPolicy::Interval, 4, 1, 1);
    let l3 = rt.report();
    assert_eq!(l3.l2_dirs, 0);
    assert_eq!(l3.l3_dirs, 1);
}

#[test]
fn l2_change_returns_to_l1_and_can_promote() {
    let rt = runtime();
    let warm = PathBuf::from("/tmp/warm");

    rt.record_scan(
        warm.as_path(),
        ScanOutcome {
            scanned: 1,
            changed: 0,
            elapsed_ms: 1,
            project_roots: Vec::new(),
        },
    );
    rt.apply_scan_policy(warm.as_path(), 1, 2, L3ScanPolicy::Interval, 4, 1, 1);
    rt.record_scan(
        warm.as_path(),
        ScanOutcome {
            scanned: 1,
            changed: 2,
            elapsed_ms: 1,
            project_roots: Vec::new(),
        },
    );
    rt.apply_scan_policy(warm.as_path(), 1, 2, L3ScanPolicy::Interval, 4, 1, 1);

    assert_eq!(rt.report().l1_dirs, 1);
    assert_eq!(
        rt.try_reserve_promotion(warm.as_path()),
        PromotionDecision::SendAdd
    );
}

#[test]
fn hotter_candidate_replaces_cold_l0_when_budget_full() {
    let rt = TieredWatchRuntime::new(
        vec![(PathBuf::from("/tmp/cold"), 2)],
        vec![(PathBuf::from("/tmp/hotter"), 2)],
        2,
        5_000,
        20,
    );
    let hotter = PathBuf::from("/tmp/hotter");

    rt.record_scan(
        hotter.as_path(),
        ScanOutcome {
            scanned: 1,
            changed: 4,
            elapsed_ms: 1,
            project_roots: Vec::new(),
        },
    );

    assert_eq!(
        rt.try_reserve_promotion(hotter.as_path()),
        PromotionDecision::Replace {
            demote: PathBuf::from("/tmp/cold"),
            promote: hotter.clone(),
        }
    );
    assert_eq!(rt.report().l0_replacements, 1);

    rt.confirm_demoted(Path::new("/tmp/cold"));
    assert!(rt.reserve_pending_promotion(hotter.as_path()));
    rt.confirm_promoted(hotter.as_path());

    let report = rt.report();
    assert_eq!(report.l0_dirs, 1);
    assert_eq!(report.l1_dirs, 1);
    assert_eq!(report.watched_dirs_estimated, 2);
}

#[test]
fn report_includes_watch_cost_per_tier() {
    let rt = runtime();
    let report = rt.report();
    assert_eq!(report.logical_watch_cost, 5);
    assert_eq!(report.kernel_watch_cost, 2);
    assert_eq!(report.skipped_watch_cost, 3);
    assert_eq!(report.l0_watch_cost, 2);
    assert_eq!(report.l1_watch_cost, 3);
    assert_eq!(report.l2_watch_cost, 0);
    assert_eq!(report.l3_watch_cost, 0);
    assert_eq!(report.watch_mount_policy_rejected, 0);
    assert_eq!(report.watch_exclude_rejected, 0);
    assert_eq!(report.last_budget_blocked_kernel_watch_cost, 0);
    assert_eq!(report.last_budget_blocked_budget_remaining, 0);
    assert!(report.last_budget_blocked_reason.is_empty());
}

#[test]
fn report_includes_scan_backlog_by_tier() {
    let rt = runtime();
    // L1 dir has next_scan = 0 (default), which is <= now, so it counts as backlog.
    let report = rt.report();
    assert_eq!(report.scan_backlog_by_tier[0], 0);
    assert_eq!(report.scan_backlog_by_tier[1], 1);
    assert_eq!(report.scan_backlog_by_tier[2], 0);
    assert_eq!(report.scan_backlog_by_tier[3], 0);
}

#[test]
fn debug_dump_returns_all_dirs_and_summary() {
    let rt = runtime();
    let dump = rt.debug_dump(None);
    assert_eq!(dump.dirs.len(), 2);
    assert_eq!(dump.summary.l0_dirs, 1);
    assert_eq!(dump.summary.l1_dirs, 1);
    assert_eq!(dump.summary.l2_dirs, 0);
    assert_eq!(dump.summary.l3_dirs, 0);
    assert!(dump.summary.total_event_score > 0);
}

#[test]
fn debug_dump_filters_by_root_prefix() {
    let rt = runtime();
    let dump = rt.debug_dump(Some("/tmp/hot"));
    assert_eq!(dump.dirs.len(), 1);
    assert!(dump.dirs[0].path.starts_with("/tmp/hot"));
}

#[test]
fn debug_dump_includes_observability_fields() {
    let rt = runtime();
    rt.record_event_paths([&PathBuf::from("/tmp/warm/file.txt")]);

    let dump = rt.debug_dump(Some("/tmp/warm"));
    assert_eq!(dump.dirs.len(), 1);
    let warm = &dump.dirs[0];
    assert_eq!(warm.watch_tier, "L1");
    assert_eq!(warm.index_tier, "WarmMemory");
    assert!(warm.dirty);
    assert_eq!(warm.freshness, "Dirty");
    assert_eq!(warm.next_scan_unix_secs, 0);
    assert_eq!(warm.budget_blocked_count, 0);
    assert_eq!(warm.last_budget_blocked_unix_secs, 0);
    assert!(!warm.high_priority_scan);
}

#[test]
fn debug_dump_explains_nested_project_relationships() {
    let rt = TieredWatchRuntime::new(
        vec![(PathBuf::from("/workspace"), 3)],
        vec![(PathBuf::from("/workspace/project"), 1)],
        4,
        5_000,
        20,
    );

    let dump = rt.debug_dump(Some("/workspace"));
    let parent = dump
        .dirs
        .iter()
        .find(|dir| dir.path == "/workspace")
        .expect("parent root should be present");
    let child = dump
        .dirs
        .iter()
        .find(|dir| dir.path == "/workspace/project")
        .expect("nested project root should be present");

    assert_eq!(parent.descendant_roots, vec!["/workspace/project"]);
    assert_eq!(parent.nearest_ancestor_root, None);
    assert_eq!(parent.l0_covering_root.as_deref(), Some("/workspace"));
    assert!(!parent.budget_isolated_from_ancestor);
    assert_eq!(parent.nested_relation, "ancestor_of_nested_roots");

    assert_eq!(child.nearest_ancestor_root.as_deref(), Some("/workspace"));
    assert!(child.descendant_roots.is_empty());
    assert_eq!(child.l0_covering_root.as_deref(), Some("/workspace"));
    assert!(child.budget_isolated_from_ancestor);
    assert_eq!(child.nested_relation, "covered_by_l0_ancestor");
}

#[test]
fn debug_dump_marks_nested_project_under_cold_root() {
    let rt = TieredWatchRuntime::new(
        Vec::new(),
        vec![
            (PathBuf::from("/workspace"), 3),
            (PathBuf::from("/workspace/project"), 1),
        ],
        4,
        5_000,
        20,
    );

    let dump = rt.debug_dump(Some("/workspace/project"));
    let child = dump
        .dirs
        .iter()
        .find(|dir| dir.path == "/workspace/project")
        .expect("nested project root should be present");

    assert_eq!(child.nearest_ancestor_root.as_deref(), Some("/workspace"));
    assert_eq!(child.l0_covering_root, None);
    assert!(child.budget_isolated_from_ancestor);
    assert_eq!(child.nested_relation, "nested_under_cold_root");
}

#[test]
fn score_decay_reduces_event_score_over_time() {
    let rt = runtime();
    let warm = PathBuf::from("/tmp/warm");

    // Give warm a non-zero event score via a changed scan
    rt.record_scan(
        warm.as_path(),
        ScanOutcome {
            scanned: 1,
            changed: 1,
            elapsed_ms: 1,
            project_roots: Vec::new(),
        },
    );

    let before = rt.report().event_score_total;
    assert!(before > 0);

    // Decay with a far-future now so interval is exceeded
    rt.decay_scores(unix_secs() + 3_600, 3_600, 5);

    let after = rt.report().event_score_total;
    assert!(
        after < before,
        "expected score to decay: before={before}, after={after}"
    );
}

#[test]
fn budget_blocked_increments_budget_blocked_count() {
    let rt = TieredWatchRuntime::new(vec![(PathBuf::from("/tmp/hot"), 2)], vec![], 2, 5_000, 20);
    let dynamic = PathBuf::from("/tmp/hot/child");

    // First block
    assert_eq!(
        rt.register_dynamic_candidate(dynamic.clone(), 5),
        PromotionDecision::BudgetBlocked
    );

    // Second block should set high_priority_scan
    assert_eq!(
        rt.register_dynamic_candidate(dynamic.clone(), 5),
        PromotionDecision::BudgetBlocked
    );

    // Verify via scan_batch ordering: dynamic should appear first
    let batch = rt.scan_batch(10);
    assert!(
        batch.contains(&dynamic),
        "dynamic should be in scan_batch after budget blocked"
    );
}

#[test]
fn high_priority_scan_affects_scan_batch_ordering() {
    let rt = TieredWatchRuntime::new(vec![(PathBuf::from("/tmp/hot"), 2)], vec![], 2, 5_000, 20);
    let hot = PathBuf::from("/tmp/hot");

    // Pump up the L0 score so replacement is impossible
    rt.record_scan(
        hot.as_path(),
        ScanOutcome {
            scanned: 1,
            changed: 10,
            elapsed_ms: 1,
            project_roots: Vec::new(),
        },
    );

    let a = PathBuf::from("/tmp/a");
    let b = PathBuf::from("/tmp/b");

    // Register as dynamic candidates; both get BudgetBlocked because budget is full
    // and L0 victim is too hot to replace.
    assert_eq!(
        rt.register_dynamic_candidate(a.clone(), 1),
        PromotionDecision::BudgetBlocked
    );
    assert_eq!(
        rt.register_dynamic_candidate(b.clone(), 1),
        PromotionDecision::BudgetBlocked
    );

    // Block /tmp/a a second time to flip high_priority_scan
    assert_eq!(
        rt.try_reserve_promotion(a.as_path()),
        PromotionDecision::BudgetBlocked
    );

    let batch = rt.scan_batch(10);
    let a_pos = batch.iter().position(|p| p == &a);
    let b_pos = batch.iter().position(|p| p == &b);

    assert!(
        a_pos.is_some() && b_pos.is_some(),
        "both a and b should be in batch"
    );
    assert!(
        a_pos.unwrap() < b_pos.unwrap(),
        "high-priority a should come before b"
    );
}

#[test]
fn sustained_l0_events_keep_hot_dir_from_being_replaced() {
    let rt = TieredWatchRuntime::new(
        vec![(PathBuf::from("/tmp/hot"), 1)],
        vec![(PathBuf::from("/tmp/candidate"), 1)],
        1,
        5_000,
        20,
    );
    let hot_event = PathBuf::from("/tmp/hot/file.txt");
    let candidate = PathBuf::from("/tmp/candidate");

    for _ in 0..12 {
        rt.record_event_paths([&hot_event]);
    }
    rt.record_scan(
        candidate.as_path(),
        ScanOutcome {
            scanned: 1,
            changed: 2,
            elapsed_ms: 1,
            project_roots: Vec::new(),
        },
    );

    assert_eq!(
            rt.try_reserve_promotion(candidate.as_path()),
            PromotionDecision::BudgetBlocked,
            "sustained L0 events should keep the hot directory ahead of a weaker replacement candidate; report={:?}",
            rt.report()
        );
    let report = rt.report();
    assert_eq!(report.l0_dirs, 1);
    assert_eq!(report.watched_dirs_estimated, 1);
    assert_eq!(report.l0_replacements, 0);
    assert_eq!(report.promotion_budget_blocked, 1);
}

#[test]
fn replacement_candidate_cannot_evict_its_ancestor_l0() {
    let rt = TieredWatchRuntime::new(
        vec![(PathBuf::from("/workspace"), 3)],
        vec![(PathBuf::from("/workspace/project"), 1)],
        3,
        5_000,
        20,
    );
    let child = PathBuf::from("/workspace/project");

    rt.record_scan(
        child.as_path(),
        ScanOutcome {
            scanned: 10,
            changed: 20,
            elapsed_ms: 1,
            project_roots: Vec::new(),
        },
    );

    assert_eq!(
        rt.try_reserve_promotion(child.as_path()),
        PromotionDecision::BudgetBlocked,
        "a child candidate must not free budget by evicting its covering ancestor; report={:?}",
        rt.report()
    );
    let report = rt.report();
    assert_eq!(report.l0_dirs, 1);
    assert_eq!(report.l1_dirs, 1);
    assert_eq!(report.watched_dirs_estimated, 3);
    assert_eq!(report.l0_replacements, 0);
}

#[test]
fn acceptance_nested_project_debug_explains_without_ancestor_eviction() {
    let rt = TieredWatchRuntime::new(
        vec![(PathBuf::from("/workspace"), 3)],
        vec![(PathBuf::from("/workspace/project"), 1)],
        3,
        5_000,
        20,
    );
    let child = PathBuf::from("/workspace/project");

    let dump = rt.debug_dump(Some("/workspace"));
    let parent = dump
        .dirs
        .iter()
        .find(|dir| dir.path == "/workspace")
        .expect("parent root should be present");
    let nested = dump
        .dirs
        .iter()
        .find(|dir| dir.path == "/workspace/project")
        .expect("nested project root should be present");

    assert_eq!(parent.nested_relation, "ancestor_of_nested_roots");
    assert_eq!(nested.nearest_ancestor_root.as_deref(), Some("/workspace"));
    assert_eq!(nested.l0_covering_root.as_deref(), Some("/workspace"));
    assert_eq!(nested.nested_relation, "covered_by_l0_ancestor");
    assert!(nested.budget_isolated_from_ancestor);

    rt.record_scan(
        child.as_path(),
        ScanOutcome {
            scanned: 10,
            changed: 20,
            elapsed_ms: 1,
            project_roots: Vec::new(),
        },
    );
    assert_eq!(
        rt.try_reserve_promotion(child.as_path()),
        PromotionDecision::BudgetBlocked,
        "nested child must not evict its L0 ancestor to satisfy promotion"
    );
    assert_eq!(rt.report().l0_replacements, 0);
}

#[test]
fn freshness_roundtrip() {
    assert_eq!(
        Freshness::from_u8(Freshness::Fresh.as_u8()),
        Freshness::Fresh
    );
    assert_eq!(
        Freshness::from_u8(Freshness::Stale.as_u8()),
        Freshness::Stale
    );
    assert_eq!(
        Freshness::from_u8(Freshness::Dirty.as_u8()),
        Freshness::Dirty
    );
    assert_eq!(
        Freshness::from_u8(Freshness::Unknown.as_u8()),
        Freshness::Unknown
    );
    assert_eq!(Freshness::from_u8(255), Freshness::Unknown);
}

#[test]
fn index_residency_roundtrip() {
    assert_eq!(
        IndexResidency::from_u8(IndexResidency::HotMemory.as_u8()),
        IndexResidency::HotMemory
    );
    assert_eq!(
        IndexResidency::from_u8(IndexResidency::WarmMemory.as_u8()),
        IndexResidency::WarmMemory
    );
    assert_eq!(
        IndexResidency::from_u8(IndexResidency::ColdMmap.as_u8()),
        IndexResidency::ColdMmap
    );
    assert_eq!(
        IndexResidency::from_u8(IndexResidency::FrozenManifestOnly.as_u8()),
        IndexResidency::FrozenManifestOnly
    );
    assert_eq!(
        IndexResidency::from_u8(255),
        IndexResidency::FrozenManifestOnly
    );
}

#[test]
fn dir_state_initializes_correctly_by_tier() {
    let now = unix_secs();
    let l0 = DirState::new(WatchTier::L0, 1, now);
    assert_eq!(l0.tier(), WatchTier::L0);
    assert_eq!(l0.freshness(), Freshness::Fresh);
    assert_eq!(l0.index_residency(), IndexResidency::HotMemory);
    assert!(!l0.dirty.load(Ordering::Relaxed));

    let l1 = DirState::new(WatchTier::L1, 1, now);
    assert_eq!(l1.tier(), WatchTier::L1);
    assert_eq!(l1.freshness(), Freshness::Unknown);
    assert_eq!(l1.index_residency(), IndexResidency::WarmMemory);

    let l2 = DirState::new(WatchTier::L2, 1, now);
    assert_eq!(l2.tier(), WatchTier::L2);
    assert_eq!(l2.freshness(), Freshness::Unknown);
    assert_eq!(l2.index_residency(), IndexResidency::ColdMmap);

    let l3 = DirState::new(WatchTier::L3, 1, now);
    assert_eq!(l3.tier(), WatchTier::L3);
    assert_eq!(l3.freshness(), Freshness::Unknown);
    assert_eq!(l3.index_residency(), IndexResidency::FrozenManifestOnly);
}

#[test]
fn record_event_paths_sets_dirty_for_non_l0() {
    let rt = runtime();
    let _warm = PathBuf::from("/tmp/warm");
    let dirty_dirs = rt.record_event_paths([&PathBuf::from("/tmp/warm/leaf/file.txt")]);
    let report = rt.report();
    assert_eq!(report.dirty_dirs, 1);
    assert_eq!(report.dirty_queue_len, 0);
    assert_eq!(report.fresh_dirs, 1); // L0 is fresh
    assert_eq!(dirty_dirs, vec![PathBuf::from("/tmp/warm/leaf")]);
}

#[test]
fn record_scan_clears_dirty_and_sets_fresh() {
    let rt = runtime();
    let warm = PathBuf::from("/tmp/warm");
    rt.record_event_paths([&PathBuf::from("/tmp/warm/file.txt")]);
    let before = rt.report();
    assert_eq!(before.dirty_dirs, 1);

    rt.record_scan(
        warm.as_path(),
        ScanOutcome {
            scanned: 1,
            changed: 0,
            elapsed_ms: 1,
            project_roots: Vec::new(),
        },
    );
    let after = rt.report();
    assert_eq!(after.dirty_dirs, 0);
    assert_eq!(after.dirty_queue_len, 0);
    assert_eq!(after.fresh_dirs, 2); // both L0 and L1 are fresh now
}

#[test]
fn record_scan_for_leaf_clears_covering_cold_root() {
    let rt = runtime();
    let leaf = PathBuf::from("/tmp/warm/deep/file.txt");
    rt.record_event_paths([&leaf]);
    assert_eq!(rt.report().dirty_dirs, 1);

    let recorded = rt.record_scan_for_path(
        Path::new("/tmp/warm/deep"),
        ScanOutcome {
            scanned: 1,
            changed: 0,
            elapsed_ms: 1,
            project_roots: Vec::new(),
        },
    );

    assert_eq!(recorded, Some(PathBuf::from("/tmp/warm")));
    let report = rt.report();
    assert_eq!(report.dirty_dirs, 0);
    assert_eq!(report.fresh_dirs, 2);
}

#[test]
fn demotion_and_promotion_update_residency() {
    let rt = runtime();
    let hot = PathBuf::from("/tmp/hot");
    let warm = PathBuf::from("/tmp/warm");

    // L0 -> L1 demotion
    rt.mark_demotion_pending(hot.as_path());
    rt.confirm_demoted(hot.as_path());
    let report = rt.report();
    assert_eq!(report.hot_memory_dirs, 0);
    assert_eq!(report.warm_memory_dirs, 2);

    // Promote warm to L0
    rt.try_reserve_promotion(warm.as_path());
    rt.confirm_promoted(warm.as_path());
    let report2 = rt.report();
    assert_eq!(report2.hot_memory_dirs, 1);
    assert_eq!(report2.warm_memory_dirs, 1);
}

#[test]
fn scan_policy_demotion_updates_residency() {
    let rt = runtime();
    let warm = PathBuf::from("/tmp/warm");

    rt.record_scan(
        warm.as_path(),
        ScanOutcome {
            scanned: 1,
            changed: 0,
            elapsed_ms: 1,
            project_roots: Vec::new(),
        },
    );
    rt.apply_scan_policy(warm.as_path(), 1, 2, L3ScanPolicy::Interval, 4, 1, 1);
    let l2 = rt.report();
    assert_eq!(l2.l2_dirs, 1);
    assert_eq!(l2.cold_mmap_dirs, 1);
    assert_eq!(l2.warm_memory_dirs, 0);
    assert_eq!(l2.cold_validate_count, 0);

    rt.record_scan(
        warm.as_path(),
        ScanOutcome {
            scanned: 1,
            changed: 0,
            elapsed_ms: 1,
            project_roots: Vec::new(),
        },
    );
    rt.apply_scan_policy(warm.as_path(), 1, 2, L3ScanPolicy::Interval, 4, 1, 1);
    let l3 = rt.report();
    assert_eq!(l3.l3_dirs, 1);
    assert_eq!(l3.frozen_manifest_dirs, 1);
    assert_eq!(l3.cold_mmap_dirs, 0);
    assert_eq!(l3.cold_validate_count, 1);
}

#[test]
fn l3_interval_policy_uses_explicit_l3_interval() {
    let rt = runtime();
    let warm = PathBuf::from("/tmp/warm");
    let before = unix_secs();

    rt.record_scan(
        warm.as_path(),
        ScanOutcome {
            scanned: 1,
            changed: 0,
            elapsed_ms: 1,
            project_roots: Vec::new(),
        },
    );
    rt.apply_scan_policy(warm.as_path(), 1, 2, L3ScanPolicy::Interval, 99, 1, 1);
    rt.record_scan(
        warm.as_path(),
        ScanOutcome {
            scanned: 1,
            changed: 0,
            elapsed_ms: 1,
            project_roots: Vec::new(),
        },
    );
    rt.apply_scan_policy(warm.as_path(), 1, 2, L3ScanPolicy::Interval, 99, 1, 1);

    let dump = rt.debug_dump(Some("/tmp/warm"));
    let dir = dump.dirs.first().expect("warm dir should be present");
    assert_eq!(dir.watch_tier, "L3");
    assert!(dir.next_scan_unix_secs >= before.saturating_add(99));
    assert!(dir.next_scan_unix_secs < before.saturating_add(120));
}

#[test]
fn l3_validate_on_query_policy_disables_periodic_l3_scan() {
    let rt = runtime();
    let warm = PathBuf::from("/tmp/warm");

    rt.record_scan(
        warm.as_path(),
        ScanOutcome {
            scanned: 1,
            changed: 0,
            elapsed_ms: 1,
            project_roots: Vec::new(),
        },
    );
    rt.apply_scan_policy(
        warm.as_path(),
        1,
        2,
        L3ScanPolicy::ValidateOnQuery,
        99,
        1,
        1,
    );
    rt.record_scan(
        warm.as_path(),
        ScanOutcome {
            scanned: 1,
            changed: 0,
            elapsed_ms: 1,
            project_roots: Vec::new(),
        },
    );
    rt.apply_scan_policy(
        warm.as_path(),
        1,
        2,
        L3ScanPolicy::ValidateOnQuery,
        99,
        1,
        1,
    );

    let dump = rt.debug_dump(Some("/tmp/warm"));
    let dir = dump.dirs.first().expect("warm dir should be present");
    assert_eq!(dir.watch_tier, "L3");
    assert_eq!(dir.next_scan_unix_secs, u64::MAX);
    assert!(rt.scan_batch(8).is_empty());
}

#[test]
fn l3_disabled_policy_disables_periodic_l3_scan() {
    let rt = runtime();
    let warm = PathBuf::from("/tmp/warm");

    rt.record_scan(
        warm.as_path(),
        ScanOutcome {
            scanned: 1,
            changed: 0,
            elapsed_ms: 1,
            project_roots: Vec::new(),
        },
    );
    rt.apply_scan_policy(warm.as_path(), 1, 2, L3ScanPolicy::Disabled, 99, 1, 1);
    rt.record_scan(
        warm.as_path(),
        ScanOutcome {
            scanned: 1,
            changed: 0,
            elapsed_ms: 1,
            project_roots: Vec::new(),
        },
    );
    rt.apply_scan_policy(warm.as_path(), 1, 2, L3ScanPolicy::Disabled, 99, 1, 1);

    let dump = rt.debug_dump(Some("/tmp/warm"));
    let dir = dump.dirs.first().expect("warm dir should be present");
    assert_eq!(dir.watch_tier, "L3");
    assert_eq!(dir.next_scan_unix_secs, u64::MAX);
    assert!(rt.scan_batch(8).is_empty());
}

#[test]
fn budget_blocked_tracks_per_dir() {
    let rt = runtime();
    let dynamic = PathBuf::from("/tmp/hot/too-large-child");

    assert_eq!(
        rt.register_dynamic_candidate(dynamic.clone(), 6),
        PromotionDecision::BudgetBlocked
    );
    let report = rt.report();
    assert_eq!(report.promotion_budget_blocked, 1);
    assert_eq!(report.last_budget_blocked_kernel_watch_cost, 6);
    assert_eq!(report.last_budget_blocked_budget_remaining, 3);
    assert!(report
        .last_budget_blocked_reason
        .contains("promotion budget blocked"));
    assert!(report
        .last_budget_blocked_reason
        .contains("kernel_watch_cost=6"));
    assert!(report
        .last_budget_blocked_reason
        .contains("budget_remaining=3"));
    assert!(report.notes.iter().any(|note| {
        note.contains("last budget rejection")
            && note.contains("kernel_watch_cost=6")
            && note.contains("budget_remaining=3")
    }));
}

#[test]
fn promotion_blocks_dynamic_candidate_over_l0_per_root_cap() {
    let rt = TieredWatchRuntime::new_with_l0_max_cost_and_ephemeral(
        Vec::new(),
        Vec::new(),
        10,
        3,
        5_000,
        20,
        0,
    );
    let dynamic = PathBuf::from("/tmp/hot/too-large-child");

    assert_eq!(
        rt.register_dynamic_candidate(dynamic.clone(), 4),
        PromotionDecision::BudgetBlocked
    );

    let report = rt.report();
    assert_eq!(report.watched_dirs_estimated, 0);
    assert_eq!(report.l0_dirs, 0);
    assert_eq!(report.l1_dirs, 1);
    assert_eq!(report.promotion_budget_blocked, 1);
    assert_eq!(report.last_budget_blocked_kernel_watch_cost, 4);
    assert_eq!(report.last_budget_blocked_budget_remaining, 3);
    assert!(report
        .last_budget_blocked_reason
        .contains("promotion per-root cost blocked"));
    assert!(report
        .last_budget_blocked_reason
        .contains("l0_max_cost_per_root=3"));
    assert!(rt.scan_batch(8).contains(&dynamic));
}

#[test]
fn watch_reject_counters_distinguish_mount_policy_and_exclude() {
    let rt = runtime();
    let cfg = EphemeralWatchConfig {
        budget: 4,
        repeat_threshold: 1,
        max_cost_per_root: 2,
        ..EphemeralWatchConfig::default()
    };

    rt.note_watch_mount_policy_rejected();
    assert_eq!(
        rt.note_dirty_scope_at(
            PathBuf::from("/tmp/cold/node_modules/pkg"),
            1,
            &["node_modules".to_string()],
            &cfg,
            100,
            1,
        ),
        EphemeralWatchDecision::NotEligible
    );

    let report = rt.report();
    assert_eq!(report.watch_mount_policy_rejected, 1);
    assert_eq!(report.watch_exclude_rejected, 1);
    assert!(report
        .notes
        .iter()
        .any(|note| note.contains("rejected by mount policy")));
    assert!(report
        .notes
        .iter()
        .any(|note| note.contains("rejected by exclude rules")));
}

#[test]
fn ephemeral_watch_created_after_repeated_dirty_scope() {
    let rt = TieredWatchRuntime::new_with_ephemeral(Vec::new(), Vec::new(), 1, 5_000, 20, 4);
    let cfg = EphemeralWatchConfig {
        budget: 4,
        repeat_window_secs: 10,
        repeat_threshold: 2,
        max_cost_per_root: 2,
        ..EphemeralWatchConfig::default()
    };
    let dir = PathBuf::from("/tmp/cold");

    assert_eq!(
        rt.note_dirty_scope_at(dir.clone(), 2, &[], &cfg, 100, 1),
        EphemeralWatchDecision::NotEligible
    );
    assert_eq!(
        rt.note_dirty_scope_at(dir.clone(), 2, &[], &cfg, 105, 1),
        EphemeralWatchDecision::Add(dir.clone())
    );

    let reserved = rt.report();
    assert_eq!(reserved.ephemeral_watch_cost, 2);
    assert_eq!(reserved.ephemeral_watch_dirs, 0);

    rt.confirm_ephemeral_added(dir.as_path());
    let report = rt.report();
    assert_eq!(report.ephemeral_watch_dirs, 1);
    assert_eq!(report.ephemeral_watch_created, 1);
    assert_eq!(report.ephemeral_watch_budget_blocked, 0);
}

#[test]
fn ephemeral_watch_respects_exclude_l0_and_cost_limits() {
    let rt = TieredWatchRuntime::new_with_ephemeral(
        vec![(PathBuf::from("/tmp/hot"), 1)],
        Vec::new(),
        1,
        5_000,
        20,
        4,
    );
    let cfg = EphemeralWatchConfig {
        budget: 4,
        repeat_threshold: 1,
        max_cost_per_root: 2,
        ..EphemeralWatchConfig::default()
    };

    assert_eq!(
        rt.note_dirty_scope_at(PathBuf::from("/tmp/hot/leaf"), 1, &[], &cfg, 100, 1),
        EphemeralWatchDecision::NotEligible
    );
    assert_eq!(
        rt.note_dirty_scope_at(
            PathBuf::from("/tmp/cold/node_modules/pkg"),
            1,
            &["node_modules".to_string()],
            &cfg,
            100,
            1,
        ),
        EphemeralWatchDecision::NotEligible
    );
    assert_eq!(
        rt.note_dirty_scope_at(PathBuf::from("/tmp/cold/too-large"), 3, &[], &cfg, 100, 1),
        EphemeralWatchDecision::NotEligible
    );

    let report = rt.report();
    assert_eq!(report.ephemeral_watch_cost, 0);
    assert_eq!(report.ephemeral_watch_dirs, 0);
    assert_eq!(report.watch_exclude_rejected, 1);
}

#[test]
fn ephemeral_budget_blocked_reports_kernel_watch_cost() {
    let rt = TieredWatchRuntime::new_with_ephemeral(Vec::new(), Vec::new(), 1, 5_000, 20, 2);
    let cfg = EphemeralWatchConfig {
        budget: 2,
        repeat_threshold: 1,
        max_cost_per_root: 2,
        ..EphemeralWatchConfig::default()
    };

    let first = PathBuf::from("/tmp/first");
    let second = PathBuf::from("/tmp/second");
    assert_eq!(
        rt.note_dirty_scope_at(first.clone(), 2, &[], &cfg, 100, 1),
        EphemeralWatchDecision::Add(first)
    );
    assert_eq!(
        rt.note_dirty_scope_at(second, 2, &[], &cfg, 101, 1),
        EphemeralWatchDecision::BudgetBlocked
    );

    let report = rt.report();
    assert_eq!(report.ephemeral_watch_budget_blocked, 1);
    assert_eq!(report.last_budget_blocked_kernel_watch_cost, 2);
    assert_eq!(report.last_budget_blocked_budget_remaining, 0);
    assert!(report
        .last_budget_blocked_reason
        .contains("ephemeral budget blocked"));
    assert!(report
        .last_budget_blocked_reason
        .contains("kernel_watch_cost=2"));
}

#[test]
fn ephemeral_watch_expires_for_idle_ttl_no_change_and_l0_cover() {
    let rt = TieredWatchRuntime::new_with_ephemeral(Vec::new(), Vec::new(), 1, 5_000, 20, 10);
    let cfg = EphemeralWatchConfig {
        budget: 10,
        repeat_threshold: 1,
        max_cost_per_root: 2,
        ..EphemeralWatchConfig::default()
    };

    let idle = PathBuf::from("/tmp/idle");
    assert_eq!(
        rt.note_dirty_scope_at(idle.clone(), 1, &[], &cfg, 100, 1),
        EphemeralWatchDecision::Add(idle.clone())
    );
    rt.confirm_ephemeral_added(idle.as_path());
    let removals = rt.expire_ephemeral_watches_at(130, 20, 100, 3);
    assert_eq!(removals.len(), 1);
    assert_eq!(removals[0].path, idle);
    assert_eq!(removals[0].reason, EphemeralWatchExpiry::Idle);
    rt.confirm_ephemeral_removed(removals[0].path.as_path());

    let ttl = PathBuf::from("/tmp/ttl");
    assert_eq!(
        rt.note_dirty_scope_at(ttl.clone(), 1, &[], &cfg, 200, 1),
        EphemeralWatchDecision::Add(ttl.clone())
    );
    rt.confirm_ephemeral_added(ttl.as_path());
    let removals = rt.expire_ephemeral_watches_at(260, 100, 50, 3);
    assert_eq!(removals[0].reason, EphemeralWatchExpiry::Ttl);
    rt.confirm_ephemeral_removed(removals[0].path.as_path());

    let quiet = PathBuf::from("/tmp/quiet");
    assert_eq!(
        rt.note_dirty_scope_at(quiet.clone(), 1, &[], &cfg, 300, 0),
        EphemeralWatchDecision::Add(quiet.clone())
    );
    rt.confirm_ephemeral_added(quiet.as_path());
    rt.record_dirty_scope_repeat(quiet.as_path(), 0, &cfg);
    rt.record_dirty_scope_repeat(quiet.as_path(), 0, &cfg);
    let removals = rt.expire_ephemeral_watches_at(301, 100, 100, 2);
    assert_eq!(removals[0].reason, EphemeralWatchExpiry::NoChange);
    rt.confirm_ephemeral_removed(removals[0].path.as_path());

    let covered = PathBuf::from("/workspace/project");
    assert_eq!(
        rt.note_dirty_scope_at(covered.clone(), 1, &[], &cfg, 400, 1),
        EphemeralWatchDecision::Add(covered.clone())
    );
    rt.confirm_ephemeral_added(covered.as_path());
    let l0 = PathBuf::from("/workspace");
    assert_eq!(
        rt.register_dynamic_candidate(l0.clone(), 1),
        PromotionDecision::SendAdd
    );
    rt.confirm_promoted(l0.as_path());
    let removals = rt.expire_ephemeral_watches_at(401, 100, 100, 3);
    assert_eq!(removals[0].reason, EphemeralWatchExpiry::CoveredByL0);
}

#[test]
fn ephemeral_budget_replaces_low_value_lease_and_rolls_back() {
    let rt = TieredWatchRuntime::new_with_ephemeral(Vec::new(), Vec::new(), 1, 5_000, 20, 2);
    let cfg = EphemeralWatchConfig {
        budget: 2,
        repeat_threshold: 1,
        max_cost_per_root: 1,
        ..EphemeralWatchConfig::default()
    };
    let old = PathBuf::from("/tmp/old");
    let new = PathBuf::from("/tmp/new");

    assert_eq!(
        rt.note_dirty_scope_at(old.clone(), 1, &[], &cfg, 100, 0),
        EphemeralWatchDecision::Add(old.clone())
    );
    rt.confirm_ephemeral_added(old.as_path());
    assert_eq!(
        rt.note_dirty_scope_at(PathBuf::from("/tmp/other"), 1, &[], &cfg, 101, 0),
        EphemeralWatchDecision::Add(PathBuf::from("/tmp/other"))
    );

    assert_eq!(
        rt.note_dirty_scope_at(new.clone(), 1, &[], &cfg, 102, 10),
        EphemeralWatchDecision::Replace {
            remove: old.clone(),
            add: new.clone(),
        }
    );
    let replaced = rt.report();
    assert_eq!(replaced.ephemeral_watch_cost, 2);
    assert_eq!(replaced.ephemeral_watch_evicted, 1);

    rt.rollback_ephemeral_replace(old.as_path(), new.as_path());
    let rolled_back = rt.report();
    assert_eq!(rolled_back.ephemeral_watch_cost, 2);
    assert_eq!(rolled_back.ephemeral_watch_dirs, 1);
}
