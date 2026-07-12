use super::*;
use crate::config::{ContentIndexConfig, MmapWarmupConfig, QueryConfig, RuntimeProfile};
use crate::core::{EventRecord, EventType, FileIdentifier, FileKey, FileKind, FileMeta};
use crate::diagnostics::{DiagnosticReport, DiagnosticSource};
use crate::event::sync::{DirtyReason, DirtyScope};
use crate::event::tiered_watch::{FastScanLeaseKind, FastScanTickConfig, TieredWatchRuntime};
use crate::fs_policy::{FsPolicyConfig, FsPolicyDecision, MountTable};
use crate::index::tiered::events::event_record_estimated_bytes;
use crate::index::tiered::sync::RebuildAdmission;
use crate::io_governor::{BackoffPolicy, IoGovernor, IoGovernorConfig, IoPressure};
use crate::stats::{EventPipelineStats, MemorySampleDepth, SmapsRollupStats};
use crate::storage::quarantine::{
    FreezeGate, MountIdentity, QuarantineRoot, QuarantineRootState, QuarantineSidecar,
    RootStateKind, RootStateRecord,
};
use crate::storage::snapshot::{
    quarantine_sidecar_path_for, stable_prev_v7_path_for, stable_v7_path_for, SnapshotStore,
};
use crate::storage::snapshot_v7::write_v7_snapshot_atomic;
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

fn file_meta_from_path(path: PathBuf) -> FileMeta {
    let metadata = std::fs::metadata(&path).unwrap();
    FileMeta {
        file_key: FileKey::from_path_and_metadata(&path, &metadata).unwrap(),
        path,
        size: metadata.len(),
        mtime: metadata.modified().ok(),
        ctime: metadata.created().ok(),
        atime: metadata.accessed().ok(),
        kind: FileKind::from_metadata(&metadata),
    }
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

// ===========================================================================
// Phase 3: manifest 大目录上限修复 — unbounded summary 二次确认
// ===========================================================================

/// 辅助：创建包含 n 个文件的目录（用于大目录测试，n > REPAIR_SLICE_MAX_ENTRIES=512）。
fn phase3_create_large_dir(root: &Path, n: usize) {
    std::fs::create_dir_all(root).unwrap();
    for i in 0..n {
        std::fs::write(root.join(format!("file_{:04}.txt", i)), b"x").unwrap();
    }
}

/// 辅助：用 filetime 或 touch 改变目录 mtime 但不改内容。
fn phase3_touch_dir_mtime(dir: &Path) {
    // 在目录中创建并立即删除一个临时文件来触发 mtime 更新，
    // 但不改变最终目录内容（创建+删除 = net zero）。
    // 注意：需要先等一小段时间确保 mtime 精度。
    std::thread::sleep(std::time::Duration::from_millis(50));
    let tmp = dir.join(".phase3_touch_tmp");
    std::fs::write(&tmp, b"").unwrap();
    std::fs::remove_file(&tmp).unwrap();
}

/// 超过 512 条的目录 manifest skip 能生效。
#[test]
fn manifest_skip_works_for_large_dir() {
    let root = unique_tmp_dir("p3-large-skip");
    phase3_create_large_dir(&root, 520); // > 512

    let idx = TieredIndex::empty(vec![root.clone()]);
    // immediate scan 用 WalkBuilder（不受 512 限制）存储 manifest summary
    let first = idx.scan_dirs_immediate_outcome(std::slice::from_ref(&root));
    assert_eq!(first.scanned, 520);
    assert_eq!(idx.directory_manifest_report().dirs, 1);

    // PeriodicColdScan：段 1 返回 Some(false)（immediate scan 未记录 dir_mtime_ns），
    // 段 1.5 用 unbounded summary 比对 → 匹配 → 跳过。
    let skip_dirs = std::collections::HashSet::from([root.clone()]);
    let reports = mtime_precheck_run_cold_scan_to_completion(&idx, &root, &skip_dirs);
    assert_eq!(reports.len(), 1);
    assert!(
        reports[0].outcomes[0].manifest_skipped,
        "large dir (>512) should be skippable via unbounded summary"
    );
    assert_eq!(reports[0].outcomes[0].outcome.scanned, 0);

    let manifest = idx.directory_manifest_report();
    assert_eq!(manifest.unbounded_summary_hits, 1);

    let _ = std::fs::remove_dir_all(&root);
}

/// 目录内容变化时 manifest skip 不生效。
#[test]
fn manifest_skip_fails_on_real_change() {
    let root = unique_tmp_dir("p3-large-change");
    phase3_create_large_dir(&root, 520);

    let idx = TieredIndex::empty(vec![root.clone()]);
    let first = idx.scan_dirs_immediate_outcome(std::slice::from_ref(&root));
    assert_eq!(first.scanned, 520);

    // 添加一个文件改变目录内容
    std::fs::write(root.join("new_file.txt"), b"new").unwrap();

    let skip_dirs = std::collections::HashSet::from([root.clone()]);
    let reports = mtime_precheck_run_cold_scan_to_completion(&idx, &root, &skip_dirs);
    // 大目录（521 文件）需要多个 slice，可能有多个 report
    assert!(!reports.is_empty());
    for r in &reports {
        assert!(
            !r.outcomes[0].manifest_skipped,
            "changed dir should not be skipped"
        );
    }
    let total_scanned: usize = reports.iter().map(|r| r.outcomes[0].outcome.scanned).sum();
    assert!(
        total_scanned > 0,
        "changed dir should be scanned, got {}",
        total_scanned
    );

    let manifest = idx.directory_manifest_report();
    // 段 1.5 未命中（内容变了）
    assert!(manifest.unbounded_summary_misses >= 1);

    let _ = std::fs::remove_dir_all(&root);
}

/// unbounded summary 对大目录（>512 条）能正确计算并比对。
#[test]
fn unbounded_summary_large_dir() {
    let root = unique_tmp_dir("p3-unbounded-large");
    phase3_create_large_dir(&root, 600); // 远超 512

    let idx = TieredIndex::empty(vec![root.clone()]);
    let first = idx.scan_dirs_immediate_outcome(std::slice::from_ref(&root));
    assert_eq!(first.scanned, 600);
    // manifest 被存储（WalkBuilder 不受 512 限制）
    assert_eq!(idx.directory_manifest_report().dirs, 1);

    // touch 目录 mtime（创建+删除临时文件，net zero 内容变化）
    phase3_touch_dir_mtime(&root);

    // PeriodicColdScan：段 1 Some(false)，段 1.5 unbounded summary 比对 → 匹配 → 跳过
    let skip_dirs = std::collections::HashSet::from([root.clone()]);
    let reports = mtime_precheck_run_cold_scan_to_completion(&idx, &root, &skip_dirs);
    assert_eq!(reports.len(), 1);
    assert!(
        reports[0].outcomes[0].manifest_skipped,
        "unbounded summary should match for large dir with no content change"
    );

    let manifest = idx.directory_manifest_report();
    assert_eq!(manifest.unbounded_summary_hits, 1);

    let _ = std::fs::remove_dir_all(&root);
}

/// unbounded summary 在目录 mtime 变了但内容没变时匹配（跳过）。
#[test]
fn unbounded_summary_matches_when_no_change() {
    let root = unique_tmp_dir("p3-match-no-change");
    std::fs::create_dir_all(&root).unwrap();
    std::fs::write(root.join("a.txt"), b"a").unwrap();
    std::fs::write(root.join("b.txt"), b"b").unwrap();

    let idx = TieredIndex::empty(vec![root.clone()]);
    let first = idx.scan_dirs_immediate_outcome(std::slice::from_ref(&root));
    assert_eq!(first.scanned, 2);

    // touch 目录 mtime（内容不变）
    phase3_touch_dir_mtime(&root);

    let skip_dirs = std::collections::HashSet::from([root.clone()]);
    let reports = mtime_precheck_run_cold_scan_to_completion(&idx, &root, &skip_dirs);
    assert_eq!(reports.len(), 1);
    assert!(
        reports[0].outcomes[0].manifest_skipped,
        "unbounded summary should match when content unchanged but mtime changed"
    );
    assert_eq!(reports[0].outcomes[0].outcome.scanned, 0);

    let manifest = idx.directory_manifest_report();
    assert_eq!(manifest.unbounded_summary_hits, 1);
    assert_eq!(manifest.unbounded_summary_misses, 0);

    let _ = std::fs::remove_dir_all(&root);
}

/// unbounded summary 在内容变化时不匹配（继续扫描）。
#[test]
fn unbounded_summary_misses_on_change() {
    let root = unique_tmp_dir("p3-miss-on-change");
    std::fs::create_dir_all(&root).unwrap();
    std::fs::write(root.join("a.txt"), b"a").unwrap();
    std::fs::write(root.join("b.txt"), b"b").unwrap();

    let idx = TieredIndex::empty(vec![root.clone()]);
    let first = idx.scan_dirs_immediate_outcome(std::slice::from_ref(&root));
    assert_eq!(first.scanned, 2);

    // 改变内容：删除一个文件
    std::fs::remove_file(root.join("b.txt")).unwrap();

    let skip_dirs = std::collections::HashSet::from([root.clone()]);
    let reports = mtime_precheck_run_cold_scan_to_completion(&idx, &root, &skip_dirs);
    assert_eq!(reports.len(), 1);
    assert!(
        !reports[0].outcomes[0].manifest_skipped,
        "unbounded summary should miss when content changed"
    );
    assert_eq!(reports[0].outcomes[0].outcome.scanned, 1);

    let manifest = idx.directory_manifest_report();
    assert_eq!(manifest.unbounded_summary_hits, 0);
    assert_eq!(manifest.unbounded_summary_misses, 1);

    let _ = std::fs::remove_dir_all(&root);
}

/// unbounded summary 使用 WalkBuilder 过滤（ignore 规则），而非 read_dir。
/// 在有 .gitignore 的目录中，被忽略的文件不计入 summary。
#[test]
fn unbounded_summary_uses_walkbuilder_filter() {
    let root = unique_tmp_dir("p3-walkbuilder-filter");
    std::fs::create_dir_all(&root).unwrap();
    std::fs::write(root.join("visible.txt"), b"v").unwrap();
    std::fs::write(root.join(".gitignore"), b"*.log\n").unwrap();
    std::fs::write(root.join("ignored.log"), b"ignored").unwrap();

    let idx = TieredIndex::empty(vec![root.clone()]); // ignore_enabled=true, include_hidden=false
                                                      // immediate scan 用 WalkBuilder：ignored.log 命中 ignore，
                                                      // .gitignore 命中 hidden 过滤，仅 visible.txt 被扫描。
    let first = idx.scan_dirs_immediate_outcome(std::slice::from_ref(&root));
    assert_eq!(first.scanned, 1, "only visible.txt should be scanned");
    assert_eq!(idx.directory_manifest_report().dirs, 1);

    // touch 目录 mtime（内容不变）
    phase3_touch_dir_mtime(&root);

    let skip_dirs = std::collections::HashSet::from([root.clone()]);
    let reports = mtime_precheck_run_cold_scan_to_completion(&idx, &root, &skip_dirs);
    assert_eq!(reports.len(), 1);
    assert!(
        reports[0].outcomes[0].manifest_skipped,
        "unbounded summary should match because it uses WalkBuilder (same filter as immediate scan)"
    );

    let manifest = idx.directory_manifest_report();
    assert_eq!(manifest.unbounded_summary_hits, 1);

    let _ = std::fs::remove_dir_all(&root);
}

/// unbounded summary 的 hits/misses 指标正确追踪。
#[test]
fn unbounded_summary_metrics() {
    let root = unique_tmp_dir("p3-metrics");
    std::fs::create_dir_all(&root).unwrap();
    std::fs::write(root.join("file.txt"), b"data").unwrap();

    let idx = TieredIndex::empty(vec![root.clone()]);
    let skip_dirs = std::collections::HashSet::from([root.clone()]);

    // 1. 首次 PeriodicColdScan：无 manifest 记录 → 段 1 None → 段 1.5 miss（无 stored）
    let reports = mtime_precheck_run_cold_scan_to_completion(&idx, &root, &skip_dirs);
    assert!(!reports[0].outcomes[0].manifest_skipped);
    let m1 = idx.directory_manifest_report();
    assert_eq!(m1.unbounded_summary_hits, 0);
    assert_eq!(m1.unbounded_summary_misses, 1);

    // 2. immediate scan 存储 WalkBuilder summary（覆盖首次扫描存储的 bounded summary）
    idx.scan_dirs_immediate_outcome(std::slice::from_ref(&root));

    // 3. touch 目录 mtime（内容不变）→ 段 1 Some(false) → 段 1.5 hit
    phase3_touch_dir_mtime(&root);
    let reports = mtime_precheck_run_cold_scan_to_completion(&idx, &root, &skip_dirs);
    assert!(reports[0].outcomes[0].manifest_skipped);
    let m2 = idx.directory_manifest_report();
    assert_eq!(m2.unbounded_summary_hits, 1);
    assert_eq!(m2.unbounded_summary_misses, 1);

    // 4. 改变内容 → 段 1 Some(false) → 段 1.5 miss
    std::fs::write(root.join("extra.txt"), b"extra").unwrap();
    let reports = mtime_precheck_run_cold_scan_to_completion(&idx, &root, &skip_dirs);
    assert!(!reports[0].outcomes[0].manifest_skipped);
    let m3 = idx.directory_manifest_report();
    assert_eq!(m3.unbounded_summary_hits, 1);
    assert_eq!(m3.unbounded_summary_misses, 2);

    let _ = std::fs::remove_dir_all(&root);
}

// ===========================================================================
// Phase 1: 目录 mtime 预检集成测试
// ===========================================================================

use crate::index::l2_partition::mtime_to_ns;

/// 辅助：获取目录 mtime 纳秒值。
fn mtime_precheck_dir_mtime(path: &std::path::Path) -> i64 {
    let meta = std::fs::symlink_metadata(path).unwrap();
    mtime_to_ns(meta.modified().ok())
}

/// 辅助：入队 + 弹出 + 处理一次 PeriodicColdScan。
fn mtime_precheck_pop_and_process(
    idx: &TieredIndex,
    skip_dirs: &std::collections::HashSet<PathBuf>,
) -> Option<DirtyProcessReport> {
    let entry = idx.dirty_queue.lock().pop_ready(u64::MAX, 1).pop()?;
    Some(
        idx.process_dirty_entry_with_project_markers_and_manifest_skip_dirs(
            entry,
            &[],
            &[],
            skip_dirs,
        ),
    )
}

/// 辅助：运行完整 PeriodicColdScan（处理所有 slice 直到队列空）。
fn mtime_precheck_run_cold_scan_to_completion(
    idx: &TieredIndex,
    dir: &Path,
    skip_dirs: &std::collections::HashSet<PathBuf>,
) -> Vec<DirtyProcessReport> {
    idx.enqueue_dirty_dirs(vec![dir.to_path_buf()], DirtyReason::PeriodicColdScan);
    let mut reports = Vec::new();
    while let Some(r) = mtime_precheck_pop_and_process(idx, skip_dirs) {
        reports.push(r);
    }
    reports
}

#[test]
fn mtime_precheck_unconfigured_dir() {
    // covering_tier=None 的目录（allow_manifest_skip=false），mtime 预检仍生效
    let root = unique_tmp_dir("mtime-unconfigured");
    std::fs::create_dir_all(&root).unwrap();
    std::fs::write(root.join("file.txt"), b"data").unwrap();

    let idx = TieredIndex::empty(vec![root.clone()]);
    let skip_dirs = std::collections::HashSet::new();

    // 第一次 PeriodicColdScan：无 manifest 记录，全量扫描，记录 mtime
    let reports = mtime_precheck_run_cold_scan_to_completion(&idx, &root, &skip_dirs);
    assert_eq!(reports.len(), 1);
    assert!(!reports[0].outcomes[0].manifest_skipped);
    assert_eq!(reports[0].outcomes[0].outcome.scanned, 1);

    // 第二次 PeriodicColdScan：mtime 未变，预检命中，跳过
    let reports = mtime_precheck_run_cold_scan_to_completion(&idx, &root, &skip_dirs);
    assert_eq!(reports.len(), 1);
    assert!(
        reports[0].outcomes[0].manifest_skipped,
        "mtime precheck should skip unchanged dir even with allow_manifest_skip=false"
    );
    assert_eq!(reports[0].outcomes[0].outcome.scanned, 0);

    let manifest = idx.directory_manifest_report();
    assert_eq!(manifest.mtime_precheck_hits, 1);
    assert_eq!(manifest.mtime_precheck_no_record, 1);

    let _ = std::fs::remove_dir_all(&root);
}

#[test]
fn mtime_precheck_l0_dir() {
    // L0 tier 覆盖的目录（allow_manifest_skip=false），mtime 预检生效
    let root = unique_tmp_dir("mtime-l0");
    std::fs::create_dir_all(&root).unwrap();
    std::fs::write(root.join("a.txt"), b"a").unwrap();
    std::fs::write(root.join("b.txt"), b"b").unwrap();

    let idx = TieredIndex::empty(vec![root.clone()]);
    let skip_dirs = std::collections::HashSet::new(); // L0: 不在 manifest_skip_dirs 中

    // 第一次扫描：全量扫描 + 记录 mtime
    mtime_precheck_run_cold_scan_to_completion(&idx, &root, &skip_dirs);

    // 第二次扫描：mtime 命中 → 跳过（即使 allow_manifest_skip=false）
    let reports = mtime_precheck_run_cold_scan_to_completion(&idx, &root, &skip_dirs);
    assert!(reports[0].outcomes[0].manifest_skipped);
    assert_eq!(reports[0].outcomes[0].outcome.scanned, 0);

    let _ = std::fs::remove_dir_all(&root);
}

#[test]
fn mtime_precheck_l1_dir() {
    // L1 tier 覆盖的目录（allow_manifest_skip=false），mtime 预检生效
    let root = unique_tmp_dir("mtime-l1");
    std::fs::create_dir_all(&root).unwrap();
    std::fs::write(root.join("file.txt"), b"data").unwrap();

    let idx = TieredIndex::empty(vec![root.clone()]);
    let skip_dirs = std::collections::HashSet::new(); // L1: 不在 manifest_skip_dirs 中

    mtime_precheck_run_cold_scan_to_completion(&idx, &root, &skip_dirs);
    let reports = mtime_precheck_run_cold_scan_to_completion(&idx, &root, &skip_dirs);
    assert!(reports[0].outcomes[0].manifest_skipped);
    assert_eq!(reports[0].outcomes[0].outcome.scanned, 0);

    let _ = std::fs::remove_dir_all(&root);
}

#[test]
fn mtime_precheck_l2_dir() {
    // L2 tier 覆盖的目录（allow_manifest_skip=true），mtime 预检在 manifest skip 之前生效
    let root = unique_tmp_dir("mtime-l2");
    std::fs::create_dir_all(&root).unwrap();
    std::fs::write(root.join("file.txt"), b"data").unwrap();

    let idx = TieredIndex::empty(vec![root.clone()]);
    let skip_dirs = std::collections::HashSet::from([root.clone()]); // L2: 在 manifest_skip_dirs 中

    // 第一次扫描：全量扫描 + 记录 mtime
    mtime_precheck_run_cold_scan_to_completion(&idx, &root, &skip_dirs);

    // 第二次扫描：mtime 预检（段 1）在 manifest skip（段 2）之前生效
    let reports = mtime_precheck_run_cold_scan_to_completion(&idx, &root, &skip_dirs);
    assert!(reports[0].outcomes[0].manifest_skipped);
    assert_eq!(reports[0].outcomes[0].outcome.scanned, 0);

    // mtime 预检命中（而非 manifest skip 命中）
    let manifest = idx.directory_manifest_report();
    assert_eq!(
        manifest.mtime_precheck_hits, 1,
        "mtime precheck should fire before manifest skip"
    );

    let _ = std::fs::remove_dir_all(&root);
}

#[test]
fn mtime_precheck_disabled_when_clock_untrusted() {
    // clock 不可信时 mtime 预检不生效，mtime_precheck_untrusted_clock 递增
    let root = unique_tmp_dir("mtime-clock-untrusted");
    std::fs::create_dir_all(&root).unwrap();
    std::fs::write(root.join("file.txt"), b"data").unwrap();

    let idx = TieredIndex::empty(vec![root.clone()]);
    let skip_dirs = std::collections::HashSet::new();

    // 第一次扫描：记录 mtime
    mtime_precheck_run_cold_scan_to_completion(&idx, &root, &skip_dirs);

    // 让 clock 不可信
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

    // 第二次扫描：clock 不可信 → mtime 预检被禁用 → 正常扫描
    let reports = mtime_precheck_run_cold_scan_to_completion(&idx, &root, &skip_dirs);
    assert_eq!(reports.len(), 1);
    assert!(
        !reports[0].outcomes[0].manifest_skipped,
        "mtime precheck should be disabled when clock is untrusted"
    );
    assert!(
        reports[0].outcomes[0].outcome.scanned > 0,
        "should do full scan"
    );

    let manifest = idx.directory_manifest_report();
    assert_eq!(
        manifest.mtime_precheck_untrusted_clock, 1,
        "mtime_precheck_untrusted_clock metric should increment"
    );

    let _ = std::fs::remove_dir_all(&root);
}

#[test]
fn mtime_record_skipped_on_stale_scan() {
    // stale_low_priority_scan=true 时 mtime 记录不写入
    let root = unique_tmp_dir("mtime-stale");
    std::fs::create_dir_all(&root).unwrap();
    // 创建大量文件使扫描耗时足够长，以便从另一线程推进 event_seq
    for i in 0..400 {
        std::fs::write(root.join(format!("file_{:04}.txt", i)), b"x").unwrap();
    }

    let idx = TieredIndex::empty(vec![root.clone()]);
    let skip_dirs = std::collections::HashSet::new();

    // 第一次扫描：记录 mtime
    mtime_precheck_run_cold_scan_to_completion(&idx, &root, &skip_dirs);
    let recorded_mtime = mtime_precheck_dir_mtime(&root);

    // 添加新文件改变目录 mtime
    std::fs::write(root.join("new_file.txt"), b"new").unwrap();
    let new_mtime = mtime_precheck_dir_mtime(&root);
    assert_ne!(
        recorded_mtime, new_mtime,
        "dir mtime should change after adding file"
    );

    // 第二次扫描：从另一线程推进 event_seq，触发 stale 条件
    idx.enqueue_dirty_dirs(vec![root.clone()], DirtyReason::PeriodicColdScan);
    let report = std::thread::scope(|s| {
        let handle = s.spawn(|| mtime_precheck_pop_and_process(&idx, &skip_dirs).unwrap());
        // 等扫描开始后推进 event_seq
        std::thread::sleep(std::time::Duration::from_millis(5));
        idx.event_seq
            .fetch_add(1_000_000, std::sync::atomic::Ordering::Relaxed);
        handle.join().unwrap()
    });

    // 验证 stale batch 被丢弃
    assert!(
        report.dropped_stale_batches >= 1,
        "stale batch should be dropped: {report:?}"
    );

    // 验证 mtime 未被更新（仍为旧值，而非 new_mtime）
    // stale 扫描不应记录 mtime，所以预检仍命中旧 mtime
    assert_eq!(
        idx.directory_manifests
            .try_mtime_precheck(&root, recorded_mtime),
        Some(true),
        "mtime should NOT be updated after stale scan"
    );

    let _ = std::fs::remove_dir_all(&root);
}

#[test]
fn mtime_precheck_in_middle_slice() {
    // 多 slice 目录的中间 slice 完成（completed=false）时不写入 mtime
    let root = unique_tmp_dir("mtime-middle-slice");
    std::fs::create_dir_all(&root).unwrap();
    // 创建超过 REPAIR_SLICE_MAX_ENTRIES(512) 的文件
    for i in 0..530 {
        std::fs::write(root.join(format!("f_{:04}.txt", i)), b"x").unwrap();
    }

    let idx = TieredIndex::empty(vec![root.clone()]);
    let skip_dirs = std::collections::HashSet::new();

    // 入队第一次 PeriodicColdScan
    idx.enqueue_dirty_dirs(vec![root.clone()], DirtyReason::PeriodicColdScan);

    // 处理第一个 slice（completed=false）
    let report1 = mtime_precheck_pop_and_process(&idx, &skip_dirs).unwrap();
    assert_eq!(report1.outcomes.len(), 1);
    // 第一个 slice 不应完成（有 530 个文件，slice 大小 512）
    assert!(
        !report1.outcomes[0].manifest_skipped,
        "first slice should not be skipped"
    );

    // 中间 slice（completed=false）后不应记录 mtime
    // try_mtime_precheck 应返回 None（无记录）或 Some(false)
    let current_mtime = mtime_precheck_dir_mtime(&root);
    let precheck_result = idx
        .directory_manifests
        .try_mtime_precheck(&root, current_mtime);
    assert!(
        precheck_result.is_none() || precheck_result == Some(false),
        "mtime should NOT be recorded after middle slice (completed=false): {:?}",
        precheck_result
    );

    // 清理队列中剩余的 slice
    while mtime_precheck_pop_and_process(&idx, &skip_dirs).is_some() {}

    let _ = std::fs::remove_dir_all(&root);
}

#[test]
fn mtime_precheck_first_slice_only() {
    // 多 slice 目录的 mtime 仅在 last slice（completed=true）完成后写入
    let root = unique_tmp_dir("mtime-last-slice");
    std::fs::create_dir_all(&root).unwrap();
    for i in 0..530 {
        std::fs::write(root.join(format!("f_{:04}.txt", i)), b"x").unwrap();
    }

    let idx = TieredIndex::empty(vec![root.clone()]);
    let skip_dirs = std::collections::HashSet::new();

    // 运行完整 PeriodicColdScan（处理所有 slice 直到完成）
    let reports = mtime_precheck_run_cold_scan_to_completion(&idx, &root, &skip_dirs);
    assert!(
        reports.len() >= 2,
        "should have multiple slices: {} reports",
        reports.len()
    );

    // 最后一个 slice 完成后应记录 mtime
    let current_mtime = mtime_precheck_dir_mtime(&root);
    assert_eq!(
        idx.directory_manifests
            .try_mtime_precheck(&root, current_mtime),
        Some(true),
        "mtime should be recorded after last slice completes"
    );

    // 再次扫描：mtime 预检应命中
    let reports = mtime_precheck_run_cold_scan_to_completion(&idx, &root, &skip_dirs);
    assert!(
        reports[0].outcomes[0].manifest_skipped,
        "mtime precheck should skip after all slices completed and mtime recorded"
    );

    let _ = std::fs::remove_dir_all(&root);
}

#[test]
fn mtime_precheck_metrics() {
    // 验证 5 个 mtime 预检指标在各种场景下正确递增
    let root = unique_tmp_dir("mtime-metrics");
    std::fs::create_dir_all(&root).unwrap();
    std::fs::write(root.join("file.txt"), b"data").unwrap();

    let idx = TieredIndex::empty(vec![root.clone()]);
    let skip_dirs = std::collections::HashSet::new();

    // 1. 第一次扫描：无记录 → mtime_precheck_no_record 递增
    mtime_precheck_run_cold_scan_to_completion(&idx, &root, &skip_dirs);
    let m1 = idx.directory_manifest_report();
    assert_eq!(m1.mtime_precheck_no_record, 1);
    assert_eq!(m1.mtime_precheck_hits, 0);
    assert_eq!(m1.mtime_precheck_misses, 0);
    assert_eq!(m1.mtime_precheck_stat_errors, 0);
    assert_eq!(m1.mtime_precheck_untrusted_clock, 0);

    // 2. 第二次扫描：mtime 匹配 → mtime_precheck_hits 递增
    mtime_precheck_run_cold_scan_to_completion(&idx, &root, &skip_dirs);
    let m2 = idx.directory_manifest_report();
    assert_eq!(m2.mtime_precheck_no_record, 1);
    assert_eq!(m2.mtime_precheck_hits, 1);
    assert_eq!(m2.mtime_precheck_misses, 0);

    // 3. 添加文件改变 mtime，第三次扫描：mtime 不匹配 → mtime_precheck_misses 递增
    std::fs::write(root.join("new.txt"), b"new").unwrap();
    mtime_precheck_run_cold_scan_to_completion(&idx, &root, &skip_dirs);
    let m3 = idx.directory_manifest_report();
    assert_eq!(m3.mtime_precheck_hits, 1);
    assert_eq!(m3.mtime_precheck_misses, 1);
    assert_eq!(m3.mtime_precheck_no_record, 1);

    // 4. clock 不可信时扫描 → mtime_precheck_untrusted_clock 递增
    {
        let mut clock = idx.clock_skew.lock();
        let base_wall = std::time::SystemTime::now();
        let base_mono = std::time::Instant::now();
        assert!(!clock.observe(base_wall, base_mono));
        assert!(clock.observe(
            base_wall - std::time::Duration::from_secs(3),
            base_mono + std::time::Duration::from_secs(3),
        ));
    }
    mtime_precheck_run_cold_scan_to_completion(&idx, &root, &skip_dirs);
    let m4 = idx.directory_manifest_report();
    assert_eq!(m4.mtime_precheck_untrusted_clock, 1);
    // stat_errors 应始终为 0（目录存在且可读）
    assert_eq!(m4.mtime_precheck_stat_errors, 0);

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
    assert_eq!(report.io.current_backoff_ms, 1);
    assert_eq!(report.io.token_bucket_consume_count, 0);
    assert_eq!(report.io.token_bucket_limited_count, 0);

    let _ = std::fs::remove_dir_all(&root);
}

#[test]
fn tiered_diagnostics_include_root_case_policy_state() {
    let root = unique_tmp_dir("case-policy-diag");
    std::fs::create_dir_all(&root).unwrap();

    let idx = TieredIndex::empty(vec![root.clone()]);

    let roots = idx.refresh_root_case_policy_diagnostics();
    assert_eq!(roots.len(), 1);
    assert_eq!(roots[0].root_path, root.display().to_string());
    assert!(!roots[0].detected_policy.is_empty());

    let mut report = DiagnosticReport::default();
    idx.collect(&mut report);
    assert_eq!(report.storage.case_policy_roots, roots);
    assert_eq!(
        report.storage.case_policy_conflict_count,
        roots.iter().map(|root| root.conflict_count).sum::<u64>()
    );
    assert_eq!(report.storage.refresh_base_count, 0);

    let _ = std::fs::remove_dir_all(&root);
}

#[tokio::test]
async fn mmap_warmup_default_disabled_and_reports_enabled_cold_snapshot() {
    let root = unique_tmp_dir("mmap-warmup");
    let state = unique_tmp_dir("mmap-warmup-state");
    std::fs::create_dir_all(&root).unwrap();
    std::fs::create_dir_all(&state).unwrap();

    let l2 = PersistentIndex::new_with_roots(vec![root.clone()]);
    l2.upsert(FileMeta {
        file_key: FileKey {
            dev: 1,
            ino: 20,
            generation: 0,
        },
        path: root.join("needle.txt"),
        size: 1,
        mtime: None,
        ctime: None,
        atime: None,
        kind: Default::default(),
    });
    let store = SnapshotStore::new(state.join("index.db"));
    let stable_path = stable_v7_path_for(store.path());
    std::fs::create_dir_all(stable_path.parent().unwrap()).unwrap();
    write_v7_snapshot_atomic(&stable_path, &l2.to_base_index_data()).unwrap();

    let idx = TieredIndex::load_or_empty_with_options_follow_excludes_fs_policy_and_io_governor(
        &store,
        vec![root.clone()],
        false,
        true,
        false,
        Vec::new(),
        crate::fs_policy::FsPolicyConfig::default(),
        IoGovernorConfig {
            enabled: true,
            stat_rate_per_sec: 1_000_000,
            ..IoGovernorConfig::default()
        },
    )
    .await
    .unwrap();
    let mut report = DiagnosticReport::default();
    idx.collect(&mut report);
    assert!(!report.storage.mmap_warmup_enabled);
    assert_eq!(report.storage.mmap_warmup_pages, 0);
    assert_eq!(report.storage.mmap_warmup_cancel_reason, "disabled");

    idx.apply_mmap_warmup_config(MmapWarmupConfig {
        enable: true,
        max_bytes: 4096,
    });
    let mut report = DiagnosticReport::default();
    idx.collect(&mut report);
    assert!(report.storage.mmap_warmup_enabled);
    assert_eq!(report.storage.mmap_warmup_pages, 1);
    assert!(matches!(
        report.storage.mmap_warmup_cancel_reason.as_str(),
        "" | "max_bytes"
    ));
    assert!(report.io.token_bucket_consume_count >= 1);

    let _ = std::fs::remove_dir_all(&root);
    let _ = std::fs::remove_dir_all(&state);
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

#[tokio::test]
async fn tiered_diagnostics_skip_manifest_only_cold_hardlink_scan() -> anyhow::Result<()> {
    let root = unique_tmp_dir("cold-hardlink-diag");
    let state = unique_tmp_dir("cold-hardlink-diag-state");
    std::fs::create_dir_all(&root)?;
    std::fs::create_dir_all(&state)?;
    let original = root.join("original.txt");
    let alias = root.join("alias.txt");
    std::fs::write(&original, b"same")?;
    std::fs::hard_link(&original, &alias)?;

    let store = Arc::new(SnapshotStore::new(state.join("index.db")));
    let idx = Arc::new(TieredIndex::empty(vec![root.clone()]));
    idx.apply_events(&[
        mk_event(1, EventType::Create, original),
        mk_event(2, EventType::Create, alias),
    ]);
    idx.refresh_base();
    idx.snapshot_now(store.clone()).await?;

    let loaded = TieredIndex::load_or_empty(&*store, vec![root.clone()]).await?;
    let cold_report = loaded.memory_report(EventPipelineStats::default()).base;
    assert_eq!(cold_report.hot_memory_entries, 0);
    assert_eq!(cold_report.manifest_only_entries, 2);
    assert_eq!(cold_report.cold_segment_count, 1);

    let mut report = DiagnosticReport::default();
    loaded.collect(&mut report);
    assert_eq!(report.storage.hardlink_group_count, 0);
    assert_eq!(report.storage.hardlink_max_group_size, 0);

    let _ = std::fs::remove_dir_all(&root);
    let _ = std::fs::remove_dir_all(&state);
    Ok(())
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
    assert_eq!(
        idx.recovery_quarantine.freeze_gate.lock().blocked_events(),
        3
    );
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

    assert_eq!(
        idx.recovery_quarantine
            .freeze_gate
            .lock()
            .frozen_root_count(),
        0
    );
    assert_eq!(idx.dirty_queue_len(), 1);
}

#[test]
fn sliced_repair_processes_large_dir_in_bounded_chunks() {
    let root = unique_tmp_dir("sliced-repair-large-dir");
    std::fs::create_dir_all(&root).unwrap();
    let idx = TieredIndex::empty(vec![root.clone()]);

    let mut expected = Vec::new();
    for i in 0..700 {
        let path = root.join(format!("sliced_repair_{i:04}.txt"));
        std::fs::write(&path, b"slice").unwrap();
        expected.push(path);
    }
    let manifest_seed = idx.scan_dirs_immediate_outcome(std::slice::from_ref(&root));
    assert_eq!(manifest_seed.scanned, 700);
    idx.refresh_base();
    for path in &expected {
        std::fs::remove_file(path).unwrap();
    }
    for path in &expected {
        std::fs::write(path, b"slice-new").unwrap();
    }

    idx.enqueue_dirty_dirs(vec![root.clone()], DirtyReason::PeriodicColdScan);
    let first_entry = idx.dirty_queue.lock().pop_ready(u64::MAX, 1).pop().unwrap();
    let first = idx.process_dirty_entry(first_entry, &[]);

    assert!(!first.failed);
    assert_eq!(first.dirs_scanned, 1);
    assert_eq!(first.outcomes.len(), 1);
    assert_eq!(first.outcomes[0].outcome.scanned, 512);
    assert_eq!(idx.dirty_queue_len(), 1);
    assert_eq!(idx.cold_sweep_last_completed(), 0);

    let second_entry = idx.dirty_queue.lock().pop_ready(u64::MAX, 1).pop().unwrap();
    let cursor = second_entry.repair_cursor.clone().unwrap();
    assert_eq!(cursor.dir, root);
    assert!(cursor.offset >= 512);

    let second = idx.process_dirty_entry(second_entry, &[]);
    assert!(!second.failed);
    assert_eq!(second.dirs_scanned, 1);
    assert_eq!(second.outcomes[0].outcome.scanned, 188);
    assert_eq!(idx.dirty_queue_len(), 0);
    assert!(idx.cold_sweep_last_completed() > 0);

    for path in expected {
        let query = path.file_name().unwrap().to_string_lossy();
        assert!(
            idx.query_limit_detailed(&query, 1)
                .iter()
                .any(|result| result.meta.path == path),
            "sliced repair should make {} searchable",
            path.display()
        );
    }

    let _ = std::fs::remove_dir_all(&root);
}

#[test]
fn sliced_repair_cursor_resumes_after_first_chunk() {
    let root = unique_tmp_dir("sliced-repair-cursor");
    std::fs::create_dir_all(&root).unwrap();
    let idx = TieredIndex::empty(vec![root.clone()]);

    for i in 0..620 {
        let path = root.join(format!("sliced_cursor_{i:04}.txt"));
        std::fs::write(path, b"slice").unwrap();
    }
    let manifest_seed = idx.scan_dirs_immediate_outcome(std::slice::from_ref(&root));
    assert_eq!(manifest_seed.scanned, 620);
    idx.refresh_base();
    for i in 0..620 {
        std::fs::remove_file(root.join(format!("sliced_cursor_{i:04}.txt"))).unwrap();
    }
    for i in 0..620 {
        std::fs::write(root.join(format!("sliced_cursor_{i:04}.txt")), b"slice-new").unwrap();
    }

    idx.enqueue_dirty_dirs(vec![root.clone()], DirtyReason::PeriodicColdScan);
    let first_entry = idx.dirty_queue.lock().pop_ready(u64::MAX, 1).pop().unwrap();
    let first = idx.process_dirty_entry(first_entry, &[]);
    assert_eq!(first.outcomes[0].outcome.scanned, 512);
    assert_eq!(first.outcomes[0].outcome.changed, 512);

    let second_entry = idx.dirty_queue.lock().pop_ready(u64::MAX, 1).pop().unwrap();
    let second = idx.process_dirty_entry(second_entry, &[]);
    assert_eq!(second.outcomes[0].outcome.scanned, 108);
    assert_eq!(second.outcomes[0].outcome.changed, 108);

    let _ = std::fs::remove_dir_all(&root);
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
    assert_eq!(
        idx.recovery_quarantine
            .verify_pending
            .load(Ordering::Relaxed),
        1
    );

    let deleted = prefix.join("blocked_delete.txt");
    idx.apply_events(&[mk_event(1, EventType::Delete, deleted)]);
    assert_eq!(idx.delta_buffer.lock().len(), 0);
    assert_eq!(
        idx.recovery_quarantine.freeze_gate.lock().blocked_events(),
        1
    );

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
    assert_eq!(
        idx.recovery_quarantine
            .verify_pending
            .load(Ordering::Relaxed),
        0
    );
    assert_eq!(
        idx.recovery_quarantine
            .verified_roots
            .load(Ordering::Relaxed),
        1
    );

    let dirty_entry = idx.dirty_queue.lock().pop_ready(u64::MAX, 1).pop().unwrap();
    let report = idx.process_dirty_entry(dirty_entry, &[]);
    assert!(!report.failed);
    assert_eq!(idx.query("restored_after_online").len(), 1);

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
    assert_eq!(
        idx.recovery_quarantine
            .verify_pending
            .load(Ordering::Relaxed),
        1
    );
    assert_eq!(
        idx.recovery_quarantine
            .verified_roots
            .load(Ordering::Relaxed),
        0
    );
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
    assert_eq!(idx.query("wal_restored_after_online").len(), 1);
    assert_eq!(
        idx.recovery_quarantine.freeze_gate.lock().blocked_events(),
        0
    );

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

    assert_eq!(idx.query("old_aaa").len(), 1);
    assert_eq!(idx.query("new_bbb").len(), 1);
}

#[test]
fn finish_rebuild_clears_current_recovery_rebuild_flags() {
    let root = unique_tmp_dir("rebuild-clears-recovery");
    std::fs::create_dir_all(&root).unwrap();

    let idx = Arc::new(TieredIndex::empty(vec![root.clone()]));
    let reason = "snapshot_too_small_for_roots".to_string();
    idx.set_startup_recovery_report(StartupRecoveryReport {
        startup_scan_required: true,
        requires_repair: true,
        requires_rebuild: true,
        hard_rebuild_needed: true,
        reasons: vec![reason.clone()],
        repair_reason_counts: vec![RecoveryReasonCount {
            reason: reason.clone(),
            count: 1,
        }],
        audit: RecoveryAuditReport {
            requires_repair: true,
            requires_rebuild: true,
            reasons: vec![reason],
            ..RecoveryAuditReport::default()
        },
        ..StartupRecoveryReport::default()
    });
    idx.set_startup_repair_stats(StartupRepairStats {
        ran: true,
        escalated: true,
        escalation_reason: "hard_rebuild_evidence".to_string(),
        ..StartupRepairStats::default()
    });

    assert!(idx.try_start_rebuild_force());
    let new_l2 = Arc::new(PersistentIndex::new_with_roots(vec![root.clone()]));
    assert!(!idx.finish_rebuild(new_l2));

    let recovery = idx.recovery_status();
    assert!(!recovery.report.startup_scan_required);
    assert!(!recovery.report.requires_repair);
    assert!(!recovery.report.requires_rebuild);
    assert!(!recovery.report.hard_rebuild_needed);
    assert!(recovery.report.reasons.is_empty());
    assert!(recovery.report.repair_reason_counts.is_empty());
    assert!(!recovery.report.audit.requires_repair);
    assert!(!recovery.report.audit.requires_rebuild);
    assert!(recovery.report.audit.reasons.is_empty());
    assert!(recovery.repair.ran);
    assert!(!recovery.repair.escalated);
    assert!(recovery.repair.escalation_reason.is_empty());

    let _ = std::fs::remove_dir_all(&root);
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

    let rebuilding_memory = idx.memory_report_light(EventPipelineStats::default());
    assert!(rebuilding_memory.rebuild.in_progress);
    assert_eq!(rebuilding_memory.l2.file_count, 1);
    assert!(rebuilding_memory.l2.estimated_bytes > 0);

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
    assert_eq!(idx.file_count(), 0);
    assert!(idx.query("partial_ready").is_empty());
    assert!(
        idx.pending_snapshot_generation.lock().is_some(),
        "completed scratch generation stays owned until durable snapshot publication"
    );
    assert!(idx.rebuild_snapshot_pending.load(Ordering::Acquire));
    assert_eq!(
        idx.reserve_rebuild_with_cooldown("pending owned generation"),
        RebuildAdmission::Coalesced
    );

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
    idx.pending_snapshot_generation.lock().take();
    idx.rebuild_snapshot_pending.store(false, Ordering::Release);
    *idx.owned_snapshot_telemetry.lock() = Default::default();

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
    idx.pending_snapshot_generation.lock().take();
    idx.rebuild_snapshot_pending.store(false, Ordering::Release);
    *idx.owned_snapshot_telemetry.lock() = Default::default();

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
    assert_eq!(
        idx.base.load().file_count(),
        0,
        "ordinary query must not materialize an initially empty BaseIndex"
    );
    assert_eq!(idx.stats_report().refresh_base_count, 0);

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
    assert_eq!(idx.stats_report().refresh_base_count, 0);

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
        with_refs.base_strong_refs == baseline.base_strong_refs + 1,
        "base strong refs should include externally held Arc generations: baseline={baseline:?} with_refs={with_refs:?}"
    );
    assert!(
        with_refs.l2_strong_refs == baseline.l2_strong_refs + 1,
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
fn memory_report_light_reuses_cached_full_snapshot() {
    let root = unique_tmp_dir("memory-light-cache");
    std::fs::create_dir_all(&root).unwrap();
    let idx = TieredIndex::empty(vec![root.clone()]);

    let cold_light = idx.memory_report_light(EventPipelineStats::default());
    assert_eq!(cold_light.sample_depth, MemorySampleDepth::Light);
    assert!(!cold_light.cache_hit);

    let full = idx.memory_report(EventPipelineStats::default());
    assert_eq!(full.sample_depth, MemorySampleDepth::Full);
    assert!(!full.cache_hit);

    let mut stale_full = full.clone();
    stale_full.process_smaps_rollup = Some(SmapsRollupStats {
        rss_bytes: 42,
        pss_bytes: 42,
        private_clean_bytes: 42,
        private_dirty_bytes: 42,
    });
    {
        let mut cache = idx.memory_report_cache.lock();
        cache.report = Some(stale_full);
        cache.sampled_at = Some(std::time::Instant::now());
    }

    let dirty_dir = root.join("project");
    std::fs::create_dir_all(&dirty_dir).unwrap();
    idx.enqueue_dirty_dirs(vec![dirty_dir], DirtyReason::QueryMiss);

    let light = idx.memory_report_light(EventPipelineStats::default());
    assert_eq!(light.sample_depth, MemorySampleDepth::Light);
    assert!(light.cache_hit);
    assert_eq!(light.base.file_count, full.base.file_count);
    assert_eq!(light.l2.file_count, full.l2.file_count);
    assert_eq!(light.dirty_queue.pending_scopes, 1);
    assert_ne!(
        light.process_smaps_rollup.as_ref().map(|s| s.rss_bytes),
        Some(42),
        "light memory reports must refresh smaps instead of reusing stale full-sample smaps"
    );
    if let Some(smaps) = light.process_smaps_rollup.as_ref() {
        assert_eq!(
            light.process_rss_bytes, smaps.rss_bytes,
            "light memory reports should use the same smaps sample for process_rss_bytes"
        );
    }

    let _ = std::fs::remove_dir_all(&root);
}

#[tokio::test]
async fn memory_report_light_cache_miss_reports_current_generations_after_snapshot() {
    let root = unique_tmp_dir("memory-light-cache-invalidate");
    std::fs::create_dir_all(&root).unwrap();
    let path = root.join("flush-me.txt");
    std::fs::write(&path, b"flush").unwrap();

    let idx = Arc::new(TieredIndex::empty(vec![root.clone()]));
    idx.apply_events(&[mk_event(1, EventType::Create, path)]);

    let full = idx.memory_report(EventPipelineStats::default());
    assert_eq!(full.sample_depth, MemorySampleDepth::Full);
    assert_eq!(full.l2.file_count, 1);

    let store = Arc::new(SnapshotStore::new(root.join("index.db")));
    idx.snapshot_now(store).await.unwrap();

    let light = idx.memory_report_light(EventPipelineStats::default());
    assert_eq!(light.sample_depth, MemorySampleDepth::Light);
    assert!(!light.cache_hit);
    assert_eq!(light.l2.file_count, 0);
    assert_eq!(light.base.file_count, 1);
    assert!(light.index_estimated_bytes >= light.base.estimated_bytes);

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
    assert_eq!(after.hold_count, before.hold_count + 1);
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
    // Linux 上删除后立即新建文件时可能复用 inode，当前实现会用一次
    // same-FileKey upsert 覆盖旧路径；此时最终状态正确，但 delete_events 可能为 0。
    assert!(
        r.delete_events >= 1 || r.upsert_events >= 1,
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

#[tokio::test]
async fn fast_sync_skips_unchanged_base_paths() {
    let root = unique_tmp_dir("fast-sync-unchanged");
    let state = unique_tmp_dir("fast-sync-unchanged-state");
    std::fs::create_dir_all(&root).unwrap();
    std::fs::create_dir_all(&state).unwrap();
    let path = root.join("unchanged_base_match.txt");
    std::fs::write(&path, b"stable").unwrap();

    let idx = Arc::new(TieredIndex::empty(vec![root.clone()]));
    idx.apply_events(&[mk_event(1, EventType::Create, path.clone())]);
    idx.snapshot_now(Arc::new(SnapshotStore::new(state.join("index.db"))))
        .await
        .unwrap();
    assert_eq!(
        idx.memory_report(EventPipelineStats::default())
            .l2
            .file_count,
        0
    );

    let report = idx.fast_sync(
        DirtyScope::Dirs {
            cutoff_ns: 0,
            dirs: vec![root.clone()],
        },
        &[],
    );

    assert!(report.dirs_scanned >= 1);
    assert_eq!(report.upsert_events, 0);
    assert_eq!(report.delete_events, 0);
    assert_eq!(
        idx.memory_report(EventPipelineStats::default())
            .l2
            .file_count,
        0
    );
    assert!(!idx.query("unchanged_base_match").is_empty());

    let _ = std::fs::remove_dir_all(&root);
    let _ = std::fs::remove_dir_all(&state);
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
    assert_eq!(report.indexed_bytes, 65);

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
fn dupe_content_respects_mount_policy_before_hashing() -> anyhow::Result<()> {
    let root = unique_tmp_dir("query-dupe-content-mount-policy");
    let denied = root.join("denied");
    std::fs::create_dir_all(&denied)?;

    let denied_a = denied.join("mount_policy_dupe_a.txt");
    let denied_b = denied.join("mount_policy_dupe_b.txt");
    std::fs::write(&denied_a, b"same denied content")?;
    std::fs::write(&denied_b, b"same denied content")?;

    let l1 = L1Cache::with_capacity(1000);
    let l2 = Arc::new(PersistentIndex::new_with_roots(vec![root.clone()]));
    l2.upsert(file_meta_from_path(denied_a.clone()));
    l2.upsert(file_meta_from_path(denied_b.clone()));
    let l3 = IndexBuilder::new(vec![root.clone()]).with_fs_policy_config(FsPolicyConfig {
        deny_mounts: vec![denied.clone()],
        ..FsPolicyConfig::default()
    });
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
        Arc::new(IoGovernor::disabled()),
    );

    let results = idx.query_limit_detailed("dupe:content mount_policy_dupe", 10);
    assert!(
        results.is_empty(),
        "dupe:content must not hash files denied by mount policy"
    );

    let mut diagnostics = DiagnosticReport::default();
    idx.collect(&mut diagnostics);
    assert_eq!(diagnostics.watchers.denied_mount_count, 2);
    assert_eq!(diagnostics.storage.content_hash_skipped_count, 2);
    assert_eq!(
        diagnostics.storage.content_hash_last_skip_reason,
        "mount_policy:deny_mount"
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
    assert_eq!(loaded.query("alpha_direct_v7").len(), 1);

    let beta = content_root.join("beta_after_direct_load.txt");
    std::fs::write(&beta, b"beta")?;
    loaded.apply_events(&[mk_event(2, EventType::Create, beta.clone())]);
    assert_eq!(loaded.query("beta_after_direct_load").len(), 1);
    loaded.snapshot_now(store.clone()).await?;

    let reloaded = TieredIndex::load_or_empty(&*store, vec![content_root.clone()]).await?;
    assert_eq!(
        reloaded.l2.load().file_count(),
        0,
        "reloaded v7 snapshot should still avoid L2 hydration"
    );
    assert_eq!(reloaded.file_count(), 2);
    assert_eq!(reloaded.query("alpha_direct_v7").len(), 1);
    assert_eq!(reloaded.query("beta_after_direct_load").len(), 1);

    let _ = std::fs::remove_dir_all(&root);
    Ok(())
}

#[tokio::test]
async fn initial_hot_streaming_snapshot_includes_same_generation_overlay() -> anyhow::Result<()> {
    let root = unique_tmp_dir("v7-hot-base-with-overlay");
    let content_root = root.join("content");
    let state_root = root.join("state");
    std::fs::create_dir_all(&content_root)?;
    std::fs::create_dir_all(&state_root)?;

    let store = Arc::new(SnapshotStore::new(state_root.join("index.db")));
    let idx = Arc::new(TieredIndex::empty(vec![content_root.clone()]));
    let base_path = content_root.join("base_before_publish.txt");
    std::fs::write(&base_path, b"base")?;
    idx.apply_events(&[mk_event(1, EventType::Create, base_path.clone())]);
    idx.refresh_base();

    let overlay_path = content_root.join("overlay_before_publish.txt");
    std::fs::write(&overlay_path, b"overlay")?;
    idx.apply_events(&[mk_event(2, EventType::Create, overlay_path.clone())]);
    idx.snapshot_now(store.clone()).await?;

    let report = idx.memory_report(EventPipelineStats::default()).base;
    assert_eq!(report.hot_memory_entries, 0);
    assert_eq!(report.manifest_only_entries, 2);
    let reloaded = TieredIndex::load_or_empty(&*store, vec![content_root.clone()]).await?;
    assert_eq!(reloaded.query("base_before_publish").len(), 1);
    assert_eq!(reloaded.query("overlay_before_publish").len(), 1);

    let _ = std::fs::remove_dir_all(&root);
    Ok(())
}

#[tokio::test]
async fn rebuild_owned_generation_streams_once_and_restarts_cold() -> anyhow::Result<()> {
    let root = unique_tmp_dir("owned-rebuild-snapshot");
    let content_root = root.join("content");
    let state_root = root.join("state");
    std::fs::create_dir_all(&content_root)?;
    std::fs::create_dir_all(&state_root)?;

    let alpha = content_root.join("owned_rebuild_alpha.txt");
    let beta = content_root.join("owned_rebuild_beta.txt");
    std::fs::write(&alpha, b"alpha")?;
    std::fs::write(&beta, b"beta")?;

    let store = Arc::new(SnapshotStore::new(state_root.join("index.db")));
    let idx = Arc::new(TieredIndex::empty(vec![content_root.clone()]));
    assert!(idx.try_start_rebuild_force());
    let generation = Arc::new(PersistentIndex::new_with_roots(vec![content_root.clone()]));
    generation.apply_events(&[
        mk_event(1, EventType::Create, alpha.clone()),
        mk_event(2, EventType::Create, beta.clone()),
    ]);

    assert!(!idx.finish_rebuild(generation));
    assert!(idx.pending_snapshot_generation.lock().is_some());
    assert_eq!(
        idx.base.load().memory_stats().hot_memory_entries,
        0,
        "rebuild publication must not materialize a hot BaseIndexData"
    );
    let pending_memory = idx.memory_report_light(EventPipelineStats::default());
    assert_eq!(pending_memory.rebuild.owned_snapshot_state, "pending");
    assert_eq!(pending_memory.l2.file_count, 2);
    assert!(pending_memory.l2.estimated_bytes > 0);

    idx.snapshot_now(store.clone()).await?;
    assert!(idx.pending_snapshot_generation.lock().is_none());
    let memory = idx.memory_report(EventPipelineStats::default()).base;
    assert_eq!(memory.hot_memory_entries, 0);
    assert_eq!(memory.manifest_only_entries, 2);

    let restarted = TieredIndex::load_or_empty(store.as_ref(), vec![content_root.clone()]).await?;
    assert_eq!(restarted.query("owned_rebuild_alpha").len(), 1);
    assert_eq!(restarted.query("owned_rebuild_beta").len(), 1);
    assert_eq!(restarted.l2.load().file_count(), 0);

    let _ = std::fs::remove_dir_all(root);
    Ok(())
}

#[tokio::test]
async fn rebuild_replays_many_exact_file_deletes_without_retry() -> anyhow::Result<()> {
    let root = unique_tmp_dir("rebuild-file-delete-churn");
    let content_root = root.join("content");
    let state_root = root.join("state");
    std::fs::create_dir_all(&content_root)?;
    std::fs::create_dir_all(&state_root)?;

    let store = Arc::new(SnapshotStore::new(state_root.join("index.db")));
    let idx = Arc::new(TieredIndex::empty(vec![content_root.clone()]));
    idx.attach_wal(store.as_ref())?;
    assert!(idx.try_start_rebuild_force());
    idx.delta_buffer.lock().begin_full_rebuild_generation();

    let generation = Arc::new(PersistentIndex::new_with_roots(vec![content_root.clone()]));
    let keep = content_root.join("keep_after_file_delete_churn.txt");
    std::fs::write(&keep, b"keep")?;
    let mut scanned_events = vec![mk_event(1, EventType::Create, keep.clone())];
    let mut delete_events = Vec::new();
    for ordinal in 0..64u64 {
        let path = content_root.join(format!("transient-file-{ordinal:03}.tmp"));
        std::fs::write(&path, b"transient")?;
        scanned_events.push(mk_event(ordinal + 2, EventType::Create, path.clone()));
        delete_events.push(mk_event(ordinal + 1000, EventType::Delete, path.clone()));
    }
    generation.apply_events(&scanned_events);
    idx.l2.store(generation.clone());
    for event in &delete_events {
        if let Some(path) = event.best_path() {
            std::fs::remove_file(path)?;
        }
    }
    idx.apply_events(&delete_events);
    assert!(idx.delta_buffer.lock().has_subtree_invalidations());

    assert!(
        !idx.finish_rebuild(generation),
        "exact file deletes must replay into the completed scan instead of restarting it"
    );
    idx.snapshot_now(store.clone()).await?;

    let restarted = TieredIndex::load_or_empty(store.as_ref(), vec![content_root]).await?;
    assert_eq!(restarted.query("keep_after_file_delete_churn").len(), 1);
    assert!(restarted.query("transient-file").is_empty());
    assert_eq!(store.open_wal()?.replay_since_seal(0)?.events_replayed, 0);

    let _ = std::fs::remove_dir_all(root);
    Ok(())
}

#[test]
fn rebuild_retries_directory_rename_when_source_subtree_was_not_scanned() {
    let root = unique_tmp_dir("rebuild-directory-rename-scan-order");
    let source = root.join("source-tree");
    let target_parent = root.join("already-scanned-target");
    let scanned_marker = target_parent.join("scanned-marker.txt");
    let source_child = source.join("nested/missed-child.txt");
    std::fs::create_dir_all(source_child.parent().unwrap()).unwrap();
    std::fs::create_dir_all(&target_parent).unwrap();
    std::fs::write(&source_child, b"must survive rename").unwrap();
    std::fs::write(&scanned_marker, b"scanner passed this parent").unwrap();

    let idx = Arc::new(TieredIndex::empty(vec![root.clone()]));
    assert!(idx.try_start_rebuild_force());
    idx.delta_buffer.lock().begin_full_rebuild_generation();

    // Model the scan order precisely: the destination parent is complete in
    // scratch, while the source subtree has not been visited yet.
    let scratch = Arc::new(PersistentIndex::new_with_roots(vec![root.clone()]));
    scratch.upsert(file_meta_from_path(target_parent.clone()));
    scratch.upsert(file_meta_from_path(scanned_marker));
    idx.l2.store(scratch.clone());

    let moved = target_parent.join("moved-tree");
    std::fs::rename(&source, &moved).unwrap();
    idx.apply_events(&[mk_event(
        1,
        EventType::Rename {
            from: FileIdentifier::Path(source.clone()),
            from_path_hint: Some(source),
        },
        moved,
    )]);

    assert!(
        idx.finish_rebuild(scratch),
        "a directory rename cannot be replayed exactly when its source subtree was not scanned"
    );
    assert!(idx.pending_snapshot_generation.lock().is_none());
    assert!(!idx.rebuild_snapshot_pending.load(Ordering::Acquire));

    let _ = std::fs::remove_dir_all(root);
}

#[tokio::test]
async fn overflow_after_scan_rebuilds_again_before_wal_cleanup() -> anyhow::Result<()> {
    let root = unique_tmp_dir("rebuild-overflow-retry");
    let content_root = root.join("content");
    let state_root = root.join("state");
    std::fs::create_dir_all(&content_root)?;
    std::fs::create_dir_all(&state_root)?;

    let store = Arc::new(SnapshotStore::new(state_root.join("index.db")));
    let idx = Arc::new(TieredIndex::empty(vec![content_root.clone()]));
    idx.attach_wal(store.as_ref())?;

    assert!(idx.try_start_rebuild_force());
    idx.delta_buffer.lock().begin_full_rebuild_generation();
    idx.delta_buffer.lock().set_max_capacity_for_test(1);
    let stale_scan = Arc::new(PersistentIndex::new_with_roots(vec![content_root.clone()]));
    idx.l2.store(stale_scan.clone());

    let late_a = content_root.join("late_after_scan_a.txt");
    let late_b = content_root.join("late_after_scan_b.txt");
    std::fs::write(&late_a, b"a")?;
    std::fs::write(&late_b, b"b")?;
    idx.apply_events(&[
        mk_event(1, EventType::Create, late_a.clone()),
        mk_event(2, EventType::Create, late_b.clone()),
    ]);
    assert!(!idx.delta_buffer.lock().is_complete());

    assert!(
        idx.finish_rebuild(stale_scan),
        "an incomplete replay generation must request another full scan"
    );
    assert!(idx.pending_snapshot_generation.lock().is_none());
    assert_eq!(idx.l2.load().file_count(), 0);
    assert_eq!(store.open_wal()?.replay_since_seal(0)?.events_replayed, 2);

    assert!(idx.try_start_rebuild_force());
    idx.delta_buffer.lock().begin_full_rebuild_generation();
    let complete_scan = Arc::new(PersistentIndex::new_with_roots(vec![content_root.clone()]));
    complete_scan.apply_events(&[
        mk_event(3, EventType::Create, late_a),
        mk_event(4, EventType::Create, late_b),
    ]);
    assert!(!idx.finish_rebuild(complete_scan));
    idx.snapshot_now(store.clone()).await?;

    assert_eq!(store.open_wal()?.replay_since_seal(0)?.events_replayed, 0);
    let restarted = TieredIndex::load_or_empty(store.as_ref(), vec![content_root]).await?;
    assert_eq!(restarted.query("late_after_scan_a").len(), 1);
    assert_eq!(restarted.query("late_after_scan_b").len(), 1);

    let _ = std::fs::remove_dir_all(root);
    Ok(())
}

#[tokio::test]
async fn empty_rebuild_generation_replaces_old_cold_snapshot() -> anyhow::Result<()> {
    let root = unique_tmp_dir("empty-rebuild-checkpoint");
    let content_root = root.join("content");
    let state_root = root.join("state");
    std::fs::create_dir_all(&content_root)?;
    std::fs::create_dir_all(&state_root)?;

    let store = Arc::new(SnapshotStore::new(state_root.join("index.db")));
    let idx = Arc::new(TieredIndex::empty(vec![content_root.clone()]));
    let old_path = content_root.join("must_not_return_after_empty_rebuild.txt");
    std::fs::write(&old_path, b"old")?;
    idx.apply_events(&[mk_event(1, EventType::Create, old_path.clone())]);
    idx.snapshot_now(store.clone()).await?;
    assert_eq!(idx.query("must_not_return_after_empty_rebuild").len(), 1);

    std::fs::remove_file(old_path)?;
    assert!(idx.try_start_rebuild_force());
    idx.delta_buffer.lock().begin_full_rebuild_generation();
    let empty_scan = Arc::new(PersistentIndex::new_with_roots(vec![content_root.clone()]));
    assert!(!idx.finish_rebuild(empty_scan));
    assert!(idx.rebuild_snapshot_pending.load(Ordering::Acquire));
    idx.snapshot_now(store.clone()).await?;

    let restarted = TieredIndex::load_or_empty(store.as_ref(), vec![content_root]).await?;
    assert_eq!(restarted.file_count(), 0);
    assert!(restarted
        .query("must_not_return_after_empty_rebuild")
        .is_empty());

    let _ = std::fs::remove_dir_all(root);
    Ok(())
}

#[test]
fn snapshot_event_gate_covers_wal_append_and_delta_mutation() -> anyhow::Result<()> {
    let root = unique_tmp_dir("snapshot-event-boundary");
    let content_root = root.join("content");
    let state_root = root.join("state");
    std::fs::create_dir_all(&content_root)?;
    std::fs::create_dir_all(&state_root)?;

    let store = SnapshotStore::new(state_root.join("index.db"));
    let idx = Arc::new(TieredIndex::empty(vec![content_root.clone()]));
    idx.attach_wal(&store)?;
    let path = content_root.join("boundary_event.txt");
    std::fs::write(&path, b"boundary")?;

    let boundary = idx.snapshot_event_gate.lock();
    let (started_tx, started_rx) = std::sync::mpsc::channel();
    let (done_tx, done_rx) = std::sync::mpsc::channel();
    let apply_idx = idx.clone();
    let apply_path = path.clone();
    let handle = std::thread::spawn(move || {
        started_tx.send(()).unwrap();
        apply_idx.apply_events(&[mk_event(1, EventType::Create, apply_path)]);
        done_tx.send(()).unwrap();
    });

    started_rx.recv_timeout(std::time::Duration::from_secs(1))?;
    assert!(
        done_rx
            .recv_timeout(std::time::Duration::from_millis(50))
            .is_err(),
        "event application must wait before both WAL append and delta mutation"
    );
    assert!(idx.delta_buffer.lock().is_empty());
    assert_eq!(store.open_wal()?.replay_since_seal(0)?.events_replayed, 0);

    drop(boundary);
    done_rx.recv_timeout(std::time::Duration::from_secs(1))?;
    handle.join().expect("event apply thread");
    assert!(idx
        .delta_buffer
        .lock()
        .is_live(path.as_os_str().as_encoded_bytes()));
    assert_eq!(store.open_wal()?.replay_since_seal(0)?.events_replayed, 1);

    let _ = std::fs::remove_dir_all(&root);
    Ok(())
}

#[tokio::test]
async fn unsupported_directory_rename_retains_delta_and_sealed_wal() -> anyhow::Result<()> {
    let root = unique_tmp_dir("snapshot-directory-rename-fail-closed");
    let content_root = root.join("content");
    let state_root = root.join("state");
    let old_dir = content_root.join("old-tree");
    let old_child = old_dir.join("child.txt");
    std::fs::create_dir_all(&old_dir)?;
    std::fs::create_dir_all(&state_root)?;
    std::fs::write(&old_child, b"child")?;

    let store = Arc::new(SnapshotStore::new(state_root.join("index.db")));
    let idx = Arc::new(TieredIndex::empty(vec![content_root.clone()]));
    idx.attach_wal(store.as_ref())?;
    idx.apply_events(&[
        mk_event(1, EventType::Create, old_dir.clone()),
        mk_event(2, EventType::Create, old_child),
    ]);
    idx.snapshot_now(store.clone()).await?;

    let moved_dir = content_root.join("moved-tree");
    std::fs::rename(&old_dir, &moved_dir)?;
    idx.apply_events(&[mk_event(
        3,
        EventType::Rename {
            from: FileIdentifier::Path(old_dir.clone()),
            from_path_hint: Some(old_dir),
        },
        moved_dir,
    )]);
    idx.begin_shutdown();

    let error = idx.snapshot_now(store.clone()).await.unwrap_err();
    assert!(error
        .to_string()
        .contains("subtree move completeness is unproven"));
    assert_eq!(idx.delta_buffer.lock().len(), 2);
    let replay = store.open_wal()?.replay_since_seal(0)?;
    assert_eq!(replay.events_replayed, 1);
    assert!(matches!(
        replay.events[0].event_type,
        EventType::Rename { .. }
    ));

    let _ = std::fs::remove_dir_all(&root);
    Ok(())
}

#[tokio::test]
async fn snapshot_converges_stale_atomic_save_tmp_after_rename() -> anyhow::Result<()> {
    let root = unique_tmp_dir("snapshot-stale-atomic-save-tmp");
    let content_root = root.join("content");
    let state_root = root.join("state");
    std::fs::create_dir_all(&content_root)?;
    std::fs::create_dir_all(&state_root)?;

    let store = Arc::new(SnapshotStore::new(state_root.join("index.db")));
    let idx = Arc::new(TieredIndex::empty(vec![content_root.clone()]));
    let tmp_path = content_root.join("report.txt.save_0022.tmp");
    let final_path = content_root.join("report.txt");

    std::fs::write(&tmp_path, b"new contents")?;
    idx.apply_events(&[
        mk_event(1, EventType::Create, tmp_path.clone()),
        mk_event(2, EventType::Modify, tmp_path.clone()),
    ]);
    std::fs::rename(&tmp_path, &final_path)?;
    idx.apply_events(&[mk_event(
        3,
        EventType::Rename {
            from: FileIdentifier::Path(tmp_path.clone()),
            from_path_hint: Some(tmp_path.clone()),
        },
        final_path.clone(),
    )]);

    // A queued pre-rename modify can arrive after the rename event. The L2 and
    // filesystem correctly have no tmp path, but last-event-wins leaves a Live
    // delta record for it.
    idx.apply_events(&[mk_event(4, EventType::Modify, tmp_path.clone())]);
    let stale_event = {
        let db = idx.delta_buffer.lock();
        assert!(db.is_live(tmp_path.as_os_str().as_encoded_bytes()));
        let stale_event = db
            .live_records()
            .find(|event| event.best_path() == Some(tmp_path.as_path()))
            .cloned()
            .expect("stale tmp Live record");
        stale_event
    };
    assert!(idx.overlay_meta_for_event(&stale_event).is_none());

    idx.snapshot_now(store.clone()).await?;

    let reloaded = TieredIndex::load_or_empty(&*store, vec![content_root.clone()]).await?;
    assert!(reloaded
        .query("report.txt")
        .iter()
        .any(|meta| meta.path == final_path));
    assert!(reloaded.query("save_0022").is_empty());

    let _ = std::fs::remove_dir_all(&root);
    Ok(())
}

#[tokio::test]
async fn snapshot_converges_stale_child_after_parent_path_disappears() -> anyhow::Result<()> {
    let root = unique_tmp_dir("snapshot-stale-renamed-subdir-child");
    let content_root = root.join("content");
    let state_root = root.join("state");
    let old_dir = content_root.join("old-tree");
    let new_dir = content_root.join("new-tree");
    let old_child = old_dir.join("child.txt");
    let new_child = new_dir.join("child.txt");
    std::fs::create_dir_all(&old_dir)?;
    std::fs::create_dir_all(&state_root)?;
    std::fs::write(&old_child, b"child")?;

    let store = Arc::new(SnapshotStore::new(state_root.join("index.db")));
    let idx = Arc::new(TieredIndex::empty(vec![content_root.clone()]));
    idx.apply_events(&[mk_event(1, EventType::Create, old_child.clone())]);
    idx.snapshot_now(store.clone()).await?;

    std::fs::rename(&old_dir, &new_dir)?;
    idx.apply_events(&[
        mk_event(2, EventType::Delete, old_dir.clone()),
        mk_event(3, EventType::Create, new_child.clone()),
        mk_event(4, EventType::Modify, old_child.clone()),
    ]);
    let stale_event = {
        let db = idx.delta_buffer.lock();
        assert!(db.is_live(old_child.as_os_str().as_encoded_bytes()));
        let stale_event = db
            .live_records()
            .find(|event| event.best_path() == Some(old_child.as_path()))
            .cloned()
            .expect("stale child Live record");
        stale_event
    };
    assert!(idx.overlay_meta_for_event(&stale_event).is_none());

    idx.snapshot_now(store.clone()).await?;

    let reloaded = TieredIndex::load_or_empty(&*store, vec![content_root.clone()]).await?;
    assert!(reloaded
        .query("new-tree")
        .iter()
        .any(|meta| meta.path == new_child));
    assert!(reloaded.query("old-tree").is_empty());

    let _ = std::fs::remove_dir_all(&root);
    Ok(())
}

#[tokio::test]
async fn shutdown_snapshot_unexplained_upsert_fails_closed_without_rebuild() -> anyhow::Result<()> {
    use crate::storage::snapshot::read_recovery_runtime_state;

    let root = unique_tmp_dir("shutdown-unresolved-upsert");
    let content_root = root.join("content");
    let state_root = root.join("state");
    std::fs::create_dir_all(&content_root)?;
    std::fs::create_dir_all(&state_root)?;

    let store = Arc::new(SnapshotStore::new(state_root.join("index.db")));
    let idx = Arc::new(TieredIndex::empty(vec![content_root.clone()]));
    idx.attach_wal(store.as_ref())?;
    let unexplained = content_root.join("unexplained-missing.txt");
    std::fs::write(&unexplained, b"transient")?;
    idx.apply_events(&[mk_event(1, EventType::Create, unexplained.clone())]);
    std::fs::remove_file(&unexplained)?;

    idx.begin_shutdown();
    let error = idx.snapshot_now(store.clone()).await.unwrap_err();
    assert!(error.to_string().contains("snapshot_upsert_unresolved"));
    assert!(idx
        .delta_buffer
        .lock()
        .is_live(unexplained.as_os_str().as_encoded_bytes()));
    {
        let rebuild = idx.rebuild_state.lock();
        assert!(!rebuild.in_progress);
        assert!(!rebuild.requested);
        assert!(!rebuild.scheduled);
        assert!(rebuild.last_started_at.is_none());
    }
    assert!(!idx.rebuild_snapshot_pending.load(Ordering::Acquire));
    assert_eq!(store.open_wal()?.replay_since_seal(0)?.events_replayed, 1);
    assert!(!read_recovery_runtime_state(store.path())?.last_clean_shutdown);

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
    assert_eq!(after_snapshot_report.cold_filter_bytes, 0);
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
    assert_eq!(cold_report.cold_filter_bytes, 0);
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
    assert_eq!(after_query.cold_filter_bytes, 0);

    let _ = std::fs::remove_dir_all(&root);
    Ok(())
}

#[tokio::test]
async fn stable_prev_used_when_stable_is_suspiciously_smaller() -> anyhow::Result<()> {
    let root = unique_tmp_dir("stable-prev-smaller");
    let content_root = root.join("content");
    let state_root = root.join("state");
    std::fs::create_dir_all(&content_root)?;
    std::fs::create_dir_all(&state_root)?;

    let store = SnapshotStore::new(state_root.join("index.db"));
    std::fs::create_dir_all(stable_v7_path_for(store.path()).parent().unwrap())?;

    let large =
        crate::index::l2_partition::PersistentIndex::new_with_roots(vec![content_root.clone()]);
    for i in 0..12_000u64 {
        let path = content_root.join(format!("bulk_{i:05}.txt"));
        large.upsert_path_alias(FileMeta {
            file_key: FileKey {
                dev: 77,
                ino: i + 1,
                generation: 0,
            },
            path,
            size: 1,
            mtime: None,
            ctime: None,
            atime: None,
            kind: FileKind::File,
        });
    }
    let chronicle = content_root.join("fd-rdd-编年史.md");
    large.upsert_path_alias(FileMeta {
        file_key: FileKey {
            dev: 77,
            ino: 20_001,
            generation: 0,
        },
        path: chronicle.clone(),
        size: 1,
        mtime: None,
        ctime: None,
        atime: None,
        kind: FileKind::File,
    });
    write_v7_snapshot_atomic(
        &stable_prev_v7_path_for(store.path()),
        &large.to_base_index_data(),
    )?;

    let small =
        crate::index::l2_partition::PersistentIndex::new_with_roots(vec![content_root.clone()]);
    for i in 0..100u64 {
        small.upsert_path_alias(FileMeta {
            file_key: FileKey {
                dev: 88,
                ino: i + 1,
                generation: 0,
            },
            path: content_root.join(format!("small_{i:03}.txt")),
            size: 1,
            mtime: None,
            ctime: None,
            atime: None,
            kind: FileKind::File,
        });
    }
    write_v7_snapshot_atomic(
        &stable_v7_path_for(store.path()),
        &small.to_base_index_data(),
    )?;
    std::fs::write(&chronicle, b"chronicle")?;

    let loaded = TieredIndex::load_or_empty(&store, vec![content_root.clone()]).await?;
    assert_eq!(
        loaded.recovery_status().report.snapshot_source,
        "stable-prev"
    );
    assert_eq!(loaded.file_count(), 12_001);
    let results = loaded.query_limit_detailed("编年史", 10);
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].meta.path, chronicle);

    let _ = std::fs::remove_dir_all(&root);
    Ok(())
}

#[tokio::test]
async fn tiny_stable_without_large_prev_requires_rebuild_after_root_probe() -> anyhow::Result<()> {
    let root = unique_tmp_dir("tiny-stable-root-probe");
    let content_root = root.join("content");
    let state_root = root.join("state");
    std::fs::create_dir_all(&content_root)?;
    std::fs::create_dir_all(&state_root)?;

    for i in 0..10_150u64 {
        std::fs::write(content_root.join(format!("actual_{i:05}.txt")), b"x")?;
    }

    let store = SnapshotStore::new(state_root.join("index.db"));
    std::fs::create_dir_all(stable_v7_path_for(store.path()).parent().unwrap())?;

    let small =
        crate::index::l2_partition::PersistentIndex::new_with_roots(vec![content_root.clone()]);
    for i in 0..100u64 {
        small.upsert_path_alias(FileMeta {
            file_key: FileKey {
                dev: 88,
                ino: i + 1,
                generation: 0,
            },
            path: content_root.join(format!("small_{i:03}.txt")),
            size: 1,
            mtime: None,
            ctime: None,
            atime: None,
            kind: FileKind::File,
        });
    }
    let small_base = small.to_base_index_data();
    write_v7_snapshot_atomic(&stable_prev_v7_path_for(store.path()), &small_base)?;
    write_v7_snapshot_atomic(&stable_v7_path_for(store.path()), &small_base)?;

    let loaded = TieredIndex::load_or_empty(&store, vec![content_root.clone()]).await?;
    let report = loaded.recovery_status().report;
    assert_eq!(report.snapshot_source, "stable");
    assert!(report.requires_rebuild);
    assert!(report.hard_rebuild_needed);
    assert!(report.startup_scan_required);
    assert!(report
        .reasons
        .iter()
        .any(|reason| reason == "snapshot_too_small_for_roots"));

    let _ = std::fs::remove_dir_all(&root);
    Ok(())
}

#[tokio::test]
async fn subtree_delete_persists_cold_child_tombstones_across_snapshot() -> anyhow::Result<()> {
    let root = unique_tmp_dir("cold-parent-delete");
    let content_root = root.join("content");
    let state_root = root.join("state");
    let parent = content_root.join("Downloads");
    std::fs::create_dir_all(&parent)?;
    std::fs::create_dir_all(&state_root)?;

    let store = Arc::new(SnapshotStore::new(state_root.join("index.db")));
    let idx = Arc::new(TieredIndex::empty(vec![content_root.clone()]));
    let chronicle = parent.join("fd-rdd-编年史.md");
    std::fs::write(&chronicle, b"chronicle")?;
    idx.apply_events(&[mk_event(1, EventType::Create, chronicle.clone())]);
    idx.snapshot_now(store.clone()).await?;

    let loaded = Arc::new(TieredIndex::load_or_empty(&*store, vec![content_root.clone()]).await?);
    assert_eq!(
        loaded
            .memory_report(EventPipelineStats::default())
            .base
            .manifest_only_entries,
        1
    );
    assert_eq!(loaded.query_limit_detailed("编年史", 10).len(), 1);

    loaded.apply_events(&[mk_event(2, EventType::Delete, parent.clone())]);
    assert_eq!(
        loaded.query_limit_detailed("编年史", 10).len(),
        0,
        "runtime subtree tombstone should hide cold children before per-path stat"
    );
    assert_eq!(loaded.stats_report().cold_validate_count, 1);
    loaded.snapshot_now(store.clone()).await?;

    let reloaded = TieredIndex::load_or_empty(&*store, vec![content_root.clone()]).await?;
    let results = reloaded.query_limit_detailed("编年史", 10);
    assert!(
        results.is_empty(),
        "directory delete must not resurrect cold descendants after restart"
    );

    let _ = std::fs::remove_dir_all(&root);
    Ok(())
}

#[tokio::test]
async fn directory_delete_recreate_rebuilds_without_resurrecting_old_children() -> anyhow::Result<()>
{
    let root = unique_tmp_dir("cold-directory-recreate");
    let content_root = root.join("content");
    let state_root = root.join("state");
    let tree = content_root.join("tree");
    let old_child = tree.join("old-child.txt");
    std::fs::create_dir_all(&tree)?;
    std::fs::create_dir_all(&state_root)?;
    std::fs::write(&old_child, b"old")?;

    let store = Arc::new(SnapshotStore::new(state_root.join("index.db")));
    let idx = Arc::new(TieredIndex::empty(vec![content_root.clone()]));
    idx.rebuild_cooldown_secs.store(1, Ordering::Relaxed);
    idx.apply_events(&[
        mk_event(1, EventType::Create, tree.clone()),
        mk_event(2, EventType::Create, old_child),
    ]);
    idx.snapshot_now(store.clone()).await?;

    std::fs::remove_dir_all(&tree)?;
    idx.apply_events(&[mk_event(3, EventType::Delete, tree.clone())]);
    std::fs::create_dir_all(&tree)?;
    idx.apply_events(&[mk_event(4, EventType::Create, tree.clone())]);

    let error = idx.snapshot_now(store.clone()).await.unwrap_err();
    assert!(error
        .to_string()
        .contains("subtree move completeness is unproven"));
    for _ in 0..200 {
        if !idx.rebuild_in_progress() && idx.rebuild_snapshot_pending.load(Ordering::Acquire) {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    }
    assert!(!idx.rebuild_in_progress());
    assert!(idx.rebuild_snapshot_pending.load(Ordering::Acquire));
    assert!(
        idx.query("old-child").is_empty(),
        "the old cold child must stay hidden while the rebuilt generation is pending durability"
    );
    idx.snapshot_now(store.clone()).await?;

    let reloaded = TieredIndex::load_or_empty(&*store, vec![content_root.clone()]).await?;
    assert!(reloaded.query("old-child").is_empty());

    let _ = std::fs::remove_dir_all(&root);
    Ok(())
}

#[tokio::test]
async fn quarantine_sidecar_failure_blocks_wal_cleanup_until_retry() -> anyhow::Result<()> {
    let root = unique_tmp_dir("snapshot-quarantine-checkpoint");
    let content_root = root.join("content");
    let state_root = root.join("state");
    let frozen_root = content_root.join("offline-root");
    std::fs::create_dir_all(&content_root)?;
    std::fs::create_dir_all(&state_root)?;

    let store = Arc::new(SnapshotStore::new(state_root.join("index.db")));
    let idx = Arc::new(TieredIndex::empty(vec![content_root.clone()]));
    idx.attach_wal(store.as_ref())?;
    let initial = content_root.join("initial.txt");
    std::fs::write(&initial, b"initial")?;
    idx.apply_events(&[mk_event(1, EventType::Create, initial)]);
    idx.snapshot_now(store.clone()).await?;

    idx.apply_root_state_record(RootStateRecord::offline(
        2,
        frozen_root.clone(),
        test_mount_identity(),
        vec![frozen_root.clone()],
        Some("test offline".to_string()),
    ));
    let trigger = content_root.join("checkpoint-trigger.txt");
    std::fs::write(&trigger, b"trigger")?;
    idx.apply_events(&[mk_event(3, EventType::Create, trigger)]);

    let sidecar_path = quarantine_sidecar_path_for(store.path());
    std::fs::remove_file(&sidecar_path)?;
    std::fs::create_dir(&sidecar_path)?;
    idx.begin_shutdown();
    let error = idx.snapshot_now(store.clone()).await.unwrap_err();
    assert!(error.to_string().contains("sealed WAL retained"));
    let replay = store.open_wal()?.replay_since_seal(0)?;
    assert_eq!(replay.events_replayed, 1);
    assert_eq!(replay.root_events_replayed, 1);
    assert_eq!(idx.delta_buffer.lock().len(), 1);

    std::fs::remove_dir(&sidecar_path)?;
    idx.snapshot_now(store.clone()).await?;
    assert_eq!(store.open_wal()?.replay_since_seal(0)?.events_replayed, 0);
    let reloaded = TieredIndex::load_or_empty(&*store, vec![content_root.clone()]).await?;
    assert!(reloaded.path_is_frozen(&frozen_root));

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
fn query_verify_budget_caps_wide_stale_candidate_stat_count() {
    let root = unique_tmp_dir("query-verify-budget");
    std::fs::create_dir_all(&root).unwrap();

    let idx = TieredIndex::empty(vec![root.clone()]);
    let mut events = Vec::new();
    let mut paths = Vec::new();
    for i in 0..10 {
        let path = root.join(format!("query_verify_budget_{i}.txt"));
        std::fs::write(&path, b"stale").unwrap();
        events.push(mk_event(i as u64 + 1, EventType::Create, path.clone()));
        paths.push(path);
    }
    idx.apply_events(&events);
    idx.refresh_base();
    idx.apply_query_config(QueryConfig {
        max_verify_per_query: 3,
        verify_timeout_ms: 1_000,
    });

    for path in &paths {
        std::fs::remove_file(path).unwrap();
    }

    let results = idx.query_limit_detailed("query_verify_budget", 10);
    assert!(results.is_empty());
    assert_eq!(idx.stats_report().cold_validate_count, 3);
    assert_eq!(idx.stats_report().query_stale_hit_count, 3);

    let _ = std::fs::remove_dir_all(&root);
}

#[test]
fn subtree_tombstone_filters_deleted_parent_before_query_verify() {
    let root = unique_tmp_dir("subtree-tombstone-filter");
    let deleted_parent = root.join("node_modules");
    std::fs::create_dir_all(&deleted_parent).unwrap();

    let idx = TieredIndex::empty(vec![root.clone()]);
    let mut events = Vec::new();
    for i in 0..20 {
        let path = deleted_parent.join(format!("subtree_tombstone_pkg_{i}.js"));
        std::fs::write(&path, b"module").unwrap();
        events.push(mk_event(i as u64 + 1, EventType::Create, path));
    }
    idx.apply_events(&events);
    idx.refresh_base();

    std::fs::remove_dir_all(&deleted_parent).unwrap();
    idx.apply_events(&[mk_event(100, EventType::Delete, deleted_parent.clone())]);

    let results = idx.query_limit_detailed("subtree_tombstone_pkg", 20);
    assert!(results.is_empty());
    assert_eq!(idx.stats_report().cold_validate_count, 0);
    assert_eq!(idx.stats_report().query_stale_hit_count, 20);
    assert_eq!(idx.runtime_subtree_tombstone_count(), 1);

    let _ = std::fs::remove_dir_all(&root);
}

#[test]
fn subtree_tombstone_ttl_cleanup_allows_runtime_filter_to_expire() {
    let root = unique_tmp_dir("subtree-tombstone-ttl");
    let deleted_parent = root.join("cache");
    std::fs::create_dir_all(&deleted_parent).unwrap();

    let idx = TieredIndex::empty(vec![root.clone()]);
    let path = deleted_parent.join("subtree_tombstone_ttl.txt");
    std::fs::write(&path, b"cache").unwrap();
    idx.apply_events(&[mk_event(1, EventType::Create, path.clone())]);
    idx.refresh_base();

    std::fs::remove_dir_all(&deleted_parent).unwrap();
    idx.apply_events(&[mk_event(2, EventType::Delete, deleted_parent.clone())]);
    assert_eq!(idx.runtime_subtree_tombstone_count(), 1);

    idx.force_expire_runtime_subtree_tombstones();
    assert_eq!(idx.runtime_subtree_tombstone_count(), 0);

    let results = idx.query_limit_detailed("subtree_tombstone_ttl", 10);
    assert!(results.is_empty());
    assert_eq!(idx.stats_report().cold_validate_count, 1);

    let _ = std::fs::remove_dir_all(&root);
}

#[test]
fn subtree_tombstone_keeps_prefix_but_allows_recreated_overlay() {
    let root = unique_tmp_dir("subtree-tombstone-recreate");
    let project = root.join("project");
    std::fs::create_dir_all(&project).unwrap();

    let idx = TieredIndex::empty(vec![root.clone()]);
    let old_path = project.join("subtree_tombstone_recreated.txt");
    std::fs::write(&old_path, b"old").unwrap();
    idx.apply_events(&[mk_event(1, EventType::Create, old_path.clone())]);
    idx.refresh_base();

    std::fs::remove_dir_all(&project).unwrap();
    idx.apply_events(&[mk_event(2, EventType::Delete, project.clone())]);
    assert!(idx
        .query_limit_detailed("subtree_tombstone_recreated", 10)
        .is_empty());

    std::fs::create_dir_all(&project).unwrap();
    let new_path = project.join("subtree_tombstone_recreated.txt");
    std::fs::write(&new_path, b"new").unwrap();
    idx.apply_events(&[mk_event(3, EventType::Create, new_path.clone())]);

    let results = idx.query_limit_detailed("subtree_tombstone_recreated", 10);
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].meta.path, new_path);
    assert_eq!(results[0].freshness, QueryResultFreshness::Fresh);
    assert_eq!(
        idx.runtime_subtree_tombstone_count(),
        1,
        "the prefix must stay invalidated until a complete generation is published"
    );

    let _ = std::fs::remove_dir_all(&root);
}

#[test]
fn query_verify_default_disables_lazy_validation_correctness_path() {
    let root = unique_tmp_dir("query-verify-default");
    std::fs::create_dir_all(&root).unwrap();

    let path = root.join("query_verify_default.txt");
    std::fs::write(&path, b"stable").unwrap();

    let idx = TieredIndex::empty(vec![root.clone()]);
    idx.apply_events(&[mk_event(1, EventType::Create, path.clone())]);
    idx.refresh_base();

    let results = idx.query_limit_detailed("query_verify_default", 10);
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].freshness, QueryResultFreshness::StaleChecked);
    assert!(results[0].validated);
    assert_eq!(idx.lazy_validation_report().pending, 0);
    assert_eq!(idx.stats_report().cold_validate_count, 1);

    let _ = std::fs::remove_dir_all(&root);
}

#[tokio::test]
async fn lazy_validation_query_enqueues_without_sync_stat_and_worker_repairs() {
    let root = unique_tmp_dir("lazy-validation-changed");
    std::fs::create_dir_all(&root).unwrap();

    let path = root.join("lazy_cold_hit.txt");
    std::fs::write(&path, b"old").unwrap();

    let idx = Arc::new(TieredIndex::empty(vec![root.clone()]));
    idx.apply_events(&[mk_event(1, EventType::Create, path.clone())]);
    idx.refresh_base();
    idx.apply_lazy_validation_config(true, 128, 10, 1_000);

    std::thread::sleep(std::time::Duration::from_millis(25));
    std::fs::write(&path, b"new-content").unwrap();

    let results = idx.query_limit_detailed("lazy_cold_hit", 10);
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].freshness, QueryResultFreshness::Unknown);
    assert!(!results[0].validated);
    assert_eq!(idx.stats_report().cold_validate_count, 0);
    assert_eq!(idx.lazy_validation_report().pending, 1);

    idx.spawn_lazy_validation_worker();
    for _ in 0..50 {
        if idx.lazy_validation_report().completed >= 1 {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    }

    assert_eq!(idx.lazy_validation_report().completed, 1);
    assert_eq!(idx.stats_report().cold_validate_count, 1);
    assert_eq!(idx.stats_report().query_stale_hit_count, 1);
    assert_eq!(idx.deferred_repair_queue_len(), 1);

    let hot_results = idx.query_limit_detailed("lazy_cold_hit", 10);
    assert_eq!(hot_results.len(), 1);
    assert_eq!(hot_results[0].freshness, QueryResultFreshness::Fresh);

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

#[test]
fn acceptance_project_marker_scan_makes_new_file_searchable() {
    let root = unique_tmp_dir("acceptance-project-marker-search");
    let project = root.join("new-app");
    let src = project.join("src");
    std::fs::create_dir_all(&src).unwrap();
    std::fs::write(
        project.join("Cargo.toml"),
        b"[package]\nname = \"acceptance-new-app\"\n",
    )
    .unwrap();

    let idx = TieredIndex::empty(vec![root.clone()]);
    idx.enqueue_dirty_dirs(vec![project.clone()], DirtyReason::QueryMiss);

    let entry = idx.dirty_queue.lock().pop_ready(u64::MAX, 1).pop().unwrap();
    let markers = vec!["Cargo.toml".to_string(), ".git".to_string()];
    let report = idx.process_dirty_entry_with_project_markers(entry, &[], &markers);

    assert!(!report.failed);
    assert_eq!(report.outcomes.len(), 1);
    assert_eq!(
        report.outcomes[0].outcome.project_roots,
        vec![project.clone()]
    );

    let main_rs = src.join("main.rs");
    std::fs::write(
        &main_rs,
        b"fn main() { println!(\"acceptance_main_probe\"); }\n",
    )
    .unwrap();
    idx.enqueue_dirty_dirs(vec![src], DirtyReason::InotifyEvent);

    let entry = idx.dirty_queue.lock().pop_ready(u64::MAX, 1).pop().unwrap();
    let report = idx.process_dirty_entry_with_project_markers(entry, &[], &markers);

    assert!(!report.failed);
    assert!(report.changed >= 1);
    let results = idx.query_limit_detailed("main", 10);
    assert!(
        results.iter().any(|result| result.meta.path == main_rs),
        "new project file should be searchable immediately after dirty scan: {results:?}"
    );

    let _ = std::fs::remove_dir_all(&root);
}

#[test]
fn fast_scan_changed_dir_reuses_dirty_apply_and_finds_deep_known_dir_create() {
    let root = unique_tmp_dir("fast-scan-dirty-apply");
    let deep = root.join("projects").join("app").join("src");
    std::fs::create_dir_all(&deep).unwrap();
    let existing = deep.join("existing.rs");
    std::fs::write(&existing, b"fn existing_probe() {}\n").unwrap();

    let idx = TieredIndex::empty(vec![root.clone()]);
    idx.apply_events(&[mk_event(1, EventType::Create, existing.clone())]);

    let known_dirs = idx.collect_fast_scan_known_dirs(128);
    assert!(
        known_dirs.iter().any(|dir| dir == &deep),
        "deep known directory should be registered for fast scan: {known_dirs:?}"
    );

    let rt = TieredWatchRuntime::new(Vec::new(), vec![(root.clone(), 1)], 16, 5_000, 20);
    let mount_table = MountTable::parse(&format!(
        "1 0 0:1 / {} rw - ext4 source rw\n",
        root.display()
    ));
    assert!(rt.grant_fast_scan_lease(deep.clone(), FastScanLeaseKind::Query, None, 1));
    assert_eq!(
        rt.bootstrap_fast_scan_dirs(known_dirs, &mount_table, 128),
        1
    );

    let cfg = FastScanTickConfig {
        target_secs: 0,
        local_stat_budget_per_tick: 128,
        local_readdir_budget_per_tick: 128,
        ..FastScanTickConfig::default()
    };
    let initial = rt.fast_scan_tick(&mount_table, cfg);
    assert!(
        initial.initial_dirs.iter().any(|dir| dir == &deep),
        "fast scan should backfill the leased deep directory first: {initial:?}"
    );

    std::thread::sleep(std::time::Duration::from_millis(5));
    let created = deep.join("fast_created.rs");
    std::fs::write(&created, b"fn fast_created_probe() {}\n").unwrap();

    let tick = rt.fast_scan_tick(&mount_table, cfg);
    assert!(
        tick.changed_dirs.iter().any(|dir| dir == &deep),
        "fast scan should emit the changed deep directory: {tick:?}"
    );

    idx.enqueue_dirty_dirs(tick.changed_dirs, DirtyReason::FastScanChangedDir);
    let entry = idx.dirty_queue.lock().pop_ready(u64::MAX, 1).pop().unwrap();
    let report = idx.process_dirty_entry(entry, &[]);

    rt.record_fast_scan_generated_events(report.changed);
    assert!(!report.failed);
    assert_eq!(report.changed, 1);
    assert!(idx
        .query_limit_detailed("fast_created", 10)
        .iter()
        .any(|result| result.meta.path == created));
    assert_eq!(rt.report().fast_scan_generated_events, 1);

    let _ = std::fs::remove_dir_all(&root);
}

#[test]
fn fast_scan_known_dir_collection_excludes_l0_covered_roots() {
    let root = unique_tmp_dir("fast-scan-exclude-l0");
    let hot_deep = root.join("hot").join("app").join("src");
    let cold_deep = root.join("cold").join("app").join("src");
    std::fs::create_dir_all(&hot_deep).unwrap();
    std::fs::create_dir_all(&cold_deep).unwrap();
    let hot_file = hot_deep.join("hot_probe.rs");
    let cold_file = cold_deep.join("cold_probe.rs");
    std::fs::write(&hot_file, b"fn hot_probe() {}\n").unwrap();
    std::fs::write(&cold_file, b"fn cold_probe() {}\n").unwrap();

    let idx = TieredIndex::empty(vec![root.clone()]);
    idx.apply_events(&[
        mk_event(1, EventType::Create, hot_file),
        mk_event(2, EventType::Create, cold_file),
    ]);

    let excluded = vec![root.join("hot")];
    let known_dirs = idx.collect_fast_scan_known_dirs_excluding(128, &excluded);
    assert!(
        known_dirs.iter().any(|dir| dir == &cold_deep),
        "cold deep directory should remain eligible for fast scan: {known_dirs:?}"
    );
    assert!(
        !known_dirs
            .iter()
            .any(|dir| dir.starts_with(excluded[0].as_path())),
        "L0-covered hot directory should not be bootstrapped into fast scan: {known_dirs:?}"
    );

    let _ = std::fs::remove_dir_all(&root);
}

#[test]
fn acceptance_large_excluded_project_tree_does_not_become_marker_candidate() {
    let root = unique_tmp_dir("acceptance-excluded-marker");
    let project = root.join("workspace").join("node_modules").join("pkg");
    std::fs::create_dir_all(&project).unwrap();
    std::fs::write(project.join("package.json"), b"{\"name\":\"pkg\"}\n").unwrap();

    let idx = TieredIndex::empty_with_options_follow_and_excludes(
        vec![root.clone()],
        false,
        true,
        false,
        vec!["node_modules".to_string()],
    );
    idx.enqueue_dirty_dirs(vec![root.join("workspace")], DirtyReason::QueryMiss);

    let entry = idx.dirty_queue.lock().pop_ready(u64::MAX, 1).pop().unwrap();
    let markers = vec!["package.json".to_string()];
    let report = idx.process_dirty_entry_with_project_markers(entry, &[], &markers);

    assert!(!report.failed);
    assert!(
        report
            .outcomes
            .iter()
            .all(|scan| scan.outcome.project_roots.is_empty()),
        "excluded node_modules project marker must not become a project candidate: {report:?}"
    );

    let _ = std::fs::remove_dir_all(&root);
}

#[test]
fn periodic_cold_scan_skips_unchanged_directory_manifest() {
    let root = unique_tmp_dir("manifest-skip");
    std::fs::create_dir_all(&root).unwrap();
    std::fs::write(root.join("stable.txt"), b"stable").unwrap();

    let idx = TieredIndex::empty(vec![root.clone()]);
    let first = idx.scan_dirs_immediate_outcome(std::slice::from_ref(&root));
    assert_eq!(first.scanned, 1);
    assert_eq!(idx.directory_manifest_report().dirs, 1);

    idx.enqueue_dirty_dirs(vec![root.clone()], DirtyReason::PeriodicColdScan);
    let entry = idx.dirty_queue.lock().pop_ready(u64::MAX, 1).pop().unwrap();
    let manifest_skip_dirs = std::collections::HashSet::from([root.clone()]);
    let report = idx.process_dirty_entry_with_project_markers_and_manifest_skip_dirs(
        entry,
        &[],
        &[],
        &manifest_skip_dirs,
    );

    assert!(!report.failed);
    assert_eq!(report.dirs_scanned, 1);
    assert_eq!(report.outcomes.len(), 1);
    assert!(report.outcomes[0].manifest_skipped);
    assert_eq!(report.outcomes[0].outcome.scanned, 0);
    assert_eq!(report.outcomes[0].outcome.changed, 0);

    let manifest = idx.directory_manifest_report();
    // Phase 3：skip 现在由段 1.5（unbounded summary 二次确认）拦截，而非段 2。
    // immediate scan 存储了 WalkBuilder summary 但未记录 dir_mtime_ns，
    // 因此段 1 返回 Some(false)，段 1.5 匹配后跳过，段 2 的 skipped_scans 不递增。
    assert_eq!(manifest.unbounded_summary_hits, 1);
    assert_eq!(manifest.skipped_scans, 0);
    assert_eq!(manifest.changed_scans, 0);

    let _ = std::fs::remove_dir_all(&root);
}

#[test]
fn periodic_cold_scan_detects_directory_manifest_hash_change() {
    let root = unique_tmp_dir("manifest-hash-change");
    std::fs::create_dir_all(&root).unwrap();
    std::fs::write(root.join("stable.txt"), b"stable").unwrap();

    let idx = TieredIndex::empty(vec![root.clone()]);
    let first = idx.scan_dirs_immediate_outcome(std::slice::from_ref(&root));
    assert_eq!(first.scanned, 1);

    std::fs::write(root.join("new_child.txt"), b"new").unwrap();
    idx.enqueue_dirty_dirs(vec![root.clone()], DirtyReason::PeriodicColdScan);
    let entry = idx.dirty_queue.lock().pop_ready(u64::MAX, 1).pop().unwrap();
    let manifest_skip_dirs = std::collections::HashSet::from([root.clone()]);
    let report = idx.process_dirty_entry_with_project_markers_and_manifest_skip_dirs(
        entry,
        &[],
        &[],
        &manifest_skip_dirs,
    );

    assert!(!report.failed);
    assert_eq!(report.outcomes.len(), 1);
    assert!(!report.outcomes[0].manifest_skipped);
    assert_eq!(report.outcomes[0].outcome.scanned, 2);
    assert_eq!(report.outcomes[0].outcome.changed, 1);

    let manifest = idx.directory_manifest_report();
    assert_eq!(manifest.skipped_scans, 0);
    assert_eq!(manifest.changed_scans, 1);

    let _ = std::fs::remove_dir_all(&root);
}

#[test]
fn periodic_cold_scan_bypasses_manifest_when_clock_cutoff_untrusted() {
    let root = unique_tmp_dir("manifest-clock-bypass");
    std::fs::create_dir_all(&root).unwrap();
    std::fs::write(root.join("stable.txt"), b"stable").unwrap();

    let idx = TieredIndex::empty(vec![root.clone()]);
    let first = idx.scan_dirs_immediate_outcome(std::slice::from_ref(&root));
    assert_eq!(first.scanned, 1);

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

    idx.enqueue_dirty_dirs(vec![root.clone()], DirtyReason::PeriodicColdScan);
    let entry = idx.dirty_queue.lock().pop_ready(u64::MAX, 1).pop().unwrap();
    let manifest_skip_dirs = std::collections::HashSet::from([root.clone()]);
    let report = idx.process_dirty_entry_with_project_markers_and_manifest_skip_dirs(
        entry,
        &[],
        &[],
        &manifest_skip_dirs,
    );

    assert!(!report.failed);
    assert_eq!(report.outcomes.len(), 1);
    assert!(!report.outcomes[0].manifest_skipped);
    assert_eq!(report.outcomes[0].outcome.scanned, 1);

    let manifest = idx.directory_manifest_report();
    assert_eq!(manifest.skipped_scans, 0);
    assert_eq!(manifest.untrusted_clock_bypass, 1);

    let _ = std::fs::remove_dir_all(&root);
}

#[test]
fn materialize_self_heals_when_cold_base_resolves_nothing() {
    use crate::index::base_index::BaseIndexData;
    use crate::index::file_entry_v2::{FileEntry, FileEntryIndex};
    use crate::index::path_table_v2::PathTableBuilder;

    let root = unique_tmp_dir("materialize-selfheal");
    std::fs::create_dir_all(&root).unwrap();

    // A base reporting many entries whose path_idx resolves to nothing: the
    // exact shape of a misread cold base (file_count large, for_each ~0).
    let mut entries = FileEntryIndex::new();
    for ino in 0..20_000u64 {
        entries.push(FileEntry::from_file_key(
            FileKey {
                dev: 1,
                ino,
                generation: 0,
            },
            999_999, // outside the (empty) path table
            100,
        ));
    }
    let corrupt_base = BaseIndexData {
        path_table: PathTableBuilder::new().build(),
        entries_by_key: entries.build(),
        ..BaseIndexData::default()
    };
    assert_eq!(corrupt_base.file_count(), 20_000);

    let l1 = L1Cache::with_capacity(16);
    let l2 = Arc::new(PersistentIndex::new_with_roots(vec![root.clone()]));
    let l3 = IndexBuilder::new(vec![root.clone()]);
    let governor = Arc::new(IoGovernor::new(false, 1_000_000));
    let idx = Arc::new(TieredIndex::new_with_base_and_io_governor(
        l1,
        l2,
        l3,
        vec![root.clone()],
        false,
        true,
        false,
        Vec::new(),
        Some(corrupt_base),
        governor,
    ));
    let overlay_path = root.join("overlay-must-survive.txt");
    std::fs::write(&overlay_path, b"overlay").unwrap();
    idx.apply_events(&[mk_event(1, EventType::Create, overlay_path)]);
    assert_eq!(idx.delta_buffer.lock().len(), 1);

    // Materialize must refuse to collapse the base into the tiny resolvable set,
    // instead reporting corruption (which also triggers a full rebuild).
    let err = idx
        .materialize_snapshot_base()
        .expect_err("corrupt cold base must abort materialization");
    let msg = err.to_string();
    assert!(
        msg.contains("corruption suspected"),
        "unexpected error: {msg}"
    );
    assert_eq!(
        idx.delta_buffer.lock().len(),
        1,
        "failed materialization must preserve the delta buffer for retry/rebuild"
    );

    let _ = std::fs::remove_dir_all(&root);
}

#[test]
fn materialize_snapshot_overlay_wins_for_the_same_path() {
    let root = unique_tmp_dir("materialize-overlay-wins");
    std::fs::create_dir_all(&root).unwrap();
    let path = root.join("same-path.txt");
    std::fs::write(&path, b"new").unwrap();

    let idx = Arc::new(TieredIndex::empty(vec![root.clone()]));
    idx.l2.load_full().upsert_path_alias(FileMeta {
        file_key: FileKey {
            dev: 99,
            ino: 123,
            generation: 0,
        },
        path: path.clone(),
        size: 0,
        mtime: None,
        ctime: None,
        atime: None,
        kind: FileKind::File,
    });
    idx.refresh_base();

    let expected_key = file_meta_from_path(path.clone()).file_key;
    idx.apply_events(&[mk_event(1, EventType::Create, path.clone())]);

    let materialized = idx.materialize_snapshot_base().unwrap();
    let matches =
        materialized.query_metas(crate::query::matcher::create_matcher("same-path", true).as_ref());
    assert_eq!(matches.len(), 1);
    assert_eq!(matches[0].meta.file_key, expected_key);
    assert_eq!(idx.delta_buffer.lock().len(), 0);

    let _ = std::fs::remove_dir_all(&root);
}

#[test]
fn materialize_snapshot_delete_does_not_resurrect_base_path() {
    let root = unique_tmp_dir("materialize-delete");
    std::fs::create_dir_all(&root).unwrap();
    let path = root.join("deleted-from-base.txt");
    std::fs::write(&path, b"delete-me").unwrap();

    let idx = Arc::new(TieredIndex::empty(vec![root.clone()]));
    idx.l2
        .load_full()
        .upsert_path_alias(file_meta_from_path(path.clone()));
    idx.refresh_base();

    std::fs::remove_file(&path).unwrap();
    idx.apply_events(&[mk_event(1, EventType::Delete, path.clone())]);

    let materialized = idx.materialize_snapshot_base().unwrap();
    assert_eq!(materialized.file_count(), 0);
    assert!(materialized
        .query_metas(crate::query::matcher::create_matcher("deleted-from-base", true).as_ref())
        .is_empty());
    assert_eq!(idx.delta_buffer.lock().len(), 0);

    let _ = std::fs::remove_dir_all(&root);
}

#[test]
fn materialize_snapshot_preserves_overlay_hardlink_aliases() {
    let root = unique_tmp_dir("materialize-hardlinks");
    std::fs::create_dir_all(&root).unwrap();
    let original = root.join("hardlink-original.txt");
    let alias = root.join("hardlink-alias.txt");
    std::fs::write(&original, b"same-inode").unwrap();
    std::fs::hard_link(&original, &alias).unwrap();

    let idx = Arc::new(TieredIndex::empty(vec![root.clone()]));
    idx.apply_events(&[
        mk_event(1, EventType::Create, original.clone()),
        mk_event(2, EventType::Create, alias.clone()),
    ]);

    let materialized = idx.materialize_snapshot_base().unwrap();
    let mut paths = materialized
        .query_metas(crate::query::matcher::create_matcher("hardlink-", true).as_ref())
        .into_iter()
        .map(|item| item.meta.path)
        .collect::<Vec<_>>();
    paths.sort();
    let mut expected = vec![original, alias];
    expected.sort();
    assert_eq!(paths, expected);
    assert_eq!(materialized.file_count(), 2);
    assert_eq!(idx.delta_buffer.lock().len(), 0);

    let _ = std::fs::remove_dir_all(&root);
}

#[tokio::test]
async fn snapshot_does_not_commit_clean_shutdown_marker() -> anyhow::Result<()> {
    use crate::storage::snapshot::read_recovery_runtime_state;

    let root = unique_tmp_dir("clean-shutdown-marker");
    let state = unique_tmp_dir("clean-shutdown-marker-state");
    std::fs::create_dir_all(&root)?;
    std::fs::create_dir_all(&state)?;
    std::fs::write(root.join("a.txt"), b"hello")?;

    let store = Arc::new(SnapshotStore::new(state.join("index.db")));
    let idx = Arc::new(TieredIndex::empty(vec![root.clone()]));
    idx.apply_events(&[mk_event(1, EventType::Create, root.join("a.txt"))]);
    idx.refresh_base();

    // While running, a snapshot must leave the marker false so a real crash is
    // detected on the next start.
    idx.snapshot_now(store.clone()).await?;
    let running = read_recovery_runtime_state(store.path())?;
    assert!(
        !running.last_clean_shutdown,
        "running snapshot must keep last_clean_shutdown=false"
    );

    // begin_shutdown only quiesces background work. A successful final
    // snapshot is still not the runtime's clean-shutdown commit point.
    idx.apply_events(&[mk_event(2, EventType::Create, root.join("a.txt"))]);
    idx.refresh_base();
    idx.begin_shutdown();
    assert!(idx.is_shutting_down());
    idx.snapshot_now(store.clone()).await?;
    let shutting_down = read_recovery_runtime_state(store.path())?;
    assert!(
        !shutting_down.last_clean_shutdown,
        "snapshot during shutdown must remain unclean until runtime persistence succeeds"
    );

    let _ = std::fs::remove_dir_all(&root);
    let _ = std::fs::remove_dir_all(&state);
    Ok(())
}

#[tokio::test]
async fn snapshot_loop_exits_promptly_on_begin_shutdown() {
    let root = unique_tmp_dir("loop-exits-shutdown");
    std::fs::create_dir_all(&root).unwrap();
    let store = Arc::new(SnapshotStore::new(root.join("index.db")));
    let idx = Arc::new(TieredIndex::empty(vec![root.clone()]));

    // Long interval: the loop would otherwise sleep for an hour. begin_shutdown
    // must wake it and make it return.
    let handle = tokio::spawn(idx.clone().snapshot_loop(store.clone(), 3600));
    idx.begin_shutdown();

    let joined = tokio::time::timeout(std::time::Duration::from_secs(5), handle).await;
    assert!(
        joined.is_ok(),
        "snapshot loop did not exit after begin_shutdown"
    );

    let _ = std::fs::remove_dir_all(&root);
}

#[test]
fn begin_shutdown_rejects_delayed_rebuild_admission() {
    let root = unique_tmp_dir("shutdown-rejects-rebuild");
    std::fs::create_dir_all(&root).unwrap();
    let idx = Arc::new(TieredIndex::empty(vec![root.clone()]));

    idx.begin_shutdown();
    assert!(matches!(
        idx.reserve_rebuild_with_cooldown("test delayed rebuild"),
        RebuildAdmission::Coalesced
    ));
    let rebuild = idx.rebuild_state.lock();
    assert!(!rebuild.in_progress);
    assert!(!rebuild.requested);
    assert!(!rebuild.scheduled);

    let _ = std::fs::remove_dir_all(&root);
}

// =========================================================================
// Phase 2: readdir 批量删除对齐单元测试
//
// 直接测试 `sync::readdir_delete_alignment`——按 parent dir 分组，每个 dirty 目录做
// 一次 `read_dir` 构建 `HashSet<OsString>`，与索引文件名做差集检测删除。
// =========================================================================

/// 将 `readdir_delete_alignment` 返回的路径收集为 HashSet，便于断言。
fn deleted_set(deleted: Vec<PathBuf>) -> std::collections::HashSet<PathBuf> {
    deleted.into_iter().collect()
}

#[test]
fn delete_alignment_uses_readdir() {
    let dir = unique_tmp_dir("p2-uses-readdir");
    std::fs::create_dir_all(&dir).unwrap();
    let keep = dir.join("keep.txt");
    let gone = dir.join("gone.txt");
    std::fs::write(&keep, b"x").unwrap();
    std::fs::write(&gone, b"x").unwrap();

    // 模拟索引中的候选条目 (doc_id, path)
    let to_delete = vec![(0u64, keep.clone()), (1u64, gone.clone())];

    // 删除 gone.txt，readdir 不再包含它 → 应被标记删除（走 readdir 路径而非逐文件 stat）
    std::fs::remove_file(&gone).unwrap();
    let set = deleted_set(super::sync::readdir_delete_alignment(to_delete, None));

    assert!(
        set.contains(&gone),
        "gone.txt should be detected as deleted via readdir"
    );
    assert!(!set.contains(&keep), "keep.txt should not be deleted");

    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn delete_alignment_detects_missing_files() {
    let dir = unique_tmp_dir("p2-detect-missing");
    std::fs::create_dir_all(&dir).unwrap();
    let keep1 = dir.join("keep1.txt");
    let keep2 = dir.join("keep2.txt");
    let gone1 = dir.join("gone1.txt");
    let gone2 = dir.join("gone2.txt");
    for p in [&keep1, &keep2, &gone1, &gone2] {
        std::fs::write(p, b"x").unwrap();
    }

    let to_delete = vec![
        (0u64, keep1.clone()),
        (1u64, keep2.clone()),
        (2u64, gone1.clone()),
        (3u64, gone2.clone()),
    ];

    std::fs::remove_file(&gone1).unwrap();
    std::fs::remove_file(&gone2).unwrap();
    let set = deleted_set(super::sync::readdir_delete_alignment(to_delete, None));

    assert_eq!(set.len(), 2);
    assert!(set.contains(&gone1), "gone1 should be deleted");
    assert!(set.contains(&gone2), "gone2 should be deleted");
    assert!(!set.contains(&keep1), "keep1 should be retained");
    assert!(!set.contains(&keep2), "keep2 should be retained");

    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn delete_alignment_keeps_existing_files() {
    let dir = unique_tmp_dir("p2-keeps-existing");
    std::fs::create_dir_all(&dir).unwrap();
    let keep = dir.join("keep.txt");
    std::fs::write(&keep, b"x").unwrap();

    let to_delete = vec![(0u64, keep.clone())];
    let set = deleted_set(super::sync::readdir_delete_alignment(to_delete, None));
    assert!(set.is_empty(), "existing file must not be marked deleted");

    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn delete_alignment_dir_gone() {
    // 目录不存在 → read_dir 失败 → 该目录下所有索引条目标记删除
    let dir = unique_tmp_dir("p2-dir-gone");
    // 故意不创建 dir，read_dir 必然失败
    let a = dir.join("a.txt");
    let b = dir.join("b.txt");
    let to_delete = vec![(0u64, a.clone()), (1u64, b.clone())];

    let set = deleted_set(super::sync::readdir_delete_alignment(to_delete, None));
    assert_eq!(set.len(), 2, "all entries under a gone dir must be deleted");
    assert!(set.contains(&a));
    assert!(set.contains(&b));
}

#[test]
fn delete_alignment_osstring_non_utf8() {
    use std::os::unix::ffi::OsStringExt;

    let dir = unique_tmp_dir("p2-non-utf8");
    std::fs::create_dir_all(&dir).unwrap();

    // 构造非 UTF-8 文件名（0xFF 0xFE 在 UTF-8 中非法）
    let existing_name = std::ffi::OsString::from_vec(vec![0xFF, 0xFE]);
    let existing_path = dir.join(&existing_name);
    std::fs::write(&existing_path, b"x").unwrap();

    let missing_name = std::ffi::OsString::from_vec(vec![0xFD, 0xFC]);
    let missing_path = dir.join(&missing_name);
    std::fs::write(&missing_path, b"x").unwrap();
    std::fs::remove_file(&missing_path).unwrap();

    let to_delete = vec![(0u64, existing_path.clone()), (1u64, missing_path.clone())];
    let set = deleted_set(super::sync::readdir_delete_alignment(to_delete, None));

    assert!(
        !set.contains(&existing_path),
        "existing non-UTF-8 file must be matched via OsString and kept"
    );
    assert!(
        set.contains(&missing_path),
        "missing non-UTF-8 file must be detected as deleted"
    );

    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn delete_alignment_multi_dir_grouping() {
    let base = unique_tmp_dir("p2-multi-dir");
    let dir_a = base.join("a");
    let dir_b = base.join("b");
    std::fs::create_dir_all(&dir_a).unwrap();
    std::fs::create_dir_all(&dir_b).unwrap();

    let keep_a = dir_a.join("keep_a.txt");
    let gone_a = dir_a.join("gone_a.txt");
    let keep_b = dir_b.join("keep_b.txt");
    let gone_b = dir_b.join("gone_b.txt");
    for p in [&keep_a, &gone_a, &keep_b, &gone_b] {
        std::fs::write(p, b"x").unwrap();
    }

    let to_delete = vec![
        (0u64, keep_a.clone()),
        (1u64, gone_a.clone()),
        (2u64, keep_b.clone()),
        (3u64, gone_b.clone()),
    ];

    std::fs::remove_file(&gone_a).unwrap();
    std::fs::remove_file(&gone_b).unwrap();
    let set = deleted_set(super::sync::readdir_delete_alignment(to_delete, None));

    assert_eq!(set.len(), 2);
    assert!(set.contains(&gone_a), "gone_a should be deleted");
    assert!(set.contains(&gone_b), "gone_b should be deleted");
    assert!(!set.contains(&keep_a), "keep_a should be retained");
    assert!(!set.contains(&keep_b), "keep_b should be retained");

    let _ = std::fs::remove_dir_all(&base);
}

#[test]
fn delete_alignment_memory_cleanup() {
    // 验证 current_names HashSet 在每个目录处理完后立即 drop，不跨目录累积。
    // 两个目录各有一个同名文件 "shared.txt"：dir_a 的保留，dir_b 的删除。
    // 若 name set 跨目录泄漏（累积），dir_b 的 shared.txt 会因 dir_a 的 name set
    // 仍包含 "shared.txt" 而被错误保留。
    let base = unique_tmp_dir("p2-mem-cleanup");
    let dir_a = base.join("a");
    let dir_b = base.join("b");
    std::fs::create_dir_all(&dir_a).unwrap();
    std::fs::create_dir_all(&dir_b).unwrap();

    let shared_a = dir_a.join("shared.txt");
    let shared_b = dir_b.join("shared.txt");
    std::fs::write(&shared_a, b"x").unwrap();
    std::fs::write(&shared_b, b"x").unwrap();
    std::fs::remove_file(&shared_b).unwrap();

    let to_delete = vec![(0u64, shared_a.clone()), (1u64, shared_b.clone())];
    let set = deleted_set(super::sync::readdir_delete_alignment(to_delete, None));

    assert!(
        !set.contains(&shared_a),
        "dir_a/shared.txt exists and must be kept (per-dir name set)"
    );
    assert!(
        set.contains(&shared_b),
        "dir_b/shared.txt is gone and must be deleted; \
         if the HashSet leaked across dirs this would falsely be kept"
    );

    let _ = std::fs::remove_dir_all(&base);
}
