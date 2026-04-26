//! P2 — Large-scale hybrid workspace correctness test.
//!
//! Orchestrates a full 800K-file + mixed scenario + search verification test.
//! Validates incremental indexing under realistic developer workflows:
//!   - initial cold scan of 800k files
//!   - git clone (large batch of new files, many ignored)
//!   - npm install (deep dependency tree, mostly ignored)
//!   - single-file CRUD (create / delete / rename)
//!
//! Marked with `#[ignore]` so it only runs when explicitly invoked in CI:
//!   cargo test --test p2_large_scale_hybrid -- --ignored
//!
//! System metrics (CPU/RAM) are collected per phase via ProcessMonitor and
//! written to `/tmp/fd-rdd-hybrid-metrics.json` (or `%TEMP%` on Windows).

mod common;
#[path = "fixtures/hybrid_workspace.rs"]
mod hybrid_workspace;

use common::sys_monitor::ProcessMonitor;
use common::*;
use hybrid_workspace::HybridWorkspace;
use std::collections::HashMap;
use std::path::Path;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

// ---------------------------------------------------------------------------
// 唯一标记生成（嵌入文件名 / 内容中，用于搜索验证）
// ---------------------------------------------------------------------------

fn make_marker(prefix: &str) -> String {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    format!("{}-{:x}", prefix, nanos)
}

// ---------------------------------------------------------------------------
// 搜索验证辅助函数
// ---------------------------------------------------------------------------

fn verify_file_searchable(port: u16, marker: &str, expected_path: &Path, timeout_secs: u64) {
    let found = wait_for_file_visible(port, expected_path, timeout_secs);
    assert!(
        found,
        "file with marker {} should be searchable at {}",
        marker,
        expected_path.display()
    );
}

fn verify_file_not_searchable(port: u16, marker: &str, old_path: &Path, timeout_secs: u64) {
    let gone = wait_for_file_gone(port, old_path, timeout_secs);
    assert!(
        gone,
        "file {} with marker {} should no longer be searchable",
        old_path.display(),
        marker
    );
}

// ---------------------------------------------------------------------------
// 性能指标收集
// ---------------------------------------------------------------------------

#[derive(Debug, Clone)]
struct PhaseMetrics {
    name: &'static str,
    duration: Duration,
    cpu_peak_percent: f64,
    cpu_100_duration_ms: u64,
    rss_peak_kb: u64,
}

/// 用 ProcessMonitor 包围一个操作，收集该阶段的性能指标。
fn monitor_phase<F, R>(name: &'static str, pid: u32, op: F) -> (R, PhaseMetrics)
where
    F: FnOnce() -> R,
{
    let mut monitor = ProcessMonitor::new(pid);
    monitor.start();
    let start = Instant::now();
    let result = op();
    let duration = start.elapsed();
    let stats = monitor.stop();

    let metrics = PhaseMetrics {
        name,
        duration,
        cpu_peak_percent: stats.max_cpu_percent as f64,
        cpu_100_duration_ms: stats.cpu_100pct_duration_ms,
        rss_peak_kb: stats.max_rss_bytes / 1024,
    };

    eprintln!(
        "[{}] dur={:?} cpu_peak={:.1}% cpu_100={}ms rss_peak={}KB",
        metrics.name,
        metrics.duration,
        metrics.cpu_peak_percent,
        metrics.cpu_100_duration_ms,
        metrics.rss_peak_kb
    );

    (result, metrics)
}

/// 将指标写入 JSON 文件，供 CI 解析。
fn write_metrics_json(metrics: &[PhaseMetrics]) {
    let mut phases = HashMap::new();
    for m in metrics {
        let entry = format!(
            "{{ \"cpu_peak_percent\": {:.0}, \"cpu_100_duration_ms\": {}, \"rss_peak_kb\": {} }}",
            m.cpu_peak_percent, m.cpu_100_duration_ms, m.rss_peak_kb
        );
        phases.insert(m.name, entry);
    }

    let mut json = String::from("{ \"phases\": {");
    let mut first = true;
    for (name, entry) in &phases {
        if !first {
            json.push(',');
        }
        first = false;
        json.push_str(&format!(" \"{}\": {}", name, entry));
    }
    json.push_str(" } }");

    let path = std::env::temp_dir().join("fd-rdd-hybrid-metrics.json");
    if let Err(e) = std::fs::write(&path, json) {
        eprintln!("warning: failed to write metrics JSON: {}", e);
    } else {
        eprintln!("metrics written to {}", path.display());
    }
}

/// CI 阈值断言。
fn assert_performance_thresholds(metrics: &[PhaseMetrics]) {
    for m in metrics {
        assert!(
            m.cpu_100_duration_ms <= 3000,
            "Phase {}: CPU 100% duration {}ms exceeds 3000ms threshold",
            m.name,
            m.cpu_100_duration_ms
        );
        assert!(
            m.rss_peak_kb <= 409_600,
            "Phase {}: Peak RSS {}KB exceeds 409600KB (400MB) threshold",
            m.name,
            m.rss_peak_kb
        );
    }
}

// ---------------------------------------------------------------------------
// 主测试
// ---------------------------------------------------------------------------

#[test]
#[ignore] // 仅在 CI 中显式运行
fn large_scale_hybrid_workspace_correctness() {
    // -----------------------------------------------------------------------
    // 1. 创建唯一临时根目录
    // -----------------------------------------------------------------------
    let root = unique_tmp_dir("hybrid-large-scale");
    std::fs::create_dir_all(&root).unwrap();

    // -----------------------------------------------------------------------
    // 2. 生成 80 万文件工作区（在启动 fd-rdd 之前，使全量构建能扫描到所有文件）
    // -----------------------------------------------------------------------
    let gen_start = Instant::now();
    let mut ws = HybridWorkspace::generate(&root).unwrap();
    let gen_dur = gen_start.elapsed();
    eprintln!(
        "[generate_workspace] dur={:?} (pre-daemon, no CPU/RSS monitoring)",
        gen_dur
    );

    // -----------------------------------------------------------------------
    // 3. 启动 fd-rdd（此时目录已包含 80 万文件，full_build 可完整索引）
    // -----------------------------------------------------------------------
    let port = 17060;
    let snapshot = root.join("snapshot");
    let process = FdRddProcess::spawn(
        &root,
        port,
        &snapshot,
        &["--debounce-ms", "10", "--event-channel-size", "524288"],
    );
    let pid = process.pid();

    let mut all_metrics: Vec<PhaseMetrics> = Vec::new();

    // 记录 generate_workspace 阶段指标（仅 wall-time；fd-rdd 尚未启动）
    all_metrics.push(PhaseMetrics {
        name: "generate_workspace",
        duration: gen_dur,
        cpu_peak_percent: 0.0,
        cpu_100_duration_ms: 0,
        rss_peak_kb: 0,
    });

    // -----------------------------------------------------------------------
    // 4. 等待初始索引完成（等待 indexed_count 稳定 5 秒）
    // -----------------------------------------------------------------------
    let (_, m) = monitor_phase("initial_indexing", pid, || {
        wait_for_index_stable(port, 5, 600).expect("initial indexing timeout")
    });
    all_metrics.push(m);

    // -----------------------------------------------------------------------
    // 5. 验证初始状态
    //    - 搜索用户文件能找到
    //    - 搜索被忽略文件找不到（抽样）
    // -----------------------------------------------------------------------
    let initial_marker = make_marker("initial");
    let initial_probe = ws
        .user_create_file(
            &format!("probes/initial-{}.txt", initial_marker),
            &format!("probe content {}", initial_marker),
        )
        .unwrap();

    verify_file_searchable(port, &initial_marker, &initial_probe, 30);

    // 抽样验证被忽略文件不可搜索
    for ignored in ws.sample_ignored_files(5) {
        verify_file_not_searchable(port, "initial-ignore", &ignored, 10);
    }

    // -----------------------------------------------------------------------
    // 6. 模拟 git clone 场景（穿插）
    // -----------------------------------------------------------------------
    let git_marker = make_marker("git");
    let git_repo_name = format!("my-project-{}", git_marker);
    let (_, m) = monitor_phase("git_clone", pid, || {
        ws.simulate_git_clone(&git_repo_name).unwrap()
    });
    all_metrics.push(m);

    // git clone 中应包含一个可搜索的 probe 文件（非忽略）
    let git_probe = ws
        .user_create_file(
            &format!("{}/probe-{}.md", git_repo_name, git_marker),
            &format!("probe content {}", git_marker),
        )
        .unwrap();
    verify_file_searchable(port, &git_marker, &git_probe, 30);

    // -----------------------------------------------------------------------
    // 7. 模拟 npm install 场景（穿插）
    // -----------------------------------------------------------------------
    let npm_marker = make_marker("npm");
    let npm_pkg_name = format!("lodash-{}", npm_marker);
    let (_, m) = monitor_phase("npm_install", pid, || {
        ws.simulate_npm_install(&npm_pkg_name, 5000).unwrap()
    });
    all_metrics.push(m);

    // npm install 中应包含一个可搜索的 probe 文件（非忽略）
    // 注意：node_modules/ 本身被 .gitignore 忽略，probe 放在 src/ 下模拟用户文件
    let npm_probe = ws
        .user_create_file(
            &format!("src/npm-probe-{}.js", npm_marker),
            &format!("probe content {}", npm_marker),
        )
        .unwrap();
    verify_file_searchable(port, &npm_marker, &npm_probe, 30);

    // -----------------------------------------------------------------------
    // 8. 穿插用户 CRUD 操作
    // -----------------------------------------------------------------------

    // 8a. 创建文件
    let crud_marker = make_marker("crud");
    let new_file = ws
        .user_create_file(
            &format!("src/main-{}.rs", crud_marker),
            &format!("fn main() {{ /* {} */ }}", crud_marker),
        )
        .unwrap();

    let create_start = Instant::now();
    verify_file_searchable(port, &crud_marker, &new_file, 30);
    let create_latency = create_start.elapsed();

    eprintln!("[file_create_latency] duration={:?}", create_latency);

    // 8b. 删除文件
    let delete_marker = make_marker("delete");
    let file_to_delete = ws
        .user_create_file(
            &format!("tmp/delete-{}.txt", delete_marker),
            &format!("to be deleted {}", delete_marker),
        )
        .unwrap();
    verify_file_searchable(port, &delete_marker, &file_to_delete, 30);

    ws.user_delete_file(&file_to_delete).unwrap();
    let delete_start = Instant::now();
    verify_file_not_searchable(port, &delete_marker, &file_to_delete, 30);
    let delete_latency = delete_start.elapsed();

    eprintln!("[file_delete_latency] duration={:?}", delete_latency);

    // 8c. 重命名文件
    let rename_marker = make_marker("rename");
    let file_to_rename = ws
        .user_create_file(
            &format!("src/old-{}.rs", rename_marker),
            &format!("fn old() {{ /* {} */ }}", rename_marker),
        )
        .unwrap();
    wait_for_indexed_count(port, ws.indexable_files().len(), 30).unwrap();
    verify_file_searchable(port, &rename_marker, &file_to_rename, 30);

    let renamed_path = root.join(format!("src/renamed-{}.rs", rename_marker));
    ws.user_rename_file(&file_to_rename, &renamed_path).unwrap();
    // 重命名不一定会改变 total_file_count，直接验证路径可见性
    let rename_start = Instant::now();
    verify_file_not_searchable(port, &rename_marker, &file_to_rename, 30);
    verify_file_searchable(port, &rename_marker, &renamed_path, 30);
    let rename_latency = rename_start.elapsed();

    eprintln!("[file_rename_latency] duration={:?}", rename_latency);

    // 将 CRUD 延迟作为附加指标输出（不纳入阈值检查，仅记录）
    eprintln!(
        "[user_crud] create={:?} delete={:?} rename={:?}",
        create_latency, delete_latency, rename_latency
    );

    // -----------------------------------------------------------------------
    // 9. 最终一致性验证
    // -----------------------------------------------------------------------

    // 9a. 数量一致性：索引总数应与可索引文件数一致
    let final_count = ws.indexable_files().len();
    wait_for_indexed_count(port, final_count, 60).expect("final consistency count timeout");

    // 9b. 抽样验证所有非忽略文件可搜索
    for searchable in ws.sample_searchable_files(10) {
        let found = wait_for_file_visible(port, &searchable, 10);
        assert!(
            found,
            "searchable file {} should be found",
            searchable.display()
        );
    }

    // 9c. 抽样验证被忽略文件不可搜索
    for ignored in ws.sample_ignored_files(10) {
        let gone = wait_for_file_gone(port, &ignored, 10);
        assert!(
            gone,
            "ignored file {} should not be searchable",
            ignored.display()
        );
    }

    // 9d. 重命名后的旧路径不应出现
    verify_file_not_searchable(port, &rename_marker, &file_to_rename, 10);

    // 9e. 删除的文件不应出现
    verify_file_not_searchable(port, &delete_marker, &file_to_delete, 10);

    // -----------------------------------------------------------------------
    // 10. 性能指标收集与阈值断言
    // -----------------------------------------------------------------------
    assert_performance_thresholds(&all_metrics);
    write_metrics_json(&all_metrics);

    // -----------------------------------------------------------------------
    // 11. 清理
    // -----------------------------------------------------------------------
    process.kill();
    ws.cleanup().unwrap();
}
