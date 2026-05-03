use clap::Parser;
use fd_rdd::config::{default_snapshot_path, default_socket_path, Config, WatchMode};
use fd_rdd::event::ignore_filter::IgnoreFilter;
use fd_rdd::event::sync::DirtyScope;
use fd_rdd::event::{EventPipeline, TieredWatchRuntime, WatchCommand};
use fd_rdd::index::TieredIndex;
use fd_rdd::query::SocketServer;
use fd_rdd::query::{HealthTelemetry, QueryServer};
use fd_rdd::stats::{EventPipelineStats, WatchStateReport};
use fd_rdd::storage::snapshot::{
    write_recovery_runtime_state, RecoveryRuntimeState, SnapshotStore,
};
use fd_rdd::util::normalize_exclude_dirs;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;
use tracing::info;

#[derive(Parser, Debug)]
#[command(
    name = "fd-rdd",
    version,
    about = "fd-rdd: atomic-snapshot file indexer"
)]
struct Args {
    /// 要索引的根目录（可重复传入）；必须至少指定一个（可以是 $HOME）
    #[arg(long = "root", value_name = "PATH")]
    roots: Vec<PathBuf>,

    /// 快照路径（默认: $XDG_RUNTIME_DIR/fd-rdd/index.db，回退到 /run/user/$UID/... 或 /tmp/fd-rdd-$UID/...）
    ///
    /// - legacy 单文件：index.db（兼容读取 v2~v6；v6 为 mmap 段式容器）
    /// - LSM 目录：同路径派生的 index.d/（MANIFEST.bin + seg-*.db/.del + events.wal）
    #[arg(long, value_name = "PATH")]
    snapshot_path: Option<PathBuf>,

    /// 将 `.` 开头的文件/目录纳入冷启动全扫、后台重建与增量补扫
    #[arg(long)]
    include_hidden: bool,

    /// HTTP 查询端口
    #[arg(long)]
    http_port: Option<u16>,

    /// Unix domain socket 查询地址（可选）：用于流式输出（避免 HTTP/JSON 聚合带来的峰值）
    #[arg(long, value_name = "PATH")]
    uds_socket: Option<PathBuf>,

    /// 快照写入间隔（秒）
    #[arg(long)]
    snapshot_interval_secs: Option<u64>,

    /// 内存报告间隔（秒）
    #[arg(long)]
    report_interval_secs: Option<u64>,

    /// watcher 事件 channel 容量（越大越不容易 overflow，但会占用更多内存）
    /// 默认 65536，足以应对 git clone 等批量操作；降低此值可减少内存占用但可能丢失事件。
    #[arg(long)]
    event_channel_size: Option<usize>,

    /// watcher 事件 debounce 窗口（毫秒）
    #[arg(long)]
    debounce_ms: Option<u64>,

    /// watcher 忽略路径前缀（可重复）；用于排除 snapshot/log 等“自触发”路径
    ///
    /// 说明：fd-rdd 会默认忽略 `--snapshot-path` 以及派生的 `index.d/`；这里用于补充额外忽略项。
    #[arg(long = "ignore-path", value_name = "PATH")]
    ignore_paths: Vec<PathBuf>,

    /// 全局排除的目录名（可重复）。命中这些目录名的路径不会进入索引。
    #[arg(long = "exclude-dir", value_name = "NAME")]
    exclude_dirs: Vec<String>,

    /// 禁用 `.gitignore` / `.ignore` / git exclude / global gitignore 规则
    #[arg(long)]
    no_ignore: bool,

    /// 跟随符号链接（默认不跟随）。启用后扫描和 watcher 会进入符号链接指向的目录。
    /// 注意：已有 inode 去重可防止无限递归，但跟随可能导致索引范围远超预期。
    #[arg(long)]
    follow_symlinks: bool,

    /// 禁用文件系统 watcher，仅使用已加载快照和手动 /scan 更新。
    #[arg(long)]
    no_watch: bool,

    /// watcher 模式：recursive（现有递归监听）、tiered（预算受控热点监听）、off（关闭）。
    #[arg(long, value_parser = ["recursive", "tiered", "off"])]
    watch_mode: Option<String>,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt::init();

    let args = Args::parse();
    let cli_watch_mode = parse_watch_mode(args.watch_mode.as_deref())?;

    // 检测首次启动：配置文件不存在视为首次启动
    let config_path = Config::config_path();
    let is_first_run = config_path.as_ref().map(|p| !p.exists()).unwrap_or(true);

    let cfg = if is_first_run {
        // 首次启动：必须提供 --root
        if args.roots.is_empty() {
            eprintln!("错误: 首次启动必须通过 --root <PATH> 指定至少一个索引根目录");
            eprintln!("示例: fd-rdd --root $HOME");
            std::process::exit(1);
        }

        // 用 CLI 参数构建配置，覆盖默认值
        let mut cfg = Config {
            roots: args.roots.clone(),
            http_port: args
                .http_port
                .unwrap_or_else(|| Config::default().http_port),
            snapshot_interval_secs: args
                .snapshot_interval_secs
                .unwrap_or_else(|| Config::default().snapshot_interval_secs),
            include_hidden: args.include_hidden,
            follow_symlinks: args.follow_symlinks,
            watch_enabled: !args.no_watch,
            watch_mode: cli_watch_mode.unwrap_or(if args.no_watch {
                WatchMode::Off
            } else {
                WatchMode::Recursive
            }),
            ignore_enabled: !args.no_ignore,
            ..Config::default()
        };
        if let Some(socket) = &args.uds_socket {
            cfg.socket_path = Some(socket.clone());
        }

        // 保存默认配置到文件
        if let Err(e) = cfg.save() {
            tracing::warn!("无法保存默认配置文件: {}", e);
        } else {
            tracing::info!("已创建默认配置文件");
        }

        cfg
    } else {
        // 非首次启动：正常加载配置文件
        Config::load().unwrap_or_else(|e| {
            tracing::warn!("配置文件加载失败，使用默认值: {}", e);
            Config::default()
        })
    };

    info!(
        "Starting fd-rdd v{}: atomic-snapshot file indexer",
        env!("CARGO_PKG_VERSION")
    );

    // 1) 确定索引根目录 (CLI > config > 报错退出)
    let mut roots = args.roots;
    if roots.is_empty() {
        roots = cfg.roots.clone();
    }
    if roots.is_empty() {
        eprintln!("错误: 配置文件中没有配置索引根目录，请通过 --root <PATH> 指定");
        std::process::exit(1);
    }

    let ignore_enabled = !args.no_ignore && cfg.ignore_enabled;
    let include_hidden = args.include_hidden || cfg.include_hidden;
    let follow_symlinks = args.follow_symlinks || cfg.follow_symlinks;
    let mut effective_watch_mode = cli_watch_mode.unwrap_or(cfg.watch_mode);
    if args.no_watch || !cfg.watch_enabled {
        effective_watch_mode = WatchMode::Off;
    }
    let watch_enabled = effective_watch_mode != WatchMode::Off;
    let http_port = args.http_port.unwrap_or(cfg.http_port);
    let snapshot_interval_secs = args
        .snapshot_interval_secs
        .unwrap_or(cfg.snapshot_interval_secs);
    let report_interval_secs = args.report_interval_secs.unwrap_or(60);
    let event_channel_size = args.event_channel_size.unwrap_or(65_536);
    let debounce_ms = args.debounce_ms.unwrap_or(10);
    let mut exclude_dirs = cfg.exclude_dirs.clone();
    exclude_dirs.extend(args.exclude_dirs.clone());
    let exclude_dirs = normalize_exclude_dirs(exclude_dirs);

    // 2) 快照存储
    let snapshot_path = args.snapshot_path.unwrap_or_else(default_snapshot_path);
    let startup_reconcile_cutoff_ns = modified_unix_ns(&snapshot_path.with_extension("v7"));
    let store = Arc::new(SnapshotStore::new(snapshot_path));

    // 3) 从快照加载或空索引启动
    let index = TieredIndex::load_with_options_follow_and_excludes(
        store.as_ref(),
        roots,
        include_hidden,
        ignore_enabled,
        follow_symlinks,
        exclude_dirs.clone(),
    )
    .await?;
    let _ = index.attach_wal(store.as_ref());
    index.set_stable_snapshot_enabled(cfg.stable_snapshot_enabled);
    let loaded_from_empty_snapshot = index.recovery_status().report.snapshot_source == "empty";
    let repair_stats = index.startup_repair_if_needed(
        cfg.startup_repair_enabled,
        &cfg.startup_repair_mode,
        cfg.startup_repair_max_dirs,
        cfg.startup_repair_budget_ms,
        cfg.startup_repair_force_rebuild_ratio,
    );
    if repair_stats.ran {
        tracing::info!(
            "startup repair completed: scanned={} changed={} elapsed_ms={} escalated={}",
            repair_stats.scanned,
            repair_stats.changed,
            repair_stats.elapsed_ms,
            repair_stats.escalated
        );
    }
    mark_runtime_state(
        store.path(),
        false,
        &index.recovery_status().report.snapshot_source,
        "running",
    );

    // 4) 若没有可信快照，或启动 repair 判断差异过大，后台全量构建。
    let needs_full_build =
        loaded_from_empty_snapshot || repair_stats.escalated || index.file_count() == 0;
    if needs_full_build && !index.rebuild_in_progress() {
        index.spawn_full_build();
    }

    let ignore_filter = if ignore_enabled {
        Some(IgnoreFilter::from_roots(&index.roots))
    } else {
        None
    };
    let mut startup_ignore_paths = args.ignore_paths.clone();
    startup_ignore_paths.push(store.path().to_path_buf());
    startup_ignore_paths.push(store.derived_lsm_dir_path());
    if watch_enabled && index.file_count() > 0 && startup_reconcile_cutoff_ns > 0 {
        index.spawn_fast_sync(
            DirtyScope::All {
                cutoff_ns: startup_reconcile_cutoff_ns,
            },
            startup_ignore_paths.clone(),
        );
    }
    let watch_plan = build_watch_plan(
        effective_watch_mode,
        &index.roots,
        &cfg.tiered_watch,
        &exclude_dirs,
    );
    let tiered_runtime = if effective_watch_mode == WatchMode::Tiered {
        Some(Arc::new(TieredWatchRuntime::new(
            watch_plan.l0_roots.clone(),
            watch_plan.l1_roots.clone(),
            cfg.tiered_watch.max_watch_dirs.max(1),
            cfg.tiered_watch.scan_items_per_sec,
            cfg.tiered_watch.scan_ms_per_tick,
        )))
    } else {
        None
    };
    let watch_state = Arc::new(watch_plan.state.clone());

    // 5) 启动事件管道（bounded + debounce）
    // 默认忽略索引自身的 snapshot/segment 写入路径，避免 watcher 反馈循环。
    // 额外忽略项可通过 --ignore-path 传入（例如将日志重定向到了被 watch 的目录下）。
    let mut pipeline = EventPipeline::new_with_config_and_ignores(
        index.clone(),
        debounce_ms,
        event_channel_size,
        startup_ignore_paths.clone(),
    )
    .with_ignore_filter(ignore_filter.clone())
    .with_exclude_dirs(exclude_dirs.clone())
    .with_tiered_runtime(tiered_runtime.clone());
    if let Some(roots) = watch_plan.watch_roots.clone() {
        pipeline = pipeline.with_watch_roots(roots);
    }
    let pipeline = Arc::new(pipeline);
    let watch_command_tx = pipeline.watch_command_sender();
    if watch_enabled {
        pipeline.start().await?;
    } else {
        tracing::warn!(
            "Filesystem watcher disabled; index updates require manual /scan or rebuild"
        );
    }
    if effective_watch_mode == WatchMode::Tiered {
        if let Some(runtime) = tiered_runtime.clone() {
            spawn_tiered_scan_loop(
                index.clone(),
                runtime,
                watch_command_tx,
                cfg.tiered_watch.clone(),
            );
        }
    }

    // 6) 启动 HTTP 查询服务
    let health_provider = {
        let index = index.clone();
        let pipeline = pipeline.clone();
        let health_watch_state = watch_state.clone();
        let health_tiered_runtime = tiered_runtime.clone();
        Arc::new(move || {
            let stats = pipeline.stats();
            let watch_state = health_tiered_runtime
                .as_ref()
                .map(|runtime| runtime.report())
                .unwrap_or_else(|| health_watch_state.as_ref().clone());
            let recovery = index.recovery_status();
            HealthTelemetry {
                last_snapshot_time: index.last_snapshot_time(),
                watch_enabled,
                watch_failures: stats.watch_failures,
                watcher_degraded: stats.watcher_degraded || watch_state.l0_rejected > 0,
                degraded_roots: stats
                    .degraded_roots
                    .saturating_add(watch_state.l1_dirs)
                    .saturating_add(watch_state.l2_dirs)
                    .saturating_add(watch_state.l3_dirs),
                overflow_drops: stats.overflow_drops,
                rescan_signals: stats.rescan_signals,
                snapshot_source: recovery.report.snapshot_source,
                wal_events_replayed: recovery.report.wal_events_replayed,
                wal_truncated_tail_records: recovery.report.wal_truncated_tail_records,
                startup_repair_ran: recovery.repair.ran,
                startup_repair_escalated: recovery.repair.escalated,
                startup_repair_scanned: recovery.repair.scanned,
                startup_repair_changed: recovery.repair.changed,
                last_clean_shutdown: recovery.report.previous_clean_shutdown,
                l1_dirs: watch_state.l1_dirs,
                l2_dirs: watch_state.l2_dirs,
                l3_dirs: watch_state.l3_dirs,
                watch_budget_utilization_pct: watch_state.watch_budget_utilization_pct,
                promotion_budget_blocked: watch_state.promotion_budget_blocked,
            }
        })
    };
    let stats_provider: Arc<dyn Fn() -> EventPipelineStats + Send + Sync> = {
        let pipeline = pipeline.clone();
        Arc::new(move || pipeline.stats())
    };
    let watch_state_provider: Arc<dyn Fn() -> WatchStateReport + Send + Sync> = {
        let watch_state = watch_state.clone();
        let tiered_runtime = tiered_runtime.clone();
        Arc::new(move || {
            tiered_runtime
                .as_ref()
                .map(|runtime| runtime.report())
                .unwrap_or_else(|| watch_state.as_ref().clone())
        })
    };
    let query_server = QueryServer::new(index.clone())
        .with_health_provider(health_provider)
        .with_stats_provider(stats_provider.clone())
        .with_watch_state_provider(watch_state_provider);
    tokio::spawn(async move {
        if let Err(e) = query_server.run(http_port).await {
            tracing::error!("Query server error: {}", e);
        }
    });

    // 6.5) 启动 UDS 查询服务（CLI > config > default_socket_path()）
    let uds_path = args
        .uds_socket
        .or(cfg.socket_path)
        .unwrap_or_else(default_socket_path);
    {
        let socket_server = SocketServer::new(index.clone());
        let path = uds_path.clone();
        tokio::spawn(async move {
            if let Err(e) = socket_server.run(&path).await {
                tracing::error!("UDS query server error: {}", e);
            }
        });
    }

    // 7) 启动定期快照循环（每 300 秒）
    let snap_index = index.clone();
    let snap_store = store.clone();
    tokio::spawn(async move {
        snap_index
            .snapshot_loop(snap_store, snapshot_interval_secs)
            .await;
    });

    // 8) 启动内存报告循环（每 60 秒）
    {
        let report_index = index.clone();

        tokio::spawn(async move {
            report_index
                .memory_report_loop(stats_provider, report_interval_secs)
                .await;
        });
    }

    info!(
        "fd-rdd ready. Query via: http://localhost:{}/search?q=keyword",
        http_port
    );

    // 9) 优雅退出：SIGINT/SIGTERM → 最终快照
    shutdown_signal().await?;
    info!("Shutting down, writing final snapshot...");
    if let Err(e) = index.snapshot_now(store.clone()).await {
        tracing::error!("Final snapshot failed: {}", e);
    }
    mark_runtime_state(
        store.path(),
        true,
        &index.recovery_status().report.snapshot_source,
        "clean-shutdown",
    );
    info!("Goodbye.");

    Ok(())
}

fn mark_runtime_state(
    snapshot_path: &std::path::Path,
    clean_shutdown: bool,
    startup_source: &str,
    recovery_mode: &str,
) {
    let state = RecoveryRuntimeState {
        last_clean_shutdown: clean_shutdown,
        last_snapshot_unix_secs: unix_secs(),
        last_wal_seal_id: 0,
        last_startup_source: startup_source.to_string(),
        last_recovery_mode: recovery_mode.to_string(),
    };
    if let Err(e) = write_recovery_runtime_state(snapshot_path, &state) {
        tracing::warn!("failed to write recovery runtime state: {}", e);
    }
}

async fn shutdown_signal() -> anyhow::Result<()> {
    #[cfg(unix)]
    {
        use tokio::signal::unix::{signal, SignalKind};
        let mut sigint = signal(SignalKind::interrupt())?;
        let mut sigterm = signal(SignalKind::terminate())?;
        tokio::select! {
            _ = sigint.recv() => {}
            _ = sigterm.recv() => {}
        }
        Ok(())
    }

    #[cfg(not(unix))]
    {
        tokio::signal::ctrl_c().await?;
        Ok(())
    }
}

fn unix_secs() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
}

#[derive(Clone, Debug)]
struct WatchPlan {
    watch_roots: Option<Vec<PathBuf>>,
    l0_roots: Vec<(PathBuf, usize)>,
    l1_roots: Vec<(PathBuf, usize)>,
    state: WatchStateReport,
}

fn parse_watch_mode(value: Option<&str>) -> anyhow::Result<Option<WatchMode>> {
    let Some(value) = value else {
        return Ok(None);
    };
    match value {
        "recursive" => Ok(Some(WatchMode::Recursive)),
        "tiered" => Ok(Some(WatchMode::Tiered)),
        "off" => Ok(Some(WatchMode::Off)),
        _ => anyhow::bail!("invalid watch mode: {value}"),
    }
}

fn watch_mode_label(mode: WatchMode) -> &'static str {
    match mode {
        WatchMode::Recursive => "recursive",
        WatchMode::Tiered => "tiered",
        WatchMode::Off => "off",
    }
}

fn modified_unix_ns(path: &std::path::Path) -> u64 {
    let Ok(meta) = std::fs::metadata(path) else {
        return 0;
    };
    let Ok(modified) = meta.modified() else {
        return 0;
    };
    let Ok(duration) = modified.duration_since(std::time::UNIX_EPOCH) else {
        return 0;
    };
    duration
        .as_secs()
        .saturating_mul(1_000_000_000)
        .saturating_add(duration.subsec_nanos() as u64)
}

fn build_watch_plan(
    mode: WatchMode,
    roots: &[PathBuf],
    tiered: &fd_rdd::config::TieredWatchConfig,
    exclude_dirs: &[String],
) -> WatchPlan {
    match mode {
        WatchMode::Recursive => WatchPlan {
            watch_roots: None,
            l0_roots: Vec::new(),
            l1_roots: Vec::new(),
            state: WatchStateReport {
                mode: watch_mode_label(mode).to_string(),
                backend: "notify".to_string(),
                l0_dirs: roots.len(),
                l0_admitted: roots.len(),
                notes: vec!["recursive mode watches every configured root".to_string()],
                ..WatchStateReport::default()
            },
        },
        WatchMode::Off => WatchPlan {
            watch_roots: Some(Vec::new()),
            l0_roots: Vec::new(),
            l1_roots: Vec::new(),
            state: WatchStateReport {
                mode: watch_mode_label(mode).to_string(),
                backend: "none".to_string(),
                notes: vec!["watcher disabled; use /scan or rebuild for updates".to_string()],
                ..WatchStateReport::default()
            },
        },
        WatchMode::Tiered => build_tiered_watch_plan(roots, tiered, exclude_dirs),
    }
}

fn build_tiered_watch_plan(
    roots: &[PathBuf],
    tiered: &fd_rdd::config::TieredWatchConfig,
    exclude_dirs: &[String],
) -> WatchPlan {
    let mut candidates = initial_hot_candidates(roots, &tiered.hot_dirs, exclude_dirs);
    if candidates.is_empty() {
        candidates.extend(roots.iter().filter(|p| p.is_dir()).cloned());
    }
    candidates.sort();
    candidates.dedup();

    let mut admitted = Vec::new();
    let mut scan_roots = Vec::new();
    let mut rejected = 0usize;
    let mut estimated_total = 0usize;
    let max_watch_dirs = tiered.max_watch_dirs.max(1);

    for candidate in candidates.iter() {
        let estimated = estimate_recursive_dir_count(candidate, max_watch_dirs, exclude_dirs);
        if estimated_total.saturating_add(estimated) <= max_watch_dirs {
            estimated_total = estimated_total.saturating_add(estimated);
            admitted.push((candidate.clone(), estimated));
        } else {
            rejected = rejected.saturating_add(1);
            scan_roots.push((candidate.clone(), estimated));
        }
    }

    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0);
    let mut notes = vec![
        "tiered mode admits only hot directory candidates into L0".to_string(),
        "L1 rejected candidates are scanned by a bounded warm-scan loop".to_string(),
    ];
    if admitted.is_empty() {
        notes.push("no L0 directories admitted under current budget".to_string());
    }

    let watch_roots = admitted
        .iter()
        .map(|(path, _)| path.clone())
        .collect::<Vec<_>>();
    WatchPlan {
        watch_roots: Some(watch_roots.clone()),
        l0_roots: admitted.clone(),
        l1_roots: scan_roots.clone(),
        state: WatchStateReport {
            mode: watch_mode_label(WatchMode::Tiered).to_string(),
            backend: "notify".to_string(),
            l0_dirs: admitted.len(),
            l1_dirs: rejected,
            l2_dirs: 0,
            l3_dirs: 0,
            watched_dirs_estimated: estimated_total,
            max_watch_dirs,
            l0_candidates: candidates.len(),
            l0_admitted: admitted.len(),
            l0_rejected: rejected,
            scan_backlog: rejected,
            scan_items_per_sec: tiered.scan_items_per_sec,
            scan_ms_per_tick: tiered.scan_ms_per_tick,
            last_adjustment_unix_secs: now,
            notes,
            ..WatchStateReport::default()
        },
    }
}

fn spawn_tiered_scan_loop(
    index: Arc<TieredIndex>,
    runtime: Arc<TieredWatchRuntime>,
    watch_command_tx: tokio::sync::mpsc::Sender<WatchCommand>,
    tiered: fd_rdd::config::TieredWatchConfig,
) {
    tokio::spawn(async move {
        let interval = Duration::from_secs(tiered.l1_scan_interval_secs.max(1));
        let max_dirs_per_tick = (tiered.scan_items_per_sec / 500).clamp(1, 10);

        loop {
            tokio::time::sleep(interval).await;

            for path in runtime.expired_l0(tiered.l0_idle_ttl_secs) {
                if runtime.mark_demotion_pending(path.as_path())
                    && watch_command_tx
                        .send(WatchCommand::Remove(path.clone()))
                        .await
                        .is_err()
                {
                    runtime.rollback_demote(path.as_path());
                }
            }

            let batch = runtime.l1_batch(max_dirs_per_tick);
            if batch.is_empty() {
                continue;
            }

            let index = index.clone();
            match tokio::task::spawn_blocking(move || {
                batch
                    .into_iter()
                    .map(|dir| {
                        let outcome = index.scan_dirs_immediate_outcome(std::slice::from_ref(&dir));
                        (dir, outcome)
                    })
                    .collect::<Vec<_>>()
            })
            .await
            {
                Ok(results) => {
                    let mut scanned = 0usize;
                    let mut changed = 0usize;
                    let mut elapsed_ms = 0u64;
                    for (dir, outcome) in results {
                        runtime.record_scan(dir.as_path(), outcome);
                        scanned = scanned.saturating_add(outcome.scanned);
                        changed = changed.saturating_add(outcome.changed);
                        elapsed_ms = elapsed_ms.saturating_add(outcome.elapsed_ms);
                        if outcome.changed > 0 {
                            match runtime.try_reserve_promotion(dir.as_path()) {
                                fd_rdd::event::tiered_watch::PromotionDecision::SendAdd => {
                                    if watch_command_tx
                                        .send(WatchCommand::Add(dir.clone()))
                                        .await
                                        .is_err()
                                    {
                                        runtime.rollback_promote(dir.as_path());
                                    }
                                }
                                fd_rdd::event::tiered_watch::PromotionDecision::BudgetBlocked
                                | fd_rdd::event::tiered_watch::PromotionDecision::NotEligible => {}
                            }
                        }
                    }
                    tracing::debug!(
                        "tiered warm scan complete: files={} changed={} elapsed_ms={}",
                        scanned,
                        changed,
                        elapsed_ms
                    );
                }
                Err(e) => {
                    tracing::warn!("tiered warm scan task failed: {}", e);
                }
            }
        }
    });
}

fn initial_hot_candidates(
    roots: &[PathBuf],
    hot_dirs: &[PathBuf],
    exclude_dirs: &[String],
) -> Vec<PathBuf> {
    hot_dirs
        .iter()
        .filter(|p| p.is_dir())
        .filter(|p| !fd_rdd::util::path_has_excluded_component(p, exclude_dirs))
        .filter(|p| roots.iter().any(|root| path_is_under_or_equal(p, root)))
        .cloned()
        .collect()
}

fn path_is_under_or_equal(path: &std::path::Path, root: &std::path::Path) -> bool {
    path == root || path.starts_with(root)
}

fn estimate_recursive_dir_count(
    root: &std::path::Path,
    cap: usize,
    exclude_dirs: &[String],
) -> usize {
    fn walk(path: &std::path::Path, cap: usize, exclude_dirs: &[String], count: &mut usize) {
        if *count >= cap {
            return;
        }
        *count = (*count).saturating_add(1);
        let Ok(entries) = std::fs::read_dir(path) else {
            return;
        };
        for entry in entries.flatten() {
            if *count >= cap {
                return;
            }
            let path = entry.path();
            if fd_rdd::util::path_has_excluded_component(&path, exclude_dirs) {
                continue;
            }
            if entry.file_type().is_ok_and(|ft| ft.is_dir()) {
                walk(&path, cap, exclude_dirs, count);
            }
        }
    }

    let mut count = 0usize;
    walk(root, cap, exclude_dirs, &mut count);
    count.max(1)
}
