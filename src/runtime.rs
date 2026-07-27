//! Daemon runtime: the orchestration logic that was previously in `main.rs`.
//!
//! This module contains everything needed to bootstrap the indexer daemon
//! (config resolution, index init, watch planning, background workers, query
//! service, signal handling, and graceful shutdown) so that `main.rs` stays a
//! thin binary entry point.

use crate::config::{
    default_snapshot_path, default_socket_path, Config, RuntimeProfile, TieredWatchProfile,
    WatchMode,
};
use crate::event::ignore_filter::IgnoreFilter;
use crate::event::proc_sampler::{
    sample_proc_write_dirs, ProcSamplerConfig, ProcSamplerCursor, ProcSamplerReport,
};
use crate::event::sync::{DirtyQueueEntry, DirtyReason, DirtyScope};
use crate::event::tiered_watch::{
    rotating_cold_window_fast_scan_max_cost, EphemeralWatchConfig, EphemeralWatchDecision,
    FastScanLeaseKind, RotatingColdWindowAction, RotatingColdWindowActionKind,
    TieredWatchDebugDump, TieredWatchDebugSummary, WatchTier,
};
use crate::event::watcher::check_inotify_limit;
use crate::event::{EventPipeline, TieredWatchRuntime, WatchCommand};
use crate::fs_policy::MountTable;
use crate::index::TieredIndex;
use crate::query::SocketServer;
use crate::query::{HealthTelemetry, QueryServer};
use crate::stats::{
    EventPipelineStats, MetricsHealthSnapshot, MetricsMemorySnapshot, MetricsReporter,
    MetricsRuntimeSnapshot, MetricsSnapshot, WatchStateReport,
};
use crate::storage::snapshot::{
    quarantine_sidecar_path_for, read_recovery_runtime_state, stable_prev_v7_path_for,
    stable_v7_path_for, write_recovery_runtime_state, RecoveryRuntimeState, SnapshotStore,
};
use crate::storage::wal::WalDurability;
use crate::util::{estimate_notify_recursive_watch_count, normalize_exclude_dirs, unix_secs};
use clap::Parser;
use std::collections::HashSet;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tracing::info;

// ---------------------------------------------------------------------------
// Named constants (extracted from magic numbers — M6)
// ---------------------------------------------------------------------------

/// Default memory-report interval in seconds.
const DEFAULT_REPORT_INTERVAL_SECS: u64 = 60;
/// Default watcher event channel capacity.
const DEFAULT_CHANNEL_SIZE: usize = 65_536;
/// Default watcher event debounce window in milliseconds.
const DEFAULT_DEBOUNCE_MS: u64 = 10;
/// Default metrics-file report interval in seconds.
const DEFAULT_METRICS_REPORT_INTERVAL_SECS: u64 = 30;
/// Dirty-queue batch size fetched per iteration.
const DIRTY_QUEUE_BATCH_SIZE: usize = 16;
/// Idle poll interval for the dirty-queue loop (milliseconds).
const DIRTY_QUEUE_IDLE_POLL_MS: u64 = 250;
/// 低优先级 paced 轮转批在 blocking 线程内联续跑的单次预算。
const DIRTY_QUEUE_PACED_INLINE_BUDGET: Duration = Duration::from_millis(300);
/// 内联续跑允许等待的最大切片间隔；超过则交回主循环调度。
const DIRTY_QUEUE_PACED_INLINE_MAX_SLEEP: Duration = Duration::from_millis(50);
/// Minimum content-index worker interval (seconds).
const CONTENT_INDEX_INTERVAL_MIN_SECS: u64 = 30;
/// Maximum content-index worker interval (seconds).
const CONTENT_INDEX_INTERVAL_MAX_SECS: u64 = 300;
/// Minimum fast-scan bootstrap target interval (seconds).
const FAST_SCAN_MIN_TARGET_SECS: u64 = 60;

#[derive(Parser, Debug)]
#[command(
    name = "fd-rdd",
    version,
    about = "fd-rdd: atomic-snapshot file indexer"
)]
pub struct Args {
    /// 要索引的根目录（可重复传入）；必须至少指定一个（可以是 $HOME）
    #[arg(long = "root", value_name = "PATH")]
    pub roots: Vec<PathBuf>,

    /// 快照路径（默认: $XDG_RUNTIME_DIR/fd-rdd/index.db，回退到 /run/user/$UID/... 或 /tmp/fd-rdd-$UID/...）
    ///
    /// - legacy 单文件：index.db（兼容读取 v2~v6；v6 为 mmap 段式容器）
    /// - LSM 目录：同路径派生的 index.d/（MANIFEST.bin + seg-*.db/.del + events.wal）
    #[arg(long, value_name = "PATH")]
    pub snapshot_path: Option<PathBuf>,

    /// 将 `.` 开头的文件/目录纳入冷启动全扫、后台重建与增量补扫
    #[arg(long)]
    pub include_hidden: bool,

    /// HTTP 查询端口
    #[arg(long)]
    pub http_port: Option<u16>,

    /// Unix domain socket 查询地址（可选）：用于流式输出（避免 HTTP/JSON 聚合带来的峰值）
    #[arg(long, value_name = "PATH")]
    pub uds_socket: Option<PathBuf>,

    /// 快照写入间隔（秒）
    #[arg(long)]
    pub snapshot_interval_secs: Option<u64>,

    /// 内存报告间隔（秒）
    #[arg(long)]
    pub report_interval_secs: Option<u64>,

    /// watcher 事件 channel 容量（越大越不容易 overflow，但会占用更多内存）
    /// 默认 65536，足以应对 git clone 等批量操作；降低此值可减少内存占用但可能丢失事件。
    #[arg(long)]
    pub event_channel_size: Option<usize>,

    /// watcher 事件 debounce 窗口（毫秒）
    #[arg(long)]
    pub debounce_ms: Option<u64>,

    /// watcher 忽略路径前缀（可重复）；用于排除 snapshot/log 等“自触发”路径
    ///
    /// 说明：fd-rdd 会默认忽略 `--snapshot-path` 以及派生的 `index.d/`；这里用于补充额外忽略项。
    #[arg(long = "ignore-path", value_name = "PATH")]
    pub ignore_paths: Vec<PathBuf>,

    /// 全局排除的目录名（可重复）。命中这些目录名的路径不会进入索引。
    #[arg(long = "exclude-dir", value_name = "NAME")]
    pub exclude_dirs: Vec<String>,

    /// 禁用 `.gitignore` / `.ignore` / git exclude / global gitignore 规则
    #[arg(long)]
    pub no_ignore: bool,

    /// 跟随符号链接（默认不跟随）。启用后扫描和 watcher 会进入符号链接指向的目录。
    /// 注意：已有 inode 去重可防止无限递归，但跟随可能导致索引范围远超预期。
    #[arg(long)]
    pub follow_symlinks: bool,

    /// 禁用文件系统 watcher，仅使用已加载快照和手动 /scan 更新。
    #[arg(long)]
    pub no_watch: bool,

    /// watcher 模式：recursive（现有递归监听）、tiered（预算受控热点监听）、off（关闭）。
    #[arg(long, value_parser = ["recursive", "tiered", "off"])]
    pub watch_mode: Option<String>,

    /// 运行时资源配置档：default 或 memory_light。
    #[arg(long, value_parser = ["default", "memory_light", "memory-light"])]
    pub runtime_profile: Option<String>,

    /// WAL 持久化模式：flush-only、sync-interval、sync-always。
    #[arg(long, value_parser = ["flush-only", "sync-interval", "sync-always"])]
    pub wal_durability: Option<String>,

    /// sync-interval 模式下的最大 sync 间隔（毫秒）。
    #[arg(long)]
    pub wal_sync_interval_ms: Option<u64>,

    /// sync-interval 模式下的最大批量记录数。
    #[arg(long)]
    pub wal_sync_batch_records: Option<usize>,
}

// ---------------------------------------------------------------------------
// EffectiveSettings — resolved CLI + config overrides (H2 refactor)
// ---------------------------------------------------------------------------

/// All runtime-effective settings after merging CLI args with the config file.
struct EffectiveSettings {
    roots: Vec<PathBuf>,
    ignore_enabled: bool,
    include_hidden: bool,
    follow_symlinks: bool,
    effective_watch_mode: WatchMode,
    watch_enabled: bool,
    http_port: u16,
    snapshot_interval_secs: u64,
    runtime_profile: RuntimeProfile,
    wal_durability: WalDurability,
    report_interval_secs: u64,
    event_channel_size: usize,
    debounce_ms: u64,
    exclude_dirs: Vec<String>,
}

// ---------------------------------------------------------------------------
// run() — daemon entry point (called by main.rs)
// ---------------------------------------------------------------------------

/// Boot the full fd-rdd daemon: config, index, watchers, query server, and
/// graceful shutdown. Returns `Ok(())` after a clean shutdown.
pub async fn run(args: Args) -> anyhow::Result<()> {
    let cli_watch_mode = parse_watch_mode(args.watch_mode.as_deref())?;
    let cli_runtime_profile = parse_runtime_profile(args.runtime_profile.as_deref())?;

    let cfg = build_config(&args, cli_watch_mode, cli_runtime_profile);

    info!(
        "Starting fd-rdd v{}: atomic-snapshot file indexer",
        env!("CARGO_PKG_VERSION")
    );

    let settings = resolve_effective_settings(&args, &cfg, cli_watch_mode, cli_runtime_profile)?;

    // 2) 快照存储
    let snapshot_path = args
        .snapshot_path
        .clone()
        .unwrap_or_else(default_snapshot_path);
    let store = Arc::new(SnapshotStore::new(snapshot_path));

    // 3) 从快照加载或空索引启动
    let (index, root_case_policies) = load_and_init_index(store.as_ref(), &settings, &cfg).await?;

    // 4) 启动修复 + 全量构建
    run_startup_repair(&index, &cfg, store.as_ref(), &root_case_policies);

    // 5) 忽略过滤器 + 启动对账
    let (ignore_filter, startup_ignore_paths) =
        build_ignore_and_reconcile(&args, &index, store.as_ref(), &settings);

    // 6) 观察计划 + 分层运行时
    let watch_plan = build_watch_plan(
        settings.effective_watch_mode,
        &index.roots,
        &cfg.tiered_watch,
        &settings.exclude_dirs,
    );
    let tiered_runtime = build_tiered_runtime(&settings, &watch_plan, &cfg, store.as_ref(), &index);
    let watch_state = Arc::new(watch_plan.state.clone());

    // 7) 事件管道
    let (pipeline, watch_command_tx) = build_event_pipeline(
        &index,
        &settings,
        &ignore_filter,
        &startup_ignore_paths,
        &watch_plan,
        &tiered_runtime,
    )
    .await?;

    // 8) 后台工作循环
    spawn_background_workers(
        &index,
        &tiered_runtime,
        &watch_command_tx,
        &cfg,
        &settings,
        &startup_ignore_paths,
    );

    // 9) 查询服务提供者
    let watch_enabled = settings.watch_enabled;
    let health_provider: Arc<dyn Fn() -> HealthTelemetry + Send + Sync> = {
        let index = index.clone();
        let pipeline = pipeline.clone();
        let health_watch_state = watch_state.clone();
        let health_tiered_runtime = tiered_runtime.clone();
        Arc::new(move || {
            build_health_telemetry(
                &index,
                &pipeline,
                &health_watch_state,
                &health_tiered_runtime,
                watch_enabled,
            )
        })
    };
    let stats_provider: Arc<dyn Fn() -> EventPipelineStats + Send + Sync> = {
        let pipeline = pipeline.clone();
        Arc::new(move || pipeline.stats())
    };
    let watch_state_provider: Arc<dyn Fn() -> WatchStateReport + Send + Sync> = {
        let index = index.clone();
        let watch_state = watch_state.clone();
        let tiered_runtime = tiered_runtime.clone();
        Arc::new(move || build_watch_state_report(&index, &watch_state, &tiered_runtime))
    };
    let tiered_watch_debug_provider: Arc<
        dyn Fn(Option<String>) -> TieredWatchDebugDump + Send + Sync,
    > = {
        let tiered_runtime = tiered_runtime.clone();
        Arc::new(move |root_filter| build_tiered_watch_debug_dump(&tiered_runtime, root_filter))
    };

    // 10) 启动 HTTP/UDS 查询服务 + 定期任务
    serve_queries(
        &index,
        &store,
        &cfg,
        &args,
        &health_provider,
        &stats_provider,
        &watch_state_provider,
        tiered_watch_debug_provider,
        &tiered_runtime,
        settings.http_port,
        settings.snapshot_interval_secs,
        settings.report_interval_secs,
    );

    info!(
        "fd-rdd ready. Query via: http://localhost:{}/search?q=keyword",
        settings.http_port
    );

    // 11) 优雅退出：SIGINT/SIGTERM → 最终快照
    shutdown_signal().await?;
    handle_shutdown(&index, &store, &tiered_runtime, &cfg).await?;

    Ok(())
}

// ---------------------------------------------------------------------------
// Extracted sub-functions (H2 refactor)
// ---------------------------------------------------------------------------

/// CLI arg parsing + config file loading + first-run creation.
fn build_config(
    args: &Args,
    cli_watch_mode: Option<WatchMode>,
    cli_runtime_profile: Option<RuntimeProfile>,
) -> Config {
    let config_path = Config::config_path();
    let is_first_run = config_path.as_ref().map(|p| !p.exists()).unwrap_or(true);

    if is_first_run {
        if args.roots.is_empty() {
            eprintln!("错误: 首次启动必须通过 --root <PATH> 指定至少一个索引根目录");
            eprintln!("示例: fd-rdd --root $HOME");
            std::process::exit(1);
        }

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
            runtime_profile: cli_runtime_profile.unwrap_or_default(),
            ignore_enabled: !args.no_ignore,
            ..Config::default()
        };
        if let Some(socket) = &args.uds_socket {
            cfg.socket_path = Some(socket.clone());
        }

        if let Err(e) = cfg.save() {
            tracing::warn!("无法保存默认配置文件: {}", e);
        } else {
            tracing::info!("已创建默认配置文件");
        }

        cfg
    } else {
        Config::load().unwrap_or_else(|e| {
            tracing::warn!("配置文件加载失败，使用默认值: {}", e);
            Config::default()
        })
    }
}

/// Merge CLI overrides with config file values into effective settings.
fn resolve_effective_settings(
    args: &Args,
    cfg: &Config,
    cli_watch_mode: Option<WatchMode>,
    cli_runtime_profile: Option<RuntimeProfile>,
) -> anyhow::Result<EffectiveSettings> {
    let mut roots = args.roots.clone();
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
    let runtime_profile = cli_runtime_profile.unwrap_or(cfg.runtime_profile);
    let wal_durability_mode = args
        .wal_durability
        .as_deref()
        .unwrap_or(cfg.wal_durability.as_str());
    let wal_sync_interval_ms = args
        .wal_sync_interval_ms
        .unwrap_or(cfg.wal_sync_interval_ms)
        .max(1);
    let wal_sync_batch_records = args
        .wal_sync_batch_records
        .unwrap_or(cfg.wal_sync_batch_records)
        .max(1);
    let wal_durability = parse_wal_durability(
        wal_durability_mode,
        wal_sync_interval_ms,
        wal_sync_batch_records,
    )?;
    tracing::info!("runtime profile: {}", runtime_profile.as_str());
    let report_interval_secs = args
        .report_interval_secs
        .unwrap_or(DEFAULT_REPORT_INTERVAL_SECS);
    let event_channel_size = args.event_channel_size.unwrap_or(DEFAULT_CHANNEL_SIZE);
    let debounce_ms = args.debounce_ms.unwrap_or(DEFAULT_DEBOUNCE_MS);
    let mut exclude_dirs = cfg.exclude_dirs.clone();
    exclude_dirs.extend(args.exclude_dirs.clone());
    let exclude_dirs = normalize_exclude_dirs(exclude_dirs);

    Ok(EffectiveSettings {
        roots,
        ignore_enabled,
        include_hidden,
        follow_symlinks,
        effective_watch_mode,
        watch_enabled,
        http_port,
        snapshot_interval_secs,
        runtime_profile,
        wal_durability,
        report_interval_secs,
        event_channel_size,
        debounce_ms,
        exclude_dirs,
    })
}

/// Load snapshot + initialize index with all config settings applied.
async fn load_and_init_index(
    store: &SnapshotStore,
    settings: &EffectiveSettings,
    cfg: &Config,
) -> anyhow::Result<(
    Arc<TieredIndex>,
    Vec<crate::diagnostics::RootCasePolicyDiagnostics>,
)> {
    let index = TieredIndex::load_with_options_follow_excludes_fs_policy_and_io_governor(
        store,
        settings.roots.clone(),
        settings.include_hidden,
        settings.ignore_enabled,
        settings.follow_symlinks,
        settings.exclude_dirs.clone(),
        cfg.fs_policy.clone(),
        cfg.io_governor.clone(),
    )
    .await?;
    index.apply_runtime_profile_settings(settings.runtime_profile.settings());
    index.apply_content_index_config(cfg.content_index.clone());
    index.apply_lazy_validation_config(
        cfg.lazy_validation_enabled,
        cfg.lazy_validation_cache_entries,
        cfg.lazy_validation_ttl_secs,
        cfg.lazy_validation_stat_per_sec,
    );
    index.apply_query_config(cfg.query);
    index.set_runtime_subtree_tombstone_ttl_secs(
        cfg.tiered_watch.runtime_subtree_tombstone_ttl_secs,
    );
    index.spawn_lazy_validation_worker();
    let root_case_policies = index.refresh_root_case_policy_diagnostics();
    index.apply_mmap_warmup_config(cfg.mmap_warmup.clone());
    if let Err(e) = index.attach_wal(store) {
        tracing::warn!("WAL attach failed: {e}");
    }
    index.set_wal_durability(settings.wal_durability);
    index.set_stable_snapshot_enabled(cfg.stable_snapshot_enabled);
    Ok((index, root_case_policies))
}

/// Run startup repair, mark runtime state, and spawn full build if needed.
fn run_startup_repair(
    index: &Arc<TieredIndex>,
    cfg: &Config,
    store: &SnapshotStore,
    root_case_policies: &[crate::diagnostics::RootCasePolicyDiagnostics],
) {
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
    index.enqueue_startup_deferred_repair();
    if let Err(error) = mark_runtime_state(
        store.path(),
        false,
        &index.recovery_status().report.snapshot_source,
        "running",
        root_case_policies.to_vec(),
    ) {
        tracing::warn!("failed to mark runtime state as running: {error}");
    }
    index.spawn_quarantine_verify_worker(quarantine_sidecar_path_for(store.path()));

    // 若没有可信快照，或启动 repair 判断差异过大，后台全量构建。
    let needs_full_build =
        loaded_from_empty_snapshot || repair_stats.escalated || index.file_count() == 0;
    if needs_full_build && !index.rebuild_in_progress() {
        index.spawn_startup_full_build();
    }
}

/// Build ignore filter, startup ignore paths, and enqueue startup reconcile.
fn build_ignore_and_reconcile(
    args: &Args,
    index: &Arc<TieredIndex>,
    store: &SnapshotStore,
    settings: &EffectiveSettings,
) -> (Option<IgnoreFilter>, Vec<PathBuf>) {
    let ignore_filter = if settings.ignore_enabled {
        Some(IgnoreFilter::from_roots(&index.roots))
    } else {
        None
    };
    let mut startup_ignore_paths = args.ignore_paths.clone();
    startup_ignore_paths.push(store.path().to_path_buf());
    startup_ignore_paths.push(store.derived_lsm_dir_path());
    let startup_reconcile_cutoff_ns = startup_reconcile_cutoff_ns_for_source(
        store.path(),
        &index.recovery_status().report.snapshot_source,
    );
    if settings.watch_enabled
        && index.file_count() > 0
        && startup_reconcile_cutoff_ns > 0
        && index.recovery_status().report.startup_scan_required
    {
        index.enqueue_dirty(
            DirtyScope::All {
                cutoff_ns: startup_reconcile_cutoff_ns,
            },
            DirtyReason::StartupRepair,
        );
    }
    (ignore_filter, startup_ignore_paths)
}

/// Create the tiered watch runtime and restore its fast-scan registry.
fn build_tiered_runtime(
    settings: &EffectiveSettings,
    watch_plan: &WatchPlan,
    cfg: &Config,
    store: &SnapshotStore,
    index: &Arc<TieredIndex>,
) -> Option<Arc<TieredWatchRuntime>> {
    let tiered_runtime = if settings.effective_watch_mode == WatchMode::Tiered {
        Some(Arc::new(
            TieredWatchRuntime::new_with_l0_max_cost_and_ephemeral(
                watch_plan.l0_roots.clone(),
                watch_plan.l1_roots.clone(),
                cfg.tiered_watch.max_watch_dirs.max(1),
                effective_l0_max_cost_per_root(&cfg.tiered_watch),
                cfg.tiered_watch.scan_items_per_sec,
                cfg.tiered_watch.scan_ms_per_tick,
                cfg.tiered_watch.ephemeral_watch_budget,
            ),
        ))
    } else {
        None
    };
    if let Some(runtime) = tiered_runtime.as_ref() {
        runtime.apply_fast_scan_config(&cfg.tiered_watch);
        runtime.apply_rotating_cold_window_config(&cfg.tiered_watch);
        runtime.apply_waterline_config(&cfg.tiered_watch);
        runtime.set_proc_sampler_enabled(
            cfg.proc_sampler.enabled
                && settings.watch_enabled
                && settings.effective_watch_mode == WatchMode::Tiered,
        );
        let restore_report = runtime.restore_fast_scan_registry(
            store.path(),
            &cfg.tiered_watch,
            index.recovery_status().report.previous_clean_shutdown,
            &index.recovery_status().report.snapshot_source,
            index.recovery_status().report.wal_checkpoint_used,
            &MountTable::current().unwrap_or_default(),
        );
        if restore_report.loaded_entries > 0 {
            tracing::info!(
                "fast scan hotset registry restore: trusted={} active={} unknown={} rejected={} reason={}",
                restore_report.trusted,
                restore_report.restored_active,
                restore_report.restored_unknown,
                restore_report.rejected_entries,
                restore_report.reason
            );
        }
        runtime.seed_fast_scan_explicit_leases(cfg.tiered_watch.hot_dirs.clone());
    }
    tiered_runtime
}

/// Build and start the event pipeline, returning the pipeline and command sender.
async fn build_event_pipeline(
    index: &Arc<TieredIndex>,
    settings: &EffectiveSettings,
    ignore_filter: &Option<IgnoreFilter>,
    startup_ignore_paths: &[PathBuf],
    watch_plan: &WatchPlan,
    tiered_runtime: &Option<Arc<TieredWatchRuntime>>,
) -> anyhow::Result<(Arc<EventPipeline>, tokio::sync::mpsc::Sender<WatchCommand>)> {
    let mut pipeline = EventPipeline::new_with_config_and_ignores(
        index.clone(),
        settings.debounce_ms,
        settings.event_channel_size,
        startup_ignore_paths.to_vec(),
    )
    .with_ignore_filter(ignore_filter.clone())
    .with_exclude_dirs(settings.exclude_dirs.clone())
    .with_tiered_runtime(tiered_runtime.clone());
    if let Some(roots) = watch_plan.watch_roots.clone() {
        pipeline = pipeline.with_watch_roots(roots);
    }
    let pipeline = Arc::new(pipeline);
    let watch_command_tx = pipeline.watch_command_sender();
    if settings.watch_enabled {
        pipeline.start().await?;
    } else {
        tracing::warn!(
            "Filesystem watcher disabled; index updates require manual /scan or rebuild"
        );
    }
    Ok((pipeline, watch_command_tx))
}

/// Spawn all background worker loops: dirty queue, tiered scan, fast scan, proc sampler.
fn spawn_background_workers(
    index: &Arc<TieredIndex>,
    tiered_runtime: &Option<Arc<TieredWatchRuntime>>,
    watch_command_tx: &tokio::sync::mpsc::Sender<WatchCommand>,
    cfg: &Config,
    settings: &EffectiveSettings,
    startup_ignore_paths: &[PathBuf],
) {
    spawn_dirty_queue_loop(
        index.clone(),
        tiered_runtime.clone(),
        watch_command_tx.clone(),
        cfg.tiered_watch.clone(),
        settings.exclude_dirs.clone(),
        startup_ignore_paths.to_vec(),
    );
    index.set_cold_sweep_period_estimate_from_tiered_policy(
        cfg.tiered_watch.l2_scan_interval_secs,
        cfg.tiered_watch.l3_scan_policy,
        cfg.tiered_watch.l3_scan_interval_secs,
    );
    if cfg.content_index.enable {
        index.spawn_content_index_worker(Duration::from_secs(
            settings.snapshot_interval_secs.clamp(
                CONTENT_INDEX_INTERVAL_MIN_SECS,
                CONTENT_INDEX_INTERVAL_MAX_SECS,
            ),
        ));
    }
    if settings.effective_watch_mode == WatchMode::Tiered {
        if let Some(runtime) = tiered_runtime.clone() {
            spawn_tiered_scan_loop(
                index.clone(),
                runtime.clone(),
                watch_command_tx.clone(),
                cfg.tiered_watch.clone(),
            );
            spawn_ephemeral_watch_maintenance_loop(
                index.clone(),
                runtime,
                watch_command_tx.clone(),
                cfg.tiered_watch.clone(),
            );
        }
        if cfg.tiered_watch.l1_l2_fast_scan_enabled {
            if let Some(runtime) = tiered_runtime.clone() {
                spawn_tiered_fast_scan_loop(index.clone(), runtime, cfg.tiered_watch.clone());
            }
        }
        if cfg.tiered_watch.rotating_cold_window_enabled {
            if let Some(runtime) = tiered_runtime.clone() {
                spawn_rotating_cold_window_loop(
                    index.clone(),
                    runtime,
                    watch_command_tx.clone(),
                    cfg.tiered_watch.clone(),
                    settings.exclude_dirs.clone(),
                );
            }
        }
        if cfg.proc_sampler.enabled {
            if let Some(runtime) = tiered_runtime.clone() {
                spawn_proc_sampler_loop(
                    runtime,
                    watch_command_tx.clone(),
                    cfg.proc_sampler.clone(),
                    index.roots.clone(),
                    startup_ignore_paths.to_vec(),
                    settings.exclude_dirs.clone(),
                    cfg.tiered_watch.clone(),
                );
            }
        }
    }
}

/// Build a `HealthTelemetry` snapshot from current index/pipeline/watch state.
///
/// Extracted from the `health_provider` closure body.
fn build_health_telemetry(
    index: &TieredIndex,
    pipeline: &EventPipeline,
    plan_watch_state: &WatchStateReport,
    tiered_runtime: &Option<Arc<TieredWatchRuntime>>,
    watch_enabled: bool,
) -> HealthTelemetry {
    let stats = pipeline.stats();
    let mut watch_state = tiered_runtime
        .as_ref()
        .map(|runtime| runtime.report())
        .unwrap_or_else(|| plan_watch_state.clone());
    apply_watch_plan_static_fields(&mut watch_state, plan_watch_state);
    let recovery = index.recovery_status();
    let lazy_validation = index.lazy_validation_report();
    let event_watcher_degraded = stats.watcher_degraded;
    let event_degraded_roots = stats.degraded_roots;
    let tiered_unwatched_dirs = watch_state
        .l1_dirs
        .saturating_add(watch_state.l2_dirs)
        .saturating_add(watch_state.l3_dirs);
    let tiered_degraded = tiered_unwatched_dirs > 0;
    HealthTelemetry {
        last_snapshot_time: index.last_snapshot_time(),
        watch_enabled,
        watch_failures: stats.watch_failures,
        watcher_degraded: event_watcher_degraded || tiered_degraded,
        degraded_roots: event_degraded_roots.saturating_add(tiered_unwatched_dirs),
        event_watcher_degraded,
        event_degraded_roots,
        tiered_degraded,
        tiered_unwatched_dirs,
        overflow_drops: stats.overflow_drops,
        rescan_signals: stats.rescan_signals,
        snapshot_source: recovery.report.snapshot_source,
        wal_events_replayed: recovery.report.wal_events_replayed,
        wal_sealed_used: recovery.report.wal_sealed_used,
        wal_truncated_tail_records: recovery.report.wal_truncated_tail_records,
        wal_gap_detected: recovery.report.wal_gap_detected,
        wal_checkpoint_used: recovery.report.wal_checkpoint_used,
        wal_durability: index.wal_durability().label().to_string(),
        wal_sync_interval_ms: index.wal_durability().sync_interval_ms(),
        wal_sync_batch_records: index.wal_durability().sync_batch_records(),
        startup_scan_required: recovery.report.startup_scan_required,
        deferred_repair: recovery.report.deferred_repair,
        deferred_dirty_dir_count: recovery.report.deferred_dirty_dirs.len(),
        deferred_unknown_scope: recovery.report.deferred_unknown_scope,
        wal_tail_dirty_dir_count: recovery.report.deferred_dirty_dirs.len(),
        deferred_repair_queue_len: index.deferred_repair_queue_len(),
        cold_sweep_last_completed: index.cold_sweep_last_completed(),
        cold_sweep_period_estimate: index.cold_sweep_period_estimate(),
        dirty_backlog: index.dirty_queue_len(),
        lazy_validation_pending: lazy_validation.pending,
        lazy_validation_rate_limited: lazy_validation.rate_limited,
        lazy_validation_cache_hits: lazy_validation.cache_hits,
        lazy_validation_queue_full: lazy_validation.queue_full,
        lazy_validation_completed: lazy_validation.completed,
        lazy_validation_stale_hits: lazy_validation.stale_hits,
        recovery_requires_repair: recovery.report.requires_repair,
        recovery_requires_rebuild: recovery.report.requires_rebuild,
        recovery_soft_repair_needed: recovery.report.soft_repair_needed,
        recovery_hard_rebuild_needed: recovery.report.hard_rebuild_needed,
        recovery_reasons: recovery.report.reasons,
        recovery_soft_reasons: recovery.report.soft_reasons,
        recovery_hard_reasons: recovery.report.hard_reasons,
        recovery_reason_counts: recovery.report.repair_reason_counts,
        recovery_audit: recovery.report.audit,
        startup_repair_ran: recovery.repair.ran,
        startup_repair_escalated: recovery.repair.escalated,
        startup_repair_scanned: recovery.repair.scanned,
        startup_repair_changed: recovery.repair.changed,
        startup_repair_budget_ms: recovery.repair.budget_ms,
        startup_repair_budget_exhausted: recovery.repair.budget_exhausted,
        startup_repair_escalation_reason: recovery.repair.escalation_reason,
        last_clean_shutdown: recovery.report.previous_clean_shutdown,
        l1_dirs: watch_state.l1_dirs,
        l2_dirs: watch_state.l2_dirs,
        l3_dirs: watch_state.l3_dirs,
        max_watch_dirs: watch_state.max_watch_dirs,
        l0_max_cost_per_root: watch_state.l0_max_cost_per_root,
        watch_budget_utilization_pct: watch_state.watch_budget_utilization_pct,
        promotion_budget_blocked: watch_state.promotion_budget_blocked,
        watch_profile: watch_state.watch_profile,
        system_max_user_watches: watch_state.system_max_user_watches,
        required_watch_cost: watch_state.required_watch_cost,
        watch_budget_shortfall: watch_state.watch_budget_shortfall,
        strict_coverage_ok: watch_state.strict_coverage_ok,
        strict_coverage_failure: watch_state.strict_coverage_failure,
        strict_fail_on_budget_exceeded: watch_state.strict_fail_on_budget_exceeded,
        strict_uncovered_dirs: watch_state.strict_uncovered_dirs,
        fast_scan_enabled: watch_state.fast_scan_enabled,
        fast_scan_sla_ok: watch_state.fast_scan_sla_ok,
        fast_scan_local_strict_ok: watch_state.fast_scan_local_strict_ok,
        fast_scan_known_dirs: watch_state.fast_scan_known_dirs,
        fast_scan_local_trusted_dirs: watch_state.fast_scan_local_trusted_dirs,
        fast_scan_untrusted_dirs: watch_state.fast_scan_untrusted_dirs,
        fast_scan_hotset_lease_count: watch_state.fast_scan_hotset_lease_count,
        fast_scan_hotset_sentinel_count: watch_state.fast_scan_hotset_sentinel_count,
        fast_scan_explicit_lease_count: watch_state.fast_scan_explicit_lease_count,
        fast_scan_auto_lease_count: watch_state.fast_scan_auto_lease_count,
        fast_scan_lease_evictions: watch_state.fast_scan_lease_evictions,
        fast_scan_lease_renewals: watch_state.fast_scan_lease_renewals,
        fast_scan_initial_backfill_pending: watch_state.fast_scan_initial_backfill_pending,
        fast_scan_real_changed_dirs: watch_state.fast_scan_real_changed_dirs,
        fast_scan_apply_dropped_stale_batches: watch_state.fast_scan_apply_dropped_stale_batches,
        fast_scan_scan_workers_active: watch_state.fast_scan_scan_workers_active,
        fast_scan_io_budget_limited_count: watch_state.fast_scan_io_budget_limited_count,
        fast_scan_coverage_lag_p95_ms: watch_state.fast_scan_coverage_lag_p95_ms,
        fast_scan_budget_degraded: watch_state.fast_scan_budget_degraded,
        fast_scan_last_degraded_reason: watch_state.fast_scan_last_degraded_reason,
        proc_sampler_enabled: watch_state.proc_sampler_enabled,
        proc_sampler_last_duration_ms: watch_state.proc_sampler_last_duration_ms,
        proc_sampler_pids_seen: watch_state.proc_sampler_pids_seen,
        proc_sampler_pids_scanned: watch_state.proc_sampler_pids_scanned,
        proc_sampler_pids_denied: watch_state.proc_sampler_pids_denied,
        proc_sampler_fdinfo_read_count: watch_state.proc_sampler_fdinfo_read_count,
        proc_sampler_readlink_count: watch_state.proc_sampler_readlink_count,
        proc_sampler_write_fd_count: watch_state.proc_sampler_write_fd_count,
        proc_sampler_sampled_dirs: watch_state.proc_sampler_sampled_dirs,
        proc_sampler_triggered_watches: watch_state.proc_sampler_triggered_watches,
        proc_sampler_budget_exhausted: watch_state.proc_sampler_budget_exhausted,
        proc_sampler_unavailable: watch_state.proc_sampler_unavailable,
        waterline_soft_degraded: watch_state.waterline_soft_degraded,
        waterline_hard_degraded: watch_state.waterline_hard_degraded,
        waterline_effective_l3_scan_interval_secs: watch_state
            .waterline_effective_l3_scan_interval_secs,
        waterline_effective_rotating_budget: watch_state.waterline_effective_rotating_budget,
        diagnostics: crate::diagnostics::DiagnosticReport::default(),
    }
}

/// Build a `WatchStateReport` from current index/tiered-runtime state.
///
/// Extracted from the `watch_state_provider` closure body.
fn build_watch_state_report(
    index: &TieredIndex,
    plan_watch_state: &WatchStateReport,
    tiered_runtime: &Option<Arc<TieredWatchRuntime>>,
) -> WatchStateReport {
    if let Some(runtime) = tiered_runtime.as_ref() {
        runtime.set_dirty_queue_len(index.dirty_queue_len());
        let stats = index.stats_report();
        runtime.set_query_stale_hit_count(stats.query_stale_hit_count);
        runtime.set_query_permission_denied_count(stats.query_permission_denied_count);
    }
    let mut report = tiered_runtime
        .as_ref()
        .map(|runtime| runtime.report())
        .unwrap_or_else(|| plan_watch_state.clone());
    apply_watch_plan_static_fields(&mut report, plan_watch_state);
    let stats = index.stats_report();
    report.cold_validate_count = report
        .cold_validate_count
        .saturating_add(stats.cold_validate_count);
    let manifest = index.directory_manifest_report();
    report.directory_manifest_dirs = manifest.dirs;
    report.directory_manifest_skipped_scans = manifest.skipped_scans;
    report.directory_manifest_changed_scans = manifest.changed_scans;
    report.directory_manifest_untrusted_clock_bypass = manifest.untrusted_clock_bypass;
    report
}

/// Build a `TieredWatchDebugDump` from the tiered runtime (or empty if absent).
///
/// Extracted from the `tiered_watch_debug_provider` closure body.
fn build_tiered_watch_debug_dump(
    tiered_runtime: &Option<Arc<TieredWatchRuntime>>,
    root_filter: Option<String>,
) -> TieredWatchDebugDump {
    tiered_runtime
        .as_ref()
        .map(|rt| rt.debug_dump(root_filter.as_deref()))
        .unwrap_or_else(|| TieredWatchDebugDump {
            dirs: vec![],
            summary: TieredWatchDebugSummary {
                l0_dirs: 0,
                l1_dirs: 0,
                l2_dirs: 0,
                l3_dirs: 0,
                ephemeral_watch_dirs: 0,
                ephemeral_watch_cost: 0,
                ephemeral_watch_budget: 0,
                rotating_cold_window_active_dirs: 0,
                rotating_cold_window_cycle_id: 0,
                rotating_cold_window_budget: 0,
                total_event_score: 0,
            },
        })
}

/// Start HTTP query server, UDS query server, snapshot loop, memory report loop,
/// and metrics file report loop.
#[allow(clippy::too_many_arguments)]
fn serve_queries(
    index: &Arc<TieredIndex>,
    store: &Arc<SnapshotStore>,
    cfg: &Config,
    args: &Args,
    health_provider: &Arc<dyn Fn() -> HealthTelemetry + Send + Sync>,
    stats_provider: &Arc<dyn Fn() -> EventPipelineStats + Send + Sync>,
    watch_state_provider: &Arc<dyn Fn() -> WatchStateReport + Send + Sync>,
    tiered_watch_debug_provider: Arc<dyn Fn(Option<String>) -> TieredWatchDebugDump + Send + Sync>,
    tiered_runtime: &Option<Arc<TieredWatchRuntime>>,
    http_port: u16,
    snapshot_interval_secs: u64,
    report_interval_secs: u64,
) {
    // 6) 启动 HTTP 查询服务
    if !cfg.tiered_watch.l1_l2_fast_scan_query_leases_enabled {
        tracing::info!(
            "query-derived fast scan leases disabled by tiered_watch.l1_l2_fast_scan_query_leases_enabled"
        );
    }
    let query_server = QueryServer::new(index.clone())
        .with_snapshot_store(store.clone())
        .with_health_provider(health_provider.clone())
        .with_stats_provider(stats_provider.clone())
        .with_watch_state_provider(watch_state_provider.clone())
        .with_tiered_watch_debug_provider(tiered_watch_debug_provider)
        .with_query_fast_scan_leases_enabled(cfg.tiered_watch.l1_l2_fast_scan_query_leases_enabled)
        .with_fast_scan_lease_provider({
            let tiered_runtime = tiered_runtime.clone();
            Arc::new(move |dirs, kind| {
                if let Some(runtime) = tiered_runtime.as_ref() {
                    if kind == FastScanLeaseKind::Query {
                        runtime.grant_query_fast_scan_leases(dirs, None, 2);
                    } else {
                        runtime.grant_fast_scan_leases(dirs, kind, None, 2);
                    }
                }
            })
        });
    tokio::spawn(async move {
        if let Err(e) = query_server.run(http_port).await {
            tracing::error!("Query server error: {}", e);
        }
    });

    // 6.5) 启动 UDS 查询服务（CLI > config > default_socket_path()）
    let uds_path = args
        .uds_socket
        .clone()
        .or(cfg.socket_path.clone())
        .unwrap_or_else(default_socket_path);
    {
        let socket_server = SocketServer::new(index.clone());
        let path = uds_path;
        tokio::spawn(async move {
            if let Err(e) = socket_server.run(&path).await {
                tracing::error!("UDS query server error: {}", e);
            }
        });
    }

    // 7) 启动定期快照循环（每 snapshot_interval_secs 秒）
    let snap_index = index.clone();
    let snap_store = store.clone();
    tokio::spawn(async move {
        snap_index
            .snapshot_loop(snap_store, snapshot_interval_secs)
            .await;
    });

    // 8) 启动内存报告循环（每 report_interval_secs 秒）
    {
        let report_index = index.clone();
        let report_stats_provider = stats_provider.clone();

        tokio::spawn(async move {
            report_index
                .memory_report_loop(report_stats_provider, report_interval_secs)
                .await;
        });
    }

    // 8.5) 启动指标文件上报循环（每 30 秒）
    {
        let output_dir = std::path::PathBuf::from("./reports/metrics");
        let metrics_provider: Arc<dyn Fn() -> MetricsSnapshot + Send + Sync> = {
            let index = index.clone();
            let watch_state_provider = watch_state_provider.clone();
            let stats_provider = stats_provider.clone();
            let health_provider = health_provider.clone();
            Arc::new(move || {
                let watch = watch_state_provider();
                let pipeline = stats_provider();
                let runtime =
                    MetricsRuntimeSnapshot::from_reports(index.stats_report(), pipeline.clone());
                let memory_report = index.memory_report_light(pipeline);
                let memory = MetricsMemorySnapshot::from_report(&memory_report);
                let health = health_provider();
                let health = MetricsHealthSnapshot::from(&health);
                MetricsSnapshot::new(watch, runtime, memory, health)
            })
        };
        let reporter = MetricsReporter::new(
            metrics_provider,
            output_dir,
            DEFAULT_METRICS_REPORT_INTERVAL_SECS,
        );
        tokio::spawn(async move {
            reporter.run().await;
        });
    }
}

/// Write final snapshot, persist fast-scan registry, and mark clean shutdown.
async fn handle_shutdown(
    index: &Arc<TieredIndex>,
    store: &Arc<SnapshotStore>,
    tiered_runtime: &Option<Arc<TieredWatchRuntime>>,
    cfg: &Config,
) -> anyhow::Result<()> {
    index.begin_shutdown();
    info!("Shutting down, writing final snapshot...");
    mark_runtime_state(
        store.path(),
        false,
        &index.recovery_status().report.snapshot_source,
        "shutdown-in-progress",
        index.root_case_policy_diagnostics(),
    )?;
    if let Err(error) = index.snapshot_now(store.clone()).await {
        tracing::error!("Final snapshot failed: {error:#}");
        return Err(anyhow::anyhow!("final snapshot failed: {error:#}"));
    }
    if let Some(runtime) = tiered_runtime.as_ref() {
        let registry_wal_checkpoint = read_recovery_runtime_state(store.path())
            .map(|state| state.last_wal_seal_id)
            .unwrap_or(index.recovery_status().report.wal_checkpoint_used);
        let recovery_report = index.recovery_status().report;
        let registry_snapshot_source = if cfg.stable_snapshot_enabled {
            "stable".to_string()
        } else {
            recovery_report.snapshot_source
        };
        let entries = runtime
            .persist_fast_scan_registry(
                store.path(),
                &cfg.tiered_watch,
                &registry_snapshot_source,
                registry_wal_checkpoint,
                true,
            )
            .map_err(|error| {
                anyhow::anyhow!("final fast-scan registry persistence failed: {error:#}")
            })?;
        tracing::info!("persisted hotset fast scan registry entries={entries}");
    }
    mark_runtime_state(
        store.path(),
        true,
        &index.recovery_status().report.snapshot_source,
        "clean-shutdown",
        index.root_case_policy_diagnostics(),
    )?;
    info!("Goodbye.");
    Ok(())
}

fn mark_runtime_state(
    snapshot_path: &std::path::Path,
    clean_shutdown: bool,
    startup_source: &str,
    recovery_mode: &str,
    root_case_policies: Vec<crate::diagnostics::RootCasePolicyDiagnostics>,
) -> anyhow::Result<()> {
    let previous = read_recovery_runtime_state(snapshot_path).unwrap_or_else(|error| {
        tracing::warn!("failed to read previous recovery runtime state; replacing it: {error}");
        RecoveryRuntimeState::default()
    });
    let state = RecoveryRuntimeState {
        last_clean_shutdown: clean_shutdown,
        last_snapshot_unix_secs: unix_secs(),
        last_wal_seal_id: previous.last_wal_seal_id,
        last_startup_source: startup_source.to_string(),
        last_recovery_mode: recovery_mode.to_string(),
        root_case_policies,
    };
    write_recovery_runtime_state(snapshot_path, &state)
}

fn apply_watch_plan_static_fields(report: &mut WatchStateReport, plan: &WatchStateReport) {
    report.watch_profile = plan.watch_profile.clone();
    report.l0_max_cost_per_root = plan.l0_max_cost_per_root;
    report.system_max_user_watches = plan.system_max_user_watches;
    report.required_watch_cost = plan.required_watch_cost;
    report.watch_budget_shortfall = plan.watch_budget_shortfall;
    report.strict_coverage_ok = plan.strict_coverage_ok;
    report.strict_coverage_failure = plan.strict_coverage_failure;
    report.strict_fail_on_budget_exceeded = plan.strict_fail_on_budget_exceeded;
    report.strict_uncovered_dirs = plan.strict_uncovered_dirs.clone();
    for note in &plan.notes {
        if !report.notes.contains(note) {
            report.notes.push(note.clone());
        }
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

fn parse_wal_durability(
    mode: &str,
    interval_ms: u64,
    batch_records: usize,
) -> anyhow::Result<WalDurability> {
    match mode {
        "flush-only" => Ok(WalDurability::flush_only()),
        "sync-interval" => Ok(WalDurability::sync_interval(interval_ms, batch_records)),
        "sync-always" => Ok(WalDurability::sync_always()),
        other => anyhow::bail!(
            "invalid wal_durability: {other}; expected flush-only, sync-interval, or sync-always"
        ),
    }
}

fn watch_mode_label(mode: WatchMode) -> &'static str {
    match mode {
        WatchMode::Recursive => "recursive",
        WatchMode::Tiered => "tiered",
        WatchMode::Off => "off",
    }
}

fn parse_runtime_profile(value: Option<&str>) -> anyhow::Result<Option<RuntimeProfile>> {
    value
        .map(|value| value.parse().map_err(anyhow::Error::msg))
        .transpose()
}

fn watch_profile_label(profile: TieredWatchProfile) -> &'static str {
    match profile {
        TieredWatchProfile::Strict => "strict",
        TieredWatchProfile::Balanced => "balanced",
        TieredWatchProfile::LowPower => "low_power",
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
        .saturating_add(u64::from(duration.subsec_nanos()))
}

fn startup_reconcile_cutoff_ns_for_source(snapshot_path: &std::path::Path, source: &str) -> u64 {
    let path = match source {
        "stable" => stable_v7_path_for(snapshot_path),
        "stable-prev" => stable_prev_v7_path_for(snapshot_path),
        "legacy-v7" => snapshot_path.with_extension("v7"),
        _ => return 0,
    };
    modified_unix_ns(path.as_path())
}

fn build_watch_plan(
    mode: WatchMode,
    roots: &[PathBuf],
    tiered: &crate::config::TieredWatchConfig,
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
                watch_profile: "recursive".to_string(),
                strict_coverage_ok: true,
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
                watch_profile: "off".to_string(),
                strict_coverage_ok: true,
                notes: vec!["watcher disabled; use /scan or rebuild for updates".to_string()],
                ..WatchStateReport::default()
            },
        },
        WatchMode::Tiered => build_tiered_watch_plan(roots, tiered, exclude_dirs),
    }
}

fn build_tiered_watch_plan(
    roots: &[PathBuf],
    tiered: &crate::config::TieredWatchConfig,
    exclude_dirs: &[String],
) -> WatchPlan {
    let mut candidates = initial_hot_candidates(roots, &tiered.hot_dirs, exclude_dirs);
    let mut required = if tiered.profile == TieredWatchProfile::Strict {
        strict_required_candidates(roots, &tiered.strict_required_hot_dirs, exclude_dirs)
    } else {
        Vec::new()
    };
    if candidates.is_empty() {
        if tiered.profile == TieredWatchProfile::Strict && !required.is_empty() {
            candidates.extend(required.iter().cloned());
        } else {
            candidates.extend(roots.iter().filter(|p| p.is_dir()).cloned());
        }
    }
    candidates.extend(required.iter().cloned());
    candidates.sort();
    candidates.dedup();
    required.sort();
    required.dedup();

    let mut admitted = Vec::new();
    let mut scan_roots = Vec::new();
    let mut rejected = 0usize;
    let mut estimated_total = 0usize;
    let max_watch_dirs = tiered.max_watch_dirs.max(1);
    let l0_max_cost_per_root = effective_l0_max_cost_per_root(tiered);
    let estimate_cap = tiered_watch_plan_estimate_cap(tiered, l0_max_cost_per_root);
    let system_max_user_watches = usize::try_from(check_inotify_limit(0).unwrap_or(0)).unwrap_or(0);
    let mut strict_uncovered_dirs = Vec::new();
    let mut required_watch_cost = 0u64;

    let required_set = required.iter().collect::<std::collections::HashSet<_>>();
    for candidate in required.iter() {
        let estimated = estimate_notify_recursive_watch_count(candidate, estimate_cap);
        required_watch_cost = required_watch_cost.saturating_add(estimated as u64);
        if estimated <= l0_max_cost_per_root
            && estimated_total.saturating_add(estimated) <= max_watch_dirs
        {
            estimated_total = estimated_total.saturating_add(estimated);
            admitted.push((candidate.clone(), estimated));
        } else {
            rejected = rejected.saturating_add(1);
            strict_uncovered_dirs.push(candidate.to_string_lossy().to_string());
            scan_roots.push((candidate.clone(), estimated));
        }
    }
    for candidate in candidates.iter() {
        if required_set.contains(candidate) {
            continue;
        }
        let estimated = estimate_notify_recursive_watch_count(candidate, estimate_cap);
        if estimated <= l0_max_cost_per_root
            && estimated_total.saturating_add(estimated) <= max_watch_dirs
        {
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
    if l0_max_cost_per_root < max_watch_dirs {
        notes.push(format!(
            "single L0 root recursive watch cost is capped at {}",
            l0_max_cost_per_root
        ));
    }
    if admitted.is_empty() {
        notes.push("no L0 directories admitted under current budget".to_string());
    }
    let watch_budget_shortfall = required_watch_cost.saturating_sub(max_watch_dirs as u64);
    let strict_coverage_ok = tiered.profile != TieredWatchProfile::Strict
        || (strict_uncovered_dirs.is_empty() && watch_budget_shortfall == 0);
    let strict_coverage_failure =
        tiered.profile == TieredWatchProfile::Strict && !strict_coverage_ok;
    if tiered.profile == TieredWatchProfile::Strict {
        notes.push(format!(
            "strict profile requires L0 coverage for {} configured hot dir(s)",
            required.len()
        ));
        if strict_coverage_failure {
            notes.push(format!(
                "strict coverage incomplete: shortfall={} uncovered={}",
                watch_budget_shortfall,
                strict_uncovered_dirs.len()
            ));
        }
    }
    let logical_watch_cost = admitted
        .iter()
        .chain(scan_roots.iter())
        .map(|(_, cost)| *cost as u64)
        .fold(0u64, u64::saturating_add);
    let kernel_watch_cost = estimated_total as u64;
    let skipped_watch_cost = logical_watch_cost.saturating_sub(kernel_watch_cost);

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
            watch_profile: watch_profile_label(tiered.profile).to_string(),
            l0_dirs: admitted.len(),
            l1_dirs: rejected,
            l2_dirs: 0,
            l3_dirs: 0,
            watched_dirs_estimated: estimated_total,
            max_watch_dirs,
            l0_max_cost_per_root,
            system_max_user_watches,
            required_watch_cost,
            watch_budget_shortfall,
            strict_coverage_ok,
            strict_coverage_failure,
            strict_fail_on_budget_exceeded: tiered.strict_fail_on_budget_exceeded,
            strict_uncovered_dirs,
            l0_candidates: candidates.len(),
            l0_admitted: admitted.len(),
            l0_rejected: rejected,
            scan_backlog: rejected,
            scan_items_per_sec: tiered.scan_items_per_sec,
            scan_ms_per_tick: tiered.scan_ms_per_tick,
            ephemeral_watch_budget: tiered.ephemeral_watch_budget,
            logical_watch_cost,
            kernel_watch_cost,
            skipped_watch_cost,
            l0_watch_cost: kernel_watch_cost,
            l1_watch_cost: skipped_watch_cost,
            last_adjustment_unix_secs: now,
            notes,
            ..WatchStateReport::default()
        },
    }
}

fn effective_l0_max_cost_per_root(tiered: &crate::config::TieredWatchConfig) -> usize {
    let max_watch_dirs = tiered.max_watch_dirs.max(1);
    if tiered.l0_max_cost_per_root == 0 {
        max_watch_dirs
    } else {
        tiered.l0_max_cost_per_root.clamp(1, max_watch_dirs)
    }
}

fn tiered_watch_plan_estimate_cap(
    tiered: &crate::config::TieredWatchConfig,
    l0_max_cost_per_root: usize,
) -> usize {
    let l0_cap = tiered.max_watch_dirs.max(1).min(l0_max_cost_per_root);
    if !tiered.rotating_cold_window_enabled {
        return l0_cap;
    }
    let rotating_fast_scan_cap =
        rotating_cold_window_fast_scan_max_cost(tiered.rotating_cold_window_max_cost_per_root);
    l0_cap.max(rotating_fast_scan_cap)
}

fn strict_required_candidates(
    roots: &[PathBuf],
    required_hot_dirs: &[PathBuf],
    exclude_dirs: &[String],
) -> Vec<PathBuf> {
    required_hot_dirs
        .iter()
        .filter(|path| path.is_dir())
        .filter(|path| {
            !crate::util::path_has_excluded_component(path.as_path(), exclude_dirs)
                && roots
                    .iter()
                    .any(|root| path_is_under_or_equal(path.as_path(), root.as_path()))
        })
        .cloned()
        .collect()
}

/// 冷扫描条目的 manifest-skip 目录集：L2/L3 覆盖下的冷扫目标可复用 manifest。
fn cold_scan_manifest_skip_dirs(
    runtime: Option<&Arc<TieredWatchRuntime>>,
    entries: &[DirtyQueueEntry],
) -> HashSet<PathBuf> {
    let Some(runtime) = runtime else {
        return HashSet::new();
    };
    let mut skip_dirs = HashSet::new();
    for entry in entries {
        if !entry.reason.is_cold_scan() {
            continue;
        }
        for dir in entry.scope.dir_paths() {
            if matches!(
                runtime.covering_tier(dir.as_path()),
                Some(WatchTier::L2 | WatchTier::L3)
            ) {
                skip_dirs.insert(dir.clone());
            }
        }
    }
    skip_dirs
}

fn spawn_dirty_queue_loop(
    index: Arc<TieredIndex>,
    runtime: Option<Arc<TieredWatchRuntime>>,
    watch_command_tx: tokio::sync::mpsc::Sender<WatchCommand>,
    tiered: crate::config::TieredWatchConfig,
    exclude_dirs: Vec<String>,
    ignore_prefixes: Vec<PathBuf>,
) {
    tokio::spawn(async move {
        loop {
            let batch = index.dirty_queue_ready_batch(DIRTY_QUEUE_BATCH_SIZE);
            if batch.is_empty() {
                let idle_delay = Duration::from_millis(DIRTY_QUEUE_IDLE_POLL_MS);
                let wake_delay = index
                    .dirty_queue_next_ready_delay()
                    .map_or(idle_delay, |delay| delay.min(idle_delay));
                tokio::select! {
                    _ = index.wait_for_dirty_queue() => {}
                    _ = tokio::time::sleep(wake_delay) => {}
                }
                continue;
            }

            let retry_batch = batch
                .iter()
                .map(|entry| entry.clone_without_repair_cursor())
                .collect::<Vec<_>>();
            let work = batch;
            let work_manifest_skip_dirs = cold_scan_manifest_skip_dirs(runtime.as_ref(), &work);
            let work_index = index.clone();
            let work_ignore_prefixes = ignore_prefixes.clone();
            let work_project_markers = tiered.project_markers.clone();
            let work_runtime = runtime.clone();
            let processed = tokio::task::spawn_blocking(move || {
                let mut processed = Vec::new();
                let mut work = work;
                let mut work_skip_dirs = work_manifest_skip_dirs;
                // 低优先级 paced 轮转批在本 blocking 线程内联续跑：保持切片间隔，
                // 消除每目录一次 spawn_blocking 往返的跨线程唤醒写。每步续跑前
                // pop_ready_paced_low 会在存在任何更高优先级/非 paced ready 项时
                // 返回空，立即交回主循环；worker panic 时内联批不重试，
                // 由下一个轮转周期自愈（与整周期重扫语义一致）。
                let inline_deadline = Instant::now() + DIRTY_QUEUE_PACED_INLINE_BUDGET;
                loop {
                    let batch_all_paced_low =
                        work.iter().all(DirtyQueueEntry::is_paced_low_priority);
                    for entry in work {
                        let post_process_entry = entry.clone_without_repair_cursor();
                        let report = work_index
                            .process_dirty_entry_with_project_markers_and_manifest_skip_dirs(
                                entry,
                                &work_ignore_prefixes,
                                &work_project_markers,
                                &work_skip_dirs,
                            );
                        processed.push((post_process_entry, report));
                    }
                    if !batch_all_paced_low || Instant::now() >= inline_deadline {
                        break;
                    }
                    let Some(delay) = work_index.dirty_queue_next_ready_delay() else {
                        break;
                    };
                    if delay > DIRTY_QUEUE_PACED_INLINE_MAX_SLEEP {
                        break;
                    }
                    if !delay.is_zero() {
                        std::thread::sleep(delay);
                    }
                    let next = work_index.dirty_queue_ready_paced_low_batch(DIRTY_QUEUE_BATCH_SIZE);
                    if next.is_empty() {
                        break;
                    }
                    work_skip_dirs = cold_scan_manifest_skip_dirs(work_runtime.as_ref(), &next);
                    work = next;
                }
                processed
            })
            .await;

            let Ok(processed) = processed else {
                tracing::warn!("dirty queue worker task failed");
                for entry in retry_batch {
                    if !index.retry_dirty_entry(entry) {
                        tracing::warn!(
                            "dirty queue retry failed, entry dropped after worker task failure"
                        );
                    }
                }
                continue;
            };

            let ephemeral_config = EphemeralWatchConfig {
                budget: tiered.ephemeral_watch_budget,
                ttl_secs: tiered.ephemeral_watch_ttl_secs,
                idle_secs: tiered.ephemeral_idle_secs,
                max_cost_per_root: tiered.ephemeral_max_cost_per_root,
                ..EphemeralWatchConfig::default()
            };
            let marker_ephemeral_config = EphemeralWatchConfig {
                repeat_threshold: 1,
                ..ephemeral_config.clone()
            };

            for (entry, report) in processed {
                if report.failed {
                    if !index.retry_dirty_entry(entry.clone()) {
                        tracing::warn!("dirty queue dropped entry after retry budget: {:?}", entry);
                    }
                    continue;
                }

                if let Some(runtime) = runtime.as_ref() {
                    if entry.reason == DirtyReason::QueryHitStale {
                        runtime.grant_fast_scan_leases(
                            entry.scope.dir_paths().iter().cloned(),
                            FastScanLeaseKind::StaleHit,
                            None,
                            4,
                        );
                    }
                    if matches!(
                        entry.reason,
                        DirtyReason::FastScanChangedDir | DirtyReason::FastScanBootstrapDir
                    ) {
                        runtime.record_fast_scan_generated_events(report.changed);
                        runtime.record_fast_scan_apply_dropped_stale_batches(
                            report.dropped_stale_batches,
                        );
                    }
                    for scan in report.outcomes {
                        if entry.requires_recursive_subtree_repair() && !scan.completion_ready {
                            continue;
                        }
                        let changed = scan.outcome.changed;
                        let project_roots = scan.outcome.project_roots.clone();
                        let rotating_cycle_id = entry.rotating_cold_window_cycle_id();
                        let policy_dir = runtime
                            .record_scan_for_path_with_manifest_status(
                                scan.dir.as_path(),
                                scan.outcome.clone(),
                                scan.manifest_skipped,
                            )
                            .unwrap_or_else(|| scan.dir.clone());
                        if let Some(cycle_id) = rotating_cycle_id {
                            runtime.record_rotating_cold_window_scan_completion(
                                policy_dir.as_path(),
                                cycle_id,
                            );
                        }
                        runtime.apply_scan_policy(
                            policy_dir.as_path(),
                            tiered.l1_scan_interval_secs,
                            tiered.l2_scan_interval_secs,
                            tiered.l3_scan_policy,
                            // Use the waterline-alarm-adjusted L3 interval so
                            // that hard degradation lengthens the L3 scan cycle.
                            runtime.effective_l3_scan_interval_secs(),
                            tiered.l1_empty_scans_to_l2,
                            tiered.l2_empty_scans_to_l3,
                        );
                        let promotion_decision = if changed > 0
                            && !runtime.recursive_watch_reserved_or_covers(policy_dir.as_path())
                        {
                            send_promotion_command(runtime, &watch_command_tx, policy_dir).await
                        } else {
                            crate::event::tiered_watch::PromotionDecision::NotEligible
                        };
                        if !matches!(
                            promotion_decision,
                            crate::event::tiered_watch::PromotionDecision::SendAdd
                                | crate::event::tiered_watch::PromotionDecision::Replace { .. }
                        ) {
                            maybe_send_ephemeral_watch_command(
                                runtime,
                                &watch_command_tx,
                                scan.dir.clone(),
                                changed,
                                &exclude_dirs,
                                &ephemeral_config,
                                None,
                            )
                            .await;
                        }

                        for project_root in project_roots {
                            if crate::util::path_has_excluded_component(
                                project_root.as_path(),
                                &exclude_dirs,
                            ) {
                                runtime.note_watch_exclude_rejected();
                                continue;
                            }
                            if !project_marker_root_is_eligible(
                                project_root.as_path(),
                                index.roots.as_slice(),
                                &exclude_dirs,
                                &ignore_prefixes,
                            ) {
                                continue;
                            }
                            if runtime.recursive_watch_reserved_or_covers(project_root.as_path()) {
                                continue;
                            }
                            let watch_cost = estimate_notify_recursive_watch_count(
                                project_root.as_path(),
                                effective_l0_max_cost_per_root(&tiered).max(1),
                            );
                            let decision = runtime.register_project_marker_candidate(
                                project_root.clone(),
                                watch_cost,
                            );
                            let marker_promotion = send_promotion_decision_command(
                                runtime,
                                &watch_command_tx,
                                project_root.clone(),
                                decision,
                            )
                            .await;
                            if !matches!(
                                marker_promotion,
                                crate::event::tiered_watch::PromotionDecision::SendAdd
                                    | crate::event::tiered_watch::PromotionDecision::Replace { .. }
                            ) {
                                maybe_send_ephemeral_watch_command(
                                    runtime,
                                    &watch_command_tx,
                                    project_root,
                                    changed.max(1),
                                    &exclude_dirs,
                                    &marker_ephemeral_config,
                                    None,
                                )
                                .await;
                            }
                        }
                    }
                }

                tracing::debug!(
                    "dirty queue processed reason={:?} dirs={} changed={} elapsed_ms={} fast_sync_upserts={} fast_sync_deletes={}",
                    entry.reason,
                    report.dirs_scanned,
                    report.changed,
                    report.elapsed_ms,
                    report.fast_sync_upserts,
                    report.fast_sync_deletes
                );
            }
        }
    });
}

fn project_marker_root_is_eligible(
    project_root: &std::path::Path,
    roots: &[PathBuf],
    exclude_dirs: &[String],
    ignore_prefixes: &[PathBuf],
) -> bool {
    project_root.is_dir()
        && roots
            .iter()
            .any(|root| path_is_under_or_equal(project_root, root.as_path()))
        && !crate::util::path_has_excluded_component(project_root, exclude_dirs)
        && !ignore_prefixes
            .iter()
            .any(|ignore| !ignore.as_os_str().is_empty() && project_root.starts_with(ignore))
}

fn spawn_tiered_scan_loop(
    index: Arc<TieredIndex>,
    runtime: Arc<TieredWatchRuntime>,
    watch_command_tx: tokio::sync::mpsc::Sender<WatchCommand>,
    tiered: crate::config::TieredWatchConfig,
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

            let batch = runtime.scan_batch(max_dirs_per_tick);
            if batch.is_empty() {
                continue;
            }
            index.enqueue_dirty_dirs(batch, DirtyReason::PeriodicColdScan);
        }
    });
}

fn spawn_ephemeral_watch_maintenance_loop(
    index: Arc<TieredIndex>,
    runtime: Arc<TieredWatchRuntime>,
    watch_command_tx: tokio::sync::mpsc::Sender<WatchCommand>,
    tiered: crate::config::TieredWatchConfig,
) {
    tokio::spawn(async move {
        let interval = Duration::from_secs(1);
        loop {
            tokio::time::sleep(interval).await;
            for (path, cycle_id) in runtime.take_due_rotating_cold_window_scans() {
                enqueue_rotating_recursive_scan(&index, vec![path], cycle_id);
            }
            for removal in runtime.expire_ephemeral_watches(
                tiered.ephemeral_idle_secs,
                tiered.ephemeral_watch_ttl_secs,
                EphemeralWatchConfig::default().no_change_limit,
            ) {
                if watch_command_tx
                    .send(WatchCommand::RemoveEphemeral(removal.path.clone()))
                    .await
                    .is_err()
                {
                    runtime.rollback_ephemeral_remove(removal.path.as_path());
                }
            }
        }
    });
}

fn spawn_tiered_fast_scan_loop(
    index: Arc<TieredIndex>,
    runtime: Arc<TieredWatchRuntime>,
    tiered: crate::config::TieredWatchConfig,
) {
    tokio::spawn(async move {
        let tick = Duration::from_millis(tiered.l1_l2_fast_scan_tick_ms.max(100));
        let bootstrap_limit = tiered.l1_l2_fast_scan_bootstrap_budget_per_tick.max(1);

        loop {
            tokio::time::sleep(tick).await;

            let mount_table = MountTable::current().unwrap_or_default();
            if runtime.should_bootstrap_fast_scan_dirs(bootstrap_limit) {
                let pending_dirs = runtime.pending_fast_scan_lease_dirs(bootstrap_limit);
                let candidate_count = pending_dirs.len();
                let inserted =
                    runtime.bootstrap_fast_scan_dirs(pending_dirs, &mount_table, bootstrap_limit);
                runtime.record_fast_scan_bootstrap_result(
                    candidate_count,
                    inserted,
                    bootstrap_limit,
                    fast_scan_bootstrap_retry_ms(&tiered),
                );
            }

            let stale_hit_dirs = index.drain_recent_stale_hit_dirs();
            if !stale_hit_dirs.is_empty() {
                runtime.grant_fast_scan_leases(
                    stale_hit_dirs,
                    FastScanLeaseKind::StaleHit,
                    None,
                    4,
                );
            }

            let result = runtime.fast_scan_tick(&mount_table, runtime.fast_scan_tick_config());
            if !result.initial_dirs.is_empty() {
                index.enqueue_dirty_dirs(result.initial_dirs, DirtyReason::FastScanBootstrapDir);
            }
            if !result.changed_dirs.is_empty() {
                index.enqueue_dirty_dirs(result.changed_dirs, DirtyReason::FastScanChangedDir);
            }
        }
    });
}

fn spawn_rotating_cold_window_loop(
    index: Arc<TieredIndex>,
    runtime: Arc<TieredWatchRuntime>,
    watch_command_tx: tokio::sync::mpsc::Sender<WatchCommand>,
    tiered: crate::config::TieredWatchConfig,
    exclude_dirs: Vec<String>,
) {
    tokio::spawn(async move {
        let interval = Duration::from_secs(tiered.rotating_cold_window_tick_secs.max(1));
        // Keep the FastScan sentinel alive across the next rotation. The rotating
        // lease is intentionally shorter, but expiring both at the same boundary
        // causes an avoidable lease/sentinel bootstrap before the next selection.
        let fast_scan_lease_ttl_secs = rotating_fast_scan_lease_ttl_secs(
            tiered.rotating_cold_window_ttl_secs.max(1),
            tiered.l1_l2_fast_scan_lease_ttl_secs.max(1),
            interval,
        );
        let sweep_period_secs = tiered.rotating_full_sweep_period_secs;
        let ephemeral_config = EphemeralWatchConfig {
            budget: tiered.ephemeral_watch_budget,
            ttl_secs: tiered.rotating_cold_window_ttl_secs.max(1),
            idle_secs: tiered.ephemeral_idle_secs,
            max_cost_per_root: tiered.rotating_cold_window_max_cost_per_root.max(1),
            repeat_threshold: 1,
            ..EphemeralWatchConfig::default()
        };

        loop {
            tokio::time::sleep(interval).await;

            let tick =
                runtime.rotating_cold_window_tick(runtime.rotating_cold_window_tick_config());
            if tick.budget_blocked {
                tracing::debug!(
                    "rotating cold window budget blocked at cycle {}",
                    tick.cycle_id
                );
                continue;
            }
            if tick.actions.is_empty() {
                continue;
            }

            let mut initial_scan_dirs = Vec::new();
            for action in tick.actions {
                match action.action {
                    RotatingColdWindowActionKind::EphemeralWatch => {
                        // 再选中即续期：watcher 跨周期连续存活时 coverage 保持
                        // Ready，不再逐周期重装+seed 重扫；未选中的根按原期自然过期。
                        runtime.renew_ephemeral_watch_ttl(
                            action.path.as_path(),
                            tiered.rotating_cold_window_ttl_secs.max(1),
                        );
                        if activate_rotating_ephemeral_action(
                            &runtime,
                            &watch_command_tx,
                            &action,
                            &exclude_dirs,
                            &ephemeral_config,
                            fast_scan_lease_ttl_secs,
                            tick.cycle_id,
                        )
                        .await
                        {
                            initial_scan_dirs.push(action.path);
                        }
                    }
                    RotatingColdWindowActionKind::FastScanLease => {
                        runtime.constrain_ephemeral_watch_ttl(
                            action.path.as_path(),
                            tiered.rotating_cold_window_ttl_secs.max(1),
                        );
                        let ready = runtime.ensure_fast_scan_lease(
                            action.path.clone(),
                            FastScanLeaseKind::RotatingColdWindow,
                            Some(fast_scan_lease_ttl_secs),
                            action.score.max(1),
                        );
                        if ready {
                            // 完整递归 sweep 只按 SLA 墙钟周期到期时入队；周期之间由
                            // sentinel/事件维持可见性。sweep_due 基于活跃 lease 下
                            // 真实完成的墙钟戳，fail-closed 判 due。
                            if runtime.rotating_cold_window_sweep_due(
                                action.path.as_path(),
                                sweep_period_secs,
                            ) {
                                initial_scan_dirs.push(action.path);
                            } else {
                                tracing::debug!(
                                    "rotating full sweep deferred for {:?} at cycle {} (period {}s)",
                                    action.path,
                                    tick.cycle_id,
                                    sweep_period_secs
                                );
                            }
                        } else {
                            tracing::debug!(
                                "rotating cold window fast-scan lease unavailable, falling back to scan-only for {:?}",
                                action.path
                            );
                            if runtime.downgrade_rotating_cold_window_lease_to_scan_only(
                                action.path.as_path(),
                                tick.cycle_id,
                            ) {
                                initial_scan_dirs.push(action.path);
                            }
                        }
                    }
                    RotatingColdWindowActionKind::ScanOnly => {
                        runtime.constrain_ephemeral_watch_ttl(
                            action.path.as_path(),
                            tiered.rotating_cold_window_ttl_secs.max(1),
                        );
                        // ScanOnly 没有 watcher/sentinel 覆盖，扫描是唯一可见性
                        // 来源：保持每周期无条件扫描。
                        initial_scan_dirs.push(action.path);
                    }
                }
            }

            if !initial_scan_dirs.is_empty() {
                enqueue_rotating_recursive_scan(&index, initial_scan_dirs, tick.cycle_id);
            }
        }
    });
}

fn enqueue_rotating_recursive_scan(index: &TieredIndex, dirs: Vec<PathBuf>, cycle_id: u64) {
    index.enqueue_paced_recursive_dirty_dirs(dirs, DirtyReason::RotatingColdWindow { cycle_id });
}

fn rotating_fast_scan_lease_ttl_secs(
    rotating_ttl_secs: u64,
    fast_scan_ttl_secs: u64,
    tick: Duration,
) -> u64 {
    fast_scan_ttl_secs.max(
        rotating_ttl_secs
            .max(1)
            .saturating_add(tick.as_secs().max(1).saturating_mul(2)),
    )
}

fn spawn_proc_sampler_loop(
    runtime: Arc<TieredWatchRuntime>,
    watch_command_tx: tokio::sync::mpsc::Sender<WatchCommand>,
    config: ProcSamplerConfig,
    roots: Vec<PathBuf>,
    ignore_prefixes: Vec<PathBuf>,
    exclude_dirs: Vec<String>,
    tiered: crate::config::TieredWatchConfig,
) {
    tokio::spawn(async move {
        let interval = Duration::from_millis(config.interval_ms.max(100));
        let mut cursor = ProcSamplerCursor::default();
        let ephemeral_config = EphemeralWatchConfig {
            budget: tiered.ephemeral_watch_budget,
            ttl_secs: tiered.ephemeral_watch_ttl_secs,
            idle_secs: tiered.ephemeral_idle_secs,
            max_cost_per_root: tiered.ephemeral_max_cost_per_root,
            repeat_threshold: 1,
            ..EphemeralWatchConfig::default()
        };

        loop {
            tokio::time::sleep(interval).await;

            let sample_config = config.clone();
            let sample_roots = roots.clone();
            let sample_ignore_prefixes = ignore_prefixes.clone();
            let sample_exclude_dirs = exclude_dirs.clone();
            let sample_cursor = cursor;
            let sampled = tokio::task::spawn_blocking(move || {
                sample_proc_write_dirs(
                    &sample_config,
                    sample_cursor,
                    &sample_roots,
                    &sample_ignore_prefixes,
                    &sample_exclude_dirs,
                )
            })
            .await;

            let Ok(tick) = sampled else {
                tracing::warn!("proc sampler task failed");
                runtime.record_proc_sampler_report(
                    ProcSamplerReport {
                        unavailable: true,
                        ..ProcSamplerReport::default()
                    },
                    0,
                );
                continue;
            };

            cursor = tick.cursor;
            let mut triggered_watches = 0u64;
            for dir in tick.dirs {
                runtime.grant_fast_scan_lease(
                    dir.clone(),
                    FastScanLeaseKind::ProcSampler,
                    Some(tiered.l1_l2_fast_scan_proc_sampler_lease_ttl_secs),
                    3,
                );
                if maybe_send_ephemeral_watch_command(
                    &runtime,
                    &watch_command_tx,
                    dir,
                    1,
                    &exclude_dirs,
                    &ephemeral_config,
                    None,
                )
                .await
                {
                    triggered_watches = triggered_watches.saturating_add(1);
                }
            }
            runtime.record_proc_sampler_report(tick.report, triggered_watches);
        }
    });
}

fn fast_scan_bootstrap_retry_ms(tiered: &crate::config::TieredWatchConfig) -> u64 {
    tiered
        .l1_l2_fast_scan_target_secs
        .max(FAST_SCAN_MIN_TARGET_SECS)
        .saturating_mul(1_000)
}

async fn send_promotion_command(
    runtime: &Arc<TieredWatchRuntime>,
    watch_command_tx: &tokio::sync::mpsc::Sender<WatchCommand>,
    dir: PathBuf,
) -> crate::event::tiered_watch::PromotionDecision {
    let decision = runtime.try_reserve_promotion(dir.as_path());
    send_promotion_decision_command(runtime, watch_command_tx, dir, decision).await
}

async fn send_promotion_decision_command(
    runtime: &Arc<TieredWatchRuntime>,
    watch_command_tx: &tokio::sync::mpsc::Sender<WatchCommand>,
    dir: PathBuf,
    decision: crate::event::tiered_watch::PromotionDecision,
) -> crate::event::tiered_watch::PromotionDecision {
    match decision.clone() {
        crate::event::tiered_watch::PromotionDecision::SendAdd => {
            if watch_command_tx
                .send(WatchCommand::Add(dir.clone()))
                .await
                .is_err()
            {
                runtime.rollback_promote(dir.as_path());
            }
        }
        crate::event::tiered_watch::PromotionDecision::Replace { demote, promote } => {
            let demote_path = demote.clone();
            let promote_path = promote.clone();
            if watch_command_tx
                .send(WatchCommand::Replace { demote, promote })
                .await
                .is_err()
            {
                runtime.rollback_replacement(demote_path.as_path(), promote_path.as_path());
            }
        }
        crate::event::tiered_watch::PromotionDecision::BudgetBlocked
        | crate::event::tiered_watch::PromotionDecision::NotEligible => {}
    }
    decision
}

async fn maybe_send_ephemeral_watch_command(
    runtime: &Arc<TieredWatchRuntime>,
    watch_command_tx: &tokio::sync::mpsc::Sender<WatchCommand>,
    dir: PathBuf,
    changed: usize,
    exclude_dirs: &[String],
    config: &EphemeralWatchConfig,
    fallback_cycle_id: Option<u64>,
) -> bool {
    if config.budget == 0 {
        return false;
    }
    let Some(cost) = estimate_ephemeral_watch_cost(runtime, dir.as_path(), exclude_dirs, config)
    else {
        return false;
    };
    send_ephemeral_watch_command_with_cost(
        runtime,
        watch_command_tx,
        dir,
        cost,
        changed,
        exclude_dirs,
        config,
        fallback_cycle_id,
    )
    .await
}

async fn send_ephemeral_watch_command_with_cost(
    runtime: &Arc<TieredWatchRuntime>,
    watch_command_tx: &tokio::sync::mpsc::Sender<WatchCommand>,
    dir: PathBuf,
    cost: usize,
    changed: usize,
    exclude_dirs: &[String],
    config: &EphemeralWatchConfig,
    fallback_cycle_id: Option<u64>,
) -> bool {
    match runtime.note_dirty_scope_with_changed(dir.clone(), cost, exclude_dirs, config, changed) {
        EphemeralWatchDecision::Add(path) => {
            if watch_command_tx
                .send(WatchCommand::AddEphemeral {
                    path: path.clone(),
                    fallback_cycle_id,
                })
                .await
                .is_err()
            {
                runtime.rollback_ephemeral_add(path.as_path());
                false
            } else {
                true
            }
        }
        EphemeralWatchDecision::Replace { remove, add } => {
            if watch_command_tx
                .send(WatchCommand::ReplaceEphemeral {
                    remove: remove.clone(),
                    add: add.clone(),
                    fallback_cycle_id,
                })
                .await
                .is_err()
            {
                runtime.rollback_ephemeral_replace(remove.as_path(), add.as_path());
                false
            } else {
                true
            }
        }
        EphemeralWatchDecision::BudgetBlocked => {
            tracing::debug!("tiered ephemeral watcher budget blocked for {:?}", dir);
            false
        }
        EphemeralWatchDecision::NotEligible => false,
    }
}

async fn ensure_rotating_ephemeral_watch_coverage(
    runtime: &Arc<TieredWatchRuntime>,
    watch_command_tx: &tokio::sync::mpsc::Sender<WatchCommand>,
    dir: PathBuf,
    exclude_dirs: &[String],
    config: &EphemeralWatchConfig,
    cycle_id: u64,
) -> RotatingEphemeralCoverage {
    if confirmed_recursive_watch_covers(runtime, dir.as_path()) {
        return RotatingEphemeralCoverage::Ready;
    }
    if config.budget == 0 {
        return RotatingEphemeralCoverage::Unavailable;
    }
    let max_fast_scan_cost = rotating_cold_window_fast_scan_max_cost(config.max_cost_per_root);
    let Some(cost) = estimate_ephemeral_watch_cost_up_to(
        runtime,
        dir.as_path(),
        exclude_dirs,
        max_fast_scan_cost,
    ) else {
        return if confirmed_recursive_watch_covers(runtime, dir.as_path()) {
            RotatingEphemeralCoverage::Ready
        } else {
            RotatingEphemeralCoverage::Unavailable
        };
    };
    if cost > config.max_cost_per_root.max(1) {
        return RotatingEphemeralCoverage::TooLarge { watch_cost: cost };
    }
    if send_ephemeral_watch_command_with_cost(
        runtime,
        watch_command_tx,
        dir.clone(),
        cost,
        1,
        exclude_dirs,
        config,
        Some(cycle_id),
    )
    .await
        || confirmed_recursive_watch_covers(runtime, dir.as_path())
    {
        RotatingEphemeralCoverage::Ready
    } else {
        RotatingEphemeralCoverage::Unavailable
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum RotatingEphemeralCoverage {
    Ready,
    TooLarge { watch_cost: usize },
    Unavailable,
}

async fn activate_rotating_ephemeral_action(
    runtime: &Arc<TieredWatchRuntime>,
    watch_command_tx: &tokio::sync::mpsc::Sender<WatchCommand>,
    action: &RotatingColdWindowAction,
    exclude_dirs: &[String],
    config: &EphemeralWatchConfig,
    ttl_secs: u64,
    cycle_id: u64,
) -> bool {
    match ensure_rotating_ephemeral_watch_coverage(
        runtime,
        watch_command_tx,
        action.path.clone(),
        exclude_dirs,
        config,
        cycle_id,
    )
    .await
    {
        RotatingEphemeralCoverage::Ready => false,
        RotatingEphemeralCoverage::TooLarge { watch_cost } => {
            runtime.record_rotating_cold_window_watch_cost(
                action.path.as_path(),
                cycle_id,
                watch_cost,
            );
            let fast_scan_eligible =
                watch_cost <= rotating_cold_window_fast_scan_max_cost(config.max_cost_per_root);
            let reclassified = fast_scan_eligible
                && runtime.reclassify_rotating_cold_window_lease_to_fast_scan(
                    action.path.as_path(),
                    cycle_id,
                );
            if reclassified
                && runtime.ensure_fast_scan_lease(
                    action.path.clone(),
                    FastScanLeaseKind::RotatingColdWindow,
                    Some(ttl_secs),
                    action.score.max(1),
                )
            {
                return true;
            }
            downgrade_rotating_action_to_scan_only(runtime, action, cycle_id)
        }
        RotatingEphemeralCoverage::Unavailable => {
            downgrade_rotating_action_to_scan_only(runtime, action, cycle_id)
        }
    }
}

fn downgrade_rotating_action_to_scan_only(
    runtime: &TieredWatchRuntime,
    action: &RotatingColdWindowAction,
    cycle_id: u64,
) -> bool {
    tracing::debug!(
        "rotating cold window coverage unavailable, falling back to scan-only for {:?}",
        action.path
    );
    runtime.downgrade_rotating_cold_window_lease_to_scan_only(action.path.as_path(), cycle_id)
}

fn confirmed_recursive_watch_covers(runtime: &TieredWatchRuntime, dir: &std::path::Path) -> bool {
    matches!(runtime.covering_tier(dir), Some(WatchTier::L0))
        || runtime.confirmed_ephemeral_watch_covers(dir)
}

fn estimate_ephemeral_watch_cost(
    runtime: &TieredWatchRuntime,
    dir: &std::path::Path,
    exclude_dirs: &[String],
    config: &EphemeralWatchConfig,
) -> Option<usize> {
    if config.budget == 0 {
        return None;
    }
    estimate_ephemeral_watch_cost_up_to(
        runtime,
        dir,
        exclude_dirs,
        config.max_cost_per_root.max(1).saturating_add(1),
    )
}

fn estimate_ephemeral_watch_cost_up_to(
    runtime: &TieredWatchRuntime,
    dir: &std::path::Path,
    exclude_dirs: &[String],
    estimate_cap: usize,
) -> Option<usize> {
    if runtime.rotating_cold_window_expired_ephemeral_covers(dir)
        || runtime.recursive_watch_reserved_or_covers(dir)
        || !dir.is_dir()
    {
        return None;
    }
    if crate::util::path_has_excluded_component(dir, exclude_dirs) {
        runtime.note_watch_exclude_rejected();
        return None;
    }
    #[cfg(test)]
    EPHEMERAL_WATCH_COST_ESTIMATE_CALLS.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    Some(estimate_notify_recursive_watch_count(
        dir,
        estimate_cap.max(1),
    ))
}

#[cfg(test)]
static EPHEMERAL_WATCH_COST_ESTIMATE_CALLS: std::sync::atomic::AtomicUsize =
    std::sync::atomic::AtomicUsize::new(0);

#[cfg(test)]
fn rotating_action_needs_initial_dirty_scan(
    action: RotatingColdWindowActionKind,
    mechanism_ready: bool,
) -> bool {
    action != RotatingColdWindowActionKind::EphemeralWatch || !mechanism_ready
}

fn initial_hot_candidates(
    roots: &[PathBuf],
    hot_dirs: &[PathBuf],
    exclude_dirs: &[String],
) -> Vec<PathBuf> {
    hot_dirs
        .iter()
        .filter(|p| p.is_dir())
        .filter(|p| !crate::util::path_has_excluded_component(p, exclude_dirs))
        .filter(|p| roots.iter().any(|root| path_is_under_or_equal(p, root)))
        .cloned()
        .collect()
}

fn path_is_under_or_equal(path: &std::path::Path, root: &std::path::Path) -> bool {
    path == root || path.starts_with(root)
}

#[cfg(test)]
mod tests {
    use super::*;

    static COST_ESTIMATION_TEST_LOCK: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

    #[tokio::test]
    async fn existing_recursive_watch_coverage_skips_ephemeral_cost_estimation() {
        let _counter_guard = COST_ESTIMATION_TEST_LOCK.lock().await;
        EPHEMERAL_WATCH_COST_ESTIMATE_CALLS.store(0, std::sync::atomic::Ordering::Relaxed);
        let root = temp_root("ephemeral-cost-precheck");
        let child = root.join("child/deep");
        std::fs::create_dir_all(&child).unwrap();
        let config = EphemeralWatchConfig {
            budget: 32,
            repeat_threshold: 1,
            max_cost_per_root: 32,
            ..EphemeralWatchConfig::default()
        };

        let l0_runtime = TieredWatchRuntime::new_with_ephemeral(
            vec![(root.clone(), 1)],
            Vec::new(),
            32,
            5_000,
            20,
            32,
        );
        assert_eq!(
            estimate_ephemeral_watch_cost(&l0_runtime, child.as_path(), &[], &config),
            None
        );

        let ephemeral_runtime = TieredWatchRuntime::new_with_ephemeral(
            Vec::new(),
            vec![(root.clone(), 1)],
            32,
            5_000,
            20,
            32,
        );
        assert_eq!(
            ephemeral_runtime.note_dirty_scope_with_changed(root.clone(), 1, &[], &config, 1,),
            EphemeralWatchDecision::Add(root.clone())
        );
        assert_eq!(
            estimate_ephemeral_watch_cost(&ephemeral_runtime, child.as_path(), &[], &config),
            None
        );
        let (watch_command_tx, mut watch_command_rx) = tokio::sync::mpsc::channel(2);
        let ephemeral_runtime = Arc::new(ephemeral_runtime);
        assert!(
            !maybe_send_ephemeral_watch_command(
                &ephemeral_runtime,
                &watch_command_tx,
                child.clone(),
                1,
                &[],
                &config,
                None,
            )
            .await
        );
        assert_eq!(
            EPHEMERAL_WATCH_COST_ESTIMATE_CALLS.load(std::sync::atomic::Ordering::Relaxed),
            0
        );
        assert!(watch_command_rx.try_recv().is_err());
        let watcher_report = ephemeral_runtime.report();
        assert_eq!(watcher_report.ephemeral_watch_created, 0);
        assert_eq!(watcher_report.ephemeral_watch_evicted, 0);
        assert_eq!(
            ensure_rotating_ephemeral_watch_coverage(
                &ephemeral_runtime,
                &watch_command_tx,
                child.clone(),
                &[],
                &config,
                7,
            )
            .await,
            RotatingEphemeralCoverage::Unavailable
        );
        assert!(ephemeral_runtime.confirm_ephemeral_added(root.as_path()));

        assert_eq!(
            ensure_rotating_ephemeral_watch_coverage(
                &ephemeral_runtime,
                &watch_command_tx,
                child.clone(),
                &[],
                &config,
                7,
            )
            .await,
            RotatingEphemeralCoverage::Ready
        );
        assert!(watch_command_rx.try_recv().is_err());
        assert_eq!(
            EPHEMERAL_WATCH_COST_ESTIMATE_CALLS.load(std::sync::atomic::Ordering::Relaxed),
            0
        );

        let uncovered_runtime = TieredWatchRuntime::new_with_ephemeral(
            Vec::new(),
            vec![(root.clone(), 1)],
            32,
            5_000,
            20,
            32,
        );
        assert!(
            estimate_ephemeral_watch_cost(&uncovered_runtime, child.as_path(), &[], &config)
                .is_some()
        );

        let _ = std::fs::remove_dir_all(&root);
    }

    #[tokio::test]
    async fn rotating_ephemeral_cost_overflow_is_classified_for_fast_scan() {
        let _counter_guard = COST_ESTIMATION_TEST_LOCK.lock().await;
        let root = temp_root("rotating-ephemeral-too-large");
        for index in 0..300 {
            std::fs::create_dir_all(root.join(format!("d{index:03}"))).unwrap();
        }
        let runtime = Arc::new(TieredWatchRuntime::new_with_ephemeral(
            Vec::new(),
            vec![(root.clone(), 2)],
            8,
            5_000,
            20,
            8,
        ));
        let config = EphemeralWatchConfig {
            budget: 8,
            repeat_threshold: 1,
            max_cost_per_root: 64,
            ..EphemeralWatchConfig::default()
        };
        let (watch_command_tx, mut watch_command_rx) = tokio::sync::mpsc::channel(1);

        assert_eq!(
            ensure_rotating_ephemeral_watch_coverage(
                &runtime,
                &watch_command_tx,
                root.clone(),
                &[],
                &config,
                9,
            )
            .await,
            RotatingEphemeralCoverage::TooLarge { watch_cost: 301 }
        );
        assert!(watch_command_rx.try_recv().is_err());

        let _ = std::fs::remove_dir_all(root);
    }

    #[tokio::test]
    async fn rotating_ephemeral_too_large_dispatches_fast_scan_without_scan_only() {
        let _counter_guard = COST_ESTIMATION_TEST_LOCK.lock().await;
        let root = temp_root("rotating-ephemeral-fast-scan-fallback");
        for index in 0..300 {
            std::fs::create_dir_all(root.join(format!("d{index:03}"))).unwrap();
        }
        let runtime = Arc::new(TieredWatchRuntime::new_with_ephemeral(
            Vec::new(),
            vec![(root.clone(), 2)],
            8,
            5_000,
            20,
            8,
        ));
        runtime.record_scan_for_path(
            root.as_path(),
            crate::index::tiered::ScanOutcome {
                scanned: 1,
                changed: 0,
                elapsed_ms: 1,
                project_roots: Vec::new(),
            },
        );
        runtime.apply_scan_policy(
            root.as_path(),
            1,
            1,
            crate::config::L3ScanPolicy::Disabled,
            1,
            1,
            1,
        );
        let tick = runtime.rotating_cold_window_tick(
            crate::event::tiered_watch::RotatingColdWindowConfig {
                enabled: true,
                budget: 1,
                ttl_secs: 5,
                max_cost_per_root: 64,
                max_dirs_per_tick: 1,
            },
        );
        assert_eq!(tick.actions.len(), 1);
        assert_eq!(
            tick.actions[0].action,
            RotatingColdWindowActionKind::EphemeralWatch
        );

        let config = EphemeralWatchConfig {
            budget: 8,
            repeat_threshold: 1,
            max_cost_per_root: 64,
            ..EphemeralWatchConfig::default()
        };
        let (watch_command_tx, mut watch_command_rx) = tokio::sync::mpsc::channel(1);
        assert!(runtime.grant_fast_scan_lease(
            root.clone(),
            FastScanLeaseKind::RotatingColdWindow,
            Some(5),
            1,
        ));
        assert!(
            activate_rotating_ephemeral_action(
                &runtime,
                &watch_command_tx,
                &tick.actions[0],
                &[],
                &config,
                5,
                tick.cycle_id,
            )
            .await
        );
        assert!(watch_command_rx.try_recv().is_err());

        let report = runtime.report();
        assert_eq!(report.fast_scan_hotset_lease_count, 1);
        assert_eq!(report.fast_scan_lease_renewals, 1);
        assert_eq!(report.rotating_cold_window_fast_scan_lease_dirs, 1);
        assert_eq!(report.rotating_cold_window_scan_only_dirs, 0);
        let debug = runtime.debug_dump(root.to_str());
        assert_eq!(debug.dirs[0].rotating_cold_window_action, "fast_scan_lease");
        assert!(runtime.take_due_rotating_cold_window_scans().is_empty());

        let _ = std::fs::remove_dir_all(root);
    }

    #[tokio::test]
    async fn recursive_repair_limits_watcher_decisions_for_deep_and_wide_trees() {
        let _counter_guard = COST_ESTIMATION_TEST_LOCK.lock().await;
        for shape in ["deep", "wide"] {
            EPHEMERAL_WATCH_COST_ESTIMATE_CALLS.store(0, std::sync::atomic::Ordering::Relaxed);
            let root = temp_root(&format!("recursive-watcher-bound-{shape}"));
            let repair_root = root.join("repair");
            match shape {
                "deep" => {
                    let deepest = repair_root.join("level1/level2/level3");
                    std::fs::create_dir_all(&deepest).unwrap();
                    std::fs::write(deepest.join("visible.txt"), b"visible").unwrap();
                }
                "wide" => {
                    std::fs::create_dir_all(&repair_root).unwrap();
                    for i in 0..16 {
                        let child = repair_root.join(format!("child-{i:02}"));
                        std::fs::create_dir_all(&child).unwrap();
                        std::fs::write(child.join("visible.txt"), b"visible").unwrap();
                    }
                }
                _ => unreachable!(),
            }

            let index = TieredIndex::empty(vec![root.clone()]);
            index.enqueue_recursive_dirty_dirs(
                vec![repair_root.clone()],
                DirtyReason::RecursiveSubtreeRepair,
            );
            std::thread::sleep(Duration::from_millis(260));
            let runtime = Arc::new(TieredWatchRuntime::new_with_ephemeral(
                Vec::new(),
                Vec::new(),
                128,
                5_000,
                20,
                128,
            ));
            let config = EphemeralWatchConfig {
                budget: 128,
                repeat_threshold: 1,
                max_cost_per_root: 128,
                ..EphemeralWatchConfig::default()
            };
            let (watch_command_tx, mut watch_command_rx) = tokio::sync::mpsc::channel(4);
            let mut watcher_decisions = 0usize;

            loop {
                let Some(entry) = index.dirty_queue_ready_batch(1).into_iter().next() else {
                    break;
                };
                let report = index.process_dirty_entry(entry, &[]);
                assert!(
                    !report.failed,
                    "{shape} recursive repair failed: {report:?}"
                );
                for scan in report.outcomes {
                    assert!(scan.completion_ready);
                    watcher_decisions = watcher_decisions.saturating_add(1);
                    assert!(
                        maybe_send_ephemeral_watch_command(
                            &runtime,
                            &watch_command_tx,
                            scan.dir,
                            scan.outcome.changed,
                            &[],
                            &config,
                            None,
                        )
                        .await
                    );
                }
            }

            assert_eq!(watcher_decisions, 1, "{shape} repair decision count");
            assert_eq!(
                EPHEMERAL_WATCH_COST_ESTIMATE_CALLS.load(std::sync::atomic::Ordering::Relaxed),
                1,
                "{shape} repair cost count"
            );
            let command = watch_command_rx.try_recv().unwrap();
            let added = match command {
                WatchCommand::AddEphemeral { path, .. } => path,
                other => panic!("unexpected {shape} watcher command: {other:?}"),
            };
            assert_eq!(added, repair_root);
            assert!(watch_command_rx.try_recv().is_err());
            assert!(runtime.confirm_ephemeral_added(added.as_path()));
            let watcher_report = runtime.report();
            assert_eq!(watcher_report.ephemeral_watch_created, 1);
            assert_eq!(watcher_report.ephemeral_watch_evicted, 0);

            let _ = std::fs::remove_dir_all(&root);
        }
    }

    #[test]
    fn rotating_ephemeral_dispatch_avoids_duplicate_dirty_scan() {
        assert!(!rotating_action_needs_initial_dirty_scan(
            RotatingColdWindowActionKind::EphemeralWatch,
            true,
        ));
        assert!(rotating_action_needs_initial_dirty_scan(
            RotatingColdWindowActionKind::EphemeralWatch,
            false,
        ));
        assert!(rotating_action_needs_initial_dirty_scan(
            RotatingColdWindowActionKind::FastScanLease,
            true,
        ));
        assert!(rotating_action_needs_initial_dirty_scan(
            RotatingColdWindowActionKind::ScanOnly,
            true,
        ));
    }

    #[test]
    fn rotating_scan_only_uses_recursive_repair_with_cycle_provenance() {
        let root = temp_root("rotating-scan-only-recursive");
        let index = TieredIndex::empty(vec![root.clone()]);

        enqueue_rotating_recursive_scan(&index, vec![root.clone()], 42);
        std::thread::sleep(Duration::from_millis(260));

        let entry = index.dirty_queue_ready_batch(1).pop().unwrap();
        assert!(entry.requires_recursive_subtree_repair());
        assert_eq!(entry.rotating_cold_window_cycle_id(), Some(42));
        assert_eq!(entry.scope.dir_paths(), &[root]);
    }

    #[test]
    fn rotating_fast_scan_lease_ttl_spans_rotation_boundary() {
        assert_eq!(
            rotating_fast_scan_lease_ttl_secs(180, 1_800, Duration::from_secs(30)),
            1_800
        );
        assert_eq!(
            rotating_fast_scan_lease_ttl_secs(1, 1, Duration::from_millis(1)),
            3
        );
    }

    fn temp_root(tag: &str) -> PathBuf {
        let nanos = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        std::env::temp_dir().join(format!("fd-rdd-main-{tag}-{}-{nanos}", std::process::id()))
    }

    #[tokio::test]
    async fn handle_shutdown_failure_returns_error_and_keeps_unclean_state() -> anyhow::Result<()> {
        let root = temp_root("shutdown-failure");
        let content_root = root.join("content");
        let state_root = root.join("state");
        std::fs::create_dir_all(&content_root)?;
        std::fs::create_dir_all(&state_root)?;

        let store = Arc::new(SnapshotStore::new(state_root.join("index.db")));
        let index = Arc::new(TieredIndex::empty(vec![content_root.clone()]));
        mark_runtime_state(store.path(), true, "stable", "previous-clean", Vec::new())?;

        let missing = content_root.join("missing-without-delete.txt");
        std::fs::write(&missing, b"transient")?;
        index.apply_events(&[crate::core::EventRecord {
            seq: 1,
            timestamp: std::time::SystemTime::now(),
            event_type: crate::core::EventType::Create,
            id: crate::core::FileIdentifier::Path(missing.clone()),
            path_hint: Some(missing.clone()),
        }]);
        std::fs::remove_file(&missing)?;

        let error = handle_shutdown(&index, &store, &None, &Config::default())
            .await
            .unwrap_err();
        assert!(error.to_string().contains("final snapshot failed"));
        assert!(error.to_string().contains("snapshot_upsert_unresolved"));
        assert!(index.is_shutting_down());
        assert!(!index.rebuild_in_progress());

        let state = read_recovery_runtime_state(store.path())?;
        assert!(!state.last_clean_shutdown);
        assert_eq!(state.last_recovery_mode, "shutdown-in-progress");

        let _ = std::fs::remove_dir_all(&root);
        Ok(())
    }

    #[tokio::test]
    async fn handle_shutdown_commits_clean_state_after_final_snapshot() -> anyhow::Result<()> {
        let root = temp_root("shutdown-success");
        let content_root = root.join("content");
        let state_root = root.join("state");
        std::fs::create_dir_all(&content_root)?;
        std::fs::create_dir_all(&state_root)?;

        let store = Arc::new(SnapshotStore::new(state_root.join("index.db")));
        let index = Arc::new(TieredIndex::empty(vec![content_root.clone()]));
        let path = content_root.join("durable.txt");
        std::fs::write(&path, b"durable")?;
        index.apply_events(&[crate::core::EventRecord {
            seq: 1,
            timestamp: std::time::SystemTime::now(),
            event_type: crate::core::EventType::Create,
            id: crate::core::FileIdentifier::Path(path.clone()),
            path_hint: Some(path),
        }]);

        handle_shutdown(&index, &store, &None, &Config::default()).await?;

        let state = read_recovery_runtime_state(store.path())?;
        assert!(state.last_clean_shutdown);
        assert_eq!(state.last_recovery_mode, "clean-shutdown");

        let _ = std::fs::remove_dir_all(&root);
        Ok(())
    }

    #[test]
    fn mark_runtime_state_replaces_corrupt_previous_state() -> anyhow::Result<()> {
        use crate::storage::snapshot::runtime_state_path_for;

        let root = temp_root("replace-corrupt-runtime-state");
        std::fs::create_dir_all(&root)?;
        let snapshot_path = root.join("index.db");
        let runtime_state_path = runtime_state_path_for(&snapshot_path);
        std::fs::create_dir_all(runtime_state_path.parent().expect("state parent"))?;
        std::fs::write(&runtime_state_path, b"not-json")?;

        mark_runtime_state(&snapshot_path, false, "none", "running", Vec::new())?;

        let state = read_recovery_runtime_state(&snapshot_path)?;
        assert!(!state.last_clean_shutdown);
        assert_eq!(state.last_recovery_mode, "running");

        let _ = std::fs::remove_dir_all(&root);
        Ok(())
    }

    #[test]
    fn runtime_profile_cli_accepts_memory_light_aliases() {
        assert_eq!(parse_runtime_profile(None).unwrap(), None);
        assert_eq!(
            parse_runtime_profile(Some("default")).unwrap(),
            Some(RuntimeProfile::Default)
        );
        assert_eq!(
            parse_runtime_profile(Some("memory_light")).unwrap(),
            Some(RuntimeProfile::MemoryLight)
        );
        assert_eq!(
            parse_runtime_profile(Some("memory-light")).unwrap(),
            Some(RuntimeProfile::MemoryLight)
        );
        assert!(parse_runtime_profile(Some("low")).is_err());
    }

    #[test]
    fn strict_watch_plan_admits_all_required_dirs_when_budget_allows() {
        let root = temp_root("strict-budget-ok");
        let documents = root.join("Documents");
        let downloads = root.join("Downloads");
        std::fs::create_dir_all(documents.join("project")).unwrap();
        std::fs::create_dir_all(downloads.join("archive/nested")).unwrap();

        let mut cfg = crate::config::TieredWatchConfig {
            profile: TieredWatchProfile::Strict,
            max_watch_dirs: 64,
            ..crate::config::TieredWatchConfig::default()
        };
        cfg.hot_dirs.clear();
        cfg.strict_required_hot_dirs = vec![documents.clone(), downloads.clone()];

        let plan = build_tiered_watch_plan(std::slice::from_ref(&root), &cfg, &[]);

        assert_eq!(plan.state.watch_profile, "strict");
        assert_eq!(plan.state.l0_dirs, 2);
        assert_eq!(plan.state.l1_dirs, 0);
        assert!(plan.state.required_watch_cost > 0);
        assert_eq!(
            plan.state.logical_watch_cost,
            plan.state.required_watch_cost
        );
        assert_eq!(
            plan.state.kernel_watch_cost,
            plan.state.watched_dirs_estimated as u64
        );
        assert_eq!(plan.state.skipped_watch_cost, 0);
        assert_eq!(plan.state.l0_watch_cost, plan.state.kernel_watch_cost);
        assert_eq!(plan.state.l1_watch_cost, 0);
        assert_eq!(plan.state.watch_budget_shortfall, 0);
        assert!(plan.state.strict_coverage_ok);
        assert!(!plan.state.strict_coverage_failure);
        assert!(plan.state.strict_uncovered_dirs.is_empty());

        let _ = std::fs::remove_dir_all(root);
    }

    #[test]
    fn strict_watch_plan_reports_uncovered_required_dirs_when_budget_is_short() {
        let root = temp_root("strict-budget-short");
        let documents = root.join("Documents");
        let downloads = root.join("Downloads");
        std::fs::create_dir_all(documents.join("project")).unwrap();
        std::fs::create_dir_all(downloads.join("archive/nested")).unwrap();

        let mut cfg = crate::config::TieredWatchConfig {
            profile: TieredWatchProfile::Strict,
            max_watch_dirs: 2,
            ..crate::config::TieredWatchConfig::default()
        };
        cfg.hot_dirs.clear();
        cfg.strict_required_hot_dirs = vec![documents.clone(), downloads.clone()];

        let plan = build_tiered_watch_plan(std::slice::from_ref(&root), &cfg, &[]);

        assert_eq!(plan.state.l0_dirs, 1);
        assert_eq!(plan.state.l1_dirs, 1);
        assert!(plan.state.required_watch_cost > plan.state.max_watch_dirs as u64);
        assert!(plan.state.watch_budget_shortfall > 0);
        assert_eq!(
            plan.state.logical_watch_cost,
            plan.state.required_watch_cost
        );
        assert_eq!(
            plan.state.kernel_watch_cost,
            plan.state.watched_dirs_estimated as u64
        );
        assert!(plan.state.skipped_watch_cost > 0);
        assert_eq!(
            plan.state.logical_watch_cost,
            plan.state
                .kernel_watch_cost
                .saturating_add(plan.state.skipped_watch_cost)
        );
        assert_eq!(plan.state.l0_watch_cost, plan.state.kernel_watch_cost);
        assert_eq!(plan.state.l1_watch_cost, plan.state.skipped_watch_cost);
        assert!(!plan.state.strict_coverage_ok);
        assert!(plan.state.strict_coverage_failure);
        assert_eq!(plan.state.strict_uncovered_dirs.len(), 1);
        assert!(plan.state.strict_uncovered_dirs[0].contains("Downloads"));

        let _ = std::fs::remove_dir_all(root);
    }

    #[test]
    fn strict_watch_plan_rejects_required_dir_over_l0_per_root_cap_even_when_total_budget_allows() {
        let root = temp_root("strict-per-root-cap");
        let documents = root.join("Documents");
        let desktop = root.join("Desktop");
        std::fs::create_dir_all(documents.join("a/b/c/d")).unwrap();
        std::fs::create_dir_all(desktop.join("project")).unwrap();

        let mut cfg = crate::config::TieredWatchConfig {
            profile: TieredWatchProfile::Strict,
            max_watch_dirs: 64,
            l0_max_cost_per_root: 3,
            ..crate::config::TieredWatchConfig::default()
        };
        cfg.hot_dirs.clear();
        cfg.strict_required_hot_dirs = vec![documents.clone(), desktop.clone()];

        let plan = build_tiered_watch_plan(std::slice::from_ref(&root), &cfg, &[]);

        assert_eq!(plan.state.l0_max_cost_per_root, 3);
        assert_eq!(plan.state.l0_dirs, 1);
        assert_eq!(plan.state.l1_dirs, 1);
        assert_eq!(plan.state.watched_dirs_estimated, 2);
        assert_eq!(plan.state.kernel_watch_cost, 2);
        assert_eq!(plan.state.skipped_watch_cost, 4);
        assert_eq!(plan.state.watch_budget_shortfall, 0);
        assert!(!plan.state.strict_coverage_ok);
        assert!(plan.state.strict_coverage_failure);
        assert_eq!(plan.state.strict_uncovered_dirs.len(), 1);
        assert!(plan.state.strict_uncovered_dirs[0].contains("Documents"));
        assert_eq!(plan.watch_roots, Some(vec![desktop.clone()]));
        assert!(plan
            .l1_roots
            .iter()
            .any(|(path, cost)| path == &documents && *cost == 4));

        let _ = std::fs::remove_dir_all(root);
    }

    #[test]
    fn rotating_watch_plan_preserves_cost_needed_for_m2_action_classification() {
        let root = temp_root("rotating-watch-plan-cost");
        for index in 0..300 {
            std::fs::create_dir_all(root.join(format!("d{index:03}"))).unwrap();
        }

        let mut cfg = crate::config::TieredWatchConfig {
            max_watch_dirs: 8,
            l0_max_cost_per_root: 1,
            rotating_cold_window_enabled: true,
            rotating_cold_window_max_cost_per_root: 64,
            ..crate::config::TieredWatchConfig::default()
        };
        cfg.hot_dirs.clear();

        let plan = build_tiered_watch_plan(std::slice::from_ref(&root), &cfg, &[]);
        let (_, watch_cost) = plan
            .l1_roots
            .iter()
            .find(|(path, _)| path == &root)
            .expect("large root should remain outside L0");
        assert_eq!(*watch_cost, 301);

        assert!(*watch_cost > cfg.rotating_cold_window_max_cost_per_root);
        assert!(*watch_cost <= cfg.rotating_cold_window_max_cost_per_root.saturating_mul(8));

        let _ = std::fs::remove_dir_all(root);
    }

    #[test]
    fn acceptance_project_marker_eligibility_respects_exclude_and_ignore() {
        let root = temp_root("marker-eligibility");
        let project = root.join("workspace").join("new-app");
        let excluded = root.join("workspace").join("node_modules").join("pkg");
        let ignored = root.join("workspace").join("ignored").join("pkg");
        std::fs::create_dir_all(&project).unwrap();
        std::fs::create_dir_all(&excluded).unwrap();
        std::fs::create_dir_all(&ignored).unwrap();

        let roots = vec![root.join("workspace")];
        let exclude_dirs = vec!["node_modules".to_string()];
        let ignore_prefixes = vec![root.join("workspace").join("ignored")];

        assert!(project_marker_root_is_eligible(
            project.as_path(),
            &roots,
            &exclude_dirs,
            &ignore_prefixes,
        ));
        assert!(!project_marker_root_is_eligible(
            excluded.as_path(),
            &roots,
            &exclude_dirs,
            &ignore_prefixes,
        ));
        assert!(!project_marker_root_is_eligible(
            ignored.as_path(),
            &roots,
            &exclude_dirs,
            &ignore_prefixes,
        ));

        let _ = std::fs::remove_dir_all(root);
    }

    #[test]
    fn acceptance_low_power_and_strict_profiles_keep_budget_semantics() {
        let root = temp_root("watch-profile-budget");
        let active = root.join("Active");
        let archive = root.join("Archive");
        std::fs::create_dir_all(active.join("project")).unwrap();
        std::fs::create_dir_all(archive.join("cold")).unwrap();

        let mut low_power = crate::config::TieredWatchConfig {
            profile: TieredWatchProfile::LowPower,
            max_watch_dirs: 2,
            ..crate::config::TieredWatchConfig::default()
        };
        low_power.hot_dirs = vec![active.clone(), archive.clone()];
        let low_power_plan = build_tiered_watch_plan(std::slice::from_ref(&root), &low_power, &[]);

        assert_eq!(low_power_plan.state.watch_profile, "low_power");
        assert!(low_power_plan.state.watched_dirs_estimated <= low_power_plan.state.max_watch_dirs);
        assert!(low_power_plan.state.strict_coverage_ok);
        assert!(!low_power_plan.state.strict_coverage_failure);
        assert!(low_power_plan.state.l1_dirs >= 1);

        let mut strict = crate::config::TieredWatchConfig {
            profile: TieredWatchProfile::Strict,
            max_watch_dirs: 2,
            ..crate::config::TieredWatchConfig::default()
        };
        strict.hot_dirs.clear();
        strict.strict_required_hot_dirs = vec![active.clone(), archive.clone()];
        strict.strict_fail_on_budget_exceeded = true;
        let strict_plan = build_tiered_watch_plan(std::slice::from_ref(&root), &strict, &[]);

        assert_eq!(strict_plan.state.watch_profile, "strict");
        assert!(strict_plan.state.strict_fail_on_budget_exceeded);
        assert!(!strict_plan.state.strict_coverage_ok);
        assert!(strict_plan.state.strict_coverage_failure);
        assert!(!strict_plan.state.strict_uncovered_dirs.is_empty());
        assert!(strict_plan.state.watched_dirs_estimated <= strict_plan.state.max_watch_dirs);

        let _ = std::fs::remove_dir_all(root);
    }
}
