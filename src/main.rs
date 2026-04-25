use clap::Parser;
use fd_rdd::config::{default_snapshot_path, default_socket_path, Config};
use fd_rdd::event::ignore_filter::IgnoreFilter;
use fd_rdd::event::EventPipeline;
use fd_rdd::index::TieredIndex;
use fd_rdd::query::SocketServer;
use fd_rdd::query::{HealthTelemetry, QueryServer};
use fd_rdd::storage::snapshot::SnapshotStore;
use std::path::PathBuf;
use std::sync::Arc;
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

    /// 启动时忽略快照（即使 snapshot_path 存在），从空索引启动
    #[arg(long)]
    no_snapshot: bool,

    /// 禁用 watcher（只启动查询服务/快照循环；适合做“仅加载快照”对照实验）
    #[arg(long)]
    no_watch: bool,

    /// 禁用后台全量构建（索引为空时也不自动 full_build）
    #[arg(long)]
    no_build: bool,

    /// 将 `.` 开头的文件/目录纳入冷启动全扫、后台重建与增量补扫
    #[arg(long)]
    include_hidden: bool,

    /// HTTP 查询端口
    #[arg(long, default_value_t = 6060)]
    http_port: u16,

    /// Unix domain socket 查询地址（可选）：用于流式输出（避免 HTTP/JSON 聚合带来的峰值）
    #[arg(long, value_name = "PATH")]
    uds_socket: Option<PathBuf>,

    /// 快照写入间隔（秒）
    #[arg(long, default_value_t = 300)]
    snapshot_interval_secs: u64,

    /// 内存报告间隔（秒）
    #[arg(long, default_value_t = 60)]
    report_interval_secs: u64,

    /// RSS trim 检查间隔（秒，0=禁用）
    #[arg(long, default_value_t = 300)]
    trim_interval_secs: u64,

    /// 触发 trim 的 Private_Dirty 阈值（MB，0=禁用）
    #[arg(long, default_value_t = 128)]
    trim_pd_threshold_mb: u64,

    /// watcher 事件 channel 容量（越大越不容易 overflow，但会占用更多内存）
    /// 默认 65536，足以应对 git clone 等批量操作；降低此值可减少内存占用但可能丢失事件。
    #[arg(long, default_value_t = 65536)]
    event_channel_size: usize,

    /// watcher 事件 debounce 窗口（毫秒）
    #[arg(long, default_value_t = 10)]
    debounce_ms: u64,

    /// watcher 忽略路径前缀（可重复）；用于排除 snapshot/log 等“自触发”路径
    ///
    /// 说明：fd-rdd 会默认忽略 `--snapshot-path` 以及派生的 `index.d/`；这里用于补充额外忽略项。
    #[arg(long = "ignore-path", value_name = "PATH")]
    ignore_paths: Vec<PathBuf>,

    /// 禁用 `.gitignore` / `.ignore` / git exclude / global gitignore 规则
    #[arg(long)]
    no_ignore: bool,

    /// 跟随符号链接（默认不跟随）。启用后扫描和 watcher 会进入符号链接指向的目录。
    /// 注意：已有 inode 去重可防止无限递归，但跟随可能导致索引范围远超预期。
    #[arg(long)]
    follow_symlinks: bool,

    /// overlay 强制 flush 阈值（路径数）。达到阈值会唤醒 snapshot_loop 立即执行一次 snapshot_now（0=禁用）
    #[arg(long, default_value_t = 250_000)]
    auto_flush_overlay_paths: u64,

    /// overlay 强制 flush 阈值（arena 字节数，近似“物理路径字节池”体量）（0=禁用）
    #[arg(long, default_value_t = 64 * 1024 * 1024)]
    auto_flush_overlay_bytes: u64,

    /// 定时 flush 的最小事件数门槛；未达到时继续保留在 WAL/L2，等待后续批量落盘（0=禁用）
    #[arg(long, default_value_t = 0)]
    batch_flush_min_events: u64,

    /// 定时 flush 的最小事件字节数门槛；未达到时继续保留在 WAL/L2，等待后续批量落盘（0=禁用）
    #[arg(long, default_value_t = 0)]
    batch_flush_min_bytes: u64,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt::init();

    let args = Args::parse();

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
            http_port: args.http_port,
            snapshot_interval_secs: args.snapshot_interval_secs,
            include_hidden: args.include_hidden,
            follow_symlinks: args.follow_symlinks,
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

    // 2) 快照存储
    let snapshot_path = args.snapshot_path.unwrap_or_else(default_snapshot_path);
    let store = Arc::new(SnapshotStore::new(snapshot_path));

    // 3) 从快照加载或空索引启动
    let index = if args.no_snapshot {
        Arc::new(TieredIndex::empty_with_options(
            roots,
            args.include_hidden,
            ignore_enabled,
        ))
    } else {
        TieredIndex::load_with_options(store.as_ref(), roots, args.include_hidden, ignore_enabled)
            .await?
    };
    index.set_auto_flush_limits(args.auto_flush_overlay_paths, args.auto_flush_overlay_bytes);
    index.set_periodic_flush_batch_limits(args.batch_flush_min_events, args.batch_flush_min_bytes);
    // WAL：即使 --no_snapshot，也允许记录后续事件（仅不回放历史）。
    let _ = index.attach_wal(store.as_ref());

    // 4) 若索引为空，后台全量构建
    if index.file_count() == 0 && !args.no_build {
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

    // 5) 启动事件管道（bounded + debounce）
    let pipeline = if args.no_watch {
        None
    } else {
        // 默认忽略索引自身的 snapshot/segment 写入路径，避免 watcher 反馈循环。
        // 额外忽略项可通过 --ignore-path 传入（例如将日志重定向到了被 watch 的目录下）。
        let pipeline = Arc::new(
            EventPipeline::new_with_config_and_ignores(
                index.clone(),
                args.debounce_ms,
                args.event_channel_size,
                startup_ignore_paths.clone(),
            )
            .with_ignore_filter(ignore_filter.clone()),
        );
        pipeline.start().await?;
        Some(pipeline)
    };

    // 5.5) 启动阶段 best-effort 补偿停机期间的离线变更。
    // 仅在已有索引内容时执行，避免与空索引冷启动 full_build 重复做全量工作。
    if index.file_count() > 0 {
        let _ = index.startup_reconcile(&startup_ignore_paths);
    }

    // 6) 启动 HTTP 查询服务
    let health_provider = {
        let index = index.clone();
        let pipeline = pipeline.clone();
        Arc::new(move || {
            let stats = pipeline.as_ref().map(|p| p.stats()).unwrap_or_default();
            HealthTelemetry {
                last_snapshot_time: index.last_snapshot_time(),
                watch_failures: stats.watch_failures,
                watcher_degraded: stats.watcher_degraded,
                degraded_roots: stats.degraded_roots,
                overflow_drops: stats.overflow_drops,
                rescan_signals: stats.rescan_signals,
            }
        })
    };
    let query_server = QueryServer::new(index.clone()).with_health_provider(health_provider);
    let http_port = args.http_port;
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
    let snapshot_interval_secs = args.snapshot_interval_secs;
    tokio::spawn(async move {
        snap_index
            .snapshot_loop(snap_store, snapshot_interval_secs)
            .await;
    });

    // 8) 启动内存报告循环（每 60 秒）
    {
        let report_index = index.clone();
        let report_interval_secs = args.report_interval_secs;

        let stats_fn = Arc::new(move || {
            if let Some(p) = pipeline.as_ref() {
                p.stats()
            } else {
                fd_rdd::stats::EventPipelineStats::default()
            }
        });

        tokio::spawn(async move {
            report_index
                .memory_report_loop(stats_fn, report_interval_secs)
                .await;
        });
    }

    // 8.5) 启动条件性 RSS trim 循环（按 smaps Private_Dirty 阈值触发）
    {
        let trim_index = index.clone();
        let trim_interval_secs = args.trim_interval_secs;
        let trim_pd_threshold_mb = args.trim_pd_threshold_mb;
        tokio::spawn(async move {
            trim_index
                .rss_trim_loop(trim_interval_secs, trim_pd_threshold_mb)
                .await;
        });
    }

    info!(
        "fd-rdd ready. Query via: http://localhost:{}/search?q=keyword",
        args.http_port
    );

    // 9) 优雅退出：Ctrl+C → 最终快照
    tokio::signal::ctrl_c().await?;
    info!("Shutting down, writing final snapshot...");
    if let Err(e) = index.snapshot_now(store.clone()).await {
        tracing::error!("Final snapshot failed: {}", e);
    }
    info!("Goodbye.");

    Ok(())
}
