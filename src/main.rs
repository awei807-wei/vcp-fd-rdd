use clap::Parser;
use fd_rdd::event::EventPipeline;
use fd_rdd::index::TieredIndex;
use fd_rdd::query::QueryServer;
use fd_rdd::query::SocketServer;
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
    /// 要索引的根目录（可重复传入）；未传入则默认使用 $HOME（以及存在时的 /tmp/vcp_test_data）
    #[arg(long = "root", value_name = "PATH")]
    roots: Vec<PathBuf>,

    /// 快照路径（默认: $XDG_DATA_HOME/fd-rdd/index.db 或 /tmp/fd-rdd/index.db）
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
    #[arg(long, default_value_t = 4096)]
    event_channel_size: usize,

    /// watcher 事件 debounce 窗口（毫秒）
    #[arg(long, default_value_t = 100)]
    debounce_ms: u64,

    /// watcher 忽略路径前缀（可重复）；用于排除 snapshot/log 等“自触发”路径
    ///
    /// 说明：fd-rdd 会默认忽略 `--snapshot-path` 以及派生的 `index.d/`；这里用于补充额外忽略项。
    #[arg(long = "ignore-path", value_name = "PATH")]
    ignore_paths: Vec<PathBuf>,

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

    info!(
        "Starting fd-rdd v{}: atomic-snapshot file indexer",
        env!("CARGO_PKG_VERSION")
    );

    // 1) 确定索引根目录
    let mut roots = args.roots;
    if roots.is_empty() {
        let home_dir = dirs::home_dir().unwrap_or_else(|| std::path::PathBuf::from("/"));
        roots.push(home_dir);

        let test_data = std::path::PathBuf::from("/tmp/vcp_test_data");
        if test_data.exists() {
            roots.push(test_data);
        }
    }

    // 2) 快照存储
    let snapshot_path = args.snapshot_path.unwrap_or_else(|| {
        dirs::data_local_dir()
            .unwrap_or_else(|| std::path::PathBuf::from("/tmp"))
            .join("fd-rdd")
            .join("index.db")
    });
    let store = Arc::new(SnapshotStore::new(snapshot_path));

    // 3) 从快照加载或空索引启动
    let index = if args.no_snapshot {
        Arc::new(TieredIndex::empty_with_hidden(roots, args.include_hidden))
    } else {
        TieredIndex::load_with_hidden(store.as_ref(), roots, args.include_hidden).await?
    };
    index.set_auto_flush_limits(args.auto_flush_overlay_paths, args.auto_flush_overlay_bytes);
    index.set_periodic_flush_batch_limits(args.batch_flush_min_events, args.batch_flush_min_bytes);
    // WAL：即使 --no_snapshot，也允许记录后续事件（仅不回放历史）。
    let _ = index.attach_wal(&store);

    // 4) 若索引为空，后台全量构建
    if index.file_count() == 0 && !args.no_build {
        index.spawn_full_build();
    }

    // 5) 启动事件管道（bounded + debounce）
    let pipeline = if args.no_watch {
        None
    } else {
        // 默认忽略索引自身的 snapshot/segment 写入路径，避免 watcher 反馈循环。
        // 额外忽略项可通过 --ignore-path 传入（例如将日志重定向到了被 watch 的目录下）。
        let mut ignores = args.ignore_paths.clone();
        ignores.push(store.path().to_path_buf());
        ignores.push(store.derived_lsm_dir_path());

        let pipeline = EventPipeline::new_with_config_and_ignores(
            index.clone(),
            args.debounce_ms,
            args.event_channel_size,
            ignores,
        );
        pipeline.start().await?;
        Some(pipeline)
    };

    // 6) 启动 HTTP 查询服务
    let query_server = QueryServer::new(index.clone());
    let http_port = args.http_port;
    tokio::spawn(async move {
        if let Err(e) = query_server.run(http_port).await {
            tracing::error!("Query server error: {}", e);
        }
    });

    // 6.5) 启动 UDS 查询服务（可选，流式）
    if let Some(path) = args.uds_socket.clone() {
        let socket_server = SocketServer::new(index.clone());
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
    if let Err(e) = index.snapshot_now(&store).await {
        tracing::error!("Final snapshot failed: {}", e);
    }
    info!("Goodbye.");

    Ok(())
}
