use notify::{Watcher, RecursiveMode, Config};
use tokio::sync::mpsc;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

/// 文件系统事件监听器
/// 使用 bounded channel 做背压，避免无限堆积
pub struct EventWatcher;

impl EventWatcher {
    /// 启动监听，返回事件接收端
    pub fn start(
        _roots: &[std::path::PathBuf],
        channel_size: usize,
        overflow_drops: Arc<AtomicU64>,
    ) -> anyhow::Result<(mpsc::Receiver<notify::Event>, notify::RecommendedWatcher)> {
        let (tx, rx) = mpsc::channel(channel_size);

        let watcher = notify::RecommendedWatcher::new(
            move |res: notify::Result<notify::Event>| {
                if let Ok(event) = res {
                    // 非阻塞发送：队列满时丢弃并计数
                    if tx.try_send(event).is_err() {
                        let drops = overflow_drops.fetch_add(1, Ordering::Relaxed);
                        if drops % 1000 == 0 {
                            eprintln!("[fd-rdd] event channel overflow, total drops: {}", drops + 1);
                        }
                    }
                }
            },
            Config::default(),
        )?;

        // 注意：watcher 必须由调用方持有，否则会被 drop
        Ok((rx, watcher))
    }
}

/// 注册监听路径（分离出来方便错误处理）
pub fn watch_roots(
    watcher: &mut notify::RecommendedWatcher,
    roots: &[std::path::PathBuf],
) {
    for root in roots {
        if let Err(e) = watcher.watch(root, RecursiveMode::Recursive) {
            tracing::warn!("Failed to watch {:?}: {}", root, e);
        }
    }
}