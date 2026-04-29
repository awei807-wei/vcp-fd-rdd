use notify::{Config, ErrorKind, RecursiveMode, Watcher};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::mpsc;

/// Heuristic check for ENOSPC / NoStorageSpace errors from notify/inotify.
fn is_enospc_error(e: &notify::Error) -> bool {
    use std::error::Error;

    if matches!(e.kind, ErrorKind::MaxFilesWatch) {
        return true;
    }

    let msg = e.to_string().to_lowercase();
    if msg.contains("no space") || msg.contains("enospc") || msg.contains("no storage") {
        return true;
    }

    let mut source: Option<&dyn Error> = e.source();
    while let Some(err) = source {
        if let Some(io_err) = err.downcast_ref::<std::io::Error>() {
            if io_err.raw_os_error() == Some(28) {
                return true;
            }
            let msg = io_err.to_string().to_lowercase();
            if msg.contains("no space") || msg.contains("enospc") {
                return true;
            }
        }
        let msg = err.to_string().to_lowercase();
        if msg.contains("no space") || msg.contains("enospc") || msg.contains("no storage") {
            return true;
        }
        source = err.source();
    }
    false
}

fn handle_notify_result(
    priority_tx: &mpsc::Sender<notify::Event>,
    normal_tx: &mpsc::Sender<notify::Event>,
    channel_size: usize,
    rescan_signals: &AtomicU64,
    res: notify::Result<notify::Event>,
) {
    match res {
        Ok(event) => {
            // inotify 队列溢出（Q_OVERFLOW）会被 notify 标记为 Rescan：仅累加可观测计数。
            // 历史的 dirty-region 兜底已删除（参考 重构方案包/causal-chain-report.md 第 8.4 节）。
            if event.need_rescan() {
                rescan_signals.fetch_add(1, Ordering::Relaxed);
            }

            // 分级队列：Create 事件走快速路径
            let is_create = matches!(event.kind, notify::EventKind::Create(_));
            let tx = if is_create { priority_tx } else { normal_tx };

            // 动态背压：channel 水位 >80% 时主动 sleep，避免事件堆积压垮下游
            let remaining = tx.capacity();
            if remaining < channel_size.saturating_mul(2) / 10 {
                let delay_ms = 10u64.saturating_add((channel_size - remaining) as u64 % 41);
                std::thread::sleep(Duration::from_millis(delay_ms));
            }

            if let Err(e) = tx.blocking_send(event) {
                tracing::warn!("event channel closed, dropping event: {:?}", e);
            }
        }
        Err(e) => {
            if is_enospc_error(&e) {
                tracing::warn!("inotify watch limit exceeded (ENOSPC): {}", e);
            } else {
                tracing::debug!("notify error (non-fatal): {}", e);
            }
        }
    }
}

/// (priority_rx, normal_rx, priority_tx, normal_tx, watcher)
pub type WatcherBundle = (
    mpsc::Receiver<notify::Event>,
    mpsc::Receiver<notify::Event>,
    mpsc::Sender<notify::Event>,
    mpsc::Sender<notify::Event>,
    notify::RecommendedWatcher,
);

/// 文件系统事件监听器
/// 使用 bounded channel 做背压，避免无限堆积
pub struct EventWatcher;

impl EventWatcher {
    /// 启动监听，返回事件接收端、发送端（用于合成事件）和 watcher。
    pub fn start(
        _roots: &[std::path::PathBuf],
        channel_size: usize,
        _overflow_drops: Arc<AtomicU64>,
        rescan_signals: Arc<AtomicU64>,
    ) -> anyhow::Result<WatcherBundle> {
        if channel_size == 0 {
            anyhow::bail!("event channel_size must be >= 1");
        }
        let (priority_tx, priority_rx) = mpsc::channel(channel_size);
        let (normal_tx, normal_rx) = mpsc::channel(channel_size);

        let priority_tx_clone = priority_tx.clone();
        let normal_tx_clone = normal_tx.clone();

        let watcher = notify::RecommendedWatcher::new(
            move |res: notify::Result<notify::Event>| {
                handle_notify_result(
                    &priority_tx,
                    &normal_tx,
                    channel_size,
                    rescan_signals.as_ref(),
                    res,
                );
            },
            Config::default(),
        )?;

        Ok((
            priority_rx,
            normal_rx,
            priority_tx_clone,
            normal_tx_clone,
            watcher,
        ))
    }
}

/// 检查 Linux inotify watch 上限，不够时发出警告。
/// 非 Linux 平台直接返回 None。
pub fn check_inotify_limit(root_count: usize) -> Option<u64> {
    #[cfg(target_os = "linux")]
    {
        if let Ok(content) = std::fs::read_to_string("/proc/sys/fs/inotify/max_user_watches") {
            if let Ok(limit) = content.trim().parse::<u64>() {
                let estimated_need = (root_count as u64).saturating_mul(1000);
                if limit < estimated_need {
                    tracing::warn!(
                        "inotify max_user_watches={} may be insufficient for {} roots (estimated need: {}). \
                         Consider: sudo sysctl fs.inotify.max_user_watches=524288",
                        limit, root_count, estimated_need
                    );
                }
                return Some(limit);
            }
        }
    }
    let _ = root_count;
    None
}

/// 注册监听路径，返回加 watch 失败的目录列表。
pub fn watch_roots(
    watcher: &mut notify::RecommendedWatcher,
    roots: &[std::path::PathBuf],
) -> Vec<std::path::PathBuf> {
    let mut failed = Vec::new();
    for root in roots {
        if let Err(e) = watcher.watch(root, RecursiveMode::Recursive) {
            tracing::warn!(
                "Failed to watch {:?}: {} — will fallback to polling",
                root,
                e
            );
            failed.push(root.clone());
        }
    }
    failed
}

#[cfg(target_os = "linux")]
/// Roughly estimate the number of inotify watches a root will need.
fn estimate_watch_count(path: &std::path::Path, max_depth: usize) -> u64 {
    if max_depth == 0 {
        return 1;
    }
    let mut count = 1u64;
    if let Ok(entries) = std::fs::read_dir(path) {
        for entry in entries.flatten() {
            if let Ok(ft) = entry.file_type() {
                if ft.is_dir() {
                    count += 1;
                    if count >= 10_000 {
                        return count;
                    }
                    count += estimate_watch_count(&entry.path(), max_depth.saturating_sub(1));
                }
            }
        }
    }
    count
}

#[cfg(target_os = "linux")]
fn read_inotify_limit() -> Option<u64> {
    std::fs::read_to_string("/proc/sys/fs/inotify/max_user_watches")
        .ok()?
        .trim()
        .parse::<u64>()
        .ok()
}
#[cfg(not(target_os = "linux"))]
#[allow(dead_code)]
fn read_inotify_limit() -> Option<u64> {
    None
}

/// 注册监听路径并区分 failed / degraded（仅作为 stats 信号使用）。
pub fn watch_roots_enhanced(
    watcher: &mut notify::RecommendedWatcher,
    roots: &[std::path::PathBuf],
) -> (Vec<std::path::PathBuf>, Vec<std::path::PathBuf>) {
    let mut failed_roots = Vec::new();
    #[allow(unused_mut)]
    let mut degraded_roots = Vec::new();

    #[cfg(target_os = "linux")]
    let limit = read_inotify_limit();

    for root in roots {
        if let Err(e) = watcher.watch(root, RecursiveMode::Recursive) {
            if is_enospc_error(&e) {
                tracing::warn!(
                    "Failed to watch {:?}: {} — inotify limit exceeded, marking degraded",
                    root,
                    e
                );
                degraded_roots.push(root.clone());
            } else {
                tracing::warn!(
                    "Failed to watch {:?}: {} — will fallback to polling",
                    root,
                    e
                );
                failed_roots.push(root.clone());
            }
        } else {
            #[cfg(target_os = "linux")]
            if let Some(limit_val) = limit {
                let estimated = estimate_watch_count(root, 3);
                let safety_margin = estimated.max(100);
                if limit_val < estimated.saturating_add(safety_margin) {
                    tracing::warn!(
                        "inotify limit tight for {:?}: limit={}, estimated_need={}+{}, marking degraded",
                        root, limit_val, estimated, safety_margin
                    );
                    degraded_roots.push(root.clone());
                }
            }
        }
    }
    (failed_roots, degraded_roots)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn reject_zero_channel_size() {
        let drops = Arc::new(AtomicU64::new(0));
        let rescans = Arc::new(AtomicU64::new(0));
        let roots: Vec<std::path::PathBuf> = Vec::new();
        let res = EventWatcher::start(&roots, 0, drops, rescans);
        assert!(res.is_err());
    }

    #[test]
    fn rescan_event_increments_signal_counter() {
        let rescans = AtomicU64::new(0);
        let (priority_tx, _priority_rx) = mpsc::channel(16);
        let (normal_tx, _normal_rx) = mpsc::channel(16);

        let ev = notify::Event::new(notify::EventKind::Other).set_flag(notify::event::Flag::Rescan);
        handle_notify_result(&priority_tx, &normal_tx, 16, &rescans, Ok(ev));
        assert_eq!(rescans.load(Ordering::Relaxed), 1);
    }
}
