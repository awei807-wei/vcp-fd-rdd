use notify::{Config, RecursiveMode, Watcher};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use tokio::sync::mpsc;

use crate::event::recovery::DirtyTracker;

fn handle_notify_result(
    tx: &mpsc::Sender<notify::Event>,
    dirty: Option<&DirtyTracker>,
    overflow_drops: &AtomicU64,
    rescan_signals: &AtomicU64,
    res: notify::Result<notify::Event>,
) {
    if let Ok(event) = res {
        // inotify 队列溢出（Q_OVERFLOW）会被 notify 标记为 Rescan：无 path，需要全局 dirty。
        if event.need_rescan() {
            rescan_signals.fetch_add(1, Ordering::Relaxed);
            if let Some(d) = dirty {
                d.mark_dirty_all();
            }
        }

        // 非阻塞发送：队列满时丢弃并计数
        match tx.try_send(event) {
            Ok(_) => {}
            Err(err) => {
                let event = err.into_inner();
                if let Some(d) = dirty {
                    // best-effort：在 overload 场景尽量记录 dirty dirs；上限触发时降级为 dirty_all。
                    d.record_overflow_paths(&event.paths);
                }
                let drops = overflow_drops.fetch_add(1, Ordering::Relaxed);
                if drops % 1000 == 0 {
                    eprintln!(
                        "[fd-rdd] event channel overflow, total drops: {}",
                        drops + 1
                    );
                }
            }
        }
    }
}

/// 文件系统事件监听器
/// 使用 bounded channel 做背压，避免无限堆积
pub struct EventWatcher;

impl EventWatcher {
    /// 启动监听，返回事件接收端
    pub fn start(
        _roots: &[std::path::PathBuf],
        channel_size: usize,
        overflow_drops: Arc<AtomicU64>,
        rescan_signals: Arc<AtomicU64>,
        dirty: Option<Arc<DirtyTracker>>,
    ) -> anyhow::Result<(mpsc::Receiver<notify::Event>, notify::RecommendedWatcher)> {
        if channel_size == 0 {
            anyhow::bail!("event channel_size must be >= 1");
        }
        let (tx, rx) = mpsc::channel(channel_size);
        let dirty = dirty.clone();

        let watcher = notify::RecommendedWatcher::new(
            move |res: notify::Result<notify::Event>| {
                handle_notify_result(
                    &tx,
                    dirty.as_deref(),
                    overflow_drops.as_ref(),
                    rescan_signals.as_ref(),
                    res,
                );
            },
            Config::default(),
        )?;

        // 注意：watcher 必须由调用方持有，否则会被 drop
        Ok((rx, watcher))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::event::recovery::DirtyScope;

    #[test]
    fn reject_zero_channel_size() {
        let drops = Arc::new(AtomicU64::new(0));
        let rescans = Arc::new(AtomicU64::new(0));
        let roots: Vec<std::path::PathBuf> = Vec::new();
        let res = EventWatcher::start(&roots, 0, drops, rescans, None);
        assert!(res.is_err());
    }

    #[test]
    fn rescan_event_marks_dirty_all() {
        let drops = AtomicU64::new(0);
        let rescans = AtomicU64::new(0);
        let (tx, _rx) = mpsc::channel(16);
        let dirty = DirtyTracker::new(16);

        let ev = notify::Event::new(notify::EventKind::Other).set_flag(notify::event::Flag::Rescan);
        handle_notify_result(&tx, Some(dirty.as_ref()), &drops, &rescans, Ok(ev));
        assert_eq!(rescans.load(Ordering::Relaxed), 1);

        let scope = dirty.try_begin_sync(0, 0, 0);
        assert!(matches!(scope, Some(DirtyScope::All { .. })));
    }
}

/// 检查 Linux inotify watch 上限，不够时发出警告。
/// 非 Linux 平台直接返回 None。
pub fn check_inotify_limit(root_count: usize) -> Option<u64> {
    #[cfg(target_os = "linux")]
    {
        if let Ok(content) = std::fs::read_to_string("/proc/sys/fs/inotify/max_user_watches") {
            if let Ok(limit) = content.trim().parse::<u64>() {
                // 粗略估算：每个根目录平均 ~1000 个子目录需要 watch
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

/// 注册监听路径，返回加 watch 失败的目录列表（供降级轮询使用）。
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
