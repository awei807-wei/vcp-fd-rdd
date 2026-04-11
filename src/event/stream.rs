use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use crate::core::{EventRecord, EventType, FileIdentifier};
use crate::event::recovery::{DirtyTracker, DirtyScope};
use crate::event::watcher::{check_inotify_limit, watch_roots, EventWatcher};
use crate::index::TieredIndex;
use crate::stats::EventPipelineStats;

fn shrink_if_large_vec<T>(v: &mut Vec<T>, keep_cap: usize) -> bool {
    if v.capacity() > keep_cap.saturating_mul(2) {
        v.shrink_to(keep_cap);
        return true;
    }
    false
}

fn shrink_if_large_map<K, V>(m: &mut HashMap<K, V>, keep_cap: usize) -> bool
where
    K: std::hash::Hash + Eq,
{
    if m.capacity() > keep_cap.saturating_mul(2) {
        m.shrink_to(keep_cap);
        return true;
    }
    false
}

use crate::util::maybe_trim_rss;

/// 事件管道：bounded channel + debounce/合并 + 批量应用
pub struct EventPipeline {
    index: Arc<TieredIndex>,
    /// debounce 窗口（毫秒）
    debounce_ms: u64,
    /// bounded channel 容量
    channel_size: usize,
    /// watcher 事件过滤：忽略这些路径前缀下的事件（用于避免索引写入反哺 watcher）
    ignore_paths: Vec<PathBuf>,
    /// 共享计数器：累计处理事件数
    pub total_events: Arc<AtomicU64>,
    /// 共享计数器：最近批次大小
    pub last_batch_size: Arc<AtomicU64>,
    /// 共享计数器：溢出丢弃次数
    pub overflow_drops: Arc<AtomicU64>,
    /// 共享计数器：notify/inotify Rescan 信号次数（可能漏事件，需要补扫）
    pub rescan_signals: Arc<AtomicU64>,
    /// 共享计数器：raw_events(Vec<notify::Event>) capacity
    pub raw_events_capacity: Arc<AtomicU64>,
    /// 共享计数器：merged(HashMap<FileIdentifier, EventRecord>) capacity
    pub merged_map_capacity: Arc<AtomicU64>,
    /// 共享计数器：records(Vec<EventRecord>) capacity
    pub records_capacity: Arc<AtomicU64>,
}

impl EventPipeline {
    pub fn new(index: Arc<TieredIndex>) -> Self {
        Self {
            index,
            debounce_ms: 100,
            channel_size: 4096,
            ignore_paths: Vec::new(),
            total_events: Arc::new(AtomicU64::new(0)),
            last_batch_size: Arc::new(AtomicU64::new(0)),
            overflow_drops: Arc::new(AtomicU64::new(0)),
            rescan_signals: Arc::new(AtomicU64::new(0)),
            raw_events_capacity: Arc::new(AtomicU64::new(0)),
            merged_map_capacity: Arc::new(AtomicU64::new(0)),
            records_capacity: Arc::new(AtomicU64::new(0)),
        }
    }

    pub fn new_with_config(index: Arc<TieredIndex>, debounce_ms: u64, channel_size: usize) -> Self {
        Self {
            index,
            debounce_ms,
            channel_size,
            ignore_paths: Vec::new(),
            total_events: Arc::new(AtomicU64::new(0)),
            last_batch_size: Arc::new(AtomicU64::new(0)),
            overflow_drops: Arc::new(AtomicU64::new(0)),
            rescan_signals: Arc::new(AtomicU64::new(0)),
            raw_events_capacity: Arc::new(AtomicU64::new(0)),
            merged_map_capacity: Arc::new(AtomicU64::new(0)),
            records_capacity: Arc::new(AtomicU64::new(0)),
        }
    }

    pub fn new_with_config_and_ignores(
        index: Arc<TieredIndex>,
        debounce_ms: u64,
        channel_size: usize,
        ignore_paths: Vec<PathBuf>,
    ) -> Self {
        Self {
            index,
            debounce_ms,
            channel_size,
            ignore_paths,
            total_events: Arc::new(AtomicU64::new(0)),
            last_batch_size: Arc::new(AtomicU64::new(0)),
            overflow_drops: Arc::new(AtomicU64::new(0)),
            rescan_signals: Arc::new(AtomicU64::new(0)),
            raw_events_capacity: Arc::new(AtomicU64::new(0)),
            merged_map_capacity: Arc::new(AtomicU64::new(0)),
            records_capacity: Arc::new(AtomicU64::new(0)),
        }
    }

    /// 获取事件管道统计
    pub fn stats(&self) -> EventPipelineStats {
        EventPipelineStats {
            last_batch_size: self.last_batch_size.load(Ordering::Relaxed) as usize,
            total_events_processed: self.total_events.load(Ordering::Relaxed),
            overflow_drops: self.overflow_drops.load(Ordering::Relaxed),
            rescan_signals: self.rescan_signals.load(Ordering::Relaxed),
            raw_events_capacity: self.raw_events_capacity.load(Ordering::Relaxed) as usize,
            merged_map_capacity: self.merged_map_capacity.load(Ordering::Relaxed) as usize,
            records_capacity: self.records_capacity.load(Ordering::Relaxed) as usize,
        }
    }

    /// 启动事件管道
    pub async fn start(&self) -> anyhow::Result<()> {
        let roots = self.index.roots.clone();
        let overflow_drops = self.overflow_drops.clone();
        let rescan_signals = self.rescan_signals.clone();
        let dirty = DirtyTracker::new(self.channel_size.saturating_mul(4).max(1024));
        let keep_cap = self.channel_size.max(256);
        let (mut rx, mut watcher) = EventWatcher::start(
            &roots,
            self.channel_size,
            overflow_drops.clone(),
            rescan_signals.clone(),
            Some(dirty.clone()),
        )?;
        // inotify watch 数兜底检查
        check_inotify_limit(roots.len());
        let failed_roots = watch_roots(&mut watcher, &roots);

        let index = self.index.clone();
        let debounce_ms = self.debounce_ms;
        let total_events = self.total_events.clone();
        let last_batch_size = self.last_batch_size.clone();
        let ignore_paths = self.ignore_paths.clone();
        let raw_events_capacity = self.raw_events_capacity.clone();
        let merged_map_capacity = self.merged_map_capacity.clone();
        let records_capacity = self.records_capacity.clone();
        let dirty_activity = dirty.clone();

        tokio::spawn(async move {
            // 保持 watcher 存活
            let _watcher = watcher;
            let mut seq: u64 = 0;
            let mut raw_events: Vec<notify::Event> = Vec::with_capacity(256);
            let mut merge_scratch = MergeScratch::default();
            let mut last_idle_trim = tokio::time::Instant::now();

            loop {
                // 收集一批事件（debounce 窗口）
                raw_events.clear();

                // 等待第一个事件；若长期空闲，则回收事件缓冲的高水位 capacity（避免 plateau 被高水位“粘住”）。
                let first = tokio::select! {
                    ev = rx.recv() => ev,
                    _ = tokio::time::sleep(std::time::Duration::from_secs(1)) => {
                        // idle maintenance：每 5s 最多触发一次 shrink+trim，避免频繁抖动。
                        if last_idle_trim.elapsed() >= std::time::Duration::from_secs(5) {
                            let mut shrunk = false;
                            shrunk |= shrink_if_large_vec(&mut raw_events, keep_cap);
                            shrunk |= shrink_if_large_map(&mut merge_scratch.merged, keep_cap);
                            shrunk |= shrink_if_large_vec(&mut merge_scratch.records, keep_cap);
                            if shrunk {
                                // 同步更新观测值，便于 fs-churn 归因。
                                raw_events_capacity.store(raw_events.capacity() as u64, Ordering::Relaxed);
                                merged_map_capacity.store(merge_scratch.merged.capacity() as u64, Ordering::Relaxed);
                                records_capacity.store(merge_scratch.records.capacity() as u64, Ordering::Relaxed);
                                maybe_trim_rss();
                            }
                            last_idle_trim = tokio::time::Instant::now();
                        }
                        continue;
                    }
                };

                match first {
                    Some(ev) => raw_events.push(ev),
                    None => break, // channel closed
                }
                // 活动信号：用于 cooldown/max-staleness 兜底调度。
                dirty_activity.record_activity();

                // debounce：在窗口内继续收集
                let deadline =
                    tokio::time::Instant::now() + std::time::Duration::from_millis(debounce_ms);

                loop {
                    let timeout = deadline.saturating_duration_since(tokio::time::Instant::now());
                    if timeout.is_zero() {
                        break;
                    }
                    match tokio::time::timeout(timeout, rx.recv()).await {
                        Ok(Some(ev)) => raw_events.push(ev),
                        _ => break,
                    }
                }

                let raw_count = raw_events.len();

                // 过滤：忽略索引自身写入（snapshot/segments/log 等）导致的“自触发事件风暴”
                if !ignore_paths.is_empty() {
                    raw_events.retain(|ev| !should_ignore_event(ev, &ignore_paths));
                }

                // 合并去重
                merge_events_in_place(&mut seq, &mut raw_events, &mut merge_scratch);

                raw_events_capacity.store(raw_events.capacity() as u64, Ordering::Relaxed);
                merged_map_capacity
                    .store(merge_scratch.merged.capacity() as u64, Ordering::Relaxed);
                records_capacity.store(merge_scratch.records.capacity() as u64, Ordering::Relaxed);

                if !merge_scratch.records.is_empty() {
                    let merged_count = merge_scratch.records.len();
                    tracing::debug!(
                        "EventPipeline: raw={} merged={} total={}",
                        raw_count,
                        merged_count,
                        total_events.load(Ordering::Relaxed) + merged_count as u64
                    );
                    index.apply_events_drain(&mut merge_scratch.records);
                    total_events.fetch_add(merged_count as u64, Ordering::Relaxed);
                    last_batch_size.store(merged_count as u64, Ordering::Relaxed);
                }
            }

            tracing::warn!("Event pipeline stopped");
        });

        // overflow 兜底调度：dirty region + cooldown/max-staleness → fast-sync
        {
            let idx = self.index.clone();
            let dirty = dirty.clone();
            let ignores = self.ignore_paths.clone();
            tokio::spawn(async move {
                // 经验值：静默 5s 触发；持续风暴 30s 强制触发一次（避免饿死）。
                let cooldown_ns: u64 = 5_000_000_000;
                let max_staleness_ns: u64 = 30_000_000_000;
                let min_interval_ns: u64 = 15_000_000_000;

                loop {
                    tokio::time::sleep(std::time::Duration::from_millis(200)).await;
                    if dirty.sync_in_progress() {
                        continue;
                    }
                    let Some(scope) =
                        dirty.try_begin_sync(cooldown_ns, max_staleness_ns, min_interval_ns)
                    else {
                        continue;
                    };
                    tracing::warn!(
                        "Event overflow recovery: triggering fast-sync ({:?})",
                        scope
                    );
                    idx.spawn_fast_sync(scope, ignores.clone(), dirty.clone());
                }
            });
        }

        // 降级轮询：对加不上 watch 的目录，定时触发 fast-sync 补扫
        if !failed_roots.is_empty() {
            let poll_idx = self.index.clone();
            let poll_ignores = self.ignore_paths.clone();
            let poll_dirty = dirty.clone();
            let poll_dirs = failed_roots;
            tracing::warn!(
                "Fallback polling enabled for {} unwatched directories",
                poll_dirs.len()
            );
            tokio::spawn(async move {
                let poll_interval = std::time::Duration::from_secs(60);
                loop {
                    tokio::time::sleep(poll_interval).await;
                    tracing::debug!("Fallback poll: scanning {} unwatched dirs", poll_dirs.len());
                    let scope = DirtyScope::Dirs {
                        cutoff_ns: 0,
                        dirs: poll_dirs.clone(),
                    };
                    poll_idx.spawn_fast_sync(scope, poll_ignores.clone(), poll_dirty.clone());
                }
            });
        }

        Ok(())
    }
}

fn should_ignore_event(ev: &notify::Event, ignore_prefixes: &[PathBuf]) -> bool {
    for p in &ev.paths {
        for ig in ignore_prefixes {
            if p.starts_with(ig) || p == ig {
                return true;
            }
        }
    }
    false
}

#[derive(Default)]
struct MergeScratch {
    merged: HashMap<PathBuf, MergedEvent>,
    records: Vec<EventRecord>,
}

struct MergedEvent {
    seq: u64,
    timestamp: std::time::SystemTime,
    event_type: EventType,
    path_hint: Option<PathBuf>,
}

/// 合并事件：同一路径的多个事件合并为最终状态
fn merge_events_in_place(seq: &mut u64, raw: &mut Vec<notify::Event>, scratch: &mut MergeScratch) {
    scratch.merged.clear();
    scratch.records.clear();
    let now = std::time::SystemTime::now();

    for ev in raw.drain(..) {
        let kind = ev.kind;
        let paths = ev.paths;
        // 处理 Rename（双路径事件）
        if matches!(
            kind,
            notify::EventKind::Modify(notify::event::ModifyKind::Name(_))
        ) && paths.len() >= 2
        {
            let mut it = paths.into_iter();
            let Some(from) = it.next() else {
                continue;
            };
            let Some(to) = it.next() else {
                continue;
            };

            // 移除 from 的旧事件
            scratch.merged.remove(&from);

            *seq += 1;
            scratch.merged.insert(
                to,
                MergedEvent {
                    seq: *seq,
                    timestamp: now,
                    event_type: EventType::Rename {
                        from: FileIdentifier::Path(from),
                        // notify 提供的是 Path 身份，from 本身已包含路径，不需要重复 path_hint。
                        from_path_hint: None,
                    },
                    // id=Path 时不重复存储 path_hint，避免多份 PathBuf clone 造成高水位。
                    path_hint: None,
                },
            );
            continue;
        }

        // 普通事件
        let mut it = paths.into_iter();
        let Some(path) = it.next() else {
            continue;
        };

        let event_type: EventType = kind.into();

        // 合并策略：后到的事件覆盖先到的
        *seq += 1;
        scratch.merged.insert(
            path,
            MergedEvent {
                seq: *seq,
                timestamp: now,
                event_type,
                // id=Path 时不重复存储 path_hint，避免多份 PathBuf clone 造成高水位。
                path_hint: None,
            },
        );
    }

    scratch
        .records
        .extend(scratch.merged.drain().map(|(path, v)| EventRecord {
            seq: v.seq,
            timestamp: v.timestamp,
            event_type: v.event_type,
            id: FileIdentifier::Path(path),
            path_hint: v.path_hint,
        }));
    scratch.records.sort_by_key(|r| r.seq);
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn shrink_helpers_reduce_capacity() {
        let mut raw: Vec<notify::Event> = Vec::with_capacity(65_536);
        let mut scratch = MergeScratch {
            merged: HashMap::with_capacity(20_000),
            records: Vec::with_capacity(20_000),
        };

        let keep = 4096;
        assert!(shrink_if_large_vec(&mut raw, keep));
        assert!(raw.capacity() <= keep);

        assert!(shrink_if_large_map(&mut scratch.merged, keep));
        assert!(scratch.merged.capacity() <= keep.saturating_mul(2));

        assert!(shrink_if_large_vec(&mut scratch.records, keep));
        assert!(scratch.records.capacity() <= keep);
    }

    #[test]
    fn shrink_helpers_noop_when_small() {
        let mut raw: Vec<notify::Event> = Vec::with_capacity(1024);
        let mut merged: HashMap<u64, u64> = HashMap::with_capacity(1024);

        assert!(!shrink_if_large_vec(&mut raw, 4096));
        assert!(!shrink_if_large_map(&mut merged, 4096));
    }

    fn mk_event(kind: notify::EventKind, paths: Vec<PathBuf>) -> notify::Event {
        notify::Event {
            kind,
            paths,
            attrs: Default::default(),
        }
    }

    #[test]
    fn merge_dedup_last_wins_without_duplicate_path_hints() {
        let mut seq: u64 = 0;
        let mut raw: Vec<notify::Event> = Vec::new();
        let mut scratch = MergeScratch::default();

        let p = PathBuf::from("/tmp/a.txt");
        raw.push(mk_event(
            notify::EventKind::Create(notify::event::CreateKind::Any),
            vec![p.clone()],
        ));
        raw.push(mk_event(
            notify::EventKind::Modify(notify::event::ModifyKind::Any),
            vec![p.clone()],
        ));

        merge_events_in_place(&mut seq, &mut raw, &mut scratch);
        assert_eq!(scratch.records.len(), 1);
        let r = &scratch.records[0];
        assert!(matches!(r.event_type, EventType::Modify));
        assert_eq!(r.id, FileIdentifier::Path(p));
        assert!(r.path_hint.is_none());
    }

    #[test]
    fn merge_rename_removes_from_and_keeps_to() {
        let mut seq: u64 = 0;
        let mut raw: Vec<notify::Event> = Vec::new();
        let mut scratch = MergeScratch::default();

        let from_path = PathBuf::from("/tmp/from.txt");
        let to = PathBuf::from("/tmp/to.txt");

        raw.push(mk_event(
            notify::EventKind::Modify(notify::event::ModifyKind::Any),
            vec![from_path.clone()],
        ));
        raw.push(mk_event(
            notify::EventKind::Modify(notify::event::ModifyKind::Name(
                notify::event::RenameMode::Any,
            )),
            vec![from_path.clone(), to.clone()],
        ));

        merge_events_in_place(&mut seq, &mut raw, &mut scratch);
        assert_eq!(scratch.records.len(), 1);
        let r = &scratch.records[0];
        assert_eq!(r.id, FileIdentifier::Path(to));
        assert!(r.path_hint.is_none());
        match &r.event_type {
            EventType::Rename {
                from,
                from_path_hint,
            } => {
                assert_eq!(from, &FileIdentifier::Path(from_path));
                assert!(from_path_hint.is_none());
            }
            _ => panic!("expected rename event type"),
        }
    }
}
