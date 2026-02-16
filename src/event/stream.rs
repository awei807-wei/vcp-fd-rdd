use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use crate::core::{EventRecord, EventType};
use crate::event::watcher::{watch_roots, EventWatcher};
use crate::index::TieredIndex;
use crate::stats::EventPipelineStats;

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
        }
    }

    /// 获取事件管道统计
    pub fn stats(&self) -> EventPipelineStats {
        EventPipelineStats {
            last_batch_size: self.last_batch_size.load(Ordering::Relaxed) as usize,
            total_events_processed: self.total_events.load(Ordering::Relaxed),
            overflow_drops: self.overflow_drops.load(Ordering::Relaxed),
        }
    }

    /// 启动事件管道
    pub async fn start(&self) -> anyhow::Result<()> {
        let roots = self.index.roots.clone();
        let overflow_drops = self.overflow_drops.clone();
        let (mut rx, mut watcher) =
            EventWatcher::start(&roots, self.channel_size, overflow_drops.clone())?;
        watch_roots(&mut watcher, &roots);

        let index = self.index.clone();
        let debounce_ms = self.debounce_ms;
        let total_events = self.total_events.clone();
        let last_batch_size = self.last_batch_size.clone();
        let overflow_drops_seen = overflow_drops.clone();
        let ignore_paths = self.ignore_paths.clone();

        tokio::spawn(async move {
            // 保持 watcher 存活
            let _watcher = watcher;
            let mut seq: u64 = 0;
            let mut last_overflow_handled: u64 = 0;
            let mut last_rebuild_at =
                tokio::time::Instant::now() - std::time::Duration::from_secs(3600);
            let rebuild_cooldown = std::time::Duration::from_secs(60);

            loop {
                // 收集一批事件（debounce 窗口）
                let mut raw_events = Vec::new();

                // 等待第一个事件
                match rx.recv().await {
                    Some(ev) => raw_events.push(ev),
                    None => break, // channel closed
                }

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
                let records = merge_events(&mut seq, raw_events);

                if !records.is_empty() {
                    let merged_count = records.len();
                    tracing::debug!(
                        "EventPipeline: raw={} merged={} total={}",
                        raw_count,
                        merged_count,
                        total_events.load(Ordering::Relaxed) + merged_count as u64
                    );
                    index.apply_events(&records);
                    total_events.fetch_add(merged_count as u64, Ordering::Relaxed);
                    last_batch_size.store(merged_count as u64, Ordering::Relaxed);
                }

                // 如果发生过 channel overflow，索引增量更新可能已经丢失事件，必须触发兜底重建。
                // 为避免风暴下频繁重建，这里加一个最小冷却时间。
                let cur_overflow = overflow_drops_seen.load(Ordering::Relaxed);
                if cur_overflow > last_overflow_handled
                    && last_rebuild_at.elapsed() >= rebuild_cooldown
                {
                    last_overflow_handled = cur_overflow;
                    last_rebuild_at = tokio::time::Instant::now();
                    index.spawn_rebuild("event channel overflow");
                }
            }

            tracing::warn!("Event pipeline stopped");
        });

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

/// 合并事件：同一路径的多个事件合并为最终状态
fn merge_events(seq: &mut u64, raw: Vec<notify::Event>) -> Vec<EventRecord> {
    let mut merged: HashMap<PathBuf, EventRecord> = HashMap::new();
    let now = std::time::SystemTime::now();

    for ev in raw {
        // 处理 Rename（双路径事件）
        if matches!(
            ev.kind,
            notify::EventKind::Modify(notify::event::ModifyKind::Name(_))
        ) && ev.paths.len() >= 2
        {
            let from = ev.paths[0].clone();
            let to = ev.paths[1].clone();

            // 移除 from 的旧事件
            merged.remove(&from);

            *seq += 1;
            merged.insert(
                to.clone(),
                EventRecord {
                    seq: *seq,
                    timestamp: now,
                    event_type: EventType::Rename { from },
                    path: to,
                },
            );
            continue;
        }

        // 普通事件
        let path = match ev.paths.first() {
            Some(p) => p.clone(),
            None => continue,
        };

        let event_type: EventType = ev.kind.into();

        // 合并策略：后到的事件覆盖先到的
        *seq += 1;
        merged.insert(
            path.clone(),
            EventRecord {
                seq: *seq,
                timestamp: now,
                event_type,
                path,
            },
        );
    }

    let mut records: Vec<EventRecord> = merged.into_values().collect();
    records.sort_by_key(|r| r.seq);
    records
}
