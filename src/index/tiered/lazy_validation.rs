use std::collections::{HashMap, VecDeque};
use std::path::PathBuf;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::Duration;

use crate::core::{EventRecord, EventType, FileIdentifier, FileKey, FileKind, FileMeta};
use crate::event::sync::{now_ns, DirtyReason};
use crate::index::l2_partition::mtime_to_ns;

use super::TieredIndex;

#[derive(Debug, Default)]
pub(super) struct LazyValidationState {
    queue: VecDeque<LazyValidationJob>,
    cache: HashMap<PathBuf, u64>,
    order: VecDeque<(PathBuf, u64)>,
}

#[derive(Clone, Debug)]
struct LazyValidationJob {
    meta: FileMeta,
}

#[derive(Clone, Debug, Default)]
pub struct LazyValidationReport {
    pub enabled: bool,
    pub pending: usize,
    pub enqueued: u64,
    pub completed: u64,
    pub stale_hits: u64,
    pub cache_hits: u64,
    pub rate_limited: u64,
    pub queue_full: u64,
}

enum LazyValidationEnqueue {
    Enqueued,
    CacheHit,
    QueueFull,
}

impl LazyValidationState {
    fn try_enqueue(
        &mut self,
        meta: FileMeta,
        now: u64,
        ttl_ns: u64,
        cache_entries: usize,
    ) -> LazyValidationEnqueue {
        let path = meta.path.clone();
        if self
            .cache
            .get(&path)
            .is_some_and(|seen| now.saturating_sub(*seen) <= ttl_ns)
        {
            return LazyValidationEnqueue::CacheHit;
        }

        let max_queue = cache_entries.saturating_mul(4).clamp(128, 65_536);
        if self.queue.len() >= max_queue {
            return LazyValidationEnqueue::QueueFull;
        }

        self.cache.insert(path.clone(), now);
        self.order.push_back((path, now));
        while self.cache.len() > cache_entries.max(1) {
            let Some((old_path, old_seen)) = self.order.pop_front() else {
                break;
            };
            if self.cache.get(&old_path).copied() == Some(old_seen) {
                self.cache.remove(&old_path);
            }
        }
        self.queue.push_back(LazyValidationJob { meta });
        LazyValidationEnqueue::Enqueued
    }

    fn pop(&mut self) -> Option<LazyValidationJob> {
        self.queue.pop_front()
    }

    fn len(&self) -> usize {
        self.queue.len()
    }
}

impl TieredIndex {
    pub fn apply_lazy_validation_config(
        &self,
        enabled: bool,
        cache_entries: usize,
        ttl_secs: u64,
        stat_per_sec: u64,
    ) {
        self.lazy_validation_enabled
            .store(enabled, Ordering::Relaxed);
        self.lazy_validation_cache_entries
            .store(cache_entries.max(1) as u64, Ordering::Relaxed);
        self.lazy_validation_ttl_ns.store(
            Duration::from_secs(ttl_secs.max(1))
                .as_nanos()
                .min(u128::from(u64::MAX)) as u64,
            Ordering::Relaxed,
        );
        self.lazy_validation_stat_per_sec
            .store(stat_per_sec.max(1), Ordering::Relaxed);
        if enabled {
            self.lazy_validation_notify.notify_one();
        }
    }

    pub fn lazy_validation_report(&self) -> LazyValidationReport {
        LazyValidationReport {
            enabled: self.lazy_validation_enabled.load(Ordering::Relaxed),
            pending: self
                .lazy_validation_state
                .try_lock()
                .map(|state| state.len())
                .unwrap_or(0),
            enqueued: self.lazy_validation_enqueued.load(Ordering::Relaxed),
            completed: self.lazy_validation_completed.load(Ordering::Relaxed),
            stale_hits: self.lazy_validation_stale_hits.load(Ordering::Relaxed),
            cache_hits: self.lazy_validation_cache_hits.load(Ordering::Relaxed),
            rate_limited: self.lazy_validation_rate_limited.load(Ordering::Relaxed),
            queue_full: self.lazy_validation_queue_full.load(Ordering::Relaxed),
        }
    }

    pub(super) fn lazy_validation_is_enabled(&self) -> bool {
        self.lazy_validation_enabled.load(Ordering::Relaxed)
    }

    pub(super) fn try_enqueue_lazy_validation(&self, meta: FileMeta) {
        let now = now_ns();
        let ttl_ns = self.lazy_validation_ttl_ns.load(Ordering::Relaxed);
        let cache_entries = self
            .lazy_validation_cache_entries
            .load(Ordering::Relaxed)
            .max(1) as usize;
        let Some(mut state) = self.lazy_validation_state.try_lock() else {
            self.lazy_validation_rate_limited
                .fetch_add(1, Ordering::Relaxed);
            return;
        };
        match state.try_enqueue(meta, now, ttl_ns, cache_entries) {
            LazyValidationEnqueue::Enqueued => {
                self.lazy_validation_enqueued
                    .fetch_add(1, Ordering::Relaxed);
                self.lazy_validation_notify.notify_one();
            }
            LazyValidationEnqueue::CacheHit => {
                self.lazy_validation_cache_hits
                    .fetch_add(1, Ordering::Relaxed);
            }
            LazyValidationEnqueue::QueueFull => {
                self.lazy_validation_queue_full
                    .fetch_add(1, Ordering::Relaxed);
            }
        }
    }

    pub fn spawn_lazy_validation_worker(self: &Arc<Self>) {
        let index = self.clone();
        tokio::spawn(async move {
            let mut last_stat_ns = 0u64;
            loop {
                if !index.lazy_validation_enabled.load(Ordering::Relaxed) {
                    index.lazy_validation_notify.notified().await;
                    continue;
                }

                let job = { index.lazy_validation_state.lock().pop() };
                let Some(job) = job else {
                    index.lazy_validation_notify.notified().await;
                    continue;
                };

                let stat_per_sec = index
                    .lazy_validation_stat_per_sec
                    .load(Ordering::Relaxed)
                    .max(1);
                let min_interval_ns = 1_000_000_000u64 / stat_per_sec;
                let now = now_ns();
                let due_ns = last_stat_ns.saturating_add(min_interval_ns);
                if due_ns > now {
                    tokio::time::sleep(Duration::from_nanos(due_ns - now)).await;
                }
                last_stat_ns = now_ns();

                let work_index = index.clone();
                let _ = tokio::task::spawn_blocking(move || {
                    work_index.validate_lazy_job(job);
                })
                .await;
            }
        });
    }

    fn validate_lazy_job(&self, job: LazyValidationJob) {
        let meta = job.meta;
        if self.path_is_frozen(meta.path.as_path()) || meta.mtime.is_none() {
            self.lazy_validation_completed
                .fetch_add(1, Ordering::Relaxed);
            return;
        }

        self.io_governor.before_io();
        self.stats.record_cold_validate(1);
        let fs_meta = match std::fs::metadata(&meta.path) {
            Ok(m) if m.is_file() || m.is_dir() => m,
            Ok(_) => {
                self.apply_lazy_delete(meta.path);
                self.lazy_validation_completed
                    .fetch_add(1, Ordering::Relaxed);
                return;
            }
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                self.apply_lazy_delete(meta.path);
                self.lazy_validation_completed
                    .fetch_add(1, Ordering::Relaxed);
                return;
            }
            Err(e) => {
                tracing::debug!(
                    "lazy validation skipped unreadable path {}: {}",
                    meta.path.display(),
                    e
                );
                self.lazy_validation_completed
                    .fetch_add(1, Ordering::Relaxed);
                return;
            }
        };

        let current_key = FileKey::from_path_and_metadata(&meta.path, &fs_meta);
        let old_mtime_ns = mtime_to_ns(meta.mtime);
        let current_mtime = fs_meta.modified().ok();
        let current_mtime_ns = mtime_to_ns(current_mtime);
        let changed =
            current_key.is_some_and(|key| key != meta.file_key) || old_mtime_ns != current_mtime_ns;

        if changed {
            self.stats.record_query_stale_hits(1);
            self.lazy_validation_stale_hits
                .fetch_add(1, Ordering::Relaxed);
            self.enqueue_lazy_dirty_parent(meta.path.as_path());
            // Phase 1：失效父目录的 mtime 预检记录，避免周期扫描错误跳过该目录。
            if let Some(parent) = meta.path.parent() {
                self.directory_manifests.invalidate_mtime_precheck(parent);
            }
            let event = EventRecord {
                seq: 0,
                timestamp: std::time::SystemTime::now(),
                event_type: EventType::Modify,
                id: FileIdentifier::Path(meta.path.clone()),
                path_hint: Some(meta.path.clone()),
            };
            let mut metas = vec![FileMeta {
                file_key: current_key.unwrap_or(meta.file_key),
                path: meta.path,
                size: fs_meta.len(),
                mtime: current_mtime,
                ctime: fs_meta.created().ok(),
                atime: fs_meta.accessed().ok(),
                kind: FileKind::from_metadata(&fs_meta),
            }];
            self.apply_upserted_metas_inner(std::slice::from_ref(&event), &mut metas, true);
        }

        self.lazy_validation_completed
            .fetch_add(1, Ordering::Relaxed);
    }

    fn apply_lazy_delete(&self, path: PathBuf) {
        self.stats.record_query_stale_hits(1);
        self.lazy_validation_stale_hits
            .fetch_add(1, Ordering::Relaxed);
        self.enqueue_lazy_dirty_parent(path.as_path());
        // Phase 1：文件删除也改变目录 mtime，失效父目录的 mtime 预检记录。
        if let Some(parent) = path.parent() {
            self.directory_manifests.invalidate_mtime_precheck(parent);
        }
        let event = EventRecord {
            seq: 0,
            timestamp: std::time::SystemTime::now(),
            event_type: EventType::Delete,
            id: FileIdentifier::Path(path.clone()),
            path_hint: Some(path),
        };
        self.apply_events(&[event]);
    }

    fn enqueue_lazy_dirty_parent(&self, path: &std::path::Path) {
        let Some(parent) = path.parent() else {
            return;
        };
        self.enqueue_dirty_dirs(
            vec![parent.to_path_buf()],
            DirtyReason::StartupRepairDeferred,
        );
    }
}
