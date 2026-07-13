use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::{Duration, UNIX_EPOCH};

use crate::storage::traits::StorageBackend;
use crate::util::maybe_trim_rss;

use super::TieredIndex;

mod generation;

use generation::{build_durable_snapshot_generation, publish_snapshot_generation};

const MIN_SNAPSHOT_INTERVAL: Duration = Duration::from_secs(10);

impl TieredIndex {
    /// 原子快照
    pub async fn snapshot_now<S>(self: &Arc<Self>, store: Arc<S>) -> anyhow::Result<()>
    where
        S: StorageBackend + 'static,
    {
        let idx = self.clone();
        let store_for_sync = store.clone();
        let wrote_snapshot = tokio::task::spawn_blocking(move || {
            snapshot_generation_blocking(&idx, store_for_sync.as_ref())
        })
        .await
        .map_err(|error| anyhow::anyhow!("snapshot sync phase panicked: {error}"))??;

        if !wrote_snapshot {
            tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        }
        maybe_trim_rss();
        Ok(())
    }

    /// 定期快照循环
    pub async fn snapshot_loop<S>(self: Arc<Self>, store: Arc<S>, interval_secs: u64)
    where
        S: StorageBackend + 'static,
    {
        // interval_secs==0 is treated as "disabled" to avoid a busy loop.
        let interval = if interval_secs == 0 {
            None
        } else {
            Some(std::time::Duration::from_secs(interval_secs))
        };
        loop {
            // Stop snapshotting once shutdown begins; the final snapshot and
            // clean-shutdown marker are driven explicitly from main.
            if self.is_shutting_down() {
                tracing::debug!("snapshot loop exiting: shutdown in progress");
                return;
            }

            // flush 请求优先：避免 overlay 长期积压。
            if self.flush_requested.load(Ordering::Acquire) {
                // Enforce minimum interval to prevent back-to-back snapshot storms
                let last = self.last_snapshot_time.load(Ordering::Relaxed);
                let now = std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .map(|d| d.as_secs())
                    .unwrap_or(0);
                if last != 0 && now.saturating_sub(last) < MIN_SNAPSHOT_INTERVAL.as_secs() {
                    let wait_secs = MIN_SNAPSHOT_INTERVAL.as_secs() - (now - last);
                    tokio::time::sleep(std::time::Duration::from_secs(wait_secs)).await;
                    continue;
                }
                if let Err(e) = self.snapshot_now(store.clone()).await {
                    tracing::error!("Snapshot failed (flush requested): {}", e);
                    tokio::time::sleep(std::time::Duration::from_secs(1)).await;
                }
                continue;
            }

            let periodic_tick = match interval {
                Some(interval) => {
                    tokio::select! {
                        _ = tokio::time::sleep(interval) => true,
                        _ = self.flush_notify.notified() => false,
                    }
                }
                None => {
                    self.flush_notify.notified().await;
                    let last = self.last_snapshot_time.load(Ordering::Relaxed);
                    let now = std::time::SystemTime::now()
                        .duration_since(std::time::UNIX_EPOCH)
                        .map(|d| d.as_secs())
                        .unwrap_or(0);
                    if last != 0 && now.saturating_sub(last) < MIN_SNAPSHOT_INTERVAL.as_secs() {
                        let wait_secs = MIN_SNAPSHOT_INTERVAL.as_secs() - (now - last);
                        tokio::time::sleep(std::time::Duration::from_secs(wait_secs)).await;
                        continue;
                    }
                    false
                }
            };

            if periodic_tick && !self.periodic_flush_batch_ready() {
                tracing::debug!(
                    "Periodic flush skipped: pending_events={} pending_bytes={} min_events={} min_bytes={}",
                    self.pending_flush_events.load(Ordering::Relaxed),
                    self.pending_flush_bytes.load(Ordering::Relaxed),
                    self.periodic_flush_min_events.load(Ordering::Relaxed),
                    self.periodic_flush_min_bytes.load(Ordering::Relaxed),
                );
                continue;
            }

            // A shutdown notification can also wake the select above; don't fire
            // one last snapshot here — the explicit final snapshot owns that.
            if self.is_shutting_down() {
                return;
            }

            if let Err(e) = self.snapshot_now(store.clone()).await {
                tracing::error!("Snapshot failed: {}", e);
            }
        }
    }

    pub fn last_snapshot_time(&self) -> u64 {
        self.last_snapshot_time.load(Ordering::Relaxed)
    }

    pub(super) fn record_snapshot_success(&self) {
        let ts = std::time::SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_secs())
            .unwrap_or(0);
        self.last_snapshot_time.store(ts, Ordering::Relaxed);
        self.stats.record_snapshot();
    }
}

fn snapshot_generation_blocking<S>(idx: &Arc<TieredIndex>, store: &S) -> anyhow::Result<bool>
where
    S: StorageBackend + ?Sized,
{
    // Event application holds the same gate from WAL append through mutable
    // L2 apply. Rebuild publication is frozen by the rebuild-state guard. The
    // captured (base, delta, WAL seal) generation therefore cannot change
    // while its files are written, validated, and mounted.
    let event_boundary = idx.snapshot_event_gate.lock();
    let rebuild_generation = idx.rebuild_state.lock();
    if rebuild_generation.in_progress
        || rebuild_generation.scheduled
        || rebuild_generation.requested
    {
        anyhow::bail!("snapshot deferred while full rebuild recovery is pending");
    }
    // Rebuild admission may mark a request while writing, but the rebuild
    // worker cannot cross `snapshot_event_gate` until this generation is fully
    // published. Releasing this mutex keeps `/memory` and health telemetry
    // responsive throughout long external-sort/fsync phases.
    drop(rebuild_generation);

    if !snapshot_work_pending(idx) {
        idx.flush_requested.store(false, Ordering::Release);
        idx.reset_pending_flush_batch();
        return Ok(false);
    }

    let result = build_durable_snapshot_generation(idx, store)
        .and_then(|generation| {
            let consumed_owned_generation = generation.consumed_owned_generation;
            publish_snapshot_generation(idx, generation).map_err(|error| {
                if consumed_owned_generation {
                    anyhow::anyhow!("owned_v7_generation_consumed: {error:#}")
                } else {
                    error
                }
            })
        })
        .map_err(|error| {
            anyhow::anyhow!("snapshot generation failed; sealed WAL retained for retry: {error:#}")
        });
    if let Err(error) = result {
        idx.flush_requested.store(true, Ordering::Release);
        let rebuild_required = snapshot_error_requires_rebuild(&error);
        let owned_generation_consumed =
            format!("{error:#}").contains("owned_v7_generation_consumed");
        if owned_generation_consumed {
            idx.pending_snapshot_generation.lock().take();
            *idx.owned_snapshot_telemetry.lock() = Default::default();
            idx.rebuild_snapshot_pending.store(false, Ordering::Release);
            idx.invalidate_memory_report_cache();
        }
        drop(event_boundary);
        if rebuild_required && !idx.is_shutting_down() {
            // A rebuild supersedes any in-memory generation that could not be
            // durably published. Drop it before scanning to avoid two full
            // rebuild generations coexisting at the next peak.
            idx.pending_snapshot_generation.lock().take();
            tracing::warn!(
                "direct snapshot entered rebuild recovery; delta and sealed WAL retained: {error:#}"
            );
            idx.spawn_snapshot_recovery_full_build();
        }
        return Err(error);
    }

    Ok(true)
}

fn snapshot_work_pending(idx: &TieredIndex) -> bool {
    let delta_dirty = idx.l2.load().is_dirty();
    let db = idx.delta_buffer.lock();
    let base = idx.base.load();
    let owned_rebuild_pending = idx.pending_snapshot_generation.lock().is_some();
    let hot_base_needs_publish = base.cold_segments.is_empty() && base.file_count() > 0;
    delta_dirty
        || !db.is_empty()
        || !db.is_complete()
        || hot_base_needs_publish
        || owned_rebuild_pending
        || idx.rebuild_snapshot_pending.load(Ordering::Acquire)
}

fn snapshot_error_requires_rebuild(error: &anyhow::Error) -> bool {
    let message = format!("{error:#}");
    [
        "direct_v7_unsupported",
        "direct_v7_corrupt",
        "direct_v7_validation_failed",
        "streaming_compaction_required",
        "snapshot_delta_incomplete",
        "snapshot_upsert_unresolved",
        "owned_v7_generation_consumed",
    ]
    .iter()
    .any(|marker| message.contains(marker))
}
