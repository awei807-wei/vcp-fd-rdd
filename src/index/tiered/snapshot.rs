use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::{Duration, UNIX_EPOCH};

use crate::storage::snapshot::{
    write_recovery_runtime_state, write_stable_v7_atomic, RecoveryRuntimeState,
};
use crate::storage::snapshot_v7::write_v7_snapshot_atomic;
use crate::storage::traits::StorageBackend;
use crate::util::maybe_trim_rss;

use super::TieredIndex;

const MIN_SNAPSHOT_INTERVAL: Duration = Duration::from_secs(10);

impl TieredIndex {
    /// 原子快照
    pub async fn snapshot_now<S>(self: &Arc<Self>, store: Arc<S>) -> anyhow::Result<()>
    where
        S: StorageBackend + 'static,
    {
        let idx = self.clone();
        let store_for_sync = store.clone();
        let result = tokio::task::spawn_blocking(move || {
            let delta = idx.l2.load_full();
            let delta_dirty = delta.is_dirty();

            let overlay_dirty = {
                let db = idx.delta_buffer.lock();
                !db.is_empty()
            };
            let pending_flush_dirty = idx.pending_flush_events.load(Ordering::Relaxed) > 0
                || idx.pending_flush_bytes.load(Ordering::Relaxed) > 0;
            let unsnapshotted_base = idx.last_snapshot_time.load(Ordering::Relaxed) == 0
                && idx.base.load().file_count() > 0;
            if !delta_dirty && !overlay_dirty && !pending_flush_dirty && !unsnapshotted_base {
                tracing::debug!("No delta/overlay changes, skipping flush");
                idx.flush_requested.store(false, Ordering::Release);
                idx.reset_pending_flush_batch();
                return None;
            }

            // WAL：在 snapshot 边界 seal，确保新事件进入新 WAL。
            let wal_seal_id = match idx.wal.lock().clone() {
                Some(w) => match w.seal() {
                    Ok(id) => id,
                    Err(e) => {
                        tracing::warn!("WAL seal failed, continuing: {}", e);
                        0
                    }
                },
                None => 0,
            };

            // Snapshot is the materialization boundary: ordinary event batches
            // update the delta path only, so the full visible BaseIndex is
            // rebuilt on this cold path and then written as v7.
            let base = idx.materialize_snapshot_base();
            let v7_path = store_for_sync.path().with_extension("v7");

            // delta_buffer has been cleared by materialize_snapshot_base after
            // its content was folded into base.
            idx.flush_requested.store(false, Ordering::Release);

            Some((base, v7_path, wal_seal_id))
        })
        .await
        .map_err(|e| anyhow::anyhow!("snapshot sync phase panicked: {}", e))?;

        let (base, v7_path, wal_seal_id) = match result {
            Some(v) => v,
            None => {
                tokio::time::sleep(std::time::Duration::from_millis(100)).await;
                return Ok(());
            }
        };

        // 写入 v7 快照（原子写：tmp + rename）
        if let Err(e) = write_v7_snapshot_atomic(&v7_path, &base) {
            tracing::warn!("v7 snapshot write failed: {}", e);
        } else {
            tracing::info!("v7 snapshot written to {:?}", v7_path);
        }

        if self.stable_snapshot_enabled.load(Ordering::Relaxed) {
            if let Err(e) = write_stable_v7_atomic(store.path(), &base) {
                tracing::warn!("stable v7 snapshot write failed: {}", e);
            } else {
                let state = RecoveryRuntimeState {
                    last_clean_shutdown: false,
                    last_snapshot_unix_secs: unix_secs(),
                    last_wal_seal_id: wal_seal_id,
                    last_startup_source: self.recovery_status().report.snapshot_source,
                    last_recovery_mode: "snapshot".to_string(),
                };
                if let Err(e) = write_recovery_runtime_state(store.path(), &state) {
                    tracing::warn!("recovery runtime state write failed: {}", e);
                }
                tracing::info!("stable v7 snapshot written for recovery");
            }
        }

        self.l1.clear();
        if let Some(w) = self.wal.lock().clone() {
            let _ = w.cleanup_sealed_up_to(wal_seal_id);
        }
        self.record_snapshot_success();
        self.reset_pending_flush_batch();

        // snapshot/flush 是临时分配大户；完成后尝试回吐。
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

fn unix_secs() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
}
