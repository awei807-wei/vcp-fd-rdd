use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::{Duration, UNIX_EPOCH};

use crate::index::l2_partition::PersistentIndex;
use crate::index::mmap_index::MmapIndex;
use crate::storage::traits::StorageBackend;
use crate::util::maybe_trim_rss;

use super::arena::{deleted_paths_stats, path_arena_set_from_paths};
use super::disk_layer::DiskLayer;
use super::pathbuf_from_bytes;
use super::TieredIndex;

const MIN_SNAPSHOT_INTERVAL: Duration = Duration::from_secs(10);

impl TieredIndex {
    /// 原子快照
    pub async fn snapshot_now<S>(self: &Arc<Self>, store: Arc<S>) -> anyhow::Result<()>
    where
        S: StorageBackend + 'static,
    {
        // Flush：把当前内存 Delta 刷盘为新 Segment；必要时触发后台 compaction。
        let idx = self.clone();
        let result = tokio::task::spawn_blocking(move || {
            let _wg = match idx.apply_gate.try_write() {
                Some(guard) => guard,
                None => {
                    tracing::debug!("apply_gate busy, deferring snapshot");
                    return None;
                }
            };

            let delta = idx.l2.load_full();
            let delta_dirty = delta.is_dirty();

            let mut ov = idx.overlay_state.lock();
            let overlay_dirty =
                ov.deleted_paths.len_paths() != 0 || ov.upserted_paths.len_paths() != 0;
            if !delta_dirty && !overlay_dirty {
                tracing::debug!("No delta/overlay changes, skipping flush");
                idx.flush_requested.store(false, Ordering::Release);
                idx.reset_pending_flush_batch();
                return None;
            }

            // WAL：在 snapshot 边界 seal，确保新事件进入新 WAL（并可由 manifest checkpoint 判定回放范围）。
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

            // FIX: export BEFORE swap, while data is still in L2
            let segs = delta.export_segments_v6();

            let old = idx
                .l2
                .swap(Arc::new(PersistentIndex::new_with_roots(idx.roots.clone())));

            // 只保留"仍然有效"的 delete：若本轮 delta 又 upsert 了同一路径，则认为 delete 被抵消。
            let mut deleted: Vec<Vec<u8>> = Vec::new();
            ov.deleted_paths.for_each_bytes(|p| {
                if !ov.upserted_paths.contains(p) {
                    deleted.push(p.to_vec());
                }
            });
            Arc::make_mut(&mut ov.deleted_paths).clear();
            Arc::make_mut(&mut ov.upserted_paths).clear();
            Arc::make_mut(&mut ov.deleted_paths).maybe_shrink_after_clear();
            Arc::make_mut(&mut ov.upserted_paths).maybe_shrink_after_clear();
            idx.flush_requested.store(false, Ordering::Release);

            Some((
                segs,
                old,
                deleted,
                idx.disk_layers.read().clone(),
                wal_seal_id,
            ))
        })
        .await
        .map_err(|e| anyhow::anyhow!("snapshot sync phase panicked: {}", e))?;

        let (segs, old_delta, deleted_paths, layers_snapshot, wal_seal_id) = match result {
            Some(v) => v,
            None => {
                tokio::time::sleep(std::time::Duration::from_millis(100)).await;
                return Ok(());
            }
        };

        // 判断是否已有 LSM manifest：无则先 bootstrap 为 base（避免 legacy base 被"遗忘"）。
        let roots = self.roots.clone();
        let lsm_present = store.load_lsm_if_valid(&roots).ok().flatten().is_some();

        if !lsm_present {
            tracing::info!("LSM manifest not found, bootstrapping a new base segment...");

            let merged = PersistentIndex::new_with_roots(roots.clone());

            // 先灌入现有 disk base（可能是 legacy v6）。
            for layer in &layers_snapshot {
                layer.idx.for_each_live_meta(|m| merged.upsert_rename(m));
            }
            drop(layers_snapshot);

            // 再应用跨段 delete（delete/rename-from）。
            for p in &deleted_paths {
                let pb = pathbuf_from_bytes(p);
                merged.mark_deleted_by_path(&pb);
            }
            drop(deleted_paths);

            // 最后灌入本次 delta（newest）。
            old_delta.for_each_live_meta(|m| merged.upsert_rename(m));
            drop(old_delta);

            let segs = merged.export_segments_v6_compacted();
            drop(merged);
            let base = store
                .replace_base_v6(&segs, None, &roots, wal_seal_id)
                .await?;
            drop(segs);
            if let Err(e) = store.gc_stale_segments() {
                tracing::warn!("LSM gc stale segments failed after replace-base: {}", e);
            }

            // deleted_paths 在 append/replace-base 后通常会经历增长与扩容；这里 shrink 一次，避免把 capacity 高水位长期带到常驻层。
            let mut base_deleted_paths = base.deleted_paths;
            base_deleted_paths.shrink_to_fit();
            let deleted_paths = Arc::new(path_arena_set_from_paths(base_deleted_paths));
            let (cnt, bytes, est) = deleted_paths_stats(deleted_paths.as_ref());
            let new_layer = DiskLayer {
                id: base.id,
                idx: Arc::new(MmapIndex::new(base.snap)),
                deleted_paths,
                deleted_paths_count: cnt,
                deleted_paths_bytes: bytes,
                deleted_paths_estimated_bytes: est,
            };

            *self.disk_layers.write() = vec![new_layer];
            self.l1.clear();
            if let Some(w) = self.wal.lock().clone() {
                if let Err(e) = w.cleanup_sealed_up_to(wal_seal_id) {
                    tracing::warn!("WAL cleanup_sealed_up_to failed: {e}");
                }
            }
            self.record_snapshot_success();
            self.reset_pending_flush_batch();
            // snapshot/flush 是临时分配大户；完成后尝试回吐。
            maybe_trim_rss();
            return Ok(());
        }

        drop(layers_snapshot);
        drop(old_delta);
        let seg = store
            .append_delta_v6(&segs, &deleted_paths, &roots, wal_seal_id)
            .await?;
        drop(segs);
        drop(deleted_paths);

        // deleted_paths 在 append 后通常会经历增长与扩容；这里 shrink 一次，避免把 capacity 高水位长期带到常驻层。
        let mut seg_deleted_paths = seg.deleted_paths;
        seg_deleted_paths.shrink_to_fit();
        let deleted_paths = Arc::new(path_arena_set_from_paths(seg_deleted_paths));
        let (cnt, bytes, est) = deleted_paths_stats(deleted_paths.as_ref());
        self.disk_layers.write().push(DiskLayer {
            id: seg.id,
            idx: Arc::new(MmapIndex::new(seg.snap)),
            deleted_paths,
            deleted_paths_count: cnt,
            deleted_paths_bytes: bytes,
            deleted_paths_estimated_bytes: est,
        });
        self.l1.clear();
        if let Some(w) = self.wal.lock().clone() {
            let _ = w.cleanup_sealed_up_to(wal_seal_id);
        }
        self.record_snapshot_success();
        self.reset_pending_flush_batch();

        // compaction：段数达到阈值后后台合并
        self.maybe_spawn_compaction(store);
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
    }
}
