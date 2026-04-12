use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::Instant;

use crate::index::l2_partition::PersistentIndex;
use crate::index::mmap_index::MmapIndex;
use crate::storage::traits::StorageBackend;
use crate::util::maybe_trim_rss;

use super::arena::{deleted_paths_stats, path_arena_set_from_paths};
use super::disk_layer::DiskLayer;
use super::pathbuf_from_bytes;
use super::{
    COMPACTION_COOLDOWN, COMPACTION_DELTA_THRESHOLD, COMPACTION_MAX_DELTAS_PER_RUN, TieredIndex,
};

impl TieredIndex {
    pub(super) fn maybe_spawn_compaction<S>(self: &Arc<Self>, store: Arc<S>)
    where
        S: StorageBackend + 'static,
    {
        let mut layers = self.disk_layers.read().clone();
        let delta_count = layers.len().saturating_sub(1);
        if delta_count < COMPACTION_DELTA_THRESHOLD {
            return;
        }
        // 为了避免一次 compaction 过重：只合并 base + 最老的一小段 delta，剩余新 delta 保留在 suffix。
        let max_layers = 1 + COMPACTION_MAX_DELTAS_PER_RUN;
        if layers.len() > max_layers {
            layers.truncate(max_layers);
        }

        // 防抖：冷却期内不重复启动 compaction（尤其是 manifest changed 场景）。
        {
            let mut g = self.compaction_last_started_at.lock();
            if let Some(last) = *g {
                if last.elapsed() < COMPACTION_COOLDOWN {
                    return;
                }
            }
            *g = Some(Instant::now());
        }

        // 避免并发 compaction
        if self.compaction_in_progress.swap(true, Ordering::AcqRel) {
            return;
        }

        let idx = self.clone();
        tokio::spawn(async move {
            struct CompactionInProgressGuard(Arc<TieredIndex>);
            impl Drop for CompactionInProgressGuard {
                fn drop(&mut self) {
                    self.0
                        .compaction_in_progress
                        .store(false, Ordering::Release);
                }
            }
            let _guard = CompactionInProgressGuard(idx.clone());
            match idx.compact_layers(store, layers).await {
                Ok(()) => tracing::debug!("Compaction attempt finished"),
                Err(e) => {
                    // manifest changed 是并发下的预期分支：并不意味着数据损坏。
                    let msg = e.to_string();
                    if msg.contains("LSM manifest changed, aborting compaction") {
                        tracing::info!("Compaction skipped due to concurrent manifest change");
                    } else {
                        tracing::error!("Compaction failed: {}", e);
                    }
                }
            }
            // compaction 是临时分配大户；无论成功/跳过/失败都尝试一次回吐。
            maybe_trim_rss();
        });
    }

    pub(super) async fn compact_layers<S>(
        self: &Arc<Self>,
        store: Arc<S>,
        layers_snapshot: Vec<DiskLayer>,
    ) -> anyhow::Result<()>
    where
        S: StorageBackend + 'static,
    {
        if layers_snapshot.is_empty() {
            return Ok(());
        }
        // 若进入执行时层列表"前缀"已变化，直接放弃本轮（避免无意义重活）。
        // 允许并发 append 新 delta：只要当前层列表仍以本次 snapshot 作为前缀，本轮 compaction 仍然有意义。
        {
            let cur_ids = self
                .disk_layers
                .read()
                .iter()
                .map(|l| l.id)
                .collect::<Vec<_>>();
            let snap_ids = layers_snapshot.iter().map(|l| l.id).collect::<Vec<_>>();
            let snap_len = snap_ids.len();
            if cur_ids.len() < snap_len || cur_ids[..snap_len] != snap_ids[..] {
                return Ok(());
            }
        }
        if layers_snapshot[0].id == 0 {
            // legacy base 只能通过 bootstrap 进入 LSM；此处不做跨体系 compaction。
            return Ok(());
        }

        tracing::info!(
            "Starting compaction: base={} deltas={}",
            layers_snapshot[0].id,
            layers_snapshot.len().saturating_sub(1)
        );

        let roots = self.roots.clone();
        let merged = PersistentIndex::new_with_roots(roots.clone());

        for layer in &layers_snapshot {
            layer.deleted_paths.for_each_bytes(|p| {
                let pb = pathbuf_from_bytes(p);
                merged.mark_deleted_by_path(&pb);
            });
            layer.idx.for_each_live_meta(|m| merged.upsert_rename(m));
        }

        let segs = merged.export_segments_v6_compacted();
        let wal_seal_id = store.lsm_manifest_wal_seal_id().unwrap_or(0);

        let base_id = layers_snapshot[0].id;
        let delta_ids = layers_snapshot
            .iter()
            .skip(1)
            .map(|l| l.id)
            .collect::<Vec<_>>();
        let new_base = store
            .replace_base_v6(
                &segs,
                Some((base_id, delta_ids.clone())),
                &roots,
                wal_seal_id,
            )
            .await?;
        if let Err(e) = store.gc_stale_segments() {
            tracing::warn!(
                "LSM gc stale segments failed after compaction replace-base: {}",
                e
            );
        }

        let deleted_paths = Arc::new(path_arena_set_from_paths(new_base.deleted_paths));
        let (cnt, bytes, est) = deleted_paths_stats(deleted_paths.as_ref());
        let new_layer = DiskLayer {
            id: new_base.id,
            idx: Arc::new(MmapIndex::new(new_base.snap)),
            deleted_paths,
            deleted_paths_count: cnt,
            deleted_paths_bytes: bytes,
            deleted_paths_estimated_bytes: est,
        };

        // 仅当段列表未变化时才替换（弱 CAS）
        {
            let mut cur = self.disk_layers.write();
            let snap_len = layers_snapshot.len();
            let prefix_matches = cur.len() >= snap_len
                && cur
                    .iter()
                    .take(snap_len)
                    .map(|l| l.id)
                    .eq(layers_snapshot.iter().map(|l| l.id));
            if prefix_matches {
                // 保留并发 append 的新 delta（suffix）；用 new_base 替换掉本次 compaction 的 prefix。
                let suffix: Vec<DiskLayer> = cur.drain(snap_len..).collect();
                cur.clear();
                cur.push(new_layer);
                cur.extend(suffix);
                self.l1.clear();
            }
        }

        // 清理旧段文件（best-effort；失败不影响正确性）
        let dir = store.derived_lsm_dir_path();
        for id in layers_snapshot.iter().map(|l| l.id) {
            if id == 0 || id == new_base.id {
                continue;
            }
            if let Err(e) = std::fs::remove_file(dir.join(format!("seg-{id:016x}.db"))) {
                tracing::warn!("Failed to remove stale segment seg-{id:016x}.db: {e}");
            }
            if let Err(e) = std::fs::remove_file(dir.join(format!("seg-{id:016x}.del"))) {
                tracing::warn!("Failed to remove stale segment seg-{id:016x}.del: {e}");
            }
        }

        tracing::info!("Compaction complete: new_base={}", new_base.id);
        Ok(())
    }
}
