use std::sync::atomic::Ordering;
use std::sync::Arc;

use parking_lot::RwLockReadGuard;

use crate::core::{EventRecord, EventType, FileMeta};
use crate::index::l2_partition::PersistentIndex;

use super::arena::PathArenaSet;
use super::disk_layer::event_record_estimated_bytes;
use super::rebuild::PendingEvent;
use super::TieredIndex;

pub(super) struct ApplyBatchState<'a> {
    pub(super) _gate: RwLockReadGuard<'a, ()>,
    pub(super) l2: Arc<PersistentIndex>,
    pub(super) rebuild_in_progress: bool,
    pub(super) event_count: usize,
}

#[derive(Debug, Default)]
pub(super) struct OverlayState {
    /// delete / rename-from：需要跨段屏蔽更老 segment 结果，并在 flush 时写入 seg-*.del
    pub(super) deleted_paths: Arc<PathArenaSet>,
    /// create/modify/rename-to：用于抵消同一路径的 deleted（delete→recreate）
    pub(super) upserted_paths: Arc<PathArenaSet>,
}

impl TieredIndex {
    /// 批量应用事件到索引
    pub fn apply_events(&self, events: &[EventRecord]) {
        self.apply_events_inner(events, true);
    }

    /// 批量应用事件到索引（drain 版本）：消费 `Vec<EventRecord>`，用于减少 PathBuf 克隆带来的非索引 PD 高水位。
    ///
    /// 说明：
    /// - 仅用于"事件生产者本就不需要保留 EventRecord"的路径（EventPipeline / fast-sync）。
    /// - 内部会清空 `events`，但保留 capacity 以便复用。
    pub fn apply_events_drain(&self, events: &mut Vec<EventRecord>) {
        self.apply_events_inner_drain(events, true);
    }

    /// 设置 overlay 强制 flush 阈值（0 表示禁用对应阈值）。
    pub fn set_auto_flush_limits(&self, overlay_paths: u64, overlay_bytes: u64) {
        self.auto_flush_overlay_paths
            .store(overlay_paths, Ordering::Relaxed);
        self.auto_flush_overlay_bytes
            .store(overlay_bytes, Ordering::Relaxed);
    }

    /// 设置"定时 flush"的最小批量门槛。
    ///
    /// - 仅影响 snapshot_loop 的周期性 flush
    /// - overlay 强制 flush / 退出前最终 snapshot 不受影响
    pub fn set_periodic_flush_batch_limits(&self, min_events: u64, min_bytes: u64) {
        self.periodic_flush_min_events
            .store(min_events, Ordering::Relaxed);
        self.periodic_flush_min_bytes
            .store(min_bytes, Ordering::Relaxed);
    }

    pub(super) fn note_pending_flush_batch(&self, events: &[EventRecord]) {
        if events.is_empty() {
            return;
        }
        let bytes = events
            .iter()
            .map(event_record_estimated_bytes)
            .fold(0u64, u64::saturating_add);
        self.pending_flush_events
            .fetch_add(events.len() as u64, Ordering::Relaxed);
        self.pending_flush_bytes.fetch_add(bytes, Ordering::Relaxed);
    }

    pub(super) fn note_pending_flush_rebuild(&self, idx: &PersistentIndex) {
        self.pending_flush_events
            .store(idx.file_count() as u64, Ordering::Relaxed);
        self.pending_flush_bytes
            .store(idx.memory_stats().estimated_bytes, Ordering::Relaxed);
    }

    pub(super) fn reset_pending_flush_batch(&self) {
        self.pending_flush_events.store(0, Ordering::Relaxed);
        self.pending_flush_bytes.store(0, Ordering::Relaxed);
    }

    pub(super) fn periodic_flush_batch_ready(&self) -> bool {
        let min_events = self.periodic_flush_min_events.load(Ordering::Relaxed);
        let min_bytes = self.periodic_flush_min_bytes.load(Ordering::Relaxed);
        if min_events == 0 && min_bytes == 0 {
            return true;
        }
        let pending_events = self.pending_flush_events.load(Ordering::Relaxed);
        let pending_bytes = self.pending_flush_bytes.load(Ordering::Relaxed);
        (min_events > 0 && pending_events >= min_events)
            || (min_bytes > 0 && pending_bytes >= min_bytes)
    }

    pub(super) fn maybe_request_flush(&self, overlay_paths: usize, overlay_arena_bytes: u64) {
        let limit_paths = self.auto_flush_overlay_paths.load(Ordering::Relaxed);
        let limit_bytes = self.auto_flush_overlay_bytes.load(Ordering::Relaxed);
        if limit_paths == 0 && limit_bytes == 0 {
            return;
        }

        let hit = (limit_paths > 0 && overlay_paths as u64 >= limit_paths)
            || (limit_bytes > 0 && overlay_arena_bytes >= limit_bytes);
        if !hit {
            return;
        }

        // 合并触发：只有从 false->true 才唤醒一次，避免 event 风暴下 notify 风暴。
        if !self.flush_requested.swap(true, Ordering::AcqRel) {
            self.flush_notify.notify_one();
        }
    }

    pub(super) fn append_events_to_wal(&self, events: &[EventRecord], log_to_wal: bool) {
        if !log_to_wal {
            return;
        }

        if let Some(wal) = self.wal.lock().clone() {
            if let Err(e) = wal.append(events) {
                tracing::warn!("WAL append failed (continuing without durability): {}", e);
            }
        }
    }

    pub(super) fn capture_l2_for_apply(
        &self,
        events: &[EventRecord],
    ) -> (Arc<PersistentIndex>, bool) {
        let mut st = self.rebuild_state.lock();
        if !st.in_progress {
            drop(st);
            return (self.l2.load_full(), false);
        }

        // 有界化：按身份去重，只保留每条身份的最新事件（避免 rebuild 期间无限堆积）。
        for ev in events {
            let key = ev.id.clone();
            match st.pending_events.get_mut(&key) {
                Some(old) if old.seq >= ev.seq => {
                    // 旧记录更新：忽略（避免乱序覆盖）。
                }
                Some(old) => {
                    // 新事件覆盖旧事件；path_hint 仅在新事件提供时覆盖（"最后一次非空覆盖"）。
                    old.seq = ev.seq;
                    old.timestamp = ev.timestamp;
                    old.event_type = ev.event_type.clone();
                    if ev.path_hint.is_some() {
                        old.path_hint = ev.path_hint.clone();
                    }
                }
                None => {
                    st.pending_events.insert(
                        key,
                        PendingEvent {
                            seq: ev.seq,
                            timestamp: ev.timestamp,
                            event_type: ev.event_type.clone(),
                            path_hint: ev.path_hint.clone(),
                        },
                    );
                }
            }
        }

        (self.l2.load_full(), true)
    }

    pub(super) fn update_overlay_for_events(&self, events: &[EventRecord]) {
        let mut ov = self.overlay_state.lock();
        for ev in events {
            let Some(path) = ev.best_path() else {
                // FID-only 且无路径：阶段 1 保守跳过 overlay 更新（后续 fanotify 反查完善）。
                continue;
            };
            let path_bytes = path.as_os_str().as_encoded_bytes();
            match &ev.event_type {
                EventType::Delete => {
                    let _ = Arc::make_mut(&mut ov.upserted_paths).remove(path_bytes);
                    let _ = Arc::make_mut(&mut ov.deleted_paths).insert(path_bytes);
                }
                EventType::Create | EventType::Modify => {
                    let _ = Arc::make_mut(&mut ov.deleted_paths).remove(path_bytes);
                    let _ = Arc::make_mut(&mut ov.upserted_paths).insert(path_bytes);
                }
                EventType::Rename {
                    from,
                    from_path_hint,
                } => {
                    let from_best = from_path_hint.as_deref().or_else(|| from.as_path());
                    if let Some(from_path) = from_best {
                        let from_bytes = from_path.as_os_str().as_encoded_bytes();
                        let _ = Arc::make_mut(&mut ov.upserted_paths).remove(from_bytes);
                        let _ = Arc::make_mut(&mut ov.deleted_paths).insert(from_bytes);
                    }

                    let _ = Arc::make_mut(&mut ov.deleted_paths).remove(path_bytes);
                    let _ = Arc::make_mut(&mut ov.upserted_paths).insert(path_bytes);
                }
            }
        }

        // overlay 达阈值时请求强制 flush（合并触发，避免无界膨胀）。
        let overlay_paths = ov.deleted_paths.len_paths() + ov.upserted_paths.len_paths();
        let overlay_arena_bytes =
            (ov.deleted_paths.arena_len() + ov.upserted_paths.arena_len()) as u64;
        drop(ov);
        self.maybe_request_flush(overlay_paths, overlay_arena_bytes);
    }

    pub(super) fn invalidate_l1_for_events(&self, events: &[EventRecord]) {
        for ev in events {
            match &ev.event_type {
                EventType::Delete => {
                    if let Some(p) = ev.best_path() {
                        self.l1.remove_by_path(p);
                    } else if let Some(fid) = ev.id.as_file_key() {
                        self.l1.remove(&fid);
                    }
                }
                EventType::Rename {
                    from,
                    from_path_hint,
                } => {
                    let from_best = from_path_hint.as_deref().or_else(|| from.as_path());
                    if let Some(p) = from_best {
                        self.l1.remove_by_path(p);
                    } else if let Some(fid) = from.as_file_key() {
                        self.l1.remove(&fid);
                    }
                }
                _ => {}
            }
        }
    }

    pub(super) fn begin_apply_batch(
        &self,
        events: &[EventRecord],
        log_to_wal: bool,
    ) -> Option<ApplyBatchState<'_>> {
        if events.is_empty() {
            return None;
        }

        // flush/compaction 期间需要短暂阻塞写入，避免"指针 swap 后仍写旧 delta"的竞态。
        let gate = self.apply_gate.read();

        // WAL：先写后用（best-effort）。replay 场景下禁用写回，避免重复追加。
        self.append_events_to_wal(events, log_to_wal);

        // 若 rebuild 在进行：先缓冲 pending 事件；并在持锁期间捕获当前 l2 指针，
        // 避免切换窗口导致"事件已缓冲但应用到了新索引"而重复回放。
        let (l2, rebuild_in_progress) = self.capture_l2_for_apply(events);
        self.update_overlay_for_events(events);
        self.note_pending_flush_batch(events);
        self.invalidate_l1_for_events(events);

        Some(ApplyBatchState {
            _gate: gate,
            l2,
            rebuild_in_progress,
            event_count: events.len(),
        })
    }

    pub(super) fn apply_events_inner(&self, events: &[EventRecord], log_to_wal: bool) {
        let Some(batch) = self.begin_apply_batch(events, log_to_wal) else {
            return;
        };
        batch.l2.apply_events(events);
        self.event_seq
            .fetch_add(batch.event_count as u64, Ordering::Relaxed);
    }

    pub(super) fn apply_events_inner_drain(&self, events: &mut Vec<EventRecord>, log_to_wal: bool) {
        let Some(batch) = self.begin_apply_batch(events.as_slice(), log_to_wal) else {
            return;
        };
        if batch.rebuild_in_progress {
            batch.l2.apply_events(events.as_slice());
            events.clear();
        } else {
            batch.l2.apply_events_drain(events);
        }
        self.event_seq
            .fetch_add(batch.event_count as u64, Ordering::Relaxed);
    }

    pub(super) fn apply_upserted_metas_inner(
        &self,
        events: &[EventRecord],
        metas: &mut Vec<FileMeta>,
        log_to_wal: bool,
    ) {
        let Some(batch) = self.begin_apply_batch(events, log_to_wal) else {
            metas.clear();
            return;
        };
        if batch.rebuild_in_progress {
            batch.l2.apply_file_metas(metas.as_slice());
            metas.clear();
        } else {
            batch.l2.apply_file_metas_drain(metas);
        }
        self.event_seq
            .fetch_add(batch.event_count as u64, Ordering::Relaxed);
    }
}
