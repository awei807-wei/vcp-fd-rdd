use std::sync::atomic::Ordering;
use std::sync::Arc;

use crate::core::{EventRecord, EventType, FileIdentifier, FileMeta};
use crate::index::l2_partition::PersistentIndex;

use super::TieredIndex;

pub(super) struct ApplyBatchState {
    pub(super) l2: Arc<PersistentIndex>,
    pub(super) rebuild_in_progress: bool,
    pub(super) event_count: usize,
}

impl TieredIndex {
    /// 批量应用事件到索引
    pub fn apply_events(&self, events: &[EventRecord]) {
        let mut normalized: Vec<EventRecord> = events.to_vec();
        for ev in &mut normalized {
            Self::normalize_event_paths(ev);
        }
        self.apply_events_inner(&normalized, true);
    }

    /// 批量应用事件到索引（drain 版本）：消费 `Vec<EventRecord>`，用于减少 PathBuf 克隆带来的非索引 PD 高水位。
    ///
    /// 说明：
    /// - 仅用于"事件生产者本就不需要保留 EventRecord"的路径（EventPipeline / fast-sync）。
    /// - 内部会清空 `events`，但保留 capacity 以便复用。
    pub fn apply_events_drain(&self, events: &mut Vec<EventRecord>) {
        for ev in events.iter_mut() {
            Self::normalize_event_paths(ev);
        }
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
        _events: &[EventRecord],
    ) -> (Arc<PersistentIndex>, bool) {
        let st = self.rebuild_state.lock();
        let in_progress = st.in_progress;
        drop(st);
        (self.l2.load_full(), in_progress)
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
    ) -> Option<ApplyBatchState> {
        if events.is_empty() {
            return None;
        }

        // WAL：先写后用（best-effort）。replay 场景下禁用写回，避免重复追加。
        self.append_events_to_wal(events, log_to_wal);

        // 若 rebuild 在进行：先缓冲 pending 事件；并在持锁期间捕获当前 l2 指针，
        // 避免切换窗口导致"事件已缓冲但应用到了新索引"而重复回放。
        let (l2, rebuild_in_progress) = self.capture_l2_for_apply(events);
        let mut db = self.delta_buffer.lock();
        let all_applied = db.apply_events(events);
        let overlay_paths = db.len();
        let overlay_arena_bytes = db.estimated_bytes() as u64;
        drop(db);
        if !all_applied {
            // 硬容量上限已满，强制触发 flush
            if !self.flush_requested.swap(true, Ordering::AcqRel) {
                self.flush_notify.notify_one();
            }
        }
        self.maybe_request_flush(overlay_paths, overlay_arena_bytes);
        self.note_pending_flush_batch(events);
        self.invalidate_l1_for_events(events);

        Some(ApplyBatchState {
            l2,
            rebuild_in_progress,
            event_count: events.len(),
        })
    }

    fn normalize_event_paths(ev: &mut EventRecord) {
        use super::normalize_path;
        if let Some(ref mut p) = ev.path_hint {
            *p = normalize_path(p);
        }
        if let FileIdentifier::Path(ref mut p) = ev.id {
            *p = normalize_path(p);
        }
        if let EventType::Rename {
            ref mut from,
            ref mut from_path_hint,
        } = &mut ev.event_type
        {
            if let FileIdentifier::Path(ref mut p) = from {
                *p = normalize_path(p);
            }
            if let Some(ref mut p) = from_path_hint {
                *p = normalize_path(p);
            }
        }
    }

    pub(super) fn apply_events_inner(&self, events: &[EventRecord], log_to_wal: bool) {
        let Some(batch) = self.begin_apply_batch(events, log_to_wal) else {
            return;
        };
        batch.l2.apply_events(events);
        batch.l2.rebuild_parent_index();
        let new_base = Arc::new(batch.l2.to_base_index_data());
        self.base.store(new_base);
        self.event_seq
            .fetch_add(batch.event_count as u64, Ordering::Relaxed);
    }

    pub(super) fn apply_events_inner_drain(&self, events: &mut Vec<EventRecord>, log_to_wal: bool) {
        let Some(batch) = self.begin_apply_batch(events.as_slice(), log_to_wal) else {
            return;
        };
        batch.l2.apply_events(events.as_slice());
        batch.l2.rebuild_parent_index();
        let new_base = Arc::new(batch.l2.to_base_index_data());
        self.base.store(new_base);
        events.clear();
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
            batch.l2.rebuild_parent_index();
        } else {
            batch.l2.apply_file_metas_drain(metas);
            batch.l2.rebuild_parent_index();
        }
        let new_base = Arc::new(batch.l2.to_base_index_data());
        self.base.store(new_base);
        metas.clear();
        self.event_seq
            .fetch_add(batch.event_count as u64, Ordering::Relaxed);
    }
}

fn file_identifier_estimated_bytes(id: &FileIdentifier) -> u64 {
    match id {
        FileIdentifier::Path(p) => p.as_os_str().as_encoded_bytes().len() as u64,
        FileIdentifier::Fid { .. } => 16,
    }
}

pub(crate) fn event_record_estimated_bytes(ev: &EventRecord) -> u64 {
    let mut bytes = file_identifier_estimated_bytes(&ev.id);
    if let Some(p) = &ev.path_hint {
        bytes = bytes.saturating_add(p.as_os_str().as_encoded_bytes().len() as u64);
    }
    if let EventType::Rename {
        from,
        from_path_hint,
    } = &ev.event_type
    {
        bytes = bytes.saturating_add(file_identifier_estimated_bytes(from));
        if let Some(p) = from_path_hint {
            bytes = bytes.saturating_add(p.as_os_str().as_encoded_bytes().len() as u64);
        }
    }
    bytes
}
