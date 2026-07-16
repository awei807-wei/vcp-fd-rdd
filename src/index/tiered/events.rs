use std::sync::atomic::Ordering;
use std::sync::Arc;

use crate::core::{EventRecord, EventType, FileIdentifier, FileKind, FileMeta};
use crate::index::l2_partition::PersistentIndex;
use crate::util::unix_secs;

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
        let now = unix_secs();
        let _ = self.pending_flush_since_unix_secs.compare_exchange(
            0,
            now,
            Ordering::AcqRel,
            Ordering::Relaxed,
        );
    }

    pub(super) fn note_pending_flush_rebuild(&self, idx: &PersistentIndex) {
        self.pending_flush_events
            .store(idx.file_count() as u64, Ordering::Relaxed);
        self.pending_flush_bytes
            .store(idx.memory_stats().estimated_bytes, Ordering::Relaxed);
        self.pending_flush_since_unix_secs
            .store(unix_secs(), Ordering::Relaxed);
    }

    pub(super) fn reset_pending_flush_batch(&self) {
        self.pending_flush_events.store(0, Ordering::Relaxed);
        self.pending_flush_bytes.store(0, Ordering::Relaxed);
        self.pending_flush_since_unix_secs
            .store(0, Ordering::Relaxed);
    }

    pub(super) fn periodic_flush_batch_ready(&self) -> bool {
        let min_events = self.periodic_flush_min_events.load(Ordering::Relaxed);
        let min_bytes = self.periodic_flush_min_bytes.load(Ordering::Relaxed);
        if min_events == 0 && min_bytes == 0 {
            return true;
        }
        let pending_events = self.pending_flush_events.load(Ordering::Relaxed);
        let pending_bytes = self.pending_flush_bytes.load(Ordering::Relaxed);
        let hit_batch = (min_events > 0 && pending_events >= min_events)
            || (min_bytes > 0 && pending_bytes >= min_bytes);
        if hit_batch {
            return true;
        }

        let max_staleness = self
            .periodic_flush_max_staleness_secs
            .load(Ordering::Relaxed);
        let since = self.pending_flush_since_unix_secs.load(Ordering::Relaxed);
        max_staleness > 0 && since > 0 && unix_secs().saturating_sub(since) >= max_staleness
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

        self.observe_clock_boundary();
        if let Some(wal) = self.wal.lock().clone() {
            if let Err(e) = wal.append(events) {
                tracing::warn!("WAL append failed (continuing without durability): {}", e);
            }
            self.maybe_request_flush_for_wal_size(wal.dir());
        }
    }

    fn maybe_request_flush_for_wal_size(&self, wal_dir: &std::path::Path) {
        let limit = self.wal_seal_bytes.load(Ordering::Relaxed);
        if limit == 0 {
            return;
        }
        let current = wal_dir.join("events.wal");
        let Ok(meta) = std::fs::metadata(&current) else {
            return;
        };
        if meta.len() < limit {
            return;
        }
        if !self.flush_requested.swap(true, Ordering::AcqRel) {
            self.flush_notify.notify_one();
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
        known_metas: Option<&[FileMeta]>,
    ) -> Option<ApplyBatchState> {
        if events.is_empty() {
            return None;
        }

        // WAL：先写后用（best-effort）。replay 场景下禁用写回，避免重复追加。
        self.append_events_to_wal(events, log_to_wal);
        self.note_runtime_subtree_tombstones_for_events(events);

        // 若 rebuild 在进行：先缓冲 pending 事件；并在持锁期间捕获当前 l2 指针，
        // 避免切换窗口导致"事件已缓冲但应用到了新索引"而重复回放。
        let (l2, rebuild_in_progress) = self.capture_l2_for_apply(events);
        let tracks_complete_subtrees = self.delta_buffer.lock().has_complete_subtree_scans();
        let target_kinds = (rebuild_in_progress || tracks_complete_subtrees).then(|| {
            events
                .iter()
                .enumerate()
                .map(|(index, event)| {
                    known_metas
                        .and_then(|metas| metas.get(index))
                        .map(|meta| meta.kind)
                        .or_else(|| resolve_event_target_kind(event))
                })
                .collect::<Vec<_>>()
        });
        let mut db = self.delta_buffer.lock();
        let all_applied = db.apply_events(events);
        if let Some(target_kinds) = target_kinds {
            for (event, target_kind) in events.iter().zip(target_kinds) {
                if rebuild_in_progress {
                    db.note_rebuild_event_target(event, target_kind);
                }
                if tracks_complete_subtrees {
                    db.invalidate_complete_subtree_scans_for_event(event, target_kind);
                }
            }
        }
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

    fn filter_events_for_freeze(&self, events: &[EventRecord]) -> Vec<EventRecord> {
        let mut gate = self.recovery_quarantine.freeze_gate.lock();
        let mut filtered = Vec::with_capacity(events.len());
        for ev in events {
            if gate.should_block_event(ev) {
                gate.note_blocked();
                tracing::warn!(
                    "freeze gate blocked {:?} under offline root: {:?}",
                    ev.event_type,
                    ev.best_path()
                );
                continue;
            }
            filtered.push(ev.clone());
        }
        filtered
    }

    fn retain_events_allowed_by_freeze(&self, events: &mut Vec<EventRecord>) {
        let mut gate = self.recovery_quarantine.freeze_gate.lock();
        events.retain(|ev| {
            if gate.should_block_event(ev) {
                gate.note_blocked();
                tracing::warn!(
                    "freeze gate blocked {:?} under offline root: {:?}",
                    ev.event_type,
                    ev.best_path()
                );
                false
            } else {
                true
            }
        });
    }

    fn filter_upserted_for_freeze(
        &self,
        events: &[EventRecord],
        metas: &mut Vec<FileMeta>,
    ) -> Vec<EventRecord> {
        if events.len() != metas.len() {
            let filtered = self.filter_events_for_freeze(events);
            if filtered.len() != events.len() {
                metas.clear();
            }
            return filtered;
        }

        let mut gate = self.recovery_quarantine.freeze_gate.lock();
        let mut filtered_events = Vec::with_capacity(events.len());
        let mut filtered_metas = Vec::with_capacity(metas.len());
        for (ev, meta) in events.iter().zip(metas.iter()) {
            if gate.should_block_event(ev) {
                gate.note_blocked();
                tracing::warn!(
                    "freeze gate blocked upsert under offline root: {:?}",
                    ev.best_path()
                );
                continue;
            }
            filtered_events.push(ev.clone());
            filtered_metas.push(meta.clone());
        }
        *metas = filtered_metas;
        filtered_events
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
        let _snapshot_boundary = self.snapshot_event_gate.lock();
        let events = self.filter_events_for_freeze(events);
        if events.is_empty() {
            return;
        }
        let Some(batch) = self.begin_apply_batch(events.as_slice(), log_to_wal, None) else {
            return;
        };
        batch.l2.apply_events(events.as_slice());
        self.event_seq
            .fetch_add(batch.event_count as u64, Ordering::Relaxed);
        self.stats.record_events_applied(batch.event_count as u64);
    }

    pub(super) fn apply_events_inner_drain(&self, events: &mut Vec<EventRecord>, log_to_wal: bool) {
        let _snapshot_boundary = self.snapshot_event_gate.lock();
        self.retain_events_allowed_by_freeze(events);
        if events.is_empty() {
            return;
        }
        let Some(batch) = self.begin_apply_batch(events.as_slice(), log_to_wal, None) else {
            return;
        };
        batch.l2.apply_events(events.as_slice());
        events.clear();
        self.event_seq
            .fetch_add(batch.event_count as u64, Ordering::Relaxed);
        self.stats.record_events_applied(batch.event_count as u64);
    }

    pub(super) fn apply_upserted_metas_inner(
        &self,
        events: &[EventRecord],
        metas: &mut Vec<FileMeta>,
        log_to_wal: bool,
    ) {
        let _snapshot_boundary = self.snapshot_event_gate.lock();
        self.apply_upserted_metas_locked(events, metas, log_to_wal);
    }

    pub(super) fn apply_upserted_metas_if_event_seq(
        &self,
        events: &[EventRecord],
        metas: &mut Vec<FileMeta>,
        log_to_wal: bool,
        expected_event_seq: u64,
    ) -> Option<u64> {
        let _snapshot_boundary = self.snapshot_event_gate.lock();
        if self.event_seq.load(Ordering::Relaxed) != expected_event_seq
            && !upserted_metas_match_filesystem(metas)
        {
            metas.clear();
            return None;
        }
        Some(self.apply_upserted_metas_locked(events, metas, log_to_wal))
    }

    fn apply_upserted_metas_locked(
        &self,
        events: &[EventRecord],
        metas: &mut Vec<FileMeta>,
        log_to_wal: bool,
    ) -> u64 {
        let events = self.filter_upserted_for_freeze(events, metas);
        let Some(batch) =
            self.begin_apply_batch(events.as_slice(), log_to_wal, Some(metas.as_slice()))
        else {
            metas.clear();
            return self.event_seq.load(Ordering::Relaxed);
        };
        if batch.rebuild_in_progress {
            batch.l2.apply_file_metas(metas.as_slice());
        } else {
            batch.l2.apply_file_metas_drain(metas);
        }
        metas.clear();
        let previous = self
            .event_seq
            .fetch_add(batch.event_count as u64, Ordering::Relaxed);
        self.stats.record_events_applied(batch.event_count as u64);
        previous.saturating_add(batch.event_count as u64)
    }
}

fn upserted_metas_match_filesystem(metas: &[FileMeta]) -> bool {
    metas.iter().all(|expected| {
        std::fs::symlink_metadata(&expected.path)
            .ok()
            .and_then(|metadata| {
                let file_key = crate::core::FileKey::from_path_and_metadata(
                    expected.path.as_path(),
                    &metadata,
                )?;
                Some(
                    file_key == expected.file_key
                        && metadata.len() == expected.size
                        && metadata.modified().ok() == expected.mtime
                        && FileKind::from_metadata(&metadata) == expected.kind,
                )
            })
            .unwrap_or(false)
    })
}

fn resolve_event_target_kind(event: &EventRecord) -> Option<FileKind> {
    if matches!(&event.event_type, EventType::Delete) {
        return None;
    }
    let path = event.best_path()?;
    std::fs::metadata(path)
        .ok()
        .map(|metadata| FileKind::from_metadata(&metadata))
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
