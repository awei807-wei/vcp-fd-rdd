use super::*;

impl TieredIndex {
    #[cfg(all(unix, test))]
    pub(in crate::index::tiered) fn apply_fast_scan_namespace_changes_if_fenced(
        &self,
        dir: &Path,
        fence: NamespaceFence,
        events: &[EventRecord],
        metas: &mut Vec<FileMeta>,
        non_indexable_deletes: &[EventRecord],
    ) -> Option<(usize, usize)> {
        let _snapshot_boundary = self.snapshot_event_gate.lock();
        self.apply_fast_scan_namespace_changes_if_fenced_locked(
            dir,
            fence,
            events,
            metas,
            non_indexable_deletes,
            &[],
        )
    }

    #[cfg(unix)]
    pub(super) fn apply_fast_scan_namespace_changes_if_fenced_locked(
        &self,
        dir: &Path,
        fence: NamespaceFence,
        events: &[EventRecord],
        metas: &mut Vec<FileMeta>,
        non_indexable_deletes: &[EventRecord],
        missing_deletes: &[EventRecord],
    ) -> Option<(usize, usize)> {
        if !self.fast_scan_namespace_fence_matches(
            dir,
            &fence,
            metas,
            non_indexable_deletes,
            missing_deletes,
        ) {
            metas.clear();
            return None;
        }
        if events.is_empty() && non_indexable_deletes.is_empty() && missing_deletes.is_empty() {
            return Some((0, 0));
        }
        let mut combined =
            Vec::with_capacity(events.len() + non_indexable_deletes.len() + missing_deletes.len());
        combined.extend_from_slice(events);
        combined.extend_from_slice(non_indexable_deletes);
        combined.extend_from_slice(missing_deletes);
        if !self.fast_scan_namespace_events_allowed(combined.as_slice()) {
            metas.clear();
            return None;
        }
        self.apply_fast_scan_namespace_batch(
            events,
            metas,
            non_indexable_deletes,
            missing_deletes,
            combined,
        )
    }

    #[cfg(unix)]
    fn fast_scan_namespace_fence_matches(
        &self,
        dir: &Path,
        fence: &NamespaceFence,
        metas: &[FileMeta],
        deletes: &[EventRecord],
        missing_deletes: &[EventRecord],
    ) -> bool {
        if self.delta_buffer.lock().invalidation_epoch() != fence.invalidation_epoch
            || self.event_seq.load(Ordering::Relaxed) != fence.event_seq
        {
            return false;
        }
        self.io_governor.before_io();
        let fingerprint = std::fs::symlink_metadata(dir)
            .ok()
            .filter(|meta| meta.is_dir())
            .and_then(|meta| directory_read_fingerprint(dir, &meta));
        fingerprint == Some(fence.fingerprint)
            && fast_scan_metas_match_filesystem(metas)
            && fast_scan_non_indexable_paths_match_filesystem(deletes)
            && fast_scan_missing_paths_match_filesystem(missing_deletes)
    }

    #[cfg(unix)]
    fn fast_scan_namespace_events_allowed(&self, events: &[EventRecord]) -> bool {
        let mut freeze_gate = self.recovery_quarantine.freeze_gate.lock();
        if !events
            .iter()
            .any(|event| freeze_gate.should_block_event(event))
        {
            return true;
        }
        for event in events {
            if freeze_gate.should_block_event(event) {
                freeze_gate.note_blocked();
            }
        }
        false
    }

    #[cfg(unix)]
    fn apply_fast_scan_namespace_batch(
        &self,
        events: &[EventRecord],
        metas: &mut Vec<FileMeta>,
        non_indexable_deletes: &[EventRecord],
        missing_deletes: &[EventRecord],
        combined: Vec<EventRecord>,
    ) -> Option<(usize, usize)> {
        let batch = self.begin_apply_batch(combined.as_slice(), true, None)?;
        if batch.rebuild_in_progress {
            batch.l2.apply_file_metas(metas.as_slice());
            metas.clear();
        } else {
            batch.l2.apply_file_metas_drain(metas);
        }
        batch.l2.apply_events(non_indexable_deletes);
        batch.l2.apply_events(missing_deletes);
        self.event_seq
            .fetch_add(batch.event_count as u64, Ordering::Relaxed);
        self.stats.record_events_applied(batch.event_count as u64);
        Some((
            events.len(),
            non_indexable_deletes.len() + missing_deletes.len(),
        ))
    }
}

#[cfg(unix)]
fn fast_scan_metas_match_filesystem(metas: &[FileMeta]) -> bool {
    metas.iter().all(|expected| {
        std::fs::symlink_metadata(&expected.path)
            .ok()
            .and_then(|metadata| {
                let key = FileKey::from_path_and_metadata(expected.path.as_path(), &metadata)?;
                Some(
                    key == expected.file_key
                        && metadata.len() == expected.size
                        && metadata.modified().ok() == expected.mtime
                        && FileKind::from_metadata(&metadata) == expected.kind,
                )
            })
            .unwrap_or(false)
    })
}

#[cfg(unix)]
fn fast_scan_non_indexable_paths_match_filesystem(events: &[EventRecord]) -> bool {
    events.iter().all(|event| {
        event
            .best_path()
            .and_then(|path| std::fs::symlink_metadata(path).ok())
            .is_some_and(|metadata| !metadata.is_file() && !metadata.is_dir())
    })
}

#[cfg(unix)]
fn fast_scan_missing_paths_match_filesystem(events: &[EventRecord]) -> bool {
    events.iter().all(|event| {
        event.best_path().is_some_and(|path| {
            std::fs::symlink_metadata(path)
                .is_err_and(|err| err.kind() == std::io::ErrorKind::NotFound)
        })
    })
}
