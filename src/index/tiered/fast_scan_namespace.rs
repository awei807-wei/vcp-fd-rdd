use std::collections::{HashMap, HashSet};
use std::ffi::OsString;
use std::path::{Path, PathBuf};
use std::sync::atomic::Ordering;

use crate::core::{EventRecord, EventType, FileIdentifier, FileKey, FileKind, FileMeta};
use crate::event::sync::{now_ns, DirectoryFingerprint, DirtyReason, DirtyScope};
use crate::index::l2_partition::mtime_to_ns;
use crate::index::PathFreshness;
use crate::util::path_has_excluded_component;

use super::sync::{directory_read_fingerprint, should_skip_dirty_dir, FastSyncReport};
use super::TieredIndex;

#[cfg(unix)]
mod apply;
#[cfg(unix)]
mod walk;

#[cfg(unix)]
struct NamespaceSnapshot {
    indexed: HashMap<PathBuf, FileMeta>,
    deletion_candidates: Vec<PathBuf>,
    expected_event_seq: u64,
    expected_invalidation_epoch: u64,
}

#[cfg(unix)]
#[derive(Default)]
struct NamespaceChanges {
    upsert_events: Vec<EventRecord>,
    upsert_metas: Vec<FileMeta>,
    non_indexable_deletes: Vec<EventRecord>,
    missing_deletes: Vec<EventRecord>,
    recursive_repair_dirs: Vec<PathBuf>,
    metadata_stats: usize,
    generation_lookups_avoided: usize,
}

#[cfg(unix)]
enum NamespaceEntryChange {
    Ignore,
    Upsert { meta: FileMeta, recursive: bool },
    NonIndexableDelete(PathBuf),
}

#[cfg(unix)]
enum NamespaceFingerprint {
    Gone,
    Ready(DirectoryFingerprint),
    Failed,
}

#[cfg(unix)]
enum NamespaceSnapshotState {
    Ready(NamespaceSnapshot),
    Fallback,
    Failed,
}

#[cfg(unix)]
pub(in crate::index::tiered) struct NamespaceFence {
    fingerprint: DirectoryFingerprint,
    event_seq: u64,
    invalidation_epoch: u64,
}

#[cfg(unix)]
impl NamespaceFence {
    pub(in crate::index::tiered) fn new(
        fingerprint: DirectoryFingerprint,
        event_seq: u64,
        invalidation_epoch: u64,
    ) -> Self {
        Self {
            fingerprint,
            event_seq,
            invalidation_epoch,
        }
    }
}

impl TieredIndex {
    #[cfg(not(unix))]
    pub(super) fn fast_scan_namespace_sync(
        &self,
        dirs: &[PathBuf],
        ignore_prefixes: &[PathBuf],
    ) -> FastSyncReport {
        self.fast_sync(DirtyScope::dirs(now_ns(), dirs.to_vec()), ignore_prefixes)
    }

    /// Reconcile the direct namespace reported changed by a FastScan sentinel.
    #[cfg(unix)]
    pub(super) fn fast_scan_namespace_sync(
        &self,
        dirs: &[PathBuf],
        ignore_prefixes: &[PathBuf],
    ) -> FastSyncReport {
        let mut report = FastSyncReport::default();
        for dir in dirs {
            let dir_report = self.reconcile_fast_scan_namespace_dir(dir, ignore_prefixes);
            merge_fast_scan_reports(&mut report, dir_report);
        }
        self.mark_clock_reconciled();
        self.stats.record_fast_sync();
        tracing::debug!(
            dirs = report.dirs_scanned,
            upserts = report.upsert_events,
            deletes = report.delete_events,
            metadata_stats = report.metadata_stats,
            generation_lookups_avoided = report.generation_lookups_avoided,
            fallback_dirs = report.fallback_dirs,
            failed = report.failed,
            "fast-scan namespace reconcile complete"
        );
        report
    }

    #[cfg(unix)]
    fn reconcile_fast_scan_namespace_dir(
        &self,
        dir: &Path,
        ignore_prefixes: &[PathBuf],
    ) -> FastSyncReport {
        if !self.roots.iter().any(|root| dir.starts_with(root)) {
            return FastSyncReport {
                failed: true,
                ..FastSyncReport::default()
            };
        }
        if should_skip_dirty_dir(dir, ignore_prefixes, &self.exclude_dirs)
            || self.path_is_frozen(dir)
        {
            return FastSyncReport::default();
        }
        let Some(parent_path) = dir.to_str() else {
            return self.fast_scan_namespace_fallback(dir, ignore_prefixes);
        };
        if self.rebuild_in_progress() || self.base.load().file_count() == 0 {
            return self.fast_scan_namespace_fallback(dir, ignore_prefixes);
        }
        self.reconcile_fast_scan_namespace_dir_fenced(dir, parent_path, ignore_prefixes)
    }

    #[cfg(unix)]
    fn reconcile_fast_scan_namespace_dir_fenced(
        &self,
        dir: &Path,
        parent_path: &str,
        ignore_prefixes: &[PathBuf],
    ) -> FastSyncReport {
        let namespace_gate = self.snapshot_event_gate.lock();
        let fingerprint = match self.fast_scan_namespace_fingerprint(dir) {
            NamespaceFingerprint::Ready(fingerprint) => fingerprint,
            NamespaceFingerprint::Gone => {
                drop(namespace_gate);
                return FastSyncReport::default();
            }
            NamespaceFingerprint::Failed => {
                drop(namespace_gate);
                return failed_fast_sync_report();
            }
        };
        let snapshot = match self.fast_scan_namespace_snapshot(dir, parent_path) {
            NamespaceSnapshotState::Ready(snapshot) => snapshot,
            NamespaceSnapshotState::Fallback => {
                drop(namespace_gate);
                return self.fast_scan_namespace_fallback(dir, ignore_prefixes);
            }
            NamespaceSnapshotState::Failed => {
                drop(namespace_gate);
                return failed_fast_sync_report();
            }
        };
        let Some(current_names) = self.read_fast_scan_namespace_names(dir) else {
            drop(namespace_gate);
            return failed_fast_sync_report();
        };
        let Some(mut changes) = self.walk_fast_scan_namespace(dir, &snapshot.indexed) else {
            drop(namespace_gate);
            return failed_fast_sync_report();
        };
        changes.missing_deletes =
            missing_delete_events(snapshot.deletion_candidates.as_slice(), current_names);
        let fence = NamespaceFence::new(
            fingerprint,
            snapshot.expected_event_seq,
            snapshot.expected_invalidation_epoch,
        );
        let applied = self.apply_fast_scan_namespace_changes_if_fenced_locked(
            dir,
            fence,
            changes.upsert_events.as_slice(),
            &mut changes.upsert_metas,
            changes.non_indexable_deletes.as_slice(),
            changes.missing_deletes.as_slice(),
        );
        drop(namespace_gate);
        self.finish_fast_scan_namespace_dir(changes, applied)
    }

    #[cfg(unix)]
    fn fast_scan_namespace_fingerprint(&self, dir: &Path) -> NamespaceFingerprint {
        self.io_governor.before_io();
        match std::fs::symlink_metadata(dir) {
            Ok(meta) if meta.is_dir() => directory_read_fingerprint(dir, &meta)
                .map(NamespaceFingerprint::Ready)
                .unwrap_or(NamespaceFingerprint::Failed),
            Ok(_) => NamespaceFingerprint::Failed,
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => NamespaceFingerprint::Gone,
            Err(err) => {
                tracing::debug!("fast-scan namespace cannot read {}: {}", dir.display(), err);
                NamespaceFingerprint::Failed
            }
        }
    }

    #[cfg(unix)]
    fn fast_scan_namespace_snapshot(
        &self,
        dir: &Path,
        parent_path: &str,
    ) -> NamespaceSnapshotState {
        let base = self.base.load_full();
        let l2 = self.l2.load_full();
        let expected_event_seq = self.event_seq.load(Ordering::Relaxed);
        let delta = self.delta_buffer.lock();
        if delta.is_overflowed() {
            return NamespaceSnapshotState::Fallback;
        }
        let expected_invalidation_epoch = delta.invalidation_epoch();
        let mut indexed = base
            .parent_query_metas(parent_path)
            .into_iter()
            .map(|hit| (hit.meta.path.clone(), hit.meta))
            .collect::<HashMap<_, _>>();
        let live_paths = delta
            .live_records()
            .filter_map(EventRecord::best_path)
            .map(Path::to_path_buf)
            .collect::<Vec<_>>();
        let deleted_paths = delta
            .deleted_paths()
            .map(|path| crate::util::pathbuf_from_encoded_vec(path.to_vec()))
            .collect::<HashSet<_>>();
        let mut deletion_candidates = indexed.keys().cloned().collect::<Vec<_>>();
        for path in live_paths {
            if let Some(child) = first_direct_child(dir, path.as_path()) {
                deletion_candidates.push(child);
            }
            if path.parent().is_some_and(|parent| parent == dir) {
                let Some(meta) = l2.meta_by_path(path.as_path()) else {
                    return NamespaceSnapshotState::Failed;
                };
                indexed.insert(path.clone(), meta);
            }
        }
        for path in &deleted_paths {
            if path.parent().is_some_and(|parent| parent == dir) {
                indexed.remove(path.as_path());
            }
        }
        deletion_candidates.retain(|path| !deleted_paths.contains(path));
        deletion_candidates.sort();
        deletion_candidates.dedup();
        NamespaceSnapshotState::Ready(NamespaceSnapshot {
            indexed,
            deletion_candidates,
            expected_event_seq,
            expected_invalidation_epoch,
        })
    }

    #[cfg(unix)]
    fn finish_fast_scan_namespace_dir(
        &self,
        changes: NamespaceChanges,
        applied: Option<(usize, usize)>,
    ) -> FastSyncReport {
        let mut report = FastSyncReport {
            dirs_scanned: 1,
            metadata_stats: changes.metadata_stats,
            generation_lookups_avoided: changes.generation_lookups_avoided,
            ..FastSyncReport::default()
        };
        let Some((upserts, deletes)) = applied else {
            report.failed = true;
            return report;
        };
        report.upsert_events = upserts;
        report.delete_events = deletes;
        if upserts > 0 && !changes.recursive_repair_dirs.is_empty() {
            self.enqueue_recursive_dirty_dirs(
                changes.recursive_repair_dirs,
                DirtyReason::RecursiveSubtreeRepair,
            );
        }
        report
    }

    #[cfg(unix)]
    fn fast_scan_namespace_fallback(
        &self,
        dir: &Path,
        ignore_prefixes: &[PathBuf],
    ) -> FastSyncReport {
        let mut report = self.fast_sync(
            DirtyScope::dirs(now_ns(), vec![dir.to_path_buf()]),
            ignore_prefixes,
        );
        report.fallback_dirs = report.fallback_dirs.saturating_add(1);
        report
    }
}

#[cfg(unix)]
fn delete_event(seq: u64, path: PathBuf) -> EventRecord {
    EventRecord {
        seq,
        timestamp: std::time::SystemTime::now(),
        event_type: EventType::Delete,
        id: FileIdentifier::Path(path),
        path_hint: None,
    }
}

#[cfg(unix)]
fn first_direct_child(root: &Path, path: &Path) -> Option<PathBuf> {
    let relative = path.strip_prefix(root).ok()?;
    let std::path::Component::Normal(name) = relative.components().next()? else {
        return None;
    };
    Some(root.join(name))
}

#[cfg(unix)]
fn missing_delete_events(
    candidates: &[PathBuf],
    current_names: HashSet<OsString>,
) -> Vec<EventRecord> {
    candidates
        .iter()
        .filter(|path| {
            path.file_name()
                .is_some_and(|name| !current_names.contains(name))
        })
        .cloned()
        .enumerate()
        .map(|(index, path)| delete_event(index as u64 + 1, path))
        .collect()
}

#[cfg(unix)]
fn merge_fast_scan_reports(target: &mut FastSyncReport, source: FastSyncReport) {
    target.dirs_scanned = target.dirs_scanned.saturating_add(source.dirs_scanned);
    target.upsert_events = target.upsert_events.saturating_add(source.upsert_events);
    target.delete_events = target.delete_events.saturating_add(source.delete_events);
    target.metadata_stats = target.metadata_stats.saturating_add(source.metadata_stats);
    target.generation_lookups_avoided = target
        .generation_lookups_avoided
        .saturating_add(source.generation_lookups_avoided);
    target.fallback_dirs = target.fallback_dirs.saturating_add(source.fallback_dirs);
    target.failed |= source.failed;
}

#[cfg(unix)]
fn failed_fast_sync_report() -> FastSyncReport {
    FastSyncReport {
        failed: true,
        ..FastSyncReport::default()
    }
}
