use std::sync::atomic::Ordering;
use std::sync::Arc;

use crate::index::base_index::BaseIndexData;
use crate::index::l2_partition::PersistentIndex;
use crate::storage::snapshot::{
    install_stable_v7_from_source, quarantine_sidecar_path_for, remove_stable_v7_recovery_copies,
    stable_v7_path_for, write_recovery_runtime_state, RecoveryRuntimeState,
};
use crate::storage::snapshot_v7::{
    load_v7_from_path, try_load_v7_cold, write_v7_cold_delta_atomic, write_v7_owned_index_atomic,
    write_v7_snapshot_atomic, ColdDeltaLimits, ColdDeltaPlan, OwnedV7WriteOptions,
};
use crate::storage::traits::StorageBackend;
use crate::util::unix_secs;

use super::TieredIndex;

pub(super) struct DurableSnapshotGeneration {
    cold_base: BaseIndexData,
    wal_seal_id: u64,
    pub(super) consumed_owned_generation: bool,
}

enum SnapshotInput {
    OwnedRebuild {
        generation: Box<PersistentIndex>,
        plan: ColdDeltaPlan,
    },
    Layered {
        base: Arc<BaseIndexData>,
        plan: ColdDeltaPlan,
    },
}

impl SnapshotInput {
    fn consumes_owned_generation(&self) -> bool {
        matches!(self, Self::OwnedRebuild { .. })
    }
}

pub(super) fn build_durable_snapshot_generation<S>(
    idx: &Arc<TieredIndex>,
    store: &S,
) -> anyhow::Result<DurableSnapshotGeneration>
where
    S: StorageBackend + ?Sized,
{
    let wal_seal_id = match idx.wal.lock().clone() {
        Some(wal) => wal
            .seal()
            .map_err(|error| anyhow::anyhow!("WAL seal failed: {error}"))?,
        None => 0,
    };
    let input = capture_snapshot_input(idx)?;
    let consumes_owned_generation = input.consumes_owned_generation();
    let v7_path = store.path().with_extension("v7");
    let result = (|| {
        write_primary_generation(&v7_path, input)?;
        let cold_base = install_and_mount_generation(idx, store.path(), &v7_path, wal_seal_id)?;
        Ok(DurableSnapshotGeneration {
            cold_base,
            wal_seal_id,
            consumed_owned_generation: consumes_owned_generation,
        })
    })();
    if consumes_owned_generation {
        result.map_err(|error: anyhow::Error| {
            anyhow::anyhow!("owned_v7_generation_consumed: {error:#}")
        })
    } else {
        result
    }
}

fn capture_snapshot_input(idx: &TieredIndex) -> anyhow::Result<SnapshotInput> {
    let db = idx.delta_buffer.lock();
    if !db.is_complete() {
        anyhow::bail!(
            "snapshot_delta_incomplete: DeltaBuffer rejected events; full rebuild required"
        );
    }

    let deleted_paths = db
        .snapshot_deleted_paths()
        .map(<[u8]>::to_vec)
        .collect::<Vec<_>>();
    let mut upserts = Vec::with_capacity(db.live_records().count());
    for event in db.live_records() {
        let meta = idx.overlay_meta_for_event(event).ok_or_else(|| {
            anyhow::anyhow!(
                "snapshot_upsert_unresolved: no filesystem or L2 metadata for {:?}",
                event.best_path()
            )
        })?;
        upserts.push(meta);
    }
    let plan = ColdDeltaPlan::new(deleted_paths, upserts);
    if let Some(generation) = idx.pending_snapshot_generation.lock().take() {
        idx.owned_snapshot_telemetry.lock().lifecycle =
            super::super::OwnedSnapshotLifecycle::Writing;
        return Ok(SnapshotInput::OwnedRebuild {
            generation: Box::new(generation),
            plan,
        });
    }

    let base = idx.base.load_full();
    if base.file_count() == 0 && plan.is_empty() && idx.l2.load().file_count() > 0 {
        anyhow::bail!(
            "snapshot_delta_incomplete: mutable L2 contains entries absent from base and delta"
        );
    }
    Ok(SnapshotInput::Layered { base, plan })
}

fn write_primary_generation(path: &std::path::Path, input: SnapshotInput) -> anyhow::Result<()> {
    match input {
        SnapshotInput::OwnedRebuild { generation, plan } => {
            write_owned_rebuild_generation(path, *generation, plan)
        }
        SnapshotInput::Layered { base, plan } => {
            write_layered_generation(path, base.as_ref(), plan)
        }
    }
}

fn write_owned_rebuild_generation(
    path: &std::path::Path,
    generation: PersistentIndex,
    plan: ColdDeltaPlan,
) -> anyhow::Result<()> {
    let deleted = plan.deleted_count();
    let upserts = plan.upsert_count();
    let report =
        write_v7_owned_index_atomic(path, generation, plan, OwnedV7WriteOptions::default())?;
    tracing::info!(
        deleted,
        upserts,
        raw_entries = report.raw_entries,
        live_entries = report.live_entries,
        path_runs = report.path_runs,
        trigram_runs = report.trigram_runs,
        peak_sort_buffer_bytes = report.peak_sort_buffer_bytes,
        "owned rebuild generation streamed to primary v7"
    );
    Ok(())
}

fn write_layered_generation(
    path: &std::path::Path,
    base: &BaseIndexData,
    plan: ColdDeltaPlan,
) -> anyhow::Result<()> {
    let deleted = plan.deleted_count();
    let upserts = plan.upsert_count();
    if base.cold_segments.is_empty() {
        write_v7_snapshot_atomic(path, base)?;
        if !plan.is_empty() {
            let source = load_v7_from_path(path)?.ok_or_else(|| {
                anyhow::anyhow!("direct_v7_validation_failed: hot base v7 was not loadable")
            })?;
            write_v7_cold_delta_atomic(path, &source, plan, ColdDeltaLimits::default())?;
        }
        tracing::info!(
            deleted,
            upserts,
            "hot base streamed to primary v7 generation"
        );
        return Ok(());
    }

    if plan.is_empty() {
        anyhow::bail!("snapshot_delta_incomplete: cold base snapshot requested without a delta");
    }
    let source = base.single_current_v7_snapshot()?;
    let report =
        write_v7_cold_delta_atomic(path, source.as_ref(), plan, ColdDeltaLimits::default())?;
    tracing::info!(
        deleted,
        upserts,
        old_entries = report.old_entries,
        live_entries = report.live_entries,
        tombstones = report.tombstone_entries,
        "cold v7 delta streamed to primary generation"
    );
    Ok(())
}

fn install_and_mount_generation(
    idx: &TieredIndex,
    snapshot_path: &std::path::Path,
    primary_path: &std::path::Path,
    wal_seal_id: u64,
) -> anyhow::Result<BaseIndexData> {
    let stable_enabled = idx
        .io_tuning
        .stable_snapshot_enabled
        .load(Ordering::Relaxed);
    let (remount_path, cold_base) = if stable_enabled {
        install_stable_v7_from_source(snapshot_path, primary_path)?;
        let path = stable_v7_path_for(snapshot_path);
        let cold_base = load_cold_generation(idx, &path)?;
        (path, cold_base)
    } else {
        sync_primary_snapshot_parent(primary_path)?;
        let cold_base = load_cold_generation(idx, primary_path)?;
        remove_stable_v7_recovery_copies(snapshot_path)?;
        (primary_path.to_path_buf(), cold_base)
    };

    persist_quarantine_sidecar(idx, snapshot_path)?;
    write_snapshot_runtime_state(idx, snapshot_path, wal_seal_id)?;
    tracing::info!(path = %remount_path.display(), "snapshot generation durably mounted cold");
    Ok(cold_base)
}

fn load_cold_generation(
    idx: &TieredIndex,
    path: &std::path::Path,
) -> anyhow::Result<BaseIndexData> {
    try_load_v7_cold(path, idx.roots.as_slice())?.ok_or_else(|| {
        anyhow::anyhow!(
            "direct_v7_validation_failed: durable generation was not cold-mountable: {}",
            path.display()
        )
    })
}

fn persist_quarantine_sidecar(
    idx: &TieredIndex,
    snapshot_path: &std::path::Path,
) -> anyhow::Result<()> {
    let sidecar = idx.recovery_quarantine.quarantine_state.lock().to_sidecar();
    sidecar.write_to(&quarantine_sidecar_path_for(snapshot_path))
}

fn write_snapshot_runtime_state(
    idx: &TieredIndex,
    snapshot_path: &std::path::Path,
    wal_seal_id: u64,
) -> anyhow::Result<()> {
    let state = RecoveryRuntimeState {
        last_clean_shutdown: idx.is_shutting_down(),
        last_snapshot_unix_secs: unix_secs(),
        last_wal_seal_id: wal_seal_id,
        last_startup_source: idx.recovery_status().report.snapshot_source,
        last_recovery_mode: "snapshot".to_string(),
        root_case_policies: idx.root_case_policy_diagnostics(),
    };
    write_recovery_runtime_state(snapshot_path, &state)
}

pub(super) fn publish_snapshot_generation(
    idx: &TieredIndex,
    generation: DurableSnapshotGeneration,
) -> anyhow::Result<()> {
    let mut db = idx.delta_buffer.lock();
    if !db.is_complete() {
        anyhow::bail!("snapshot_delta_incomplete: captured generation changed before publish");
    }
    idx.base.store(Arc::new(generation.cold_base));
    *idx.pending_snapshot_generation.lock() = None;
    *idx.owned_snapshot_telemetry.lock() = Default::default();
    db.reset_complete_generation();
    idx.l2
        .store(Arc::new(PersistentIndex::new_with_roots(idx.roots.clone())));
    idx.clear_runtime_subtree_tombstones_after_generation();
    idx.invalidate_memory_report_cache();
    drop(db);

    idx.flush_requested.store(false, Ordering::Release);
    idx.rebuild_snapshot_pending.store(false, Ordering::Release);
    idx.l1.clear();
    if let Some(wal) = idx.wal.lock().clone() {
        if let Err(error) = wal.cleanup_sealed_up_to(generation.wal_seal_id) {
            tracing::warn!("snapshot durable but sealed WAL cleanup failed: {error}");
        }
    }
    idx.record_snapshot_success();
    idx.reset_pending_flush_batch();
    Ok(())
}

fn sync_primary_snapshot_parent(path: &std::path::Path) -> anyhow::Result<()> {
    #[cfg(unix)]
    if let Some(parent) = path.parent() {
        std::fs::File::open(parent)?.sync_all()?;
    }
    Ok(())
}
