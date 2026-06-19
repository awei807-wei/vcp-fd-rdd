use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use parking_lot::Mutex;

use crate::fs_policy::MountTable;
use crate::storage::quarantine::{
    FreezeGate, MountIdentity, QuarantineSidecar, QuarantineState, RootStateRecord,
};

use super::{RecoveryStatus, TieredIndex};

/// Recovery & quarantine state extracted from `TieredIndex`.
pub struct RecoveryQuarantine {
    pub status: Mutex<RecoveryStatus>,
    pub quarantine_state: Mutex<QuarantineState>,
    pub freeze_gate: Mutex<FreezeGate>,
    pub verify_pending: AtomicU64,
    pub verified_roots: AtomicU64,
}

impl Default for RecoveryQuarantine {
    fn default() -> Self {
        Self {
            status: Mutex::new(RecoveryStatus::default()),
            quarantine_state: Mutex::new(QuarantineState::default()),
            freeze_gate: Mutex::new(FreezeGate::default()),
            verify_pending: AtomicU64::new(0),
            verified_roots: AtomicU64::new(0),
        }
    }
}

#[derive(Clone, Debug)]
struct VerifiedRoot {
    root_path: PathBuf,
    identity: MountIdentity,
    affected_prefixes: Vec<PathBuf>,
}

impl TieredIndex {
    pub fn restore_quarantine_from_sidecar_path(&self, path: &Path) -> anyhow::Result<()> {
        if !path.exists() {
            return Ok(());
        }
        let sidecar = QuarantineSidecar::read_from(path)?;
        self.restore_quarantine_from_sidecar(sidecar);
        Ok(())
    }

    pub fn restore_quarantine_from_sidecar(&self, sidecar: QuarantineSidecar) {
        let state = QuarantineState::from_sidecar(sidecar);
        let pending = state.active_root_count() as u64;
        let gate = state.freeze_gate();
        *self.recovery_quarantine.quarantine_state.lock() = state;
        self.install_freeze_gate(gate);
        self.recovery_quarantine
            .verify_pending
            .store(pending, Ordering::Relaxed);
    }

    pub fn spawn_quarantine_verify_worker(self: &Arc<Self>, sidecar_path: PathBuf) {
        let index = self.clone();
        std::thread::spawn(move || {
            index.verify_quarantine_sidecar_once(sidecar_path);
        });
    }

    pub(crate) fn verify_quarantine_sidecar_once(&self, sidecar_path: PathBuf) {
        if !sidecar_path.exists() {
            self.recovery_quarantine
                .verify_pending
                .store(0, Ordering::Relaxed);
            return;
        }

        let sidecar = match QuarantineSidecar::read_from(&sidecar_path) {
            Ok(sidecar) => sidecar,
            Err(e) => {
                tracing::warn!(
                    "quarantine sidecar verify skipped: read failed for {}: {}",
                    sidecar_path.display(),
                    e
                );
                return;
            }
        };
        let table = match MountTable::current() {
            Ok(table) => table,
            Err(e) => {
                tracing::warn!(
                    "quarantine sidecar verify skipped: mount table unavailable: {}",
                    e
                );
                return;
            }
        };
        self.verify_quarantine_sidecar_once_with_mount_table(sidecar_path, &table, sidecar);
    }

    pub(crate) fn verify_quarantine_sidecar_once_with_mount_table(
        &self,
        sidecar_path: PathBuf,
        table: &MountTable,
        mut sidecar: QuarantineSidecar,
    ) {
        let verified = verified_roots(&sidecar, table);
        let mut completed = 0u64;
        for root in verified {
            let seq = self.event_seq.fetch_add(1, Ordering::Relaxed) + 1;
            let record = RootStateRecord::online(
                seq,
                root.root_path.clone(),
                root.identity,
                root.affected_prefixes,
            );
            if self.try_apply_root_state_record_after_wal(record) {
                sidecar.mark_online(&root.root_path);
                completed = completed.saturating_add(1);
            }
        }

        if completed > 0 {
            if let Err(e) = sidecar.write_to(&sidecar_path) {
                tracing::warn!(
                    "quarantine sidecar online update failed for {}: {}",
                    sidecar_path.display(),
                    e
                );
            }
            self.recovery_quarantine
                .verified_roots
                .fetch_add(completed, Ordering::Relaxed);
        }
        self.recovery_quarantine
            .verify_pending
            .store(sidecar.active_roots().count() as u64, Ordering::Relaxed);
    }
}

fn verified_roots(sidecar: &QuarantineSidecar, table: &MountTable) -> Vec<VerifiedRoot> {
    sidecar
        .active_roots()
        .filter(|root| root.root_path.exists())
        .filter_map(|root| {
            let mount = table.best_match(&root.root_path)?;
            let current = MountIdentity::from(mount);
            root.verify_online(&current)?;
            Some(VerifiedRoot {
                root_path: root.root_path.clone(),
                identity: current,
                affected_prefixes: root.affected_prefixes.clone(),
            })
        })
        .collect()
}
