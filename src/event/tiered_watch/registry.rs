//! Persistence and restore of the fast scan registry.

use std::path::{Path, PathBuf};
use std::sync::atomic::Ordering;

use std::hash::{Hash, Hasher};

use serde::{Deserialize, Serialize};

use crate::config::TieredWatchConfig;
use crate::fs_policy::MountTable;
use crate::storage::snapshot::stable_snapshot_dir_for;
use crate::util::unix_secs;

use super::unix_millis;

use super::fast_scan::{
    dir_sentinel_signature, fast_scan_mount_info, network_fast_scan_mode_to_u8,
    normalize_fast_scan_dir, DirSentinel, DirSentinelSignature, FastScanLease,
};
use super::types::*;
use super::TieredWatchRuntime;

#[derive(Clone, Debug, Serialize, Deserialize)]
pub(super) struct FastScanPersistedRegistry {
    pub(super) version: u32,
    pub(super) clean_shutdown: bool,
    pub(super) snapshot_source: String,
    pub(super) wal_checkpoint_used: u64,
    pub(super) config_fingerprint: u64,
    pub(super) created_unix_secs: u64,
    pub(super) entries: Vec<FastScanPersistedEntry>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub(super) struct FastScanPersistedEntry {
    pub(super) path: PathBuf,
    pub(super) lease_kind: FastScanLeaseKind,
    pub(super) created_unix_secs: u64,
    pub(super) expires_unix_secs: u64,
    pub(super) last_used_unix_secs: u64,
    pub(super) priority: u32,
    pub(super) estimated_scan_cost: u64,
    pub(super) strict_sla_allowed: bool,
    pub(super) source_score: u64,
    pub(super) renew_count: u64,
    pub(super) sentinel_state: FastScanSentinelState,
    pub(super) mount_id: u32,
    pub(super) fstype: String,
    pub(super) class: FastScanMountClass,
    pub(super) signature: DirSentinelSignature,
    pub(super) last_checked_unix_ms: u64,
    pub(super) last_changed_unix_ms: u64,
}

impl TieredWatchRuntime {
    pub fn restore_fast_scan_registry(
        &self,
        snapshot_path: &Path,
        config: &TieredWatchConfig,
        previous_clean_shutdown: bool,
        snapshot_source: &str,
        wal_checkpoint_used: u64,
        mount_table: &MountTable,
    ) -> FastScanRegistryRestoreReport {
        let path = fast_scan_registry_path_for(snapshot_path);
        let bytes = match std::fs::read(path) {
            Ok(bytes) => bytes,
            Err(_) => {
                return FastScanRegistryRestoreReport {
                    trusted: false,
                    reason: "registry_missing".to_string(),
                    ..FastScanRegistryRestoreReport::default()
                };
            }
        };
        let registry = match serde_json::from_slice::<FastScanPersistedRegistry>(&bytes) {
            Ok(registry) => registry,
            Err(_) => {
                return FastScanRegistryRestoreReport {
                    trusted: false,
                    reason: "registry_parse_failed".to_string(),
                    ..FastScanRegistryRestoreReport::default()
                };
            }
        };

        let config_fingerprint = fast_scan_config_fingerprint(config);
        let gate_trusted = previous_clean_shutdown
            && registry.clean_shutdown
            && registry.snapshot_source == snapshot_source
            && registry.wal_checkpoint_used == wal_checkpoint_used
            && registry.config_fingerprint == config_fingerprint
            && matches!(snapshot_source, "stable" | "stable-prev" | "legacy-v7");
        let reason = if gate_trusted {
            "trusted".to_string()
        } else if !previous_clean_shutdown || !registry.clean_shutdown {
            "unclean_shutdown".to_string()
        } else if registry.snapshot_source != snapshot_source {
            "snapshot_source_mismatch".to_string()
        } else if registry.wal_checkpoint_used != wal_checkpoint_used {
            "wal_checkpoint_mismatch".to_string()
        } else if registry.config_fingerprint != config_fingerprint {
            "config_fingerprint_mismatch".to_string()
        } else {
            "snapshot_not_stable".to_string()
        };

        let mut report = FastScanRegistryRestoreReport {
            loaded_entries: registry.entries.len(),
            trusted: gate_trusted,
            reason,
            ..FastScanRegistryRestoreReport::default()
        };
        let now = unix_secs();
        let now_ms = unix_millis();
        let max_entries = config
            .l1_l2_fast_scan_sentinel_registry_max_entries
            .max(1)
            .min(
                self.fast_scan_hotset_max_leases
                    .load(Ordering::Relaxed)
                    .max(1),
            );
        let mut state = self.fast_scan_state.write();
        for entry in registry.entries.into_iter().take(max_entries) {
            // Rotating leases are process-local scheduling claims. Restoring one
            // without its active cycle would first emit a shallow bootstrap and
            // then a second full sweep, so let the cold-window loop recreate it.
            if entry.lease_kind == FastScanLeaseKind::RotatingColdWindow {
                report.rejected_entries = report.rejected_entries.saturating_add(1);
                continue;
            }
            if entry.lease_kind == FastScanLeaseKind::Query
                && !config.l1_l2_fast_scan_query_leases_enabled
            {
                report.rejected_entries = report.rejected_entries.saturating_add(1);
                continue;
            }
            let path = normalize_fast_scan_dir(entry.path);
            let Ok(meta) = std::fs::symlink_metadata(&path) else {
                report.rejected_entries = report.rejected_entries.saturating_add(1);
                continue;
            };
            if !meta.is_dir() {
                report.rejected_entries = report.rejected_entries.saturating_add(1);
                continue;
            }
            let (mount_id, fstype, class) = fast_scan_mount_info(path.as_path(), mount_table);
            let mount_matches = mount_id == entry.mount_id && fstype == entry.fstype;
            let active = gate_trusted && mount_matches;
            let sentinel_state = if active {
                FastScanSentinelState::Active
            } else {
                FastScanSentinelState::BackfillPending
            };
            let strict_sla_allowed = class.strict_sla_allowed();
            let expires_unix_secs = if entry.lease_kind.is_explicit()
                && config.l1_l2_fast_scan_explicit_lease_ttl_secs == 0
            {
                u64::MAX
            } else {
                if entry.expires_unix_secs <= now {
                    report.rejected_entries = report.rejected_entries.saturating_add(1);
                    continue;
                }
                entry.expires_unix_secs
            };

            state.leases.insert(
                path.clone(),
                FastScanLease {
                    lease_kind: entry.lease_kind,
                    created_unix_secs: entry.created_unix_secs,
                    expires_unix_secs,
                    last_used_unix_secs: entry.last_used_unix_secs.max(now),
                    priority: entry.priority,
                    estimated_scan_cost: entry.estimated_scan_cost,
                    strict_sla_allowed,
                    source_score: entry.source_score,
                    renew_count: entry.renew_count,
                    sentinel_state,
                    bootstrap_recursive_cycle_id: None,
                },
            );
            let signature = if active {
                entry.signature
            } else {
                dir_sentinel_signature(&meta)
            };
            state.sentinels.insert(
                path.clone(),
                DirSentinel {
                    mount_id,
                    fstype,
                    class,
                    lease_kind: entry.lease_kind,
                    signature,
                    trust_clock: strict_sla_allowed,
                    trust_nlink: strict_sla_allowed,
                    strict_sla_allowed,
                    sentinel_state,
                    last_checked_unix_ms: if active {
                        entry.last_checked_unix_ms
                    } else {
                        0
                    },
                    last_changed_unix_ms: entry.last_changed_unix_ms.max(now_ms),
                    coverage_lag_ms: 0,
                },
            );
            if active {
                report.restored_active = report.restored_active.saturating_add(1);
            } else {
                report.restored_unknown = report.restored_unknown.saturating_add(1);
                if !state
                    .initial_backfill_queue
                    .iter()
                    .any(|queued| queued == &path)
                {
                    state.initial_backfill_queue.push_back(path);
                }
            }
        }
        if report.restored_unknown > 0 {
            self.fast_scan_budget_degraded
                .store(true, Ordering::Relaxed);
            state.last_degraded_reason = format!(
                "hotset registry restore untrusted: reason={} unknown={}",
                report.reason, report.restored_unknown
            );
        }
        report
    }

    pub fn persist_fast_scan_registry(
        &self,
        snapshot_path: &Path,
        config: &TieredWatchConfig,
        snapshot_source: &str,
        wal_checkpoint_used: u64,
        clean_shutdown: bool,
    ) -> anyhow::Result<usize> {
        let max_entries = config.l1_l2_fast_scan_sentinel_registry_max_entries.max(1);
        let state = self.fast_scan_state.read();
        let mut entries = state
            .sentinels
            .iter()
            .filter_map(|(path, sentinel)| {
                let lease = state.leases.get(path)?;
                let persist = lease.lease_kind.is_explicit()
                    || lease.sentinel_state == FastScanSentinelState::Active
                    || sentinel.sentinel_state == FastScanSentinelState::Active;
                if !persist {
                    return None;
                }
                Some(FastScanPersistedEntry {
                    path: path.clone(),
                    lease_kind: lease.lease_kind,
                    created_unix_secs: lease.created_unix_secs,
                    expires_unix_secs: lease.expires_unix_secs,
                    last_used_unix_secs: lease.last_used_unix_secs,
                    priority: lease.priority,
                    estimated_scan_cost: lease.estimated_scan_cost,
                    strict_sla_allowed: lease.strict_sla_allowed,
                    source_score: lease.source_score,
                    renew_count: lease.renew_count,
                    sentinel_state: lease.sentinel_state,
                    mount_id: sentinel.mount_id,
                    fstype: sentinel.fstype.clone(),
                    class: sentinel.class,
                    signature: sentinel.signature,
                    last_checked_unix_ms: sentinel.last_checked_unix_ms,
                    last_changed_unix_ms: sentinel.last_changed_unix_ms,
                })
            })
            .collect::<Vec<_>>();
        entries.sort_by_key(|entry| {
            (
                std::cmp::Reverse(entry.lease_kind.priority()),
                std::cmp::Reverse(entry.source_score),
                entry.path.clone(),
            )
        });
        entries.truncate(max_entries);
        let count = entries.len();
        let registry = FastScanPersistedRegistry {
            version: 1,
            clean_shutdown,
            snapshot_source: snapshot_source.to_string(),
            wal_checkpoint_used,
            config_fingerprint: fast_scan_config_fingerprint(config),
            created_unix_secs: unix_secs(),
            entries,
        };
        write_fast_scan_registry_atomic(snapshot_path, &registry)?;
        Ok(count)
    }
}

pub(super) fn fast_scan_registry_path_for(snapshot_path: &Path) -> PathBuf {
    stable_snapshot_dir_for(snapshot_path).join("fast-scan-hotset-registry.json")
}

pub(super) fn write_fast_scan_registry_atomic(
    snapshot_path: &Path,
    registry: &FastScanPersistedRegistry,
) -> anyhow::Result<()> {
    let path = fast_scan_registry_path_for(snapshot_path);
    crate::util::atomic_write_json(&path, registry)
}

pub(super) fn fast_scan_config_fingerprint(config: &TieredWatchConfig) -> u64 {
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    config.l1_l2_fast_scan_enabled.hash(&mut hasher);
    config.l1_l2_fast_scan_target_secs.hash(&mut hasher);
    config.l1_l2_fast_scan_tick_ms.hash(&mut hasher);
    config
        .l1_l2_fast_scan_stat_budget_per_tick
        .hash(&mut hasher);
    config
        .l1_l2_fast_scan_readdir_budget_per_tick
        .hash(&mut hasher);
    config
        .l1_l2_fast_scan_bootstrap_budget_per_tick
        .hash(&mut hasher);
    config.l1_l2_fast_scan_hotset_max_leases.hash(&mut hasher);
    config.l1_l2_fast_scan_lease_ttl_secs.hash(&mut hasher);
    config
        .l1_l2_fast_scan_proc_sampler_lease_ttl_secs
        .hash(&mut hasher);
    config
        .l1_l2_fast_scan_explicit_lease_ttl_secs
        .hash(&mut hasher);
    config
        .l1_l2_fast_scan_sentinel_registry_max_entries
        .hash(&mut hasher);
    network_fast_scan_mode_to_u8(config.network_fast_scan_mode).hash(&mut hasher);
    config
        .network_fast_scan_stat_budget_per_tick
        .hash(&mut hasher);
    config
        .network_fast_scan_readdir_budget_per_tick
        .hash(&mut hasher);
    for dir in &config.hot_dirs {
        dir.hash(&mut hasher);
    }
    hasher.finish()
}
