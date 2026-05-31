use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};

use crate::storage::snapshot::{
    lsm_read_deleted_paths, lsm_read_manifest, parse_lsm_seg_id, stable_prev_v7_path_for,
    stable_v7_path_for, RecoveryRuntimeState,
};
use crate::storage::wal::{WAL_MAGIC, WAL_VERSION};

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct RecoveryAuditReport {
    pub stable_ok: bool,
    pub stable_prev_ok: bool,
    pub legacy_v7_ok: bool,
    pub manifest_ok: bool,
    pub current_wal_ok: bool,
    pub sealed_wal_count: usize,
    pub sealed_wal_ids: Vec<u64>,
    pub sealed_wal_used_after_checkpoint: usize,
    pub wal_checkpoint: u64,
    pub manifest_wal_seal_id: u64,
    pub runtime_wal_seal_id: u64,
    pub wal_gap_detected: bool,
    pub orphan_segment_count: usize,
    pub tmp_file_count: usize,
    pub missing_segment_count: usize,
    pub unreadable_sidecar_count: usize,
    pub unknown_wal_file_count: usize,
    pub requires_repair: bool,
    pub requires_rebuild: bool,
    pub reasons: Vec<String>,
}

impl RecoveryAuditReport {
    pub fn reason_csv(&self) -> String {
        self.reasons.join(",")
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SnapshotCheckpointSource {
    Stable,
    StablePrev,
    LegacyV7,
    Empty,
}

impl SnapshotCheckpointSource {
    pub fn from_label(label: &str) -> Self {
        match label {
            "stable" => Self::Stable,
            "stable-prev" => Self::StablePrev,
            "legacy-v7" => Self::LegacyV7,
            _ => Self::Empty,
        }
    }
}

pub fn audit_recovery_ledger(
    snapshot_path: &Path,
    expected_roots: &[PathBuf],
    runtime_state: &RecoveryRuntimeState,
) -> RecoveryAuditReport {
    let mut report = RecoveryAuditReport {
        runtime_wal_seal_id: runtime_state.last_wal_seal_id,
        ..RecoveryAuditReport::default()
    };

    let stable_path = stable_v7_path_for(snapshot_path);
    let stable_prev_path = stable_prev_v7_path_for(snapshot_path);
    let legacy_v7_path = snapshot_path.with_extension("v7");

    report.stable_ok = snapshot_loadable(&stable_path, expected_roots);
    report.stable_prev_ok = snapshot_loadable(&stable_prev_path, expected_roots);
    report.legacy_v7_ok = snapshot_loadable(&legacy_v7_path, expected_roots);

    if stable_path.exists() && !report.stable_ok {
        report.requires_repair = true;
        report.reasons.push("bad_stable_snapshot".to_string());
    }
    if stable_prev_path.exists() && !report.stable_prev_ok {
        report.reasons.push("bad_stable_prev_snapshot".to_string());
    }

    audit_lsm(snapshot_path, &mut report);
    audit_wal(snapshot_path, &mut report);

    report.wal_checkpoint = choose_checkpoint(SnapshotCheckpointSource::Stable, &report);
    report.sealed_wal_used_after_checkpoint = report
        .sealed_wal_ids
        .iter()
        .filter(|id| **id > report.wal_checkpoint)
        .count();
    if report.wal_gap_detected {
        report.requires_repair = true;
        report.reasons.push("wal_gap".to_string());
    }

    report
}

pub fn choose_checkpoint(source: SnapshotCheckpointSource, audit: &RecoveryAuditReport) -> u64 {
    match source {
        SnapshotCheckpointSource::Stable if audit.stable_ok => {
            audit.runtime_wal_seal_id.max(audit.manifest_wal_seal_id)
        }
        SnapshotCheckpointSource::StablePrev
        | SnapshotCheckpointSource::LegacyV7
        | SnapshotCheckpointSource::Empty
        | SnapshotCheckpointSource::Stable => 0,
    }
}

fn snapshot_loadable(path: &Path, expected_roots: &[PathBuf]) -> bool {
    let _ = expected_roots;
    crate::storage::snapshot_v7::shallow_validate_v7(path).unwrap_or(false)
}

fn lsm_dir_path(snapshot_path: &Path) -> PathBuf {
    if snapshot_path.extension().and_then(|s| s.to_str()) == Some("d") || snapshot_path.is_dir() {
        snapshot_path.to_path_buf()
    } else {
        snapshot_path.with_extension("d")
    }
}

fn audit_lsm(snapshot_path: &Path, report: &mut RecoveryAuditReport) {
    let dir = lsm_dir_path(snapshot_path);
    let manifest_path = dir.join("MANIFEST.bin");
    if !manifest_path.exists() {
        return;
    }

    let manifest = match lsm_read_manifest(&manifest_path) {
        Ok(manifest) => {
            report.manifest_ok = true;
            report.manifest_wal_seal_id = manifest.wal_seal_id;
            manifest
        }
        Err(e) => {
            report.requires_rebuild = true;
            report.requires_repair = true;
            report
                .reasons
                .push(format!("bad_manifest:{}", compact_err(e)));
            return;
        }
    };

    let live_ids = manifest_live_ids(manifest.base_id, &manifest.delta_ids);
    let mut segment_files: HashMap<u64, usize> = HashMap::new();
    let Ok(entries) = std::fs::read_dir(&dir) else {
        report.requires_repair = true;
        report.reasons.push("lsm_dir_unreadable".to_string());
        return;
    };

    for entry in entries.flatten() {
        let name = entry.file_name();
        let name = name.to_string_lossy();
        if name.ends_with(".tmp") {
            report.tmp_file_count += 1;
        }
        if let Some(id) = parse_lsm_seg_id(&name) {
            *segment_files.entry(id).or_insert(0) += 1;
            if !live_ids.contains(&id) {
                report.orphan_segment_count += 1;
            }
        }
        if name.starts_with("events.wal")
            && name != "events.wal"
            && parse_wal_seal_id(&name).is_none()
        {
            report.unknown_wal_file_count += 1;
        }
    }

    for id in live_ids {
        let db_path = dir.join(format!("seg-{id:016x}.db"));
        if !db_path.exists() {
            report.missing_segment_count += 1;
            report.requires_rebuild = true;
            report.requires_repair = true;
            report.reasons.push("missing_segment".to_string());
        }

        let del_path = dir.join(format!("seg-{id:016x}.del"));
        if del_path.exists() && lsm_read_deleted_paths(&del_path).is_err() {
            report.unreadable_sidecar_count += 1;
            report.requires_rebuild = true;
            report.requires_repair = true;
            report.reasons.push("bad_segment_sidecar".to_string());
        }
    }
}

fn manifest_live_ids(base_id: u64, delta_ids: &[u64]) -> HashSet<u64> {
    std::iter::once(base_id)
        .filter(|id| *id != 0)
        .chain(delta_ids.iter().copied())
        .collect()
}

fn audit_wal(snapshot_path: &Path, report: &mut RecoveryAuditReport) {
    let dir = lsm_dir_path(snapshot_path);
    let current = dir.join("events.wal");
    report.current_wal_ok = current_wal_header_ok(&current);
    if !report.current_wal_ok {
        report.requires_repair = true;
        report.reasons.push("bad_current_wal".to_string());
    }

    let Ok(entries) = std::fs::read_dir(&dir) else {
        return;
    };

    let mut ids = Vec::new();
    let mut seen = HashSet::new();
    for entry in entries.flatten() {
        let name = entry.file_name();
        let name = name.to_string_lossy();
        if let Some(id) = parse_wal_seal_id(&name) {
            if !seen.insert(id) {
                report.wal_gap_detected = true;
            }
            ids.push(id);
        } else if name.starts_with("events.wal") && name != "events.wal" {
            report.unknown_wal_file_count += 1;
        }
    }
    ids.sort_unstable();
    report.wal_gap_detected |= small_sequence_gap_detected(&ids);
    report.sealed_wal_count = ids.len();
    report.sealed_wal_ids = ids;
}

fn current_wal_header_ok(path: &Path) -> bool {
    use std::io::Read;

    if !path.exists() {
        return true;
    }
    let Ok(mut file) = std::fs::File::open(path) else {
        return false;
    };
    let mut header = [0u8; 8];
    match file.read_exact(&mut header) {
        Ok(()) => {}
        Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => {
            return file.metadata().map(|m| m.len() == 0).unwrap_or(false);
        }
        Err(_) => return false,
    }
    let magic = u32::from_le_bytes(header[0..4].try_into().unwrap_or_default());
    let version = u32::from_le_bytes(header[4..8].try_into().unwrap_or_default());
    magic == WAL_MAGIC && (1..=WAL_VERSION).contains(&version)
}

pub(crate) fn parse_wal_seal_id(name: &str) -> Option<u64> {
    let prefix = "events.wal.seal-";
    let rest = name.strip_prefix(prefix)?;
    let hex: String = rest.chars().take_while(|c| c.is_ascii_hexdigit()).collect();
    if hex.is_empty() {
        return None;
    }
    u64::from_str_radix(&hex, 16).ok()
}

pub(crate) fn small_sequence_gap_detected(ids: &[u64]) -> bool {
    if ids.len() < 2 {
        return false;
    }
    let Some(max_id) = ids.iter().copied().max() else {
        return false;
    };
    if max_id > 1_000_000 {
        return false;
    }
    ids.windows(2).any(|pair| pair[1] > pair[0] + 1)
}

fn compact_err(err: anyhow::Error) -> String {
    err.to_string().replace([' ', ','], "_")
}
