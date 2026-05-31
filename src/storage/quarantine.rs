//! Persistent quarantine state for offline roots and freeze gates.

use serde::{Deserialize, Serialize};
use std::path::{Path, PathBuf};

use crate::core::{EventRecord, EventType};
use crate::fs_policy::MountEntry;

pub const QUARANTINE_SIDECAR_VERSION: u32 = 1;

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct MountIdentity {
    pub mount_id: u32,
    pub major_minor: String,
    pub fs_uuid: Option<String>,
    pub source: String,
    pub fstype: String,
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum QuarantineRootState {
    SuspectedOffline,
    #[default]
    Offline,
    Verifying,
    Online,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct QuarantineRoot {
    pub root_path: PathBuf,
    pub identity: MountIdentity,
    pub affected_prefixes: Vec<PathBuf>,
    pub state: QuarantineRootState,
    pub reason: Option<String>,
    pub last_observed_unix_ns: u64,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct QuarantineSidecar {
    pub version: u32,
    pub roots: Vec<QuarantineRoot>,
}

impl Default for QuarantineSidecar {
    fn default() -> Self {
        Self {
            version: QUARANTINE_SIDECAR_VERSION,
            roots: Vec::new(),
        }
    }
}

impl QuarantineSidecar {
    pub fn read_from(path: &Path) -> anyhow::Result<Self> {
        let text = std::fs::read_to_string(path)?;
        let mut sidecar: Self = serde_json::from_str(&text)?;
        if sidecar.version == 0 {
            sidecar.version = QUARANTINE_SIDECAR_VERSION;
        }
        Ok(sidecar)
    }

    pub fn write_to(&self, path: &Path) -> anyhow::Result<()> {
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        let text = serde_json::to_string_pretty(self)?;
        std::fs::write(path, text)?;
        Ok(())
    }

    pub fn active_roots(&self) -> impl Iterator<Item = &QuarantineRoot> {
        self.roots
            .iter()
            .filter(|root| !matches!(root.state, QuarantineRootState::Online))
    }

    pub fn mark_online(&mut self, root_path: &Path) {
        for root in &mut self.roots {
            if root.root_path == root_path {
                root.state = QuarantineRootState::Online;
                root.reason = None;
            }
        }
    }
}

impl From<&MountEntry> for MountIdentity {
    fn from(entry: &MountEntry) -> Self {
        Self {
            mount_id: entry.mount_id,
            major_minor: entry.major_minor.clone(),
            fs_uuid: None,
            source: entry.source.clone(),
            fstype: entry.fstype.clone(),
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd)]
pub enum RecoveryConfidence {
    None,
    MajorMinorSourceFstype,
    FsUuid,
}

impl QuarantineRoot {
    pub fn recovery_confidence(&self, current: &MountIdentity) -> RecoveryConfidence {
        if let (Some(expected), Some(actual)) = (&self.identity.fs_uuid, &current.fs_uuid) {
            if expected == actual {
                return RecoveryConfidence::FsUuid;
            }
        }

        if self.identity.major_minor == current.major_minor
            && self.identity.source == current.source
            && self.identity.fstype == current.fstype
        {
            return RecoveryConfidence::MajorMinorSourceFstype;
        }

        RecoveryConfidence::None
    }

    pub fn verify_online(&self, current: &MountIdentity) -> Option<RecoveryConfidence> {
        let confidence = self.recovery_confidence(current);
        (confidence != RecoveryConfidence::None).then_some(confidence)
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum RootStateKind {
    OfflineRoot,
    OnlineRoot,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct RootStateRecord {
    pub kind: RootStateKind,
    pub root_path: PathBuf,
    pub identity: MountIdentity,
    pub affected_prefixes: Vec<PathBuf>,
    pub reason: Option<String>,
    pub seq: u64,
    pub timestamp: std::time::SystemTime,
}

impl RootStateRecord {
    pub fn offline(
        seq: u64,
        root_path: PathBuf,
        identity: MountIdentity,
        affected_prefixes: Vec<PathBuf>,
        reason: impl Into<Option<String>>,
    ) -> Self {
        Self {
            kind: RootStateKind::OfflineRoot,
            root_path,
            identity,
            affected_prefixes,
            reason: reason.into(),
            seq,
            timestamp: std::time::SystemTime::now(),
        }
    }

    pub fn online(
        seq: u64,
        root_path: PathBuf,
        identity: MountIdentity,
        affected_prefixes: Vec<PathBuf>,
    ) -> Self {
        Self {
            kind: RootStateKind::OnlineRoot,
            root_path,
            identity,
            affected_prefixes,
            reason: None,
            seq,
            timestamp: std::time::SystemTime::now(),
        }
    }
}

#[derive(Clone, Debug, Default)]
pub struct QuarantineState {
    roots: Vec<QuarantineRoot>,
}

impl QuarantineState {
    pub fn from_wal_records(records: &[RootStateRecord]) -> Self {
        let mut state = Self::default();
        for record in records {
            state.apply_wal_record(record.clone());
        }
        state
    }

    pub fn from_sidecar(sidecar: QuarantineSidecar) -> Self {
        Self {
            roots: sidecar.active_roots().cloned().collect(),
        }
    }

    pub fn apply_wal_records(&mut self, records: &[RootStateRecord]) {
        for record in records {
            self.apply_wal_record(record.clone());
        }
    }

    pub fn apply_wal_record(&mut self, record: RootStateRecord) {
        match record.kind {
            RootStateKind::OfflineRoot => {
                let root = QuarantineRoot {
                    root_path: record.root_path,
                    identity: record.identity,
                    affected_prefixes: record.affected_prefixes,
                    state: QuarantineRootState::Offline,
                    reason: record.reason,
                    last_observed_unix_ns: unix_ns(record.timestamp),
                };
                self.roots
                    .retain(|existing| existing.root_path != root.root_path);
                self.roots.push(root);
                self.roots.sort_by(|a, b| a.root_path.cmp(&b.root_path));
            }
            RootStateKind::OnlineRoot => {
                self.roots
                    .retain(|existing| existing.root_path != record.root_path);
            }
        }
    }

    pub fn active_roots(&self) -> &[QuarantineRoot] {
        self.roots.as_slice()
    }

    pub fn active_root_count(&self) -> usize {
        self.roots.len()
    }

    pub fn freeze_gate(&self) -> FreezeGate {
        let mut gate = FreezeGate::default();
        for root in &self.roots {
            gate.freeze_root(root.root_path.clone());
            for prefix in &root.affected_prefixes {
                gate.freeze_root(prefix.clone());
            }
        }
        gate
    }
}

#[derive(Clone, Debug, Default)]
pub struct FreezeGate {
    frozen_roots: Vec<PathBuf>,
    blocked_events: u64,
}

impl FreezeGate {
    pub fn from_roots(roots: Vec<PathBuf>) -> Self {
        let mut gate = Self::default();
        for root in roots {
            gate.freeze_root(root);
        }
        gate
    }

    pub fn freeze_root(&mut self, root: PathBuf) {
        if root.as_os_str().is_empty() {
            return;
        }
        if !self.frozen_roots.iter().any(|existing| existing == &root) {
            self.frozen_roots.push(root);
            self.frozen_roots.sort();
        }
    }

    pub fn clear_root(&mut self, root: &Path) {
        self.frozen_roots.retain(|existing| existing != root);
    }

    pub fn frozen_root_count(&self) -> usize {
        self.frozen_roots.len()
    }

    pub fn blocked_events(&self) -> u64 {
        self.blocked_events
    }

    pub fn is_path_frozen(&self, path: &Path) -> bool {
        self.frozen_roots.iter().any(|root| path.starts_with(root))
    }

    pub fn should_block_event(&self, event: &EventRecord) -> bool {
        match &event.event_type {
            EventType::Delete | EventType::Modify => event
                .best_path()
                .is_some_and(|path| self.is_path_frozen(path)),
            EventType::Rename {
                from,
                from_path_hint,
            } => {
                let from_best = from_path_hint.as_deref().or_else(|| from.as_path());
                event
                    .best_path()
                    .is_some_and(|path| self.is_path_frozen(path))
                    || from_best.is_some_and(|path| self.is_path_frozen(path))
            }
            EventType::Create => false,
        }
    }

    pub fn note_blocked(&mut self) {
        self.blocked_events = self.blocked_events.saturating_add(1);
    }
}

fn unix_ns(timestamp: std::time::SystemTime) -> u64 {
    timestamp
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_nanos() as u64)
        .unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::FileIdentifier;

    fn identity(
        uuid: Option<&str>,
        major_minor: &str,
        source: &str,
        fstype: &str,
    ) -> MountIdentity {
        MountIdentity {
            mount_id: 42,
            major_minor: major_minor.to_string(),
            fs_uuid: uuid.map(ToString::to_string),
            source: source.to_string(),
            fstype: fstype.to_string(),
        }
    }

    fn event(event_type: EventType, path: &str) -> EventRecord {
        EventRecord {
            seq: 1,
            timestamp: std::time::SystemTime::UNIX_EPOCH,
            event_type,
            id: FileIdentifier::Path(PathBuf::from(path)),
            path_hint: None,
        }
    }

    #[test]
    fn sidecar_roundtrips_physical_mount_identity() {
        let dir =
            std::env::temp_dir().join(format!("fd-rdd-quarantine-sidecar-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        let path = dir.join("quarantine.json");
        let sidecar = QuarantineSidecar {
            version: QUARANTINE_SIDECAR_VERSION,
            roots: vec![QuarantineRoot {
                root_path: PathBuf::from("/mnt/samba"),
                identity: identity(Some("uuid-a"), "8:1", "//srv/share", "cifs"),
                affected_prefixes: vec![PathBuf::from("/mnt/samba/project")],
                state: QuarantineRootState::Offline,
                reason: Some("probe_timeout".to_string()),
                last_observed_unix_ns: 123,
            }],
        };

        sidecar.write_to(&path).unwrap();
        let loaded = QuarantineSidecar::read_from(&path).unwrap();

        assert_eq!(loaded, sidecar);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn recovery_confidence_never_accepts_root_path_alone() {
        let root = QuarantineRoot {
            root_path: PathBuf::from("/mnt/samba"),
            identity: identity(Some("uuid-a"), "8:1", "//srv/share", "cifs"),
            affected_prefixes: vec![],
            state: QuarantineRootState::Offline,
            reason: None,
            last_observed_unix_ns: 0,
        };

        assert_eq!(
            root.recovery_confidence(&identity(Some("uuid-a"), "9:9", "other", "ext4")),
            RecoveryConfidence::FsUuid
        );
        assert_eq!(
            root.recovery_confidence(&identity(None, "8:1", "//srv/share", "cifs")),
            RecoveryConfidence::MajorMinorSourceFstype
        );
        assert_eq!(
            root.recovery_confidence(&identity(None, "9:9", "//srv/share", "cifs")),
            RecoveryConfidence::None
        );
    }

    #[test]
    fn freeze_gate_blocks_destructive_events_under_offline_root() {
        let gate = FreezeGate::from_roots(vec![PathBuf::from("/mnt/offline")]);

        assert!(gate.should_block_event(&event(EventType::Delete, "/mnt/offline/a")));
        assert!(gate.should_block_event(&event(EventType::Modify, "/mnt/offline/a")));
        assert!(!gate.should_block_event(&event(EventType::Create, "/mnt/offline/a")));
        assert!(!gate.should_block_event(&event(EventType::Delete, "/mnt/online/a")));

        let rename = EventRecord {
            seq: 1,
            timestamp: std::time::SystemTime::UNIX_EPOCH,
            event_type: EventType::Rename {
                from: FileIdentifier::Path(PathBuf::from("/mnt/offline/old")),
                from_path_hint: None,
            },
            id: FileIdentifier::Path(PathBuf::from("/tmp/new")),
            path_hint: None,
        };
        assert!(gate.should_block_event(&rename));
    }

    #[test]
    fn online_wal_record_clears_offline_freeze_state() {
        let offline = RootStateRecord::offline(
            1,
            PathBuf::from("/mnt/offline"),
            identity(None, "8:1", "/dev/sda1", "ext4"),
            vec![PathBuf::from("/mnt/offline/project")],
            Some("missing".to_string()),
        );
        let online = RootStateRecord::online(
            2,
            PathBuf::from("/mnt/offline"),
            identity(None, "8:1", "/dev/sda1", "ext4"),
            vec![PathBuf::from("/mnt/offline/project")],
        );

        let state = QuarantineState::from_wal_records(&[offline, online]);

        assert_eq!(state.active_root_count(), 0);
        assert_eq!(state.freeze_gate().frozen_root_count(), 0);
    }
}
