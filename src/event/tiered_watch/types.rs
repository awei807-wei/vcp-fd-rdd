//! Type definitions for the tiered watch subsystem.
//!
//! Contains all public enums and structs that describe watch tiers,
//! freshness, residency, fast-scan configuration, promotion decisions,
//! ephemeral watch configuration, and debug dump shapes.

use std::path::PathBuf;

use serde::{Deserialize, Serialize};

use crate::config::{NetworkFastScanMode, TieredWatchConfig};
use crate::fs_policy::is_remote_fstype;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum WatchTier {
    L0,
    L1,
    L2,
    L3,
}

impl WatchTier {
    pub(super) fn as_u8(self) -> u8 {
        match self {
            Self::L0 => 0,
            Self::L1 => 1,
            Self::L2 => 2,
            Self::L3 => 3,
        }
    }

    pub(super) fn from_u8(value: u8) -> Self {
        match value {
            0 => Self::L0,
            1 => Self::L1,
            2 => Self::L2,
            _ => Self::L3,
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Freshness {
    Fresh,
    Stale,
    Dirty,
    Unknown,
}

impl Freshness {
    pub(super) fn as_u8(self) -> u8 {
        match self {
            Self::Fresh => 0,
            Self::Stale => 1,
            Self::Dirty => 2,
            Self::Unknown => 3,
        }
    }
    pub(super) fn from_u8(v: u8) -> Self {
        match v {
            0 => Self::Fresh,
            1 => Self::Stale,
            2 => Self::Dirty,
            _ => Self::Unknown,
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum IndexResidency {
    HotMemory,
    WarmMemory,
    ColdMmap,
    FrozenManifestOnly,
}

impl IndexResidency {
    pub(super) fn as_u8(self) -> u8 {
        match self {
            Self::HotMemory => 0,
            Self::WarmMemory => 1,
            Self::ColdMmap => 2,
            Self::FrozenManifestOnly => 3,
        }
    }
    pub(super) fn from_u8(v: u8) -> Self {
        match v {
            0 => Self::HotMemory,
            1 => Self::WarmMemory,
            2 => Self::ColdMmap,
            _ => Self::FrozenManifestOnly,
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum FastScanMountClass {
    LocalTrusted,
    NetworkUntrusted,
    FuseUntrusted,
    Unknown,
}

impl FastScanMountClass {
    pub(super) fn strict_sla_allowed(self) -> bool {
        matches!(self, Self::LocalTrusted)
    }
}

pub fn classify_fast_scan_fstype(fstype: &str) -> FastScanMountClass {
    let fstype = fstype.trim();
    if matches!(fstype, "ext4" | "xfs" | "btrfs" | "tmpfs" | "f2fs") {
        FastScanMountClass::LocalTrusted
    } else if matches!(fstype, "nfs" | "nfs4" | "cifs" | "smb3") {
        FastScanMountClass::NetworkUntrusted
    } else if matches!(fstype, "fuse" | "fuseblk" | "sshfs" | "rclone")
        || fstype.starts_with("fuse.")
    {
        FastScanMountClass::FuseUntrusted
    } else if is_remote_fstype(fstype) {
        FastScanMountClass::NetworkUntrusted
    } else {
        FastScanMountClass::Unknown
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum FastScanLeaseKind {
    Explicit,
    Query,
    StaleHit,
    RotatingColdWindow,
    ProjectMarker,
    L0Event,
    ProcSampler,
}

impl FastScanLeaseKind {
    pub(super) fn priority(self) -> u32 {
        match self {
            Self::Explicit => 1_000,
            Self::StaleHit => 900,
            Self::ProcSampler => 850,
            Self::L0Event => 700,
            Self::Query => 650,
            Self::RotatingColdWindow => 625,
            Self::ProjectMarker => 600,
        }
    }

    pub(super) fn is_explicit(self) -> bool {
        self == Self::Explicit
    }
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum FastScanSentinelState {
    Active,
    #[default]
    Unknown,
    BackfillPending,
}

#[derive(Clone, Copy, Debug)]
pub struct FastScanTickConfig {
    pub enabled: bool,
    pub target_secs: u64,
    pub tick_ms: u64,
    pub local_stat_budget_per_tick: usize,
    pub network_stat_budget_per_tick: usize,
    pub local_readdir_budget_per_tick: usize,
    pub network_readdir_budget_per_tick: usize,
    pub initial_backfill_budget_per_tick: usize,
    pub max_hotset_leases: usize,
    pub lease_ttl_secs: u64,
    pub proc_sampler_lease_ttl_secs: u64,
    pub explicit_lease_ttl_secs: u64,
    pub sentinel_registry_max_entries: usize,
    pub network_mode: NetworkFastScanMode,
}

impl Default for FastScanTickConfig {
    fn default() -> Self {
        let defaults = TieredWatchConfig::default();
        Self {
            enabled: defaults.l1_l2_fast_scan_enabled,
            target_secs: defaults.l1_l2_fast_scan_target_secs,
            tick_ms: defaults.l1_l2_fast_scan_tick_ms,
            local_stat_budget_per_tick: defaults.l1_l2_fast_scan_stat_budget_per_tick,
            network_stat_budget_per_tick: defaults.network_fast_scan_stat_budget_per_tick,
            local_readdir_budget_per_tick: defaults.l1_l2_fast_scan_readdir_budget_per_tick,
            network_readdir_budget_per_tick: defaults.network_fast_scan_readdir_budget_per_tick,
            initial_backfill_budget_per_tick: defaults.l1_l2_fast_scan_bootstrap_budget_per_tick,
            max_hotset_leases: defaults.l1_l2_fast_scan_hotset_max_leases,
            lease_ttl_secs: defaults.l1_l2_fast_scan_lease_ttl_secs,
            proc_sampler_lease_ttl_secs: defaults.l1_l2_fast_scan_proc_sampler_lease_ttl_secs,
            explicit_lease_ttl_secs: defaults.l1_l2_fast_scan_explicit_lease_ttl_secs,
            sentinel_registry_max_entries: defaults.l1_l2_fast_scan_sentinel_registry_max_entries,
            network_mode: defaults.network_fast_scan_mode,
        }
    }
}

#[derive(Clone, Debug, Default)]
pub struct FastScanTickResult {
    pub checked_dirs: usize,
    pub initial_dirs: Vec<PathBuf>,
    pub changed_dirs: Vec<PathBuf>,
    pub budget_degraded: bool,
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct FastScanRegistryRestoreReport {
    pub loaded_entries: usize,
    pub restored_active: usize,
    pub restored_unknown: usize,
    pub rejected_entries: usize,
    pub trusted: bool,
    pub reason: String,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum PromotionDecision {
    SendAdd,
    Replace { demote: PathBuf, promote: PathBuf },
    BudgetBlocked,
    NotEligible,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum EphemeralWatchDecision {
    Add(PathBuf),
    Replace { remove: PathBuf, add: PathBuf },
    NotEligible,
    BudgetBlocked,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum EphemeralWatchExpiry {
    Idle,
    Ttl,
    NoChange,
    CoveredByL0,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct EphemeralWatchRemoval {
    pub path: PathBuf,
    pub reason: EphemeralWatchExpiry,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RotatingColdWindowActionKind {
    EphemeralWatch,
    FastScanLease,
    ScanOnly,
}

impl RotatingColdWindowActionKind {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::EphemeralWatch => "ephemeral_watch",
            Self::FastScanLease => "fast_scan_lease",
            Self::ScanOnly => "scan_only",
        }
    }
}

#[derive(Clone, Debug)]
pub struct RotatingColdWindowConfig {
    pub enabled: bool,
    pub budget: usize,
    pub ttl_secs: u64,
    pub max_cost_per_root: usize,
    pub max_dirs_per_tick: usize,
}

impl Default for RotatingColdWindowConfig {
    fn default() -> Self {
        let defaults = TieredWatchConfig::default();
        Self {
            enabled: defaults.rotating_cold_window_enabled,
            budget: defaults.rotating_cold_window_budget,
            ttl_secs: defaults.rotating_cold_window_ttl_secs,
            max_cost_per_root: defaults.rotating_cold_window_max_cost_per_root,
            max_dirs_per_tick: defaults.rotating_cold_window_max_dirs_per_tick,
        }
    }
}

#[derive(Clone, Debug)]
pub struct RotatingColdWindowAction {
    pub path: PathBuf,
    pub action: RotatingColdWindowActionKind,
    pub watch_cost: u64,
    pub score: u64,
    pub expires_unix_secs: u64,
}

#[derive(Clone, Debug, Default)]
pub struct RotatingColdWindowTick {
    pub cycle_id: u64,
    pub actions: Vec<RotatingColdWindowAction>,
    pub budget_blocked: bool,
}

#[derive(Clone, Debug)]
pub struct EphemeralWatchConfig {
    pub budget: usize,
    pub ttl_secs: u64,
    pub idle_secs: u64,
    pub max_cost_per_root: usize,
    pub repeat_window_secs: u64,
    pub repeat_threshold: u32,
    pub no_change_limit: u32,
}

impl Default for EphemeralWatchConfig {
    fn default() -> Self {
        Self {
            budget: 0,
            ttl_secs: 600,
            idle_secs: 120,
            max_cost_per_root: 64,
            repeat_window_secs: 60,
            repeat_threshold: 2,
            no_change_limit: 3,
        }
    }
}

#[derive(Clone, Debug, Default, serde::Serialize)]
pub struct TieredWatchDebugDir {
    pub path: String,
    pub watch_tier: String,
    pub index_tier: String,
    pub watch_cost: u64,
    pub event_score: u64,
    pub last_event: u64,
    pub last_scan: u64,
    pub empty_scan_count: u32,
    pub promotion_pending: bool,
    pub demotion_pending: bool,
    pub dirty: bool,
    pub freshness: String,
    pub next_scan_unix_secs: u64,
    pub budget_blocked_count: u32,
    pub last_budget_blocked_unix_secs: u64,
    pub high_priority_scan: bool,
    pub ephemeral_watch: bool,
    pub rotating_cold_window_seen: bool,
    pub rotating_cold_window: bool,
    pub rotating_cold_window_action: String,
    pub rotating_cold_window_expires_unix_secs: u64,
    pub rotating_cold_window_cycle_id: u64,
    pub rotating_cold_window_score: u64,
    pub rotating_cold_window_last_scan_seq: u64,
    pub rotating_cold_window_last_scan_cycle_id: u64,
    pub rotating_cold_window_last_event_seq: u64,
    pub rotating_cold_window_last_event_cycle_id: u64,
    pub nearest_ancestor_root: Option<String>,
    pub descendant_roots: Vec<String>,
    pub l0_covering_root: Option<String>,
    pub budget_isolated_from_ancestor: bool,
    pub nested_relation: String,
}

#[derive(Clone, Debug, Default, serde::Serialize)]
pub struct TieredWatchDebugSummary {
    pub l0_dirs: usize,
    pub l1_dirs: usize,
    pub l2_dirs: usize,
    pub l3_dirs: usize,
    pub ephemeral_watch_dirs: usize,
    pub ephemeral_watch_cost: u64,
    pub ephemeral_watch_budget: usize,
    pub rotating_cold_window_active_dirs: usize,
    pub rotating_cold_window_cycle_id: u64,
    pub rotating_cold_window_budget: usize,
    pub total_event_score: u64,
}

#[derive(Clone, Debug, Default, serde::Serialize)]
pub struct TieredWatchDebugDump {
    pub dirs: Vec<TieredWatchDebugDir>,
    pub summary: TieredWatchDebugSummary,
}
