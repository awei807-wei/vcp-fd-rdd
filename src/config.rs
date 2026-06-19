//! Configuration file support for fd-rdd.
//!
//! Priority: CLI args > config file > defaults.
//! Config path: `~/.config/fd-rdd/config.toml`

use serde::{Deserialize, Serialize};
use std::path::{Path, PathBuf};

use crate::event::proc_sampler::ProcSamplerConfig;
use crate::fs_policy::FsPolicyConfig;
use crate::io_governor::IoGovernorConfig;
use crate::util::{default_exclude_dirs, normalize_exclude_dirs};

pub const DEFAULT_L3_SCAN_INTERVAL_SECS: u64 = 21_600;
pub const DEFAULT_TIERED_MAX_WATCH_DIRS: usize = 131_072;
pub const DEFAULT_TIERED_L0_MAX_COST_PER_ROOT: usize = 8_192;

/// Returns the platform-appropriate default socket path (user-isolated).
///
/// - Linux: `$XDG_RUNTIME_DIR/fd-rdd/fd-rdd.sock`
///   fallback: `/run/user/$UID/fd-rdd/fd-rdd.sock`
///   fallback: `/tmp/fd-rdd-$UID.sock`
/// - macOS: `$TMPDIR/fd-rdd/fd-rdd.sock` (TMPDIR is already per-user)
/// - Windows: `\\.\pipe\fd-rdd-{username}`
pub fn default_socket_path() -> PathBuf {
    #[cfg(target_os = "macos")]
    {
        let dir = PathBuf::from(std::env::var("TMPDIR").unwrap_or_else(|_| "/tmp".to_string()))
            .join("fd-rdd");
        if let Err(e) = std::fs::create_dir_all(&dir) {
            tracing::warn!("Failed to create socket dir {}: {e}", dir.display());
        }
        return dir.join("fd-rdd.sock");
    }

    #[cfg(target_os = "linux")]
    {
        if let Ok(runtime_dir) = std::env::var("XDG_RUNTIME_DIR") {
            let dir = PathBuf::from(runtime_dir).join("fd-rdd");
            if let Err(e) = std::fs::create_dir_all(&dir) {
                tracing::warn!("Failed to create socket dir {}: {e}", dir.display());
            }
            return dir.join("fd-rdd.sock");
        }

        // SAFETY: libc::getuid() is a simple syscall that returns the real user ID.
        // It has no failure mode and requires no preconditions.
        let uid = unsafe { libc::getuid() };
        let run_user_dir = PathBuf::from(format!("/run/user/{}", uid));
        if run_user_dir.is_dir() {
            let dir = run_user_dir.join("fd-rdd");
            if let Err(e) = std::fs::create_dir_all(&dir) {
                tracing::warn!("Failed to create socket dir {}: {e}", dir.display());
            }
            return dir.join("fd-rdd.sock");
        }

        PathBuf::from(format!("/tmp/fd-rdd-{}.sock", uid))
    }

    #[cfg(target_os = "windows")]
    {
        let username = std::env::var("USERNAME").unwrap_or_else(|_| "default".to_string());
        PathBuf::from(format!(r"\\.\pipe\fd-rdd-{}", username))
    }

    #[cfg(not(any(target_os = "macos", target_os = "linux", target_os = "windows")))]
    {
        PathBuf::from("/tmp/fd-rdd.sock")
    }
}

/// Returns the platform-appropriate default snapshot path (user-isolated).
///
/// - Linux: `$XDG_RUNTIME_DIR/fd-rdd/index.db`
///   fallback: `/run/user/$UID/fd-rdd/index.db`
///   fallback: `/tmp/fd-rdd-$UID/index.db`
/// - macOS: `$TMPDIR/fd-rdd/index.db`
/// - Windows: `%LOCALAPPDATA%/fd-rdd/index.db`
pub fn default_snapshot_path() -> PathBuf {
    #[cfg(target_os = "macos")]
    {
        let dir = PathBuf::from(std::env::var("TMPDIR").unwrap_or_else(|_| "/tmp".to_string()))
            .join("fd-rdd");
        if let Err(e) = std::fs::create_dir_all(&dir) {
            tracing::warn!("Failed to create snapshot dir {}: {e}", dir.display());
        }
        return dir.join("index.db");
    }

    #[cfg(target_os = "linux")]
    {
        if let Ok(runtime_dir) = std::env::var("XDG_RUNTIME_DIR") {
            let dir = PathBuf::from(runtime_dir).join("fd-rdd");
            if let Err(e) = std::fs::create_dir_all(&dir) {
                tracing::warn!("Failed to create snapshot dir {}: {e}", dir.display());
            }
            return dir.join("index.db");
        }

        // SAFETY: libc::getuid() is a simple syscall that returns the real user ID.
        // It has no failure mode and requires no preconditions.
        let uid = unsafe { libc::getuid() };
        let run_user_dir = PathBuf::from(format!("/run/user/{}", uid));
        if run_user_dir.is_dir() {
            let dir = run_user_dir.join("fd-rdd");
            if let Err(e) = std::fs::create_dir_all(&dir) {
                tracing::warn!("Failed to create snapshot dir {}: {e}", dir.display());
            }
            return dir.join("index.db");
        }

        let dir = PathBuf::from(format!("/tmp/fd-rdd-{}", uid));
        if let Err(e) = std::fs::create_dir_all(&dir) {
            tracing::warn!("Failed to create snapshot dir {}: {e}", dir.display());
        }
        dir.join("index.db")
    }

    #[cfg(target_os = "windows")]
    {
        let dir = dirs::data_local_dir()
            .unwrap_or_else(std::env::temp_dir)
            .join("fd-rdd");
        if let Err(e) = std::fs::create_dir_all(&dir) {
            tracing::warn!("Failed to create snapshot dir {}: {e}", dir.display());
        }
        dir.join("index.db")
    }

    #[cfg(not(any(target_os = "macos", target_os = "linux", target_os = "windows")))]
    {
        let dir = std::env::temp_dir().join("fd-rdd");
        if let Err(e) = std::fs::create_dir_all(&dir) {
            tracing::warn!("Failed to create snapshot dir {}: {e}", dir.display());
        }
        dir.join("index.db")
    }
}

/// Top-level configuration loaded from `~/.config/fd-rdd/config.toml`.
#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(default)]
pub struct Config {
    /// UDS socket path override.
    pub socket_path: Option<PathBuf>,
    /// Index root directories.
    pub roots: Vec<PathBuf>,
    /// Structured per-root user intent. Runtime detected state is intentionally not persisted here.
    #[serde(skip)]
    pub root_configs: Vec<RootConfig>,
    /// Whether .gitignore / .ignore rules are applied during scan.
    pub ignore_enabled: bool,
    /// Log level (e.g. "info", "debug", "trace").
    pub log_level: String,
    /// HTTP query port.
    pub http_port: u16,
    /// Snapshot write interval in seconds.
    pub snapshot_interval_secs: u64,
    /// Include hidden (dot) files.
    pub include_hidden: bool,
    /// Follow symlinks during scan and watch.
    pub follow_symlinks: bool,
    /// Enable filesystem watcher for incremental updates.
    pub watch_enabled: bool,
    /// Watcher operating mode. `watch_enabled = false` is treated as `off` for legacy configs.
    pub watch_mode: WatchMode,
    /// Budgeted tiered watcher configuration.
    pub tiered_watch: TieredWatchConfig,
    /// Linux `/proc/<pid>/fd` write handle sampler. It only feeds freshness hints.
    pub proc_sampler: ProcSamplerConfig,
    /// Enable stable v7 snapshot rotation (`stable.v7` / `stable.prev.v7`).
    pub stable_snapshot_enabled: bool,
    /// Enable startup repair when previous shutdown or WAL replay is untrusted.
    pub startup_repair_enabled: bool,
    /// Startup repair mode: `dirty-only`, `always`, or `never`.
    pub startup_repair_mode: String,
    /// Maximum roots repaired during startup.
    pub startup_repair_max_dirs: usize,
    /// Soft startup repair budget in milliseconds. First phase records this as policy.
    pub startup_repair_budget_ms: u64,
    /// If repair failure ratio exceeds this value, full rebuild may be scheduled.
    pub startup_repair_force_rebuild_ratio: f32,
    /// Validate cold query hits asynchronously instead of running stat on the query thread.
    pub lazy_validation_enabled: bool,
    /// LRU-style cache capacity for lazy validation enqueue dedupe.
    pub lazy_validation_cache_entries: usize,
    /// Lazy validation cache TTL in seconds.
    pub lazy_validation_ttl_secs: u64,
    /// Global stat rate limit for lazy validation worker.
    pub lazy_validation_stat_per_sec: u64,
    /// Query-time synchronous verification budget.
    pub query: QueryConfig,
    /// Runtime resource profile. `memory_light` lowers hot-memory residency at higher I/O cost.
    pub runtime_profile: RuntimeProfile,
    /// WAL durability mode: `flush-only`, `sync-interval`, or `sync-always`.
    pub wal_durability: String,
    /// WAL sync interval in milliseconds when `wal_durability = "sync-interval"`.
    pub wal_sync_interval_ms: u64,
    /// WAL sync batch size when `wal_durability = "sync-interval"`.
    pub wal_sync_batch_records: usize,
    /// Background scan I/O governor.
    pub io_governor: IoGovernorConfig,
    /// Optional mmap warmup for cold v7 snapshots. Disabled by default.
    pub mmap_warmup: MmapWarmupConfig,
    /// Optional lightweight content index. Disabled by default to keep filename queries unaffected.
    pub content_index: ContentIndexConfig,
    /// Filesystem boundary policy for mount traversal.
    pub fs_policy: FsPolicyConfig,
    /// Directory names that are never indexed, regardless of .gitignore rules.
    pub exclude_dirs: Vec<String>,
}

#[derive(Debug, Clone, Copy, Default, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "PascalCase")]
pub enum RootCasePolicy {
    Sensitive,
    Insensitive,
    #[default]
    Auto,
    Unknown,
}

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq)]
#[serde(default)]
pub struct RootConfig {
    pub path: PathBuf,
    pub case_policy: RootCasePolicy,
    pub allow_remote: bool,
    pub one_file_system: bool,
}

impl Default for RootConfig {
    fn default() -> Self {
        Self {
            path: PathBuf::new(),
            case_policy: RootCasePolicy::Auto,
            allow_remote: false,
            one_file_system: true,
        }
    }
}

impl RootConfig {
    pub fn from_path(path: PathBuf) -> Self {
        Self {
            path,
            ..Self::default()
        }
    }
}

#[derive(Debug, Clone, Copy, Deserialize, Serialize, PartialEq, Eq)]
#[serde(default)]
pub struct QueryConfig {
    /// Maximum cold/base candidates synchronously verified by one query.
    pub max_verify_per_query: usize,
    /// Maximum synchronous verification wall time per query.
    pub verify_timeout_ms: u64,
}

impl QueryConfig {
    /// Validates cross-field constraints. Returns `Err` with a description on failure.
    pub fn validate(&self) -> Result<(), String> {
        if self.max_verify_per_query < 1 {
            return Err(format!(
                "max_verify_per_query must be >= 1, got {}",
                self.max_verify_per_query
            ));
        }
        if self.verify_timeout_ms < 1 {
            return Err(format!(
                "verify_timeout_ms must be >= 1, got {}",
                self.verify_timeout_ms
            ));
        }
        Ok(())
    }
}

impl Default for QueryConfig {
    fn default() -> Self {
        Self {
            max_verify_per_query: 150,
            verify_timeout_ms: 75,
        }
    }
}

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq)]
#[serde(default)]
pub struct ContentIndexConfig {
    /// Enable background content indexing. Disabled by default.
    pub enable: bool,
    /// Maximum file size eligible for content indexing.
    pub max_file_size: u64,
    /// Extension allow-list. Empty means no files are indexed until configured.
    pub include_ext: Vec<String>,
    /// Extension deny-list applied after include_ext.
    pub exclude_ext: Vec<String>,
}

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq)]
#[serde(default)]
pub struct MmapWarmupConfig {
    /// Enable best-effort MADV_WILLNEED for cold v7 mmap snapshots.
    pub enable: bool,
    /// Maximum mmap bytes to warm in one pass. 0 means no limit.
    pub max_bytes: u64,
}

impl Default for MmapWarmupConfig {
    fn default() -> Self {
        Self {
            enable: false,
            max_bytes: 64 * 1024 * 1024,
        }
    }
}

impl Default for ContentIndexConfig {
    fn default() -> Self {
        Self {
            enable: false,
            max_file_size: 1024 * 1024,
            include_ext: Vec::new(),
            exclude_ext: Vec::new(),
        }
    }
}

#[derive(Debug, Clone, Copy, Default, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "kebab-case")]
pub enum WatchMode {
    #[default]
    Recursive,
    Tiered,
    Off,
}

#[derive(Debug, Clone, Serialize)]
pub struct TieredWatchConfig {
    /// Consistency/cost profile for tiered watcher behavior.
    pub profile: TieredWatchProfile,
    /// Upper bound for estimated recursive inotify watches admitted into L0.
    pub max_watch_dirs: usize,
    /// Upper bound for one recursive L0 root. 0 disables the per-root guard.
    pub l0_max_cost_per_root: usize,
    /// Token budget for L1/L2 scan work. First tiered implementation uses this as diagnostics.
    pub scan_items_per_sec: usize,
    /// Maximum scan wall time per scheduler tick.
    pub scan_ms_per_tick: u64,
    /// L0 idle TTL before demotion is considered.
    pub l0_idle_ttl_secs: u64,
    /// Warm verification interval.
    pub l1_scan_interval_secs: u64,
    /// Cold verification interval.
    pub l2_scan_interval_secs: u64,
    /// L3 scan behavior: periodic interval, validate-on-query only, or disabled.
    pub l3_scan_policy: L3ScanPolicy,
    /// Low-frequency L3 verification interval used when l3_scan_policy = "interval".
    pub l3_scan_interval_secs: u64,
    /// Prototype: rotate a bounded freshness window over L2/L3 without changing formal tiers.
    pub rotating_cold_window_enabled: bool,
    /// Maximum concurrently active rotating cold-window leases.
    pub rotating_cold_window_budget: usize,
    /// Scheduler tick interval for the rotating cold-window prototype.
    pub rotating_cold_window_tick_secs: u64,
    /// TTL for one rotating cold-window coverage lease.
    pub rotating_cold_window_ttl_secs: u64,
    /// Maximum estimated recursive watcher cost eligible for ephemeral watch coverage.
    pub rotating_cold_window_max_cost_per_root: usize,
    /// Maximum cold directories selected by one rotating cold-window tick.
    pub rotating_cold_window_max_dirs_per_tick: usize,
    /// Enable the lease-hotset fast scan lane for directories outside L0.
    pub l1_l2_fast_scan_enabled: bool,
    /// Target coverage window for local trusted lease-hotset directories.
    pub l1_l2_fast_scan_target_secs: u64,
    /// Fast scan scheduler tick interval.
    pub l1_l2_fast_scan_tick_ms: u64,
    /// Local trusted hotset sentinel checks allowed per tick.
    pub l1_l2_fast_scan_stat_budget_per_tick: usize,
    /// Local trusted changed-dir readdir work allowed per tick.
    pub l1_l2_fast_scan_readdir_budget_per_tick: usize,
    /// Hotset sentinel registrations/backfill directories allowed per tick.
    pub l1_l2_fast_scan_bootstrap_budget_per_tick: usize,
    /// Maximum active lease-hotset directories tracked by fast scan.
    pub l1_l2_fast_scan_hotset_max_leases: usize,
    /// Default TTL for automatic fast scan leases.
    pub l1_l2_fast_scan_lease_ttl_secs: u64,
    /// TTL for proc-sampler fast scan leases.
    pub l1_l2_fast_scan_proc_sampler_lease_ttl_secs: u64,
    /// TTL for explicit hot_dirs fast scan leases. 0 means permanent while configured.
    pub l1_l2_fast_scan_explicit_lease_ttl_secs: u64,
    /// Maximum hotset sentinel entries restored or kept active.
    pub l1_l2_fast_scan_sentinel_registry_max_entries: usize,
    /// Network/FUSE fast scan mode. Default best-effort never reports strict SLA success.
    pub network_fast_scan_mode: NetworkFastScanMode,
    /// Network/FUSE sentinel checks allowed per tick.
    pub network_fast_scan_stat_budget_per_tick: usize,
    /// Network/FUSE strict_poll changed-dir readdir work allowed per tick.
    pub network_fast_scan_readdir_budget_per_tick: usize,
    /// Empty L1 scans before demotion to L2.
    pub l1_empty_scans_to_l2: u32,
    /// Empty L2 scans before demotion to L3.
    pub l2_empty_scans_to_l3: u32,
    /// Independent budget for temporary watcher leases. These do not count as L0/L1/L2/L3.
    pub ephemeral_watch_budget: usize,
    /// Hard lifetime for a temporary watcher lease.
    pub ephemeral_watch_ttl_secs: u64,
    /// Idle lifetime for a temporary watcher lease since the last matching event.
    pub ephemeral_idle_secs: u64,
    /// Maximum estimated recursive watch cost allowed for one temporary root.
    pub ephemeral_max_cost_per_root: usize,
    /// Project marker names used by balanced tiered watcher project-root detection.
    pub project_markers: Vec<String>,
    /// Initial hot directory candidates. `~` is expanded during config load.
    pub hot_dirs: Vec<PathBuf>,
    /// TTL in seconds for runtime subtree tombstones (default 300).
    pub runtime_subtree_tombstone_ttl_secs: u64,
    /// Directories that must be covered by L0 when profile = "strict".
    pub strict_required_hot_dirs: Vec<PathBuf>,
    /// Treat strict required coverage shortfall as degraded health instead of warning.
    pub strict_fail_on_budget_exceeded: bool,
    // ── Waterline alarm (adaptive L3 scan degradation) ──────────────────────
    /// Master switch for the waterline alarm backpressure mechanism.
    pub waterline_alarm_enabled: bool,
    /// Fast-scan SLA in milliseconds used as the soft-degradation baseline.
    pub waterline_sla_ms: u64,
    /// Soft trigger threshold as a fraction of the SLA (default 0.8 = 80%).
    pub waterline_soft_trigger_pct: f64,
    /// Soft recovery threshold as a fraction of the SLA (default 0.4 = 40%).
    pub waterline_soft_recover_pct: f64,
    /// Hard trigger threshold as a fraction of the L2 scan interval (default 0.8).
    pub waterline_hard_trigger_pct: f64,
    /// Hard recovery threshold as a fraction of the L2 scan interval (default 0.4).
    pub waterline_hard_recover_pct: f64,
    /// L3 scan interval (seconds) forced when hard degradation is active.
    pub waterline_hard_degraded_l3_interval_secs: u64,
    /// Fraction by which the rotating cold-window budget is reduced during soft
    /// degradation (default 0.5 = reduce to 50% of configured budget).
    pub waterline_soft_budget_reduction_pct: f64,
}

#[derive(Debug, Clone, Copy, Default, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum TieredWatchProfile {
    Strict,
    #[default]
    Balanced,
    LowPower,
}

#[derive(Debug, Clone, Copy, Default, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum L3ScanPolicy {
    #[default]
    Interval,
    #[serde(alias = "validate-on-query")]
    ValidateOnQuery,
    Disabled,
}

#[derive(Debug, Clone, Copy, Default, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum NetworkFastScanMode {
    #[default]
    BestEffort,
    StrictPoll,
    Disabled,
}

#[derive(Debug, Clone, Copy, Default, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum RuntimeProfile {
    #[default]
    Default,
    MemoryLight,
}

impl RuntimeProfile {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Default => "default",
            Self::MemoryLight => "memory_light",
        }
    }

    pub fn settings(self) -> RuntimeProfileSettings {
        match self {
            Self::Default => RuntimeProfileSettings::default(),
            Self::MemoryLight => RuntimeProfileSettings {
                auto_flush_overlay_paths: 50_000,
                auto_flush_overlay_bytes: 16 * 1024 * 1024,
                periodic_flush_min_events: 1_024,
                periodic_flush_min_bytes: 1024 * 1024,
                periodic_flush_max_staleness_secs: 30,
                rebuild_cooldown_secs: 15,
                wal_seal_bytes: 16 * 1024 * 1024,
            },
        }
    }
}

impl std::str::FromStr for RuntimeProfile {
    type Err = String;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value {
            "default" => Ok(Self::Default),
            "memory_light" | "memory-light" => Ok(Self::MemoryLight),
            other => Err(format!(
                "unsupported runtime profile {other:?}; expected default or memory_light"
            )),
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct RuntimeProfileSettings {
    pub auto_flush_overlay_paths: u64,
    pub auto_flush_overlay_bytes: u64,
    pub periodic_flush_min_events: u64,
    pub periodic_flush_min_bytes: u64,
    pub periodic_flush_max_staleness_secs: u64,
    pub rebuild_cooldown_secs: u64,
    pub wal_seal_bytes: u64,
}

impl Default for RuntimeProfileSettings {
    fn default() -> Self {
        Self {
            auto_flush_overlay_paths: 250_000,
            auto_flush_overlay_bytes: 64 * 1024 * 1024,
            periodic_flush_min_events: 4_096,
            periodic_flush_min_bytes: 4 * 1024 * 1024,
            periodic_flush_max_staleness_secs: 0,
            rebuild_cooldown_secs: 60,
            wal_seal_bytes: 0,
        }
    }
}

impl L3ScanPolicy {
    pub fn schedules_periodic_scan(self) -> bool {
        matches!(self, Self::Interval)
    }
}

impl std::str::FromStr for L3ScanPolicy {
    type Err = String;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value {
            "interval" => Ok(Self::Interval),
            "validate_on_query" | "validate-on-query" => Ok(Self::ValidateOnQuery),
            "disabled" => Ok(Self::Disabled),
            other => Err(format!(
                "unsupported L3 scan policy {other:?}; expected interval, validate_on_query, or disabled"
            )),
        }
    }
}

impl TieredWatchConfig {
    /// Validates cross-field constraints. Returns `Err` with a description on failure.
    pub fn validate(&self) -> Result<(), String> {
        if self.l1_l2_fast_scan_tick_ms < 100 {
            return Err(format!(
                "l1_l2_fast_scan_tick_ms must be >= 100, got {}",
                self.l1_l2_fast_scan_tick_ms
            ));
        }
        if self.l1_l2_fast_scan_stat_budget_per_tick < 1 {
            return Err("l1_l2_fast_scan_stat_budget_per_tick must be >= 1".to_string());
        }
        if self.rotating_cold_window_enabled {
            if self.rotating_cold_window_budget < 1 {
                return Err("rotating_cold_window_budget must be >= 1".to_string());
            }
            if self.rotating_cold_window_tick_secs < 1 {
                return Err("rotating_cold_window_tick_secs must be >= 1".to_string());
            }
            if self.rotating_cold_window_ttl_secs < 1 {
                return Err("rotating_cold_window_ttl_secs must be >= 1".to_string());
            }
            if self.rotating_cold_window_max_cost_per_root < 1 {
                return Err("rotating_cold_window_max_cost_per_root must be >= 1".to_string());
            }
            if self.rotating_cold_window_max_dirs_per_tick < 1 {
                return Err("rotating_cold_window_max_dirs_per_tick must be >= 1".to_string());
            }
        }
        // Waterline alarm validation.
        if self.waterline_alarm_enabled {
            if self.waterline_sla_ms == 0 {
                return Err("waterline_sla_ms must be > 0".to_string());
            }
            if self.waterline_hard_degraded_l3_interval_secs == 0 {
                return Err("waterline_hard_degraded_l3_interval_secs must be > 0".to_string());
            }
            for (name, val) in [
                (
                    "waterline_soft_trigger_pct",
                    self.waterline_soft_trigger_pct,
                ),
                (
                    "waterline_soft_recover_pct",
                    self.waterline_soft_recover_pct,
                ),
                (
                    "waterline_hard_trigger_pct",
                    self.waterline_hard_trigger_pct,
                ),
                (
                    "waterline_hard_recover_pct",
                    self.waterline_hard_recover_pct,
                ),
                (
                    "waterline_soft_budget_reduction_pct",
                    self.waterline_soft_budget_reduction_pct,
                ),
            ] {
                if !(0.0..=1.0).contains(&val) {
                    return Err(format!("{name} must be in [0.0, 1.0], got {val}"));
                }
            }
        }
        Ok(())
    }
}

impl Default for TieredWatchConfig {
    fn default() -> Self {
        Self {
            profile: TieredWatchProfile::Balanced,
            max_watch_dirs: DEFAULT_TIERED_MAX_WATCH_DIRS,
            l0_max_cost_per_root: DEFAULT_TIERED_L0_MAX_COST_PER_ROOT,
            scan_items_per_sec: 5_000,
            scan_ms_per_tick: 20,
            l0_idle_ttl_secs: 7_200,
            l1_scan_interval_secs: 30,
            l2_scan_interval_secs: 300,
            l3_scan_policy: L3ScanPolicy::Interval,
            l3_scan_interval_secs: DEFAULT_L3_SCAN_INTERVAL_SECS,
            rotating_cold_window_enabled: false,
            rotating_cold_window_budget: 128,
            rotating_cold_window_tick_secs: 30,
            rotating_cold_window_ttl_secs: 180,
            rotating_cold_window_max_cost_per_root: 64,
            rotating_cold_window_max_dirs_per_tick: 8,
            l1_l2_fast_scan_enabled: true,
            l1_l2_fast_scan_target_secs: 5,
            l1_l2_fast_scan_tick_ms: 1_000,
            l1_l2_fast_scan_stat_budget_per_tick: 5_000,
            l1_l2_fast_scan_readdir_budget_per_tick: 512,
            l1_l2_fast_scan_bootstrap_budget_per_tick: 2_048,
            l1_l2_fast_scan_hotset_max_leases: 512,
            l1_l2_fast_scan_lease_ttl_secs: 1_800,
            l1_l2_fast_scan_proc_sampler_lease_ttl_secs: 300,
            l1_l2_fast_scan_explicit_lease_ttl_secs: 0,
            l1_l2_fast_scan_sentinel_registry_max_entries: 512,
            network_fast_scan_mode: NetworkFastScanMode::BestEffort,
            network_fast_scan_stat_budget_per_tick: 128,
            network_fast_scan_readdir_budget_per_tick: 16,
            l1_empty_scans_to_l2: 5,
            l2_empty_scans_to_l3: 3,
            ephemeral_watch_budget: 256,
            ephemeral_watch_ttl_secs: 600,
            ephemeral_idle_secs: 120,
            ephemeral_max_cost_per_root: 64,
            runtime_subtree_tombstone_ttl_secs: 300,
            project_markers: default_project_markers(),
            hot_dirs: default_hot_dirs(),
            strict_required_hot_dirs: default_hot_dirs(),
            strict_fail_on_budget_exceeded: true,
            waterline_alarm_enabled: true,
            waterline_sla_ms: 5_000,
            waterline_soft_trigger_pct: 0.8,
            waterline_soft_recover_pct: 0.4,
            waterline_hard_trigger_pct: 0.8,
            waterline_hard_recover_pct: 0.4,
            waterline_hard_degraded_l3_interval_secs: 86_400,
            waterline_soft_budget_reduction_pct: 0.5,
        }
    }
}

impl<'de> Deserialize<'de> for TieredWatchConfig {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        #[derive(Deserialize)]
        #[serde(default)]
        struct RawTieredWatchConfig {
            profile: TieredWatchProfile,
            max_watch_dirs: Option<usize>,
            l0_max_cost_per_root: usize,
            scan_items_per_sec: usize,
            scan_ms_per_tick: u64,
            l0_idle_ttl_secs: u64,
            l1_scan_interval_secs: u64,
            l2_scan_interval_secs: u64,
            l3_scan_policy: L3ScanPolicy,
            l3_scan_interval_secs: u64,
            rotating_cold_window_enabled: bool,
            rotating_cold_window_budget: usize,
            rotating_cold_window_tick_secs: u64,
            rotating_cold_window_ttl_secs: u64,
            rotating_cold_window_max_cost_per_root: usize,
            rotating_cold_window_max_dirs_per_tick: usize,
            l1_l2_fast_scan_enabled: bool,
            l1_l2_fast_scan_target_secs: u64,
            l1_l2_fast_scan_tick_ms: Option<u64>,
            l1_l2_fast_scan_stat_budget_per_tick: Option<usize>,
            l1_l2_fast_scan_readdir_budget_per_tick: usize,
            l1_l2_fast_scan_bootstrap_budget_per_tick: usize,
            l1_l2_fast_scan_hotset_max_leases: usize,
            l1_l2_fast_scan_lease_ttl_secs: u64,
            l1_l2_fast_scan_proc_sampler_lease_ttl_secs: u64,
            l1_l2_fast_scan_explicit_lease_ttl_secs: u64,
            l1_l2_fast_scan_sentinel_registry_max_entries: usize,
            network_fast_scan_mode: NetworkFastScanMode,
            network_fast_scan_stat_budget_per_tick: usize,
            network_fast_scan_readdir_budget_per_tick: usize,
            l1_empty_scans_to_l2: u32,
            l2_empty_scans_to_l3: u32,
            ephemeral_watch_budget: usize,
            ephemeral_watch_ttl_secs: u64,
            ephemeral_idle_secs: u64,
            ephemeral_max_cost_per_root: usize,
            runtime_subtree_tombstone_ttl_secs: u64,
            project_markers: Vec<String>,
            hot_dirs: Vec<PathBuf>,
            strict_required_hot_dirs: Vec<PathBuf>,
            strict_fail_on_budget_exceeded: bool,
            waterline_alarm_enabled: bool,
            waterline_sla_ms: u64,
            waterline_soft_trigger_pct: f64,
            waterline_soft_recover_pct: f64,
            waterline_hard_trigger_pct: f64,
            waterline_hard_recover_pct: f64,
            waterline_hard_degraded_l3_interval_secs: u64,
            waterline_soft_budget_reduction_pct: f64,
        }

        impl Default for RawTieredWatchConfig {
            fn default() -> Self {
                let defaults = TieredWatchConfig::default();
                Self {
                    profile: defaults.profile,
                    max_watch_dirs: None,
                    l0_max_cost_per_root: defaults.l0_max_cost_per_root,
                    scan_items_per_sec: defaults.scan_items_per_sec,
                    scan_ms_per_tick: defaults.scan_ms_per_tick,
                    l0_idle_ttl_secs: defaults.l0_idle_ttl_secs,
                    l1_scan_interval_secs: defaults.l1_scan_interval_secs,
                    l2_scan_interval_secs: defaults.l2_scan_interval_secs,
                    l3_scan_policy: defaults.l3_scan_policy,
                    l3_scan_interval_secs: defaults.l3_scan_interval_secs,
                    rotating_cold_window_enabled: defaults.rotating_cold_window_enabled,
                    rotating_cold_window_budget: defaults.rotating_cold_window_budget,
                    rotating_cold_window_tick_secs: defaults.rotating_cold_window_tick_secs,
                    rotating_cold_window_ttl_secs: defaults.rotating_cold_window_ttl_secs,
                    rotating_cold_window_max_cost_per_root: defaults
                        .rotating_cold_window_max_cost_per_root,
                    rotating_cold_window_max_dirs_per_tick: defaults
                        .rotating_cold_window_max_dirs_per_tick,
                    l1_l2_fast_scan_enabled: defaults.l1_l2_fast_scan_enabled,
                    l1_l2_fast_scan_target_secs: defaults.l1_l2_fast_scan_target_secs,
                    l1_l2_fast_scan_tick_ms: None,
                    l1_l2_fast_scan_stat_budget_per_tick: None,
                    l1_l2_fast_scan_readdir_budget_per_tick: defaults
                        .l1_l2_fast_scan_readdir_budget_per_tick,
                    l1_l2_fast_scan_bootstrap_budget_per_tick: defaults
                        .l1_l2_fast_scan_bootstrap_budget_per_tick,
                    l1_l2_fast_scan_hotset_max_leases: defaults.l1_l2_fast_scan_hotset_max_leases,
                    l1_l2_fast_scan_lease_ttl_secs: defaults.l1_l2_fast_scan_lease_ttl_secs,
                    l1_l2_fast_scan_proc_sampler_lease_ttl_secs: defaults
                        .l1_l2_fast_scan_proc_sampler_lease_ttl_secs,
                    l1_l2_fast_scan_explicit_lease_ttl_secs: defaults
                        .l1_l2_fast_scan_explicit_lease_ttl_secs,
                    l1_l2_fast_scan_sentinel_registry_max_entries: defaults
                        .l1_l2_fast_scan_sentinel_registry_max_entries,
                    network_fast_scan_mode: defaults.network_fast_scan_mode,
                    network_fast_scan_stat_budget_per_tick: defaults
                        .network_fast_scan_stat_budget_per_tick,
                    network_fast_scan_readdir_budget_per_tick: defaults
                        .network_fast_scan_readdir_budget_per_tick,
                    l1_empty_scans_to_l2: defaults.l1_empty_scans_to_l2,
                    l2_empty_scans_to_l3: defaults.l2_empty_scans_to_l3,
                    ephemeral_watch_budget: defaults.ephemeral_watch_budget,
                    ephemeral_watch_ttl_secs: defaults.ephemeral_watch_ttl_secs,
                    ephemeral_idle_secs: defaults.ephemeral_idle_secs,
                    ephemeral_max_cost_per_root: defaults.ephemeral_max_cost_per_root,
                    runtime_subtree_tombstone_ttl_secs: defaults.runtime_subtree_tombstone_ttl_secs,
                    project_markers: defaults.project_markers,
                    hot_dirs: defaults.hot_dirs,
                    strict_required_hot_dirs: defaults.strict_required_hot_dirs,
                    strict_fail_on_budget_exceeded: defaults.strict_fail_on_budget_exceeded,
                    waterline_alarm_enabled: defaults.waterline_alarm_enabled,
                    waterline_sla_ms: defaults.waterline_sla_ms,
                    waterline_soft_trigger_pct: defaults.waterline_soft_trigger_pct,
                    waterline_soft_recover_pct: defaults.waterline_soft_recover_pct,
                    waterline_hard_trigger_pct: defaults.waterline_hard_trigger_pct,
                    waterline_hard_recover_pct: defaults.waterline_hard_recover_pct,
                    waterline_hard_degraded_l3_interval_secs: defaults
                        .waterline_hard_degraded_l3_interval_secs,
                    waterline_soft_budget_reduction_pct: defaults
                        .waterline_soft_budget_reduction_pct,
                }
            }
        }

        let raw = RawTieredWatchConfig::deserialize(deserializer)?;
        let max_watch_dirs = raw.max_watch_dirs.unwrap_or(DEFAULT_TIERED_MAX_WATCH_DIRS);
        let defaults = TieredWatchConfig::default();

        // Profile-specific defaults: LowPower tunes fast scan parameters down
        // when the user hasn't explicitly set them.
        let (default_tick_ms, default_stat_budget) = match raw.profile {
            TieredWatchProfile::LowPower => (2_000, 1_000),
            _ => (
                defaults.l1_l2_fast_scan_tick_ms,
                defaults.l1_l2_fast_scan_stat_budget_per_tick,
            ),
        };
        let l1_l2_fast_scan_tick_ms = raw.l1_l2_fast_scan_tick_ms.unwrap_or(default_tick_ms);
        let l1_l2_fast_scan_stat_budget_per_tick = raw
            .l1_l2_fast_scan_stat_budget_per_tick
            .unwrap_or(default_stat_budget);

        Ok(Self {
            profile: raw.profile,
            max_watch_dirs,
            l0_max_cost_per_root: raw.l0_max_cost_per_root,
            scan_items_per_sec: raw.scan_items_per_sec,
            scan_ms_per_tick: raw.scan_ms_per_tick,
            l0_idle_ttl_secs: raw.l0_idle_ttl_secs,
            l1_scan_interval_secs: raw.l1_scan_interval_secs,
            l2_scan_interval_secs: raw.l2_scan_interval_secs,
            l3_scan_policy: raw.l3_scan_policy,
            l3_scan_interval_secs: raw.l3_scan_interval_secs,
            rotating_cold_window_enabled: raw.rotating_cold_window_enabled,
            rotating_cold_window_budget: raw.rotating_cold_window_budget,
            rotating_cold_window_tick_secs: raw.rotating_cold_window_tick_secs,
            rotating_cold_window_ttl_secs: raw.rotating_cold_window_ttl_secs,
            rotating_cold_window_max_cost_per_root: raw.rotating_cold_window_max_cost_per_root,
            rotating_cold_window_max_dirs_per_tick: raw.rotating_cold_window_max_dirs_per_tick,
            l1_l2_fast_scan_enabled: raw.l1_l2_fast_scan_enabled,
            l1_l2_fast_scan_target_secs: raw.l1_l2_fast_scan_target_secs,
            l1_l2_fast_scan_tick_ms,
            l1_l2_fast_scan_stat_budget_per_tick,
            l1_l2_fast_scan_readdir_budget_per_tick: raw.l1_l2_fast_scan_readdir_budget_per_tick,
            l1_l2_fast_scan_bootstrap_budget_per_tick: raw
                .l1_l2_fast_scan_bootstrap_budget_per_tick,
            l1_l2_fast_scan_hotset_max_leases: raw.l1_l2_fast_scan_hotset_max_leases,
            l1_l2_fast_scan_lease_ttl_secs: raw.l1_l2_fast_scan_lease_ttl_secs,
            l1_l2_fast_scan_proc_sampler_lease_ttl_secs: raw
                .l1_l2_fast_scan_proc_sampler_lease_ttl_secs,
            l1_l2_fast_scan_explicit_lease_ttl_secs: raw.l1_l2_fast_scan_explicit_lease_ttl_secs,
            l1_l2_fast_scan_sentinel_registry_max_entries: raw
                .l1_l2_fast_scan_sentinel_registry_max_entries,
            network_fast_scan_mode: raw.network_fast_scan_mode,
            network_fast_scan_stat_budget_per_tick: raw.network_fast_scan_stat_budget_per_tick,
            network_fast_scan_readdir_budget_per_tick: raw
                .network_fast_scan_readdir_budget_per_tick,
            l1_empty_scans_to_l2: raw.l1_empty_scans_to_l2,
            l2_empty_scans_to_l3: raw.l2_empty_scans_to_l3,
            ephemeral_watch_budget: raw.ephemeral_watch_budget,
            ephemeral_watch_ttl_secs: raw.ephemeral_watch_ttl_secs,
            ephemeral_idle_secs: raw.ephemeral_idle_secs,
            ephemeral_max_cost_per_root: raw.ephemeral_max_cost_per_root,
            runtime_subtree_tombstone_ttl_secs: raw.runtime_subtree_tombstone_ttl_secs,
            project_markers: raw.project_markers,
            hot_dirs: raw.hot_dirs,
            strict_required_hot_dirs: raw.strict_required_hot_dirs,
            strict_fail_on_budget_exceeded: raw.strict_fail_on_budget_exceeded,
            waterline_alarm_enabled: raw.waterline_alarm_enabled,
            waterline_sla_ms: raw.waterline_sla_ms,
            waterline_soft_trigger_pct: raw.waterline_soft_trigger_pct,
            waterline_soft_recover_pct: raw.waterline_soft_recover_pct,
            waterline_hard_trigger_pct: raw.waterline_hard_trigger_pct,
            waterline_hard_recover_pct: raw.waterline_hard_recover_pct,
            waterline_hard_degraded_l3_interval_secs: raw.waterline_hard_degraded_l3_interval_secs,
            waterline_soft_budget_reduction_pct: raw.waterline_soft_budget_reduction_pct,
        })
    }
}

fn default_project_markers() -> Vec<String> {
    [
        ".git",
        "Cargo.toml",
        "package.json",
        "pnpm-workspace.yaml",
        "go.mod",
        "pyproject.toml",
        "requirements.txt",
        "deno.json",
        "Makefile",
    ]
    .into_iter()
    .map(str::to_string)
    .collect()
}

fn default_hot_dirs() -> Vec<PathBuf> {
    [
        "~/Downloads",
        "~/Documents",
        "~/Desktop",
        "~/Music",
        "~/Pictures",
        "~/Videos",
    ]
    .into_iter()
    .map(PathBuf::from)
    .collect()
}

impl Default for Config {
    fn default() -> Self {
        Self {
            socket_path: None,
            roots: Vec::new(),
            root_configs: Vec::new(),
            ignore_enabled: true,
            log_level: "info".to_string(),
            http_port: 6060,
            snapshot_interval_secs: 300,
            include_hidden: false,
            follow_symlinks: false,
            watch_enabled: true,
            watch_mode: WatchMode::Recursive,
            tiered_watch: TieredWatchConfig::default(),
            proc_sampler: ProcSamplerConfig::default(),
            stable_snapshot_enabled: true,
            startup_repair_enabled: true,
            startup_repair_mode: "dirty-only".to_string(),
            startup_repair_max_dirs: 16,
            startup_repair_budget_ms: 10_000,
            startup_repair_force_rebuild_ratio: 0.25,
            lazy_validation_enabled: false,
            lazy_validation_cache_entries: 4096,
            lazy_validation_ttl_secs: 10,
            lazy_validation_stat_per_sec: 50,
            query: QueryConfig::default(),
            runtime_profile: RuntimeProfile::Default,
            wal_durability: "flush-only".to_string(),
            wal_sync_interval_ms: 1000,
            wal_sync_batch_records: 1024,
            io_governor: IoGovernorConfig::default(),
            mmap_warmup: MmapWarmupConfig::default(),
            content_index: ContentIndexConfig::default(),
            fs_policy: FsPolicyConfig::default(),
            exclude_dirs: default_exclude_dirs(),
        }
    }
}

impl Config {
    /// Standard config file location: `~/.config/fd-rdd/config.toml`.
    pub fn config_path() -> Option<PathBuf> {
        dirs::config_dir().map(|d| d.join("fd-rdd").join("config.toml"))
    }

    /// Load config from the default path. Returns `Config::default()` if the file
    /// does not exist. Returns an error only on parse failures.
    pub fn load() -> anyhow::Result<Self> {
        let Some(path) = Self::config_path() else {
            return Ok(Self::default());
        };
        Self::load_from_path(&path)
    }

    fn load_from_path(path: &Path) -> anyhow::Result<Self> {
        if !path.exists() {
            return Ok(Self::default());
        }
        let text = std::fs::read_to_string(path)?;
        let mut value: toml::Value = toml::from_str(&text)?;
        let has_exclude_dirs = value.get("exclude_dirs").is_some();
        let root_configs = extract_root_configs(&mut value)?;
        let mut cfg: Config = value.try_into()?;
        cfg.root_configs = root_configs;
        cfg.exclude_dirs = normalize_exclude_dirs(cfg.exclude_dirs);
        if !has_exclude_dirs {
            append_missing_exclude_dirs(path, &text, &cfg.exclude_dirs)?;
        }
        cfg.roots = cfg.roots.into_iter().map(expand_tilde_path).collect();
        if cfg.root_configs.is_empty() {
            cfg.root_configs = cfg
                .roots
                .iter()
                .cloned()
                .map(RootConfig::from_path)
                .collect();
        } else {
            for root in &mut cfg.root_configs {
                root.path = expand_tilde_path(std::mem::take(&mut root.path));
            }
        }
        cfg.tiered_watch.hot_dirs = cfg
            .tiered_watch
            .hot_dirs
            .into_iter()
            .map(expand_tilde_path)
            .collect();
        cfg.tiered_watch.strict_required_hot_dirs = cfg
            .tiered_watch
            .strict_required_hot_dirs
            .into_iter()
            .map(expand_tilde_path)
            .collect();
        if let Some(socket) = cfg.socket_path.take() {
            cfg.socket_path = Some(expand_tilde_path(socket));
        }
        Ok(cfg)
    }

    /// Save config to the default path (`~/.config/fd-rdd/config.toml`).
    /// Creates parent directories if needed.
    pub fn save(&self) -> anyhow::Result<()> {
        let Some(path) = Self::config_path() else {
            anyhow::bail!("Could not determine config directory");
        };
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        let text = self.to_toml_string()?;
        std::fs::write(&path, text)?;
        Ok(())
    }

    pub fn to_toml_string(&self) -> anyhow::Result<String> {
        let mut value = toml::Value::try_from(self)?;
        let roots = if self.root_configs.is_empty() {
            self.roots
                .iter()
                .cloned()
                .map(RootConfig::from_path)
                .collect::<Vec<_>>()
        } else {
            self.root_configs.clone()
        };
        value
            .as_table_mut()
            .expect("Config serializes as a TOML table")
            .insert("roots".to_string(), toml::Value::try_from(roots)?);
        Ok(toml::to_string_pretty(&value)?)
    }
}

fn extract_root_configs(value: &mut toml::Value) -> anyhow::Result<Vec<RootConfig>> {
    let Some(roots_value) = value.get_mut("roots") else {
        return Ok(Vec::new());
    };

    let Some(items) = roots_value.as_array() else {
        anyhow::bail!("config roots must be an array");
    };

    if items.iter().all(|item| item.as_str().is_some()) {
        let paths = items
            .iter()
            .filter_map(|item| item.as_str())
            .map(|path| RootConfig::from_path(PathBuf::from(path)))
            .collect::<Vec<_>>();
        return Ok(paths);
    }

    if items.iter().all(|item| item.as_table().is_some()) {
        let configs: Vec<RootConfig> = items
            .iter()
            .cloned()
            .map(|item| item.try_into())
            .collect::<Result<Vec<_>, _>>()?;
        let legacy_paths = configs
            .iter()
            .map(|root| toml::Value::String(root.path.to_string_lossy().into_owned()))
            .collect::<Vec<_>>();
        *roots_value = toml::Value::Array(legacy_paths);
        return Ok(configs);
    }

    anyhow::bail!("config roots must be either [\"/path\"] or [[roots]] objects");
}

#[derive(Serialize)]
struct ExcludeDirsPatch<'a> {
    exclude_dirs: &'a [String],
}

fn append_missing_exclude_dirs(
    path: &Path,
    existing_text: &str,
    exclude_dirs: &[String],
) -> anyhow::Result<()> {
    let patch = format!(
        "# fd-rdd default index-time directory exclusions. Edit this list to customize.\n{}",
        toml::to_string_pretty(&ExcludeDirsPatch { exclude_dirs })?
    );
    if existing_text.contains("[[roots]]") {
        let mut text = patch;
        if !text.ends_with('\n') {
            text.push('\n');
        }
        text.push('\n');
        text.push_str(existing_text);
        std::fs::write(path, text)?;
        return Ok(());
    }

    let mut text = existing_text.to_string();
    if !text.is_empty() && !text.ends_with('\n') {
        text.push('\n');
    }
    if !text.is_empty() {
        text.push('\n');
    }
    text.push_str(&patch);
    std::fs::write(path, text)?;
    Ok(())
}

fn expand_tilde_path(path: PathBuf) -> PathBuf {
    let Some(s) = path.to_str() else {
        return path;
    };
    if s == "~" {
        return dirs::home_dir().unwrap_or(path);
    }
    if let Some(rest) = s.strip_prefix("~/") {
        if let Some(home) = dirs::home_dir() {
            return home.join(rest);
        }
    }
    path
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn missing_exclude_dirs_uses_default_exclusions_and_persists_them() {
        let root = std::env::temp_dir().join(format!("fd-rdd-config-test-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&root);
        std::fs::create_dir_all(&root).expect("create temp dir");
        let path = root.join("config.toml");
        std::fs::write(
            &path,
            r#"
roots = ["~"]
http_port = 6060
"#,
        )
        .expect("write config");

        let cfg = Config::load_from_path(&path).expect("config should parse");

        assert!(cfg.exclude_dirs.contains(&"node_modules".to_string()));
        assert!(cfg.exclude_dirs.contains(&"target".to_string()));
        assert!(cfg.exclude_dirs.contains(&".git".to_string()));

        let persisted = std::fs::read_to_string(&path).expect("read persisted config");
        assert!(persisted.contains("exclude_dirs"));
        assert!(persisted.contains("node_modules"));

        let _ = std::fs::remove_dir_all(root);
    }

    #[test]
    fn explicit_exclude_dirs_are_normalized_after_load_step() {
        let mut cfg: Config = toml::from_str(
            r#"
roots = ["~"]
exclude_dirs = ["node_modules", "/target/", "", "node_modules"]
"#,
        )
        .expect("config should parse");

        cfg.exclude_dirs = normalize_exclude_dirs(cfg.exclude_dirs);

        assert_eq!(
            cfg.exclude_dirs,
            vec!["node_modules".to_string(), "target".to_string()]
        );
    }

    #[test]
    fn explicit_exclude_dirs_are_not_replaced_by_defaults() {
        let root = std::env::temp_dir().join(format!(
            "fd-rdd-config-explicit-test-{}",
            std::process::id()
        ));
        let _ = std::fs::remove_dir_all(&root);
        std::fs::create_dir_all(&root).expect("create temp dir");
        let path = root.join("config.toml");
        std::fs::write(
            &path,
            r#"
roots = ["~"]
exclude_dirs = ["custom_cache"]
"#,
        )
        .expect("write config");

        let cfg = Config::load_from_path(&path).expect("config should parse");

        assert_eq!(cfg.exclude_dirs, vec!["custom_cache".to_string()]);
        let persisted = std::fs::read_to_string(&path).expect("read persisted config");
        assert!(!persisted.contains("node_modules"));

        let _ = std::fs::remove_dir_all(root);
    }

    #[test]
    fn tiered_watch_defaults_include_ephemeral_lease_controls() {
        let cfg: Config = toml::from_str(
            r#"
roots = ["~"]
watch_mode = "tiered"

[tiered_watch]
max_watch_dirs = 16
"#,
        )
        .expect("config should parse with partial tiered_watch table");

        assert_eq!(cfg.tiered_watch.max_watch_dirs, 16);
        assert_eq!(
            cfg.tiered_watch.l0_max_cost_per_root,
            DEFAULT_TIERED_L0_MAX_COST_PER_ROOT
        );
        assert_eq!(cfg.tiered_watch.ephemeral_watch_budget, 256);
        assert_eq!(cfg.tiered_watch.ephemeral_watch_ttl_secs, 600);
        assert_eq!(cfg.tiered_watch.ephemeral_idle_secs, 120);
        assert_eq!(cfg.tiered_watch.ephemeral_max_cost_per_root, 64);
        assert!(!cfg.tiered_watch.rotating_cold_window_enabled);
        assert_eq!(cfg.tiered_watch.rotating_cold_window_budget, 128);
        assert_eq!(cfg.tiered_watch.rotating_cold_window_tick_secs, 30);
        assert_eq!(cfg.tiered_watch.rotating_cold_window_ttl_secs, 180);
        assert_eq!(cfg.tiered_watch.rotating_cold_window_max_cost_per_root, 64);
        assert_eq!(cfg.tiered_watch.rotating_cold_window_max_dirs_per_tick, 8);
        assert!(cfg.tiered_watch.l1_l2_fast_scan_enabled);
        assert_eq!(cfg.tiered_watch.l1_l2_fast_scan_target_secs, 5);
        assert_eq!(cfg.tiered_watch.l1_l2_fast_scan_tick_ms, 1_000);
        assert_eq!(
            cfg.tiered_watch.network_fast_scan_mode,
            NetworkFastScanMode::BestEffort
        );
        assert!(cfg
            .tiered_watch
            .project_markers
            .contains(&"Cargo.toml".to_string()));
        assert_eq!(cfg.tiered_watch.profile, TieredWatchProfile::Balanced);
        assert_eq!(cfg.tiered_watch.l3_scan_policy, L3ScanPolicy::Interval);
        assert_eq!(
            cfg.tiered_watch.l3_scan_interval_secs,
            DEFAULT_L3_SCAN_INTERVAL_SECS
        );

        let toml = toml::to_string_pretty(&Config::default()).expect("serialize default config");
        assert!(toml.contains("l3_scan_policy"));
        assert!(toml.contains("profile"));
        assert!(toml.contains("l0_max_cost_per_root"));
        assert!(toml.contains("strict_required_hot_dirs"));
        assert!(toml.contains("strict_fail_on_budget_exceeded"));
        assert!(toml.contains("l3_scan_interval_secs"));
        assert!(toml.contains("ephemeral_watch_budget"));
        assert!(toml.contains("ephemeral_watch_ttl_secs"));
        assert!(toml.contains("ephemeral_idle_secs"));
        assert!(toml.contains("ephemeral_max_cost_per_root"));
        assert!(toml.contains("rotating_cold_window_enabled"));
        assert!(toml.contains("rotating_cold_window_budget"));
        assert!(toml.contains("rotating_cold_window_tick_secs"));
        assert!(toml.contains("rotating_cold_window_ttl_secs"));
        assert!(toml.contains("rotating_cold_window_max_cost_per_root"));
        assert!(toml.contains("rotating_cold_window_max_dirs_per_tick"));
        assert!(toml.contains("l1_l2_fast_scan_enabled"));
        assert!(toml.contains("l1_l2_fast_scan_target_secs"));
        assert!(toml.contains("network_fast_scan_mode"));
        assert!(toml.contains("project_markers"));
    }

    #[test]
    fn tiered_watch_rotating_cold_window_overrides_parse() {
        let cfg: Config = toml::from_str(
            r#"
roots = ["~"]
watch_mode = "tiered"

[tiered_watch]
rotating_cold_window_enabled = true
rotating_cold_window_budget = 9
rotating_cold_window_tick_secs = 2
rotating_cold_window_ttl_secs = 11
rotating_cold_window_max_cost_per_root = 7
rotating_cold_window_max_dirs_per_tick = 3
"#,
        )
        .expect("rotating cold-window config should parse");

        assert!(cfg.tiered_watch.rotating_cold_window_enabled);
        assert_eq!(cfg.tiered_watch.rotating_cold_window_budget, 9);
        assert_eq!(cfg.tiered_watch.rotating_cold_window_tick_secs, 2);
        assert_eq!(cfg.tiered_watch.rotating_cold_window_ttl_secs, 11);
        assert_eq!(cfg.tiered_watch.rotating_cold_window_max_cost_per_root, 7);
        assert_eq!(cfg.tiered_watch.rotating_cold_window_max_dirs_per_tick, 3);
    }

    #[test]
    fn tiered_watch_waterline_alarm_defaults() {
        let cfg = Config::default();
        assert!(cfg.tiered_watch.waterline_alarm_enabled);
        assert_eq!(cfg.tiered_watch.waterline_sla_ms, 5_000);
        assert_eq!(cfg.tiered_watch.waterline_soft_trigger_pct, 0.8);
        assert_eq!(cfg.tiered_watch.waterline_soft_recover_pct, 0.4);
        assert_eq!(cfg.tiered_watch.waterline_hard_trigger_pct, 0.8);
        assert_eq!(cfg.tiered_watch.waterline_hard_recover_pct, 0.4);
        assert_eq!(
            cfg.tiered_watch.waterline_hard_degraded_l3_interval_secs,
            86_400
        );
        assert_eq!(cfg.tiered_watch.waterline_soft_budget_reduction_pct, 0.5);
    }

    #[test]
    fn tiered_watch_waterline_alarm_overrides_parse() {
        let cfg: Config = toml::from_str(
            r#"
roots = ["~"]
watch_mode = "tiered"

[tiered_watch]
waterline_alarm_enabled = false
waterline_sla_ms = 3000
waterline_soft_trigger_pct = 0.9
waterline_soft_recover_pct = 0.3
waterline_hard_trigger_pct = 0.7
waterline_hard_recover_pct = 0.2
waterline_hard_degraded_l3_interval_secs = 43200
waterline_soft_budget_reduction_pct = 0.25
"#,
        )
        .expect("waterline alarm config should parse");

        assert!(!cfg.tiered_watch.waterline_alarm_enabled);
        assert_eq!(cfg.tiered_watch.waterline_sla_ms, 3000);
        assert_eq!(cfg.tiered_watch.waterline_soft_trigger_pct, 0.9);
        assert_eq!(cfg.tiered_watch.waterline_soft_recover_pct, 0.3);
        assert_eq!(cfg.tiered_watch.waterline_hard_trigger_pct, 0.7);
        assert_eq!(cfg.tiered_watch.waterline_hard_recover_pct, 0.2);
        assert_eq!(
            cfg.tiered_watch.waterline_hard_degraded_l3_interval_secs,
            43200
        );
        assert_eq!(cfg.tiered_watch.waterline_soft_budget_reduction_pct, 0.25);
    }

    #[test]
    fn tiered_watch_waterline_alarm_validation_rejects_invalid_pct() {
        let mut cfg = Config::default();
        cfg.tiered_watch.waterline_soft_trigger_pct = 1.5;
        assert!(cfg.tiered_watch.validate().is_err());

        cfg.tiered_watch.waterline_soft_trigger_pct = 0.8;
        cfg.tiered_watch.waterline_sla_ms = 0;
        assert!(cfg.tiered_watch.validate().is_err());
    }

    #[test]
    fn tiered_watch_fast_scan_overrides_parse() {
        let cfg: Config = toml::from_str(
            r#"
roots = ["~"]
watch_mode = "tiered"

[tiered_watch]
l1_l2_fast_scan_enabled = false
l1_l2_fast_scan_target_secs = 7
l1_l2_fast_scan_tick_ms = 250
l1_l2_fast_scan_stat_budget_per_tick = 111
l1_l2_fast_scan_readdir_budget_per_tick = 22
l1_l2_fast_scan_bootstrap_budget_per_tick = 333
l1_l2_fast_scan_hotset_max_leases = 77
l1_l2_fast_scan_lease_ttl_secs = 88
l1_l2_fast_scan_proc_sampler_lease_ttl_secs = 99
l1_l2_fast_scan_explicit_lease_ttl_secs = 11
l1_l2_fast_scan_sentinel_registry_max_entries = 55
network_fast_scan_mode = "strict_poll"
network_fast_scan_stat_budget_per_tick = 44
network_fast_scan_readdir_budget_per_tick = 5
"#,
        )
        .expect("fast scan config should parse");

        assert!(!cfg.tiered_watch.l1_l2_fast_scan_enabled);
        assert_eq!(cfg.tiered_watch.l1_l2_fast_scan_target_secs, 7);
        assert_eq!(cfg.tiered_watch.l1_l2_fast_scan_tick_ms, 250);
        assert_eq!(cfg.tiered_watch.l1_l2_fast_scan_stat_budget_per_tick, 111);
        assert_eq!(cfg.tiered_watch.l1_l2_fast_scan_readdir_budget_per_tick, 22);
        assert_eq!(
            cfg.tiered_watch.l1_l2_fast_scan_bootstrap_budget_per_tick,
            333
        );
        assert_eq!(cfg.tiered_watch.l1_l2_fast_scan_hotset_max_leases, 77);
        assert_eq!(cfg.tiered_watch.l1_l2_fast_scan_lease_ttl_secs, 88);
        assert_eq!(
            cfg.tiered_watch.l1_l2_fast_scan_proc_sampler_lease_ttl_secs,
            99
        );
        assert_eq!(cfg.tiered_watch.l1_l2_fast_scan_explicit_lease_ttl_secs, 11);
        assert_eq!(
            cfg.tiered_watch
                .l1_l2_fast_scan_sentinel_registry_max_entries,
            55
        );
        assert_eq!(
            cfg.tiered_watch.network_fast_scan_mode,
            NetworkFastScanMode::StrictPoll
        );
        assert_eq!(cfg.tiered_watch.network_fast_scan_stat_budget_per_tick, 44);
        assert_eq!(
            cfg.tiered_watch.network_fast_scan_readdir_budget_per_tick,
            5
        );
    }

    #[test]
    fn runtime_profile_defaults_and_accepts_memory_light() {
        let default_cfg: Config = toml::from_str(
            r#"
roots = ["~"]
"#,
        )
        .expect("config should parse without runtime profile");

        assert_eq!(default_cfg.runtime_profile, RuntimeProfile::Default);

        let memory_light: Config = toml::from_str(
            r#"
roots = ["~"]
runtime_profile = "memory_light"
"#,
        )
        .expect("memory_light runtime profile should parse");

        assert_eq!(memory_light.runtime_profile, RuntimeProfile::MemoryLight);
        assert_eq!(RuntimeProfile::MemoryLight.as_str(), "memory_light");

        let toml = toml::to_string_pretty(&Config::default()).expect("serialize default config");
        assert!(toml.contains("runtime_profile"));
    }

    #[test]
    fn query_verify_defaults_and_overrides_parse() {
        let default_cfg: Config = toml::from_str(
            r#"
roots = ["~"]
"#,
        )
        .expect("config should parse without query table");

        assert_eq!(default_cfg.query.max_verify_per_query, 150);
        assert_eq!(default_cfg.query.verify_timeout_ms, 75);
        assert!(!default_cfg.lazy_validation_enabled);

        let cfg: Config = toml::from_str(
            r#"
roots = ["~"]

[query]
max_verify_per_query = 123
verify_timeout_ms = 60
"#,
        )
        .expect("query config should parse");

        assert_eq!(cfg.query.max_verify_per_query, 123);
        assert_eq!(cfg.query.verify_timeout_ms, 60);

        // Old config files with allow_sync_readdir should still parse (field is ignored).
        let _legacy: Config = toml::from_str(
            r#"
roots = ["~"]

[query]
allow_sync_readdir = true
"#,
        )
        .expect("legacy config with allow_sync_readdir should still parse");

        let toml = toml::to_string_pretty(&Config::default()).expect("serialize default config");
        assert!(toml.contains("[query]"));
        assert!(toml.contains("max_verify_per_query"));
    }

    #[test]
    fn proc_sampler_defaults_and_overrides_parse() {
        let default_cfg: Config = toml::from_str(
            r#"
roots = ["~"]
"#,
        )
        .expect("config should parse without proc_sampler table");

        assert!(default_cfg.proc_sampler.enabled);
        assert_eq!(default_cfg.proc_sampler.interval_ms, 1_000);
        assert_eq!(default_cfg.proc_sampler.max_pids_per_tick, 128);
        assert_eq!(default_cfg.proc_sampler.max_fds_per_pid, 64);
        assert_eq!(default_cfg.proc_sampler.max_dirs_per_tick, 32);

        let cfg: Config = toml::from_str(
            r#"
roots = ["~"]

[proc_sampler]
enabled = false
interval_ms = 250
max_pids_per_tick = 7
max_fds_per_pid = 8
max_dirs_per_tick = 9
"#,
        )
        .expect("proc sampler config should parse");

        assert!(!cfg.proc_sampler.enabled);
        assert_eq!(cfg.proc_sampler.interval_ms, 250);
        assert_eq!(cfg.proc_sampler.max_pids_per_tick, 7);
        assert_eq!(cfg.proc_sampler.max_fds_per_pid, 8);
        assert_eq!(cfg.proc_sampler.max_dirs_per_tick, 9);

        let toml = toml::to_string_pretty(&Config::default()).expect("serialize default config");
        assert!(toml.contains("[proc_sampler]"));
        assert!(toml.contains("max_pids_per_tick"));
        assert!(toml.contains("max_dirs_per_tick"));
    }

    #[test]
    fn content_index_defaults_disabled_and_accepts_policy() {
        let default_cfg: Config = toml::from_str(
            r#"
roots = ["~"]
"#,
        )
        .expect("config should parse without content_index table");

        assert!(!default_cfg.content_index.enable);
        assert_eq!(default_cfg.content_index.max_file_size, 1024 * 1024);
        assert!(default_cfg.content_index.include_ext.is_empty());
        assert!(default_cfg.content_index.exclude_ext.is_empty());

        let cfg: Config = toml::from_str(
            r#"
roots = ["~"]

[content_index]
enable = true
max_file_size = 4096
include_ext = ["txt", "md"]
exclude_ext = ["log"]
"#,
        )
        .expect("content_index config should parse");

        assert!(cfg.content_index.enable);
        assert_eq!(cfg.content_index.max_file_size, 4096);
        assert_eq!(cfg.content_index.include_ext, vec!["txt", "md"]);
        assert_eq!(cfg.content_index.exclude_ext, vec!["log"]);

        let toml = toml::to_string_pretty(&Config::default()).expect("serialize default config");
        assert!(toml.contains("[content_index]"));
        assert!(toml.contains("max_file_size"));
    }

    #[test]
    fn tiered_watch_profile_defaults_to_high_watch_budget() {
        let balanced: Config = toml::from_str(
            r#"
roots = ["~"]
watch_mode = "tiered"

[tiered_watch]
"#,
        )
        .expect("balanced profile should parse");

        assert_eq!(balanced.tiered_watch.profile, TieredWatchProfile::Balanced);
        assert_eq!(
            balanced.tiered_watch.max_watch_dirs,
            DEFAULT_TIERED_MAX_WATCH_DIRS
        );

        let cfg: Config = toml::from_str(
            r#"
roots = ["~"]
watch_mode = "tiered"

[tiered_watch]
profile = "strict"
"#,
        )
        .expect("strict profile should parse");

        assert_eq!(cfg.tiered_watch.profile, TieredWatchProfile::Strict);
        assert_eq!(
            cfg.tiered_watch.max_watch_dirs,
            DEFAULT_TIERED_MAX_WATCH_DIRS
        );
        assert!(cfg.tiered_watch.strict_fail_on_budget_exceeded);
        assert_eq!(
            cfg.tiered_watch.strict_required_hot_dirs,
            super::default_hot_dirs()
        );
        assert_eq!(
            cfg.tiered_watch.project_markers,
            super::default_project_markers()
        );
    }

    #[test]
    fn strict_tiered_watch_profile_accepts_explicit_overrides() {
        let cfg: Config = toml::from_str(
            r#"
roots = ["~"]
watch_mode = "tiered"

[tiered_watch]
profile = "strict"
max_watch_dirs = 32
l0_max_cost_per_root = 8
strict_required_hot_dirs = ["~/Documents"]
strict_fail_on_budget_exceeded = false
project_markers = [".git", "WORKSPACE"]
"#,
        )
        .expect("strict overrides should parse");

        assert_eq!(cfg.tiered_watch.profile, TieredWatchProfile::Strict);
        assert_eq!(cfg.tiered_watch.max_watch_dirs, 32);
        assert_eq!(cfg.tiered_watch.l0_max_cost_per_root, 8);
        assert_eq!(
            cfg.tiered_watch.strict_required_hot_dirs,
            vec![PathBuf::from("~/Documents")]
        );
        assert!(!cfg.tiered_watch.strict_fail_on_budget_exceeded);
        assert_eq!(cfg.tiered_watch.project_markers, vec![".git", "WORKSPACE"]);
    }

    #[test]
    fn tiered_watch_l3_scan_policy_accepts_documented_values() {
        let cfg: Config = toml::from_str(
            r#"
roots = ["~"]
watch_mode = "tiered"

[tiered_watch]
l3_scan_policy = "validate_on_query"
l3_scan_interval_secs = 999
"#,
        )
        .expect("config should parse documented L3 policy");

        assert_eq!(
            cfg.tiered_watch.l3_scan_policy,
            L3ScanPolicy::ValidateOnQuery
        );
        assert_eq!(cfg.tiered_watch.l3_scan_interval_secs, 999);

        let legacy_hyphen: TieredWatchConfig = toml::from_str(
            r#"
l3_scan_policy = "validate-on-query"
"#,
        )
        .expect("hyphenated value should remain accepted for CLI-style configs");

        assert_eq!(legacy_hyphen.l3_scan_policy, L3ScanPolicy::ValidateOnQuery);
    }

    #[test]
    fn load_accepts_structured_roots_and_keeps_runtime_state_out_of_config() {
        let root = std::env::temp_dir().join(format!(
            "fd-rdd-config-structured-roots-{}",
            std::process::id()
        ));
        let _ = std::fs::remove_dir_all(&root);
        std::fs::create_dir_all(&root).expect("create temp dir");
        let path = root.join("config.toml");
        std::fs::write(
            &path,
            r#"
exclude_dirs = ["target"]

[[roots]]
path = "/mnt/samba"
case_policy = "Auto"
allow_remote = false
one_file_system = true
detected_policy = "Insensitive"
conflict_count = 14
"#,
        )
        .expect("write config");

        let cfg = Config::load_from_path(&path).expect("structured roots should parse");

        assert_eq!(cfg.roots, vec![PathBuf::from("/mnt/samba")]);
        assert_eq!(cfg.root_configs.len(), 1);
        assert_eq!(cfg.root_configs[0].case_policy, RootCasePolicy::Auto);
        assert!(!cfg.root_configs[0].allow_remote);
        assert!(cfg.root_configs[0].one_file_system);

        let toml = cfg.to_toml_string().expect("serialize config");
        assert!(toml.contains("[[roots]]"));
        assert!(toml.contains("case_policy = \"Auto\""));
        assert!(!toml.contains("detected_policy"));
        assert!(!toml.contains("conflict_count"));

        let _ = std::fs::remove_dir_all(root);
    }

    #[test]
    fn load_accepts_legacy_roots_array() {
        let root =
            std::env::temp_dir().join(format!("fd-rdd-config-legacy-roots-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&root);
        std::fs::create_dir_all(&root).expect("create temp dir");
        let path = root.join("config.toml");
        std::fs::write(
            &path,
            r#"
roots = ["/tmp"]
exclude_dirs = ["target"]
"#,
        )
        .expect("write config");

        let cfg = Config::load_from_path(&path).expect("legacy roots should parse");

        assert_eq!(cfg.roots, vec![PathBuf::from("/tmp")]);
        assert_eq!(cfg.root_configs[0].path, PathBuf::from("/tmp"));
        assert_eq!(cfg.root_configs[0].case_policy, RootCasePolicy::Auto);

        let _ = std::fs::remove_dir_all(root);
    }
}
