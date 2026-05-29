//! Configuration file support for fd-rdd.
//!
//! Priority: CLI args > config file > defaults.
//! Config path: `~/.config/fd-rdd/config.toml`

use serde::{Deserialize, Serialize};
use std::path::{Path, PathBuf};

use crate::util::{default_exclude_dirs, normalize_exclude_dirs};

pub const DEFAULT_L3_SCAN_INTERVAL_SECS: u64 = 21_600;
pub const DEFAULT_TIERED_MAX_WATCH_DIRS: usize = 131_072;

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
    /// WAL durability mode: `flush-only`, `sync-interval`, or `sync-always`.
    pub wal_durability: String,
    /// WAL sync interval in milliseconds when `wal_durability = "sync-interval"`.
    pub wal_sync_interval_ms: u64,
    /// WAL sync batch size when `wal_durability = "sync-interval"`.
    pub wal_sync_batch_records: usize,
    /// Directory names that are never indexed, regardless of .gitignore rules.
    pub exclude_dirs: Vec<String>,
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
    /// Initial hot directory candidates. `~` is expanded during config load.
    pub hot_dirs: Vec<PathBuf>,
    /// Directories that must be covered by L0 when profile = "strict".
    pub strict_required_hot_dirs: Vec<PathBuf>,
    /// Treat strict required coverage shortfall as degraded health instead of warning.
    pub strict_fail_on_budget_exceeded: bool,
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

impl Default for TieredWatchConfig {
    fn default() -> Self {
        Self {
            profile: TieredWatchProfile::Balanced,
            max_watch_dirs: DEFAULT_TIERED_MAX_WATCH_DIRS,
            scan_items_per_sec: 5_000,
            scan_ms_per_tick: 20,
            l0_idle_ttl_secs: 7_200,
            l1_scan_interval_secs: 30,
            l2_scan_interval_secs: 300,
            l3_scan_policy: L3ScanPolicy::Interval,
            l3_scan_interval_secs: DEFAULT_L3_SCAN_INTERVAL_SECS,
            l1_empty_scans_to_l2: 5,
            l2_empty_scans_to_l3: 3,
            ephemeral_watch_budget: 256,
            ephemeral_watch_ttl_secs: 600,
            ephemeral_idle_secs: 120,
            ephemeral_max_cost_per_root: 64,
            hot_dirs: default_hot_dirs(),
            strict_required_hot_dirs: default_hot_dirs(),
            strict_fail_on_budget_exceeded: true,
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
            scan_items_per_sec: usize,
            scan_ms_per_tick: u64,
            l0_idle_ttl_secs: u64,
            l1_scan_interval_secs: u64,
            l2_scan_interval_secs: u64,
            l3_scan_policy: L3ScanPolicy,
            l3_scan_interval_secs: u64,
            l1_empty_scans_to_l2: u32,
            l2_empty_scans_to_l3: u32,
            ephemeral_watch_budget: usize,
            ephemeral_watch_ttl_secs: u64,
            ephemeral_idle_secs: u64,
            ephemeral_max_cost_per_root: usize,
            hot_dirs: Vec<PathBuf>,
            strict_required_hot_dirs: Vec<PathBuf>,
            strict_fail_on_budget_exceeded: bool,
        }

        impl Default for RawTieredWatchConfig {
            fn default() -> Self {
                let defaults = TieredWatchConfig::default();
                Self {
                    profile: defaults.profile,
                    max_watch_dirs: None,
                    scan_items_per_sec: defaults.scan_items_per_sec,
                    scan_ms_per_tick: defaults.scan_ms_per_tick,
                    l0_idle_ttl_secs: defaults.l0_idle_ttl_secs,
                    l1_scan_interval_secs: defaults.l1_scan_interval_secs,
                    l2_scan_interval_secs: defaults.l2_scan_interval_secs,
                    l3_scan_policy: defaults.l3_scan_policy,
                    l3_scan_interval_secs: defaults.l3_scan_interval_secs,
                    l1_empty_scans_to_l2: defaults.l1_empty_scans_to_l2,
                    l2_empty_scans_to_l3: defaults.l2_empty_scans_to_l3,
                    ephemeral_watch_budget: defaults.ephemeral_watch_budget,
                    ephemeral_watch_ttl_secs: defaults.ephemeral_watch_ttl_secs,
                    ephemeral_idle_secs: defaults.ephemeral_idle_secs,
                    ephemeral_max_cost_per_root: defaults.ephemeral_max_cost_per_root,
                    hot_dirs: defaults.hot_dirs,
                    strict_required_hot_dirs: defaults.strict_required_hot_dirs,
                    strict_fail_on_budget_exceeded: defaults.strict_fail_on_budget_exceeded,
                }
            }
        }

        let raw = RawTieredWatchConfig::deserialize(deserializer)?;
        let max_watch_dirs = raw.max_watch_dirs.unwrap_or(DEFAULT_TIERED_MAX_WATCH_DIRS);

        Ok(Self {
            profile: raw.profile,
            max_watch_dirs,
            scan_items_per_sec: raw.scan_items_per_sec,
            scan_ms_per_tick: raw.scan_ms_per_tick,
            l0_idle_ttl_secs: raw.l0_idle_ttl_secs,
            l1_scan_interval_secs: raw.l1_scan_interval_secs,
            l2_scan_interval_secs: raw.l2_scan_interval_secs,
            l3_scan_policy: raw.l3_scan_policy,
            l3_scan_interval_secs: raw.l3_scan_interval_secs,
            l1_empty_scans_to_l2: raw.l1_empty_scans_to_l2,
            l2_empty_scans_to_l3: raw.l2_empty_scans_to_l3,
            ephemeral_watch_budget: raw.ephemeral_watch_budget,
            ephemeral_watch_ttl_secs: raw.ephemeral_watch_ttl_secs,
            ephemeral_idle_secs: raw.ephemeral_idle_secs,
            ephemeral_max_cost_per_root: raw.ephemeral_max_cost_per_root,
            hot_dirs: raw.hot_dirs,
            strict_required_hot_dirs: raw.strict_required_hot_dirs,
            strict_fail_on_budget_exceeded: raw.strict_fail_on_budget_exceeded,
        })
    }
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
            ignore_enabled: true,
            log_level: "info".to_string(),
            http_port: 6060,
            snapshot_interval_secs: 300,
            include_hidden: false,
            follow_symlinks: false,
            watch_enabled: true,
            watch_mode: WatchMode::Recursive,
            tiered_watch: TieredWatchConfig::default(),
            stable_snapshot_enabled: true,
            startup_repair_enabled: true,
            startup_repair_mode: "dirty-only".to_string(),
            startup_repair_max_dirs: 16,
            startup_repair_budget_ms: 10_000,
            startup_repair_force_rebuild_ratio: 0.25,
            wal_durability: "flush-only".to_string(),
            wal_sync_interval_ms: 1000,
            wal_sync_batch_records: 1024,
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
        let value: toml::Value = toml::from_str(&text)?;
        let has_exclude_dirs = value.get("exclude_dirs").is_some();
        let mut cfg: Config = toml::from_str(&text)?;
        cfg.exclude_dirs = normalize_exclude_dirs(cfg.exclude_dirs);
        if !has_exclude_dirs {
            append_missing_exclude_dirs(path, &text, &cfg.exclude_dirs)?;
        }
        cfg.roots = cfg.roots.into_iter().map(expand_tilde_path).collect();
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
        let text = toml::to_string_pretty(self)?;
        std::fs::write(&path, text)?;
        Ok(())
    }
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
    let mut text = existing_text.to_string();
    if !text.is_empty() && !text.ends_with('\n') {
        text.push('\n');
    }
    if !text.is_empty() {
        text.push('\n');
    }
    text.push_str(
        "# fd-rdd default index-time directory exclusions. Edit this list to customize.\n",
    );
    text.push_str(&toml::to_string_pretty(&ExcludeDirsPatch { exclude_dirs })?);
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
        assert_eq!(cfg.tiered_watch.ephemeral_watch_budget, 256);
        assert_eq!(cfg.tiered_watch.ephemeral_watch_ttl_secs, 600);
        assert_eq!(cfg.tiered_watch.ephemeral_idle_secs, 120);
        assert_eq!(cfg.tiered_watch.ephemeral_max_cost_per_root, 64);
        assert_eq!(cfg.tiered_watch.profile, TieredWatchProfile::Balanced);
        assert_eq!(cfg.tiered_watch.l3_scan_policy, L3ScanPolicy::Interval);
        assert_eq!(
            cfg.tiered_watch.l3_scan_interval_secs,
            DEFAULT_L3_SCAN_INTERVAL_SECS
        );

        let toml = toml::to_string_pretty(&Config::default()).expect("serialize default config");
        assert!(toml.contains("l3_scan_policy"));
        assert!(toml.contains("profile"));
        assert!(toml.contains("strict_required_hot_dirs"));
        assert!(toml.contains("strict_fail_on_budget_exceeded"));
        assert!(toml.contains("l3_scan_interval_secs"));
        assert!(toml.contains("ephemeral_watch_budget"));
        assert!(toml.contains("ephemeral_watch_ttl_secs"));
        assert!(toml.contains("ephemeral_idle_secs"));
        assert!(toml.contains("ephemeral_max_cost_per_root"));
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
strict_required_hot_dirs = ["~/Documents"]
strict_fail_on_budget_exceeded = false
"#,
        )
        .expect("strict overrides should parse");

        assert_eq!(cfg.tiered_watch.profile, TieredWatchProfile::Strict);
        assert_eq!(cfg.tiered_watch.max_watch_dirs, 32);
        assert_eq!(
            cfg.tiered_watch.strict_required_hot_dirs,
            vec![PathBuf::from("~/Documents")]
        );
        assert!(!cfg.tiered_watch.strict_fail_on_budget_exceeded);
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
}
