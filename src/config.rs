//! Configuration file support for fd-rdd.
//!
//! Priority: CLI args > config file > defaults.
//! Config path: `~/.config/fd-rdd/config.toml`

use serde::Deserialize;
use std::path::PathBuf;

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
        let _ = std::fs::create_dir_all(&dir);
        return dir.join("fd-rdd.sock");
    }

    #[cfg(target_os = "linux")]
    {
        if let Ok(runtime_dir) = std::env::var("XDG_RUNTIME_DIR") {
            let dir = PathBuf::from(runtime_dir).join("fd-rdd");
            let _ = std::fs::create_dir_all(&dir);
            return dir.join("fd-rdd.sock");
        }

        let uid = unsafe { libc::getuid() };
        let run_user_dir = PathBuf::from(format!("/run/user/{}", uid));
        if run_user_dir.is_dir() {
            let dir = run_user_dir.join("fd-rdd");
            let _ = std::fs::create_dir_all(&dir);
            return dir.join("fd-rdd.sock");
        }

        return PathBuf::from(format!("/tmp/fd-rdd-{}.sock", uid));
    }

    #[cfg(target_os = "windows")]
    {
        let username = std::env::var("USERNAME").unwrap_or_else(|_| "default".to_string());
        return PathBuf::from(format!(r"\\.\pipe\fd-rdd-{}", username));
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
        let _ = std::fs::create_dir_all(&dir);
        return dir.join("index.db");
    }

    #[cfg(target_os = "linux")]
    {
        if let Ok(runtime_dir) = std::env::var("XDG_RUNTIME_DIR") {
            let dir = PathBuf::from(runtime_dir).join("fd-rdd");
            let _ = std::fs::create_dir_all(&dir);
            return dir.join("index.db");
        }

        let uid = unsafe { libc::getuid() };
        let run_user_dir = PathBuf::from(format!("/run/user/{}", uid));
        if run_user_dir.is_dir() {
            let dir = run_user_dir.join("fd-rdd");
            let _ = std::fs::create_dir_all(&dir);
            return dir.join("index.db");
        }

        let dir = PathBuf::from(format!("/tmp/fd-rdd-{}", uid));
        let _ = std::fs::create_dir_all(&dir);
        return dir.join("index.db");
    }

    #[cfg(target_os = "windows")]
    {
        let dir = dirs::data_local_dir()
            .unwrap_or_else(std::env::temp_dir)
            .join("fd-rdd");
        let _ = std::fs::create_dir_all(&dir);
        return dir.join("index.db");
    }

    #[cfg(not(any(target_os = "macos", target_os = "linux", target_os = "windows")))]
    {
        let dir = std::env::temp_dir().join("fd-rdd");
        let _ = std::fs::create_dir_all(&dir);
        dir.join("index.db")
    }
}

/// Top-level configuration loaded from `~/.config/fd-rdd/config.toml`.
#[derive(Debug, Clone, Deserialize)]
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
        if !path.exists() {
            return Ok(Self::default());
        }
        let text = std::fs::read_to_string(&path)?;
        let cfg: Config = toml::from_str(&text)?;
        Ok(cfg)
    }
}
