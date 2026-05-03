//! Shared helpers for fd-rdd integration tests.

pub mod fd_rdd_client;
pub mod sys_monitor;

use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use fd_rdd_client::SearchResult;

/// Create a unique temporary directory under the system temp folder.
///
/// Pattern: `{temp_dir}/fd-rdd-{tag}-{nanos_since_epoch}`
pub fn unique_tmp_dir(tag: &str) -> PathBuf {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    std::env::temp_dir().join(format!("fd-rdd-{}-{}", tag, nanos))
}

/// Managed fd-rdd child process.
///
/// Spawns the `fd-rdd` binary built from the same workspace and exposes
/// helpers to query its HTTP endpoints.
pub struct FdRddProcess {
    pub child: Child,
    #[allow(dead_code)]
    pub port: u16,
}

impl FdRddProcess {
    /// Spawn `fd-rdd` with the given root, port and snapshot path.
    ///
    /// Extra CLI arguments can be passed via `extra_args`.
    pub fn spawn(root: &Path, port: u16, snapshot_path: &Path, extra_args: &[&str]) -> Self {
        let exe = fd_rdd_exe_path();
        let mut cmd = Command::new(&exe);
        cmd.arg("--root")
            .arg(root)
            .arg("--http-port")
            .arg(port.to_string())
            .arg("--snapshot-path")
            .arg(snapshot_path)
            .args(extra_args)
            .stdout(Stdio::null())
            .stderr(Stdio::null());

        let child = cmd
            .spawn()
            .unwrap_or_else(|e| panic!("failed to spawn fd-rdd from {}: {}", exe.display(), e));

        // Give the server a moment to bind.
        std::thread::sleep(Duration::from_millis(500));

        Self { child, port }
    }

    /// HTTP GET `/health` – returns `true` if the server responds with 2xx.
    #[allow(dead_code)]
    pub fn health_check(&self) -> bool {
        fd_rdd_client::health_check(self.port)
    }

    /// HTTP GET `/status` – returns the parsed JSON value.
    #[allow(dead_code)]
    pub fn status(&self) -> Option<serde_json::Value> {
        fd_rdd_client::status(self.port)
    }

    /// HTTP GET `/search` – returns typed search results.
    #[allow(dead_code)]
    pub fn search(&self, q: &str, limit: usize) -> Vec<SearchResult> {
        fd_rdd_client::search(self.port, q, limit)
    }

    /// HTTP GET `/search` – returns the raw JSON body.
    #[allow(dead_code)]
    pub fn search_raw(&self, q: &str, limit: usize) -> String {
        fd_rdd_client::search_raw(self.port, q, limit)
    }

    /// Kill the child process and wait for it to exit.
    pub fn kill(mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }

    /// Ask the child process to shut down gracefully and wait for final snapshot.
    #[cfg(unix)]
    #[allow(dead_code)]
    pub fn terminate(mut self) {
        // sanitizers (TSan / ASan / MSan) can make graceful shutdown
        // (final snapshot + runtime-state write) several times slower,
        // so we give the child extra time when running under a sanitizer.
        //
        // We check TSAN_OPTIONS rather than RUSTFLAGS because cargo does
        // not forward RUSTFLAGS to the test binary's runtime environment.
        let sanitized = std::env::var("TSAN_OPTIONS")
            .or_else(|_| std::env::var("ASAN_OPTIONS"))
            .or_else(|_| std::env::var("MSAN_OPTIONS"))
            .is_ok();
        let max_attempts = if sanitized { 1200 } else { 150 };

        unsafe {
            libc::kill(self.child.id() as i32, libc::SIGTERM);
        }
        for _ in 0..max_attempts {
            match self.child.try_wait() {
                Ok(Some(_)) => return,
                Ok(None) => std::thread::sleep(Duration::from_millis(100)),
                Err(_) => return,
            }
        }
        let _ = self.child.kill();
        let _ = self.child.wait();
    }

    #[cfg(not(unix))]
    #[allow(dead_code)]
    pub fn terminate(self) {
        self.kill();
    }

    /// PID of the spawned child process.
    pub fn pid(&self) -> u32 {
        self.child.id()
    }
}

/// Resolve the path to the `fd-rdd` binary in the Cargo target directory.
///
/// Works for both `cargo test` (debug) and `cargo test --release`.
fn fd_rdd_exe_path() -> PathBuf {
    let current_exe = std::env::current_exe().expect("current_exe");
    // current_exe is roughly target/{debug|release}/deps/test-binary-xxx.exe
    let target_dir = current_exe
        .parent()
        .and_then(|p| p.parent())
        .expect("target dir");
    target_dir
        .join("fd-rdd")
        .with_extension(std::env::consts::EXE_EXTENSION)
}

/// Poll `/status` until `indexed_count` stops changing for `stable_secs`.
///
/// Returns the stabilized count on success, or an error message on timeout.
pub fn wait_for_index_stable(
    port: u16,
    stable_secs: u64,
    timeout_secs: u64,
) -> Result<usize, String> {
    let start = std::time::Instant::now();
    let timeout = Duration::from_secs(timeout_secs);
    let stable_duration = Duration::from_secs(stable_secs);
    let mut last_count = 0usize;
    let mut last_change = start;

    loop {
        // 跳过 rebuild 进行中的状态：full_build 期间
        // file_count() 可能不变（正构建新索引），不应视为"稳定"。
        if fd_rdd_client::is_rebuilding(port).unwrap_or(false) {
            last_change = std::time::Instant::now();
            std::thread::sleep(Duration::from_millis(200));
            continue;
        }

        if let Some(count) = fd_rdd_client::indexed_count(port) {
            if count != last_count {
                last_count = count;
                last_change = std::time::Instant::now();
            } else if last_change.elapsed() >= stable_duration {
                return Ok(last_count);
            }
        }

        if start.elapsed() >= timeout {
            return Err(format!(
                "Timeout waiting for index to stabilize (last_count: {}, elapsed: {:?})",
                last_count,
                start.elapsed()
            ));
        }

        std::thread::sleep(Duration::from_millis(200));
    }
}

/// Poll `/status` until `indexed_count` reaches at least `expected`.
///
/// Returns the elapsed duration on success, or an error message on timeout.
pub fn wait_for_indexed_count(
    port: u16,
    expected: usize,
    timeout_secs: u64,
) -> Result<Duration, String> {
    let start = std::time::Instant::now();
    let timeout = Duration::from_secs(timeout_secs);

    loop {
        if let Some(count) = fd_rdd_client::indexed_count(port) {
            if count >= expected {
                return Ok(start.elapsed());
            }
        }

        if start.elapsed() >= timeout {
            let actual = fd_rdd_client::indexed_count(port).unwrap_or(0);
            return Err(format!(
                "Timeout waiting for indexed_count >= {} (actual: {}, elapsed: {:?})",
                expected,
                actual,
                start.elapsed()
            ));
        }

        std::thread::sleep(Duration::from_millis(200));
    }
}

/// Poll `/search` until the file at `path` appears in results.
///
/// Returns `true` if the file was found before the timeout.
pub fn wait_for_file_visible(port: u16, path: &Path, timeout_secs: u64) -> bool {
    let start = std::time::Instant::now();
    let timeout = Duration::from_secs(timeout_secs);
    let file_name = path
        .file_name()
        .map(|n| n.to_string_lossy().into_owned())
        .unwrap_or_default();

    loop {
        let results = fd_rdd_client::search(port, &file_name, 100);
        if results.iter().any(|r| r.path == path) {
            return true;
        }

        if start.elapsed() >= timeout {
            return false;
        }

        std::thread::sleep(Duration::from_millis(200));
    }
}

/// Poll `/search` until the file at `path` disappears from results.
///
/// Returns `true` if the file was gone before the timeout.
pub fn wait_for_file_gone(port: u16, path: &Path, timeout_secs: u64) -> bool {
    let start = std::time::Instant::now();
    let timeout = Duration::from_secs(timeout_secs);
    let file_name = path
        .file_name()
        .map(|n| n.to_string_lossy().into_owned())
        .unwrap_or_default();

    loop {
        let results = fd_rdd_client::search(port, &file_name, 100);
        if !results.iter().any(|r| r.path == path) {
            return true;
        }

        if start.elapsed() >= timeout {
            return false;
        }

        std::thread::sleep(Duration::from_millis(200));
    }
}
