use std::collections::HashSet;
use std::path::{Path, PathBuf};
use std::time::Instant;

use crate::util::path_has_excluded_component;

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize, PartialEq, Eq)]
#[serde(default)]
pub struct ProcSamplerConfig {
    pub enabled: bool,
    pub interval_ms: u64,
    pub max_pids_per_tick: usize,
    pub max_fds_per_pid: usize,
    pub max_dirs_per_tick: usize,
}

impl Default for ProcSamplerConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            interval_ms: 1_000,
            max_pids_per_tick: 128,
            max_fds_per_pid: 64,
            max_dirs_per_tick: 32,
        }
    }
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct ProcSamplerCursor {
    pub last_pid: u32,
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct ProcSamplerReport {
    pub duration_ms: u64,
    pub pids_seen: u64,
    pub pids_scanned: u64,
    pub pids_denied: u64,
    pub fdinfo_read_count: u64,
    pub readlink_count: u64,
    pub write_fd_count: u64,
    pub sampled_dirs: u64,
    pub budget_exhausted: bool,
    pub unavailable: bool,
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct ProcSamplerTick {
    pub dirs: Vec<PathBuf>,
    pub cursor: ProcSamplerCursor,
    pub report: ProcSamplerReport,
}

pub fn sample_proc_write_dirs(
    config: &ProcSamplerConfig,
    cursor: ProcSamplerCursor,
    roots: &[PathBuf],
    ignore_prefixes: &[PathBuf],
    exclude_dirs: &[String],
) -> ProcSamplerTick {
    sample_proc_root_write_dirs(
        Path::new("/proc"),
        current_uid(),
        config,
        cursor,
        roots,
        ignore_prefixes,
        exclude_dirs,
    )
}

#[cfg(target_os = "linux")]
pub fn sample_proc_root_write_dirs(
    proc_root: &Path,
    current_uid: u32,
    config: &ProcSamplerConfig,
    cursor: ProcSamplerCursor,
    roots: &[PathBuf],
    ignore_prefixes: &[PathBuf],
    exclude_dirs: &[String],
) -> ProcSamplerTick {
    use std::os::unix::fs::MetadataExt;

    let start = Instant::now();
    let mut report = ProcSamplerReport::default();
    if !config.enabled
        || config.max_pids_per_tick == 0
        || config.max_fds_per_pid == 0
        || config.max_dirs_per_tick == 0
    {
        report.duration_ms = start.elapsed().as_millis() as u64;
        return ProcSamplerTick {
            cursor,
            report,
            ..ProcSamplerTick::default()
        };
    }

    let mut pids = match read_proc_pids(proc_root) {
        Ok(pids) => pids,
        Err(_) => {
            report.unavailable = true;
            report.duration_ms = start.elapsed().as_millis() as u64;
            return ProcSamplerTick {
                cursor,
                report,
                ..ProcSamplerTick::default()
            };
        }
    };
    pids.sort_unstable();
    report.pids_seen = pids.len() as u64;
    if pids.is_empty() {
        report.duration_ms = start.elapsed().as_millis() as u64;
        return ProcSamplerTick {
            cursor,
            report,
            ..ProcSamplerTick::default()
        };
    }

    let selected = select_pids_after_cursor(&pids, cursor.last_pid, config.max_pids_per_tick);
    report.budget_exhausted = pids.len() > selected.len();
    let mut next_cursor = cursor.last_pid;

    let mut dirs = Vec::new();
    let mut seen_dirs = HashSet::new();
    'pids: for pid in selected {
        next_cursor = pid;
        let pid_dir = proc_root.join(pid.to_string());
        let Ok(meta) = std::fs::metadata(&pid_dir) else {
            report.pids_denied = report.pids_denied.saturating_add(1);
            continue;
        };
        if meta.uid() != current_uid {
            report.pids_denied = report.pids_denied.saturating_add(1);
            continue;
        }
        report.pids_scanned = report.pids_scanned.saturating_add(1);

        let mut fd_names = match read_fd_names(pid_dir.as_path()) {
            Ok(fd_names) => fd_names,
            Err(_) => {
                report.pids_denied = report.pids_denied.saturating_add(1);
                continue;
            }
        };
        fd_names.sort_unstable();
        if fd_names.len() > config.max_fds_per_pid {
            report.budget_exhausted = true;
            fd_names.truncate(config.max_fds_per_pid);
        }

        for fd_name in fd_names {
            let fdinfo = pid_dir.join("fdinfo").join(&fd_name);
            report.fdinfo_read_count = report.fdinfo_read_count.saturating_add(1);
            let Ok(text) = std::fs::read_to_string(fdinfo) else {
                continue;
            };
            let Some(flags) = parse_fdinfo_flags(&text) else {
                continue;
            };
            if !fd_flags_allow_write(flags) {
                continue;
            }
            report.write_fd_count = report.write_fd_count.saturating_add(1);
            report.readlink_count = report.readlink_count.saturating_add(1);
            let Ok(target) = std::fs::read_link(pid_dir.join("fd").join(fd_name)) else {
                continue;
            };
            let Some(dir) = writable_target_dir(target.as_path()) else {
                continue;
            };
            if !proc_sample_dir_allowed(dir.as_path(), roots, ignore_prefixes, exclude_dirs) {
                continue;
            }
            if seen_dirs.insert(dir.clone()) {
                dirs.push(dir);
                if dirs.len() >= config.max_dirs_per_tick {
                    report.budget_exhausted = true;
                    break 'pids;
                }
            }
        }
    }

    report.sampled_dirs = dirs.len() as u64;
    report.duration_ms = start.elapsed().as_millis() as u64;
    ProcSamplerTick {
        dirs,
        cursor: ProcSamplerCursor {
            last_pid: next_cursor,
        },
        report,
    }
}

#[cfg(not(target_os = "linux"))]
pub fn sample_proc_root_write_dirs(
    _proc_root: &Path,
    _current_uid: u32,
    _config: &ProcSamplerConfig,
    cursor: ProcSamplerCursor,
    _roots: &[PathBuf],
    _ignore_prefixes: &[PathBuf],
    _exclude_dirs: &[String],
) -> ProcSamplerTick {
    ProcSamplerTick {
        cursor,
        report: ProcSamplerReport {
            unavailable: true,
            ..ProcSamplerReport::default()
        },
        ..ProcSamplerTick::default()
    }
}

#[cfg(target_os = "linux")]
fn read_proc_pids(proc_root: &Path) -> std::io::Result<Vec<u32>> {
    let mut pids = Vec::new();
    for entry in std::fs::read_dir(proc_root)? {
        let entry = entry?;
        let name = entry.file_name();
        let Some(name) = name.to_str() else {
            continue;
        };
        let Ok(pid) = name.parse::<u32>() else {
            continue;
        };
        pids.push(pid);
    }
    Ok(pids)
}

#[cfg(target_os = "linux")]
fn read_fd_names(pid_dir: &Path) -> std::io::Result<Vec<String>> {
    let mut names = Vec::new();
    for entry in std::fs::read_dir(pid_dir.join("fdinfo"))? {
        let entry = entry?;
        let name = entry.file_name();
        let Some(name) = name.to_str() else {
            continue;
        };
        if name.parse::<u32>().is_ok() {
            names.push(name.to_string());
        }
    }
    Ok(names)
}

#[cfg(target_os = "linux")]
fn select_pids_after_cursor(pids: &[u32], last_pid: u32, limit: usize) -> Vec<u32> {
    if limit == 0 || pids.is_empty() {
        return Vec::new();
    }
    let start = pids.iter().position(|pid| *pid > last_pid).unwrap_or(0);
    pids.iter()
        .cycle()
        .skip(start)
        .take(pids.len().min(limit))
        .copied()
        .collect()
}

pub fn parse_fdinfo_flags(text: &str) -> Option<u64> {
    let raw = text.lines().find_map(|line| {
        let line = line.trim();
        line.strip_prefix("flags:").map(str::trim).or_else(|| {
            line.strip_prefix("flags")
                .and_then(|s| s.strip_prefix(':'))
                .map(str::trim)
        })
    })?;
    if raw.starts_with('0') {
        u64::from_str_radix(raw, 8)
            .or_else(|_| raw.parse::<u64>())
            .ok()
    } else {
        raw.parse::<u64>()
            .or_else(|_| u64::from_str_radix(raw, 8))
            .ok()
    }
}

pub fn fd_flags_allow_write(flags: u64) -> bool {
    let accmode = flags & libc::O_ACCMODE as u64;
    accmode == libc::O_WRONLY as u64 || accmode == libc::O_RDWR as u64
}

fn writable_target_dir(target: &Path) -> Option<PathBuf> {
    let text = target.to_string_lossy();
    if !target.is_absolute() || text.ends_with(" (deleted)") || text.starts_with("socket:[") {
        return None;
    }

    match std::fs::metadata(target) {
        Ok(meta) if meta.is_dir() => Some(target.to_path_buf()),
        Ok(_) => target.parent().map(Path::to_path_buf),
        Err(_) => None,
    }
}

fn proc_sample_dir_allowed(
    dir: &Path,
    roots: &[PathBuf],
    ignore_prefixes: &[PathBuf],
    exclude_dirs: &[String],
) -> bool {
    roots
        .iter()
        .any(|root| path_is_under_or_equal(dir, root.as_path()))
        && !ignore_prefixes
            .iter()
            .any(|ignore| !ignore.as_os_str().is_empty() && dir.starts_with(ignore))
        && !path_has_excluded_component(dir, exclude_dirs)
}

fn path_is_under_or_equal(path: &Path, root: &Path) -> bool {
    path == root || path.starts_with(root)
}

fn current_uid() -> u32 {
    #[cfg(target_os = "linux")]
    {
        // SAFETY: getuid is a side-effect-free libc call with no preconditions.
        unsafe { libc::getuid() }
    }
    #[cfg(not(target_os = "linux"))]
    {
        0
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_write_flags_from_fdinfo() {
        assert_eq!(
            parse_fdinfo_flags("pos:\t0\nflags:\t0100002\n"),
            Some(0o100002)
        );
        assert_eq!(parse_fdinfo_flags("flags:\t32770\n"), Some(32770));
        assert!(fd_flags_allow_write(0o100001));
        assert!(fd_flags_allow_write(0o100002));
        assert!(fd_flags_allow_write(32770));
        assert!(!fd_flags_allow_write(0o100000));
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn samples_same_user_write_fd_dirs_with_budgets() {
        use std::os::unix::fs::symlink;

        let root = unique_tmp_dir("proc-sampler");
        let proc_root = root.join("proc");
        let indexed = root.join("indexed");
        let out_dir = indexed.join("outputs");
        let _ = std::fs::remove_dir_all(&root);
        std::fs::create_dir_all(&out_dir).unwrap();
        std::fs::create_dir_all(proc_root.join("100/fd")).unwrap();
        std::fs::create_dir_all(proc_root.join("100/fdinfo")).unwrap();
        let target = out_dir.join("image.tmp");
        std::fs::write(&target, b"open").unwrap();
        symlink(&target, proc_root.join("100/fd/3")).unwrap();
        std::fs::write(proc_root.join("100/fdinfo/3"), "pos:\t0\nflags:\t0100002\n").unwrap();

        let tick = sample_proc_root_write_dirs(
            proc_root.as_path(),
            current_uid(),
            &ProcSamplerConfig {
                max_pids_per_tick: 8,
                max_fds_per_pid: 8,
                max_dirs_per_tick: 8,
                ..ProcSamplerConfig::default()
            },
            ProcSamplerCursor::default(),
            std::slice::from_ref(&indexed),
            &[],
            &[],
        );

        assert_eq!(tick.dirs, vec![out_dir]);
        assert_eq!(tick.report.pids_scanned, 1);
        assert_eq!(tick.report.fdinfo_read_count, 1);
        assert_eq!(tick.report.readlink_count, 1);
        assert_eq!(tick.report.write_fd_count, 1);
        assert_eq!(tick.report.sampled_dirs, 1);

        let _ = std::fs::remove_dir_all(&root);
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn denies_non_matching_uid_without_reading_fds() {
        let root = unique_tmp_dir("proc-sampler-denied");
        let proc_root = root.join("proc");
        let indexed = root.join("indexed");
        let _ = std::fs::remove_dir_all(&root);
        std::fs::create_dir_all(&indexed).unwrap();
        std::fs::create_dir_all(proc_root.join("100/fd")).unwrap();
        std::fs::create_dir_all(proc_root.join("100/fdinfo")).unwrap();

        let tick = sample_proc_root_write_dirs(
            proc_root.as_path(),
            current_uid().saturating_add(1),
            &ProcSamplerConfig::default(),
            ProcSamplerCursor::default(),
            &[indexed],
            &[],
            &[],
        );

        assert!(tick.dirs.is_empty());
        assert_eq!(tick.report.pids_denied, 1);
        assert_eq!(tick.report.fdinfo_read_count, 0);

        let _ = std::fs::remove_dir_all(&root);
    }

    fn unique_tmp_dir(tag: &str) -> PathBuf {
        let nanos = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        std::env::temp_dir().join(format!("fd-rdd-{tag}-{}-{nanos}", std::process::id()))
    }
}
