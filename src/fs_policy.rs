//! Mount table parsing and default filesystem boundary policy.

use serde::{Deserialize, Serialize};
use std::path::{Path, PathBuf};
use std::sync::mpsc;
use std::time::Duration;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MountEntry {
    pub mount_id: u32,
    pub parent_id: u32,
    pub major_minor: String,
    pub root: String,
    pub mount_point: PathBuf,
    pub fstype: String,
    pub source: String,
    pub options: String,
}

#[derive(Clone, Debug, Default)]
pub struct MountTable {
    entries: Vec<MountEntry>,
}

impl MountTable {
    pub fn parse(text: &str) -> Self {
        let mut entries = Vec::new();
        for line in text.lines() {
            let Some((left, right)) = line.split_once(" - ") else {
                continue;
            };
            let mut left_parts = left.split_whitespace();
            let Some(mount_id) = left_parts.next().and_then(|v| v.parse::<u32>().ok()) else {
                continue;
            };
            let Some(parent_id) = left_parts.next().and_then(|v| v.parse::<u32>().ok()) else {
                continue;
            };
            let Some(major_minor) = left_parts.next() else {
                continue;
            };
            let Some(root) = left_parts.next() else {
                continue;
            };
            let Some(mount_point) = left_parts.next() else {
                continue;
            };
            let Some(options) = left_parts.next() else {
                continue;
            };

            let mut right_parts = right.split_whitespace();
            let Some(fstype) = right_parts.next() else {
                continue;
            };
            let Some(source) = right_parts.next() else {
                continue;
            };
            entries.push(MountEntry {
                mount_id,
                parent_id,
                major_minor: major_minor.to_string(),
                root: root.to_string(),
                mount_point: unescape_mount_path(mount_point),
                fstype: fstype.to_string(),
                source: source.to_string(),
                options: options.to_string(),
            });
        }
        entries.sort_by_key(|e| e.mount_point.as_os_str().as_encoded_bytes().len());
        Self { entries }
    }

    pub fn current() -> std::io::Result<Self> {
        std::fs::read_to_string("/proc/self/mountinfo").map(|s| Self::parse(&s))
    }

    pub fn best_match<'a>(&'a self, path: &Path) -> Option<&'a MountEntry> {
        self.entries
            .iter()
            .filter(|entry| path.starts_with(&entry.mount_point))
            .max_by_key(|entry| entry.mount_point.as_os_str().as_encoded_bytes().len())
    }
}

fn unescape_mount_path(input: &str) -> PathBuf {
    let mut out = Vec::with_capacity(input.len());
    let bytes = input.as_bytes();
    let mut i = 0;
    while i < bytes.len() {
        if bytes[i] == b'\\' && i + 3 < bytes.len() {
            if let Ok(value) = u8::from_str_radix(&input[i + 1..i + 4], 8) {
                out.push(value);
                i += 4;
                continue;
            }
        }
        out.push(bytes[i]);
        i += 1;
    }
    crate::util::pathbuf_from_encoded_vec(out)
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct FsPolicyConfig {
    #[serde(default)]
    pub allow_remote: bool,
    #[serde(default)]
    pub one_file_system: bool,
    #[serde(default)]
    pub allow_fstypes: Vec<String>,
    #[serde(default)]
    pub deny_fstypes: Vec<String>,
    #[serde(default)]
    pub allow_mounts: Vec<PathBuf>,
    #[serde(default)]
    pub deny_mounts: Vec<PathBuf>,
}

impl Default for FsPolicyConfig {
    fn default() -> Self {
        Self {
            allow_remote: false,
            one_file_system: false,
            allow_fstypes: Vec::new(),
            deny_fstypes: default_deny_fstypes(),
            allow_mounts: Vec::new(),
            deny_mounts: vec![
                PathBuf::from("/proc"),
                PathBuf::from("/sys"),
                PathBuf::from("/dev"),
            ],
        }
    }
}

fn default_deny_fstypes() -> Vec<String> {
    [
        "proc", "sysfs", "devtmpfs", "devpts", "cgroup", "cgroup2", "autofs", "nfs", "nfs4",
        "cifs", "smb3", "fuse", "fuseblk",
    ]
    .into_iter()
    .map(ToString::to_string)
    .collect()
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum FsPolicyDecision {
    Allow,
    Deny { reason: String },
}

impl FsPolicyDecision {
    pub fn is_allowed(&self) -> bool {
        matches!(self, Self::Allow)
    }
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct MountPolicyCounters {
    pub fstype_blocked_count: u64,
    pub network_fs_ignored_count: u64,
    pub one_file_system_boundary_count: u64,
    pub fuse_probe_timeout_count: u64,
    pub denied_mount_count: u64,
    pub allowed_override_count: u64,
}

impl MountPolicyCounters {
    pub fn record_decision(&mut self, decision: &FsPolicyDecision) {
        let FsPolicyDecision::Deny { reason } = decision else {
            return;
        };
        self.denied_mount_count = self.denied_mount_count.saturating_add(1);
        if reason == "one_file_system" {
            self.one_file_system_boundary_count =
                self.one_file_system_boundary_count.saturating_add(1);
        } else if let Some(fstype) = reason.strip_prefix("remote_fstype:") {
            self.network_fs_ignored_count = self.network_fs_ignored_count.saturating_add(1);
            if fstype.starts_with("fuse.") {
                self.fstype_blocked_count = self.fstype_blocked_count.saturating_add(1);
            }
        } else if reason.starts_with("deny_fstype:") {
            self.fstype_blocked_count = self.fstype_blocked_count.saturating_add(1);
            if let Some(fstype) = reason.strip_prefix("deny_fstype:") {
                if is_remote_fstype(fstype) {
                    self.network_fs_ignored_count = self.network_fs_ignored_count.saturating_add(1);
                }
            }
        }
    }

    pub fn record_fuse_probe_timeout(&mut self) {
        self.fuse_probe_timeout_count = self.fuse_probe_timeout_count.saturating_add(1);
    }

    pub fn record_allowed_override(&mut self) {
        self.allowed_override_count = self.allowed_override_count.saturating_add(1);
    }
}

#[derive(Clone, Debug)]
pub struct FsPolicy {
    table: MountTable,
    config: FsPolicyConfig,
}

impl FsPolicy {
    pub fn new(table: MountTable, config: FsPolicyConfig) -> Self {
        Self { table, config }
    }

    pub fn current_default() -> Option<Self> {
        MountTable::current()
            .ok()
            .map(|table| Self::new(table, FsPolicyConfig::default()))
    }

    pub fn check_path(&self, path: &Path, scan_root: Option<&Path>) -> FsPolicyDecision {
        if self
            .config
            .allow_mounts
            .iter()
            .any(|allowed| path.starts_with(allowed))
        {
            return FsPolicyDecision::Allow;
        }
        if self
            .config
            .deny_mounts
            .iter()
            .any(|denied| path.starts_with(denied))
        {
            return FsPolicyDecision::Deny {
                reason: "deny_mount".to_string(),
            };
        }

        let Some(mount) = self.table.best_match(path) else {
            return FsPolicyDecision::Allow;
        };

        if self.config.one_file_system {
            if let Some(root) = scan_root {
                if let Some(root_mount) = self.table.best_match(root) {
                    if root_mount.mount_id != mount.mount_id {
                        return FsPolicyDecision::Deny {
                            reason: "one_file_system".to_string(),
                        };
                    }
                }
            }
        }

        if self
            .config
            .allow_fstypes
            .iter()
            .any(|fs| fs == &mount.fstype)
        {
            return FsPolicyDecision::Allow;
        }
        if self
            .config
            .deny_fstypes
            .iter()
            .any(|fs| fs == &mount.fstype || (fs == "fuse" && mount.fstype.starts_with("fuse.")))
        {
            return FsPolicyDecision::Deny {
                reason: format!("deny_fstype:{}", mount.fstype),
            };
        }
        if !self.config.allow_remote && is_remote_fstype(&mount.fstype) {
            return FsPolicyDecision::Deny {
                reason: format!("remote_fstype:{}", mount.fstype),
            };
        }
        FsPolicyDecision::Allow
    }
}

pub fn is_remote_fstype(fstype: &str) -> bool {
    matches!(fstype, "nfs" | "nfs4" | "cifs" | "smb3") || fstype.starts_with("fuse.")
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct DeleteCircuitDecision {
    pub quarantine: bool,
    pub reason: Option<String>,
}

pub fn delete_circuit_breaker(
    delete_count: usize,
    known_count: usize,
    mount_offline: bool,
    absolute_threshold: usize,
    ratio_threshold: f32,
) -> DeleteCircuitDecision {
    let ratio_hit = known_count > 0 && (delete_count as f32 / known_count as f32) > ratio_threshold;
    let absolute_hit = delete_count > absolute_threshold;
    if mount_offline && (absolute_hit || ratio_hit) {
        DeleteCircuitDecision {
            quarantine: true,
            reason: Some("offline_delete_burst".to_string()),
        }
    } else {
        DeleteCircuitDecision::default()
    }
}

pub fn probe_mount_with_timeout<T, F>(timeout: Duration, f: F) -> Option<T>
where
    T: Send + 'static,
    F: FnOnce() -> T + Send + 'static,
{
    let (tx, rx) = mpsc::channel();
    std::thread::spawn(move || {
        let _ = tx.send(f());
    });
    rx.recv_timeout(timeout).ok()
}

#[cfg(test)]
mod tests {
    use super::*;

    const SAMPLE: &str = "20 1 0:18 / /proc rw,nosuid,nodev,noexec,relatime - proc proc rw\n\
21 1 0:19 / /sys rw,nosuid,nodev,noexec,relatime - sysfs sysfs rw\n\
30 1 8:1 / /home rw,relatime - ext4 /dev/sda1 rw\n\
31 30 0:42 / /home/user/remote rw,relatime - nfs server:/export rw\n\
32 30 0:43 / /home/user/rclone rw,relatime - fuse.rclone rclone rw\n";

    #[test]
    fn parses_mountinfo_and_finds_longest_prefix() {
        let table = MountTable::parse(SAMPLE);
        let m = table.best_match(Path::new("/home/user/remote/a")).unwrap();
        assert_eq!(m.fstype, "nfs");
        assert_eq!(m.mount_point, PathBuf::from("/home/user/remote"));
    }

    #[test]
    fn default_policy_rejects_virtual_and_remote_fstypes() {
        let policy = FsPolicy::new(MountTable::parse(SAMPLE), FsPolicyConfig::default());
        assert!(!policy
            .check_path(Path::new("/proc/cpuinfo"), None)
            .is_allowed());
        assert!(!policy
            .check_path(Path::new("/home/user/remote/a"), None)
            .is_allowed());
        assert!(!policy
            .check_path(Path::new("/home/user/rclone/a"), None)
            .is_allowed());
        assert!(policy
            .check_path(Path::new("/home/user/local.txt"), None)
            .is_allowed());
    }

    #[test]
    fn allow_fstype_can_override_default_remote_reject() {
        let cfg = FsPolicyConfig {
            allow_fstypes: vec!["nfs".to_string()],
            ..FsPolicyConfig::default()
        };
        let policy = FsPolicy::new(MountTable::parse(SAMPLE), cfg);
        assert!(policy
            .check_path(Path::new("/home/user/remote/a"), None)
            .is_allowed());
    }

    #[test]
    fn one_file_system_rejects_cross_mount_subtree() {
        let cfg = FsPolicyConfig {
            one_file_system: true,
            allow_remote: true,
            allow_fstypes: vec!["nfs".to_string()],
            ..FsPolicyConfig::default()
        };
        let policy = FsPolicy::new(MountTable::parse(SAMPLE), cfg);
        assert!(!policy
            .check_path(
                Path::new("/home/user/remote/a"),
                Some(Path::new("/home/user"))
            )
            .is_allowed());
    }

    #[test]
    fn delete_burst_on_offline_mount_is_quarantined() {
        let d = delete_circuit_breaker(2000, 10_000, true, 1000, 0.05);
        assert!(d.quarantine);
    }

    #[test]
    fn mount_probe_timeout_returns_none_without_blocking_caller() {
        let result = probe_mount_with_timeout(Duration::from_millis(10), || {
            std::thread::sleep(Duration::from_millis(100));
            1
        });
        assert_eq!(result, None);
    }

    #[test]
    fn mount_policy_reason_matrix_counts_rejections() {
        let policy = FsPolicy::new(MountTable::parse(SAMPLE), FsPolicyConfig::default());
        let mut counters = MountPolicyCounters::default();

        counters.record_decision(&policy.check_path(Path::new("/home/user/remote/a"), None));
        counters.record_decision(&policy.check_path(Path::new("/home/user/rclone/a"), None));
        counters.record_fuse_probe_timeout();

        assert_eq!(counters.denied_mount_count, 2);
        assert_eq!(counters.network_fs_ignored_count, 2);
        assert_eq!(counters.fstype_blocked_count, 2);
        assert_eq!(counters.fuse_probe_timeout_count, 1);
    }
}
