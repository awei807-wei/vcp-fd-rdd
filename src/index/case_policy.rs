//! Case folding helpers used by lookup indexes.
//!
//! The raw path bytes remain authoritative. These helpers only build lookup
//! keys, so full Unicode folds that change byte length are allowed.

use serde::{Deserialize, Serialize};
use std::path::Path;

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum CasePolicy {
    Sensitive,
    Insensitive,
    Auto,
    #[default]
    Unknown,
}

impl CasePolicy {
    pub fn lookup_is_folded(self) -> bool {
        matches!(self, Self::Insensitive)
    }
}

/// Fold text for case-insensitive lookup.
///
/// Rust's `to_lowercase()` is not a full Unicode case-fold. The German sharp S
/// is the practical edge case for fd-rdd: `ß` and `ẞ` must be searchable as
/// `ss`, which changes byte length. All trigram callers must therefore operate
/// on the returned bytes, not on original character offsets.
pub fn unicode_case_fold_lookup(input: &str) -> String {
    let mut out = String::with_capacity(input.len());
    for ch in input.chars() {
        match ch {
            'ß' | 'ẞ' => out.push_str("ss"),
            _ => out.extend(ch.to_lowercase()),
        }
    }
    out
}

pub fn folded_lookup_bytes_lossy(bytes: &[u8]) -> Vec<u8> {
    unicode_case_fold_lookup(&String::from_utf8_lossy(bytes)).into_bytes()
}

pub fn for_each_folded_trigram(bytes: &[u8], mut f: impl FnMut([u8; 3])) {
    let folded = folded_lookup_bytes_lossy(bytes);
    if folded.len() < 3 {
        return;
    }
    for tri in folded.windows(3) {
        f([tri[0], tri[1], tri[2]]);
    }
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct CasePolicyProbeResult {
    pub detected_policy: CasePolicy,
    pub conflict_count: u64,
}

pub fn fstype_case_policy_hint(fstype: &str) -> Option<CasePolicy> {
    match fstype {
        "ntfs" | "ntfs3" | "exfat" | "vfat" | "cifs" | "smb3" => Some(CasePolicy::Insensitive),
        _ if fstype.starts_with("fuse.sshfs") => Some(CasePolicy::Sensitive),
        _ => None,
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[cfg_attr(not(any(test, target_os = "macos")), allow(dead_code))]
enum PathconfCasePolicyProbe {
    Known(CasePolicy),
    Unsupported,
    Unknown,
}

#[cfg(any(test, target_os = "macos"))]
fn pathconf_case_sensitive_result(raw: libc::c_long, errno: i32) -> PathconfCasePolicyProbe {
    match (raw, errno) {
        (0, _) => PathconfCasePolicyProbe::Known(CasePolicy::Insensitive),
        (1, _) => PathconfCasePolicyProbe::Known(CasePolicy::Sensitive),
        (-1, libc::EINVAL) => PathconfCasePolicyProbe::Unsupported,
        (-1, _) => PathconfCasePolicyProbe::Unknown,
        _ => PathconfCasePolicyProbe::Unknown,
    }
}

#[cfg(target_os = "macos")]
fn probe_case_policy_with_pathconf(root: &Path) -> PathconfCasePolicyProbe {
    use std::ffi::CString;
    use std::os::unix::ffi::OsStrExt;

    let Ok(c_path) = CString::new(root.as_os_str().as_bytes()) else {
        return PathconfCasePolicyProbe::Unknown;
    };

    unsafe {
        *libc::__error() = 0;
    }
    let raw = unsafe { libc::pathconf(c_path.as_ptr(), libc::_PC_CASE_SENSITIVE) };
    let errno = unsafe { *libc::__error() };
    pathconf_case_sensitive_result(raw, errno)
}

#[cfg(not(target_os = "macos"))]
fn probe_case_policy_with_pathconf(_root: &Path) -> PathconfCasePolicyProbe {
    PathconfCasePolicyProbe::Unsupported
}

pub fn detect_root_case_policy(root: &Path, fstype_hint: Option<&str>) -> CasePolicyProbeResult {
    detect_root_case_policy_with_pathconf(root, fstype_hint, probe_case_policy_with_pathconf)
}

fn detect_root_case_policy_with_pathconf(
    root: &Path,
    fstype_hint: Option<&str>,
    pathconf_probe: impl FnOnce(&Path) -> PathconfCasePolicyProbe,
) -> CasePolicyProbeResult {
    if let Some(policy) = fstype_hint.and_then(fstype_case_policy_hint) {
        return CasePolicyProbeResult {
            detected_policy: policy,
            conflict_count: 0,
        };
    }

    let Ok(meta) = std::fs::metadata(root) else {
        return CasePolicyProbeResult {
            detected_policy: CasePolicy::Unknown,
            conflict_count: 0,
        };
    };
    if !meta.is_dir() || meta.permissions().readonly() {
        return CasePolicyProbeResult {
            detected_policy: CasePolicy::Unknown,
            conflict_count: 0,
        };
    }

    match pathconf_probe(root) {
        PathconfCasePolicyProbe::Known(policy) => {
            return CasePolicyProbeResult {
                detected_policy: policy,
                conflict_count: 0,
            };
        }
        PathconfCasePolicyProbe::Unsupported => {}
        PathconfCasePolicyProbe::Unknown => {
            return CasePolicyProbeResult {
                detected_policy: CasePolicy::Unknown,
                conflict_count: 0,
            };
        }
    }

    let probe_dir = root.join(format!(
        ".fd-rdd-case-probe-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_nanos())
            .unwrap_or(0)
    ));
    if std::fs::create_dir(&probe_dir).is_err() {
        return CasePolicyProbeResult {
            detected_policy: CasePolicy::Unknown,
            conflict_count: 0,
        };
    }

    let lower = probe_dir.join("caseprobe");
    let upper = probe_dir.join("CASEPROBE");
    let detected_policy = match std::fs::write(&lower, b"fd-rdd") {
        Ok(()) if upper.exists() => CasePolicy::Insensitive,
        Ok(()) => CasePolicy::Sensitive,
        Err(_) => CasePolicy::Unknown,
    };
    let _ = std::fs::remove_file(&lower);
    let _ = std::fs::remove_file(&upper);
    let _ = std::fs::remove_dir(&probe_dir);

    CasePolicyProbeResult {
        detected_policy,
        conflict_count: 0,
    }
}

pub fn folded_conflict_count<I, B>(items: I) -> u64
where
    I: IntoIterator<Item = B>,
    B: AsRef<[u8]>,
{
    use std::collections::HashSet;

    let mut seen = HashSet::new();
    let mut conflicts = 0u64;
    for item in items {
        let folded = folded_lookup_bytes_lossy(item.as_ref());
        if !seen.insert(folded) {
            conflicts = conflicts.saturating_add(1);
        }
    }
    conflicts
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;

    fn temp_probe_root(name: &str) -> std::path::PathBuf {
        let dir = std::env::temp_dir().join(format!(
            "fd-rdd-{name}-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|d| d.as_nanos())
                .unwrap_or(0)
        ));
        fs::create_dir(&dir).expect("create temp probe root");
        dir
    }

    #[test]
    fn sharp_s_fold_changes_byte_len_and_still_trigrams_by_bytes() {
        let folded = unicode_case_fold_lookup("Straße");
        assert_eq!(folded, "strasse");
        assert_ne!("Straße".chars().count(), folded.chars().count());

        let mut tris = Vec::new();
        for_each_folded_trigram("Straße".as_bytes(), |tri| tris.push(tri));
        assert!(tris.contains(b"str"));
        assert!(tris.contains(b"ras"));
        assert!(tris.contains(b"ass"));
        assert!(tris.contains(b"sse"));
    }

    #[test]
    fn fstype_hint_detects_common_case_insensitive_roots() {
        assert_eq!(
            fstype_case_policy_hint("cifs"),
            Some(CasePolicy::Insensitive)
        );
        assert_eq!(
            fstype_case_policy_hint("ntfs3"),
            Some(CasePolicy::Insensitive)
        );
        assert_eq!(fstype_case_policy_hint("ext4"), None);
    }

    #[test]
    fn pathconf_result_maps_case_sensitivity_and_einval() {
        assert_eq!(
            pathconf_case_sensitive_result(1, 0),
            PathconfCasePolicyProbe::Known(CasePolicy::Sensitive)
        );
        assert_eq!(
            pathconf_case_sensitive_result(0, 0),
            PathconfCasePolicyProbe::Known(CasePolicy::Insensitive)
        );
        assert_eq!(
            pathconf_case_sensitive_result(-1, libc::EINVAL),
            PathconfCasePolicyProbe::Unsupported
        );
        assert_eq!(
            pathconf_case_sensitive_result(-1, libc::EACCES),
            PathconfCasePolicyProbe::Unknown
        );
    }

    #[test]
    fn pathconf_known_policy_short_circuits_side_effect_probe() {
        let root = temp_probe_root("case-policy-pathconf-known");
        let result = detect_root_case_policy_with_pathconf(&root, None, |_| {
            PathconfCasePolicyProbe::Known(CasePolicy::Insensitive)
        });
        fs::remove_dir(&root).expect("remove temp probe root");

        assert_eq!(result.detected_policy, CasePolicy::Insensitive);
    }

    #[test]
    fn pathconf_einval_falls_back_to_side_effect_probe() {
        let root = temp_probe_root("case-policy-pathconf-einval");
        let result = detect_root_case_policy_with_pathconf(&root, None, |_| {
            pathconf_case_sensitive_result(-1, libc::EINVAL)
        });
        fs::remove_dir(&root).expect("remove temp probe root");

        assert!(matches!(
            result.detected_policy,
            CasePolicy::Sensitive | CasePolicy::Insensitive
        ));
    }

    #[test]
    fn pathconf_unknown_error_keeps_policy_unknown_without_side_effects() {
        let root = temp_probe_root("case-policy-pathconf-unknown");
        let result = detect_root_case_policy_with_pathconf(&root, None, |_| {
            pathconf_case_sensitive_result(-1, libc::EACCES)
        });
        fs::remove_dir(&root).expect("remove temp probe root");

        assert_eq!(result.detected_policy, CasePolicy::Unknown);
    }

    #[test]
    fn unreadable_or_missing_root_returns_unknown() {
        let missing =
            std::env::temp_dir().join(format!("fd-rdd-missing-case-policy-{}", std::process::id()));
        let result = detect_root_case_policy(&missing, None);
        assert_eq!(result.detected_policy, CasePolicy::Unknown);
    }

    #[test]
    fn readonly_root_returns_unknown() {
        let root = temp_probe_root("case-policy-readonly");
        let mut perms = fs::metadata(&root)
            .expect("stat temp probe root")
            .permissions();
        perms.set_readonly(true);
        fs::set_permissions(&root, perms).expect("set readonly temp probe root");

        let result = detect_root_case_policy_with_pathconf(&root, None, |_| {
            panic!("readonly roots must not run pathconf or side-effect probes")
        });

        let mut perms = fs::metadata(&root)
            .expect("stat readonly temp probe root")
            .permissions();
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            perms.set_mode(0o700);
        }
        #[cfg(not(unix))]
        {
            perms.set_readonly(false);
        }
        fs::set_permissions(&root, perms).expect("restore temp probe root permissions");
        fs::remove_dir(&root).expect("remove temp probe root");

        assert_eq!(result.detected_policy, CasePolicy::Unknown);
    }

    #[test]
    fn folded_conflict_count_uses_lookup_bytes() {
        let conflicts = folded_conflict_count(["Straße.txt".as_bytes(), "STRASSE.txt".as_bytes()]);
        assert_eq!(conflicts, 1);
    }
}
