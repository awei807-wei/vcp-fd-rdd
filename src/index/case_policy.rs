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

pub fn detect_root_case_policy(root: &Path, fstype_hint: Option<&str>) -> CasePolicyProbeResult {
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

    #[test]
    fn sharp_s_fold_changes_byte_len_and_still_trigrams_by_bytes() {
        let folded = unicode_case_fold_lookup("Straße");
        assert_eq!(folded, "strasse");
        assert_ne!("Straße".chars().count(), folded.chars().count());

        let mut tris = Vec::new();
        for_each_folded_trigram("Straße".as_bytes(), |tri| tris.push(tri));
        assert!(tris.contains(&*b"str"));
        assert!(tris.contains(&*b"ras"));
        assert!(tris.contains(&*b"ass"));
        assert!(tris.contains(&*b"sse"));
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
    fn unreadable_or_missing_root_returns_unknown() {
        let missing =
            std::env::temp_dir().join(format!("fd-rdd-missing-case-policy-{}", std::process::id()));
        let result = detect_root_case_policy(&missing, None);
        assert_eq!(result.detected_policy, CasePolicy::Unknown);
    }

    #[test]
    fn folded_conflict_count_uses_lookup_bytes() {
        let conflicts = folded_conflict_count(["Straße.txt".as_bytes(), "STRASSE.txt".as_bytes()]);
        assert_eq!(conflicts, 1);
    }
}
