//! Case folding helpers used by lookup indexes.
//!
//! The raw path bytes remain authoritative. These helpers only build lookup
//! keys, so full Unicode folds that change byte length are allowed.

use serde::{Deserialize, Serialize};

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
}
