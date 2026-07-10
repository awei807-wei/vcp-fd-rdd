use std::hash::{Hash, Hasher};
use std::path::Path;

use crate::core::{FileKey, FileKind};
use crate::index::case_policy::{folded_lookup_bytes_lossy, for_each_folded_trigram};

use super::Trigram;

#[derive(Clone, Copy)]
pub(super) struct ResolvedFsMeta {
    pub file_key: FileKey,
    pub mtime: Option<std::time::SystemTime>,
    pub kind: FileKind,
}

pub(crate) fn mtime_to_ns(mtime: Option<std::time::SystemTime>) -> i64 {
    mtime
        .and_then(|t| t.duration_since(std::time::UNIX_EPOCH).ok())
        .and_then(|d| i64::try_from(d.as_nanos()).ok())
        .unwrap_or(-1)
}

pub(super) fn mtime_from_ns(ns: i64) -> Option<std::time::SystemTime> {
    if ns < 0 {
        None
    } else {
        Some(std::time::UNIX_EPOCH + std::time::Duration::from_nanos(ns as u64))
    }
}

pub(super) fn normalize_short_hint(hint: &[u8]) -> Option<Vec<u8>> {
    let normalized = folded_lookup_bytes_lossy(hint);
    if (1..=2).contains(&normalized.len()) {
        Some(normalized)
    } else {
        None
    }
}

pub(super) fn trigram_matches_short_hint(tri: Trigram, hint: &[u8]) -> bool {
    match hint.len() {
        1 => tri.contains(&hint[0]),
        2 => tri[0..2] == hint[..] || tri[1..3] == hint[..],
        _ => false,
    }
}

/// 从查询词中提取 trigram 列表
pub(super) fn query_trigrams(query: &str) -> Vec<Trigram> {
    let mut tris = Vec::new();
    let folded = crate::index::case_policy::unicode_case_fold_lookup(query);
    let bytes = folded.as_bytes();
    if bytes.len() >= 3 {
        for w in bytes.windows(3) {
            tris.push([w[0], w[1], w[2]]);
        }
    }
    tris
}

/// 从路径的 basename（`path.file_name()`）中枚举 trigram（可能重复）。
///
/// - 标准化：lossy UTF-8 + Unicode lookup case-fold
/// - 目的：让 trigram 候选集成为 basename 精确匹配的严格超集（避免假阴性）
pub(super) fn for_each_basename_trigram(path: &Path, f: impl FnMut(Trigram)) {
    let Some(os) = path.file_name() else {
        return;
    };
    for_each_folded_trigram(os.as_encoded_bytes(), f);
}

pub(super) fn path_hash_bytes(bytes: &[u8]) -> u64 {
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    bytes.hash(&mut hasher);
    hasher.finish()
}

pub(super) fn normalize_roots_with_fallback(
    mut roots: Vec<std::path::PathBuf>,
) -> Vec<std::path::PathBuf> {
    use std::path::PathBuf;
    use unicode_normalization::UnicodeNormalization;
    for r in &mut roots {
        let s = r.to_string_lossy();
        *r = PathBuf::from(s.nfc().collect::<String>());
    }

    // 去重 + 排序，保证 root_id 的解释在"同一组 roots"下稳定。
    roots.sort_by(|a, b| {
        a.as_os_str()
            .as_encoded_bytes()
            .cmp(b.as_os_str().as_encoded_bytes())
    });
    roots.dedup();

    // root_id=0 固定为 "/"（用于兜底匹配与快照兼容）。
    let slash = PathBuf::from("/");
    roots.retain(|p| p != &slash);
    let mut out = Vec::with_capacity(roots.len() + 1);
    out.push(slash);
    out.extend(roots);
    out
}

pub(super) fn intern_parent_dirs(
    path_table: &mut super::parent_path::CompactPathTable,
    path: &[u8],
) {
    let mut end = match path.iter().rposition(|&b| b == b'/') {
        Some(0) => {
            let _ = path_table.intern(b"/", true);
            return;
        }
        Some(pos) => pos,
        None => return,
    };

    loop {
        if end == 0 {
            let _ = path_table.intern(b"/", true);
            break;
        }
        let _ = path_table.intern(&path[..end], true);
        end = match path[..end].iter().rposition(|&b| b == b'/') {
            Some(0) => 0,
            Some(pos) => pos,
            None => break,
        };
    }
}
