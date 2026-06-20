use std::collections::HashMap;
use std::path::{Path, PathBuf};

use crate::core::{FileKey, FileMeta};

/// Derived view of live paths that point at the same physical file.
///
/// This is not a search primary key. Search remains path/docid based; the
/// grouping is rebuilt from currently visible entries when requested.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct HardlinkGroup {
    pub file_key: FileKey,
    pub paths: Vec<PathBuf>,
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct PhysicalDedupeStats {
    pub live_path_count: usize,
    pub physical_file_count: usize,
    pub hardlink_group_count: usize,
    pub hardlink_path_count: usize,
    pub duplicate_path_count: usize,
    pub max_group_size: usize,
}

pub fn physical_dedupe_stats_from_metas(
    metas: impl IntoIterator<Item = FileMeta>,
    prefix: Option<&Path>,
) -> PhysicalDedupeStats {
    physical_dedupe_stats_from_groups(&group_paths_by_file_key(metas, prefix))
}

fn group_paths_by_file_key(
    metas: impl IntoIterator<Item = FileMeta>,
    prefix: Option<&Path>,
) -> HashMap<FileKey, Vec<PathBuf>> {
    let normalized_prefix = prefix.map(crate::index::tiered::normalize_path);
    let mut groups: HashMap<FileKey, Vec<PathBuf>> = HashMap::new();

    for meta in metas {
        if !meta.kind.is_file() {
            continue;
        }
        if normalized_prefix
            .as_ref()
            .is_some_and(|prefix| !meta.path.starts_with(prefix))
        {
            continue;
        }
        groups.entry(meta.file_key).or_default().push(meta.path);
    }

    groups
}

pub(super) fn physical_dedupe_stats_from_groups(
    groups: &HashMap<FileKey, Vec<PathBuf>>,
) -> PhysicalDedupeStats {
    let mut stats = PhysicalDedupeStats {
        live_path_count: groups.values().map(Vec::len).sum(),
        physical_file_count: groups.len(),
        ..PhysicalDedupeStats::default()
    };

    for paths in groups.values() {
        if paths.len() < 2 {
            continue;
        }
        stats.hardlink_group_count += 1;
        stats.hardlink_path_count += paths.len();
        stats.duplicate_path_count += paths.len() - 1;
        stats.max_group_size = stats.max_group_size.max(paths.len());
    }

    stats
}
