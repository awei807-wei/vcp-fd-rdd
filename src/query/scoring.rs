use crate::core::FileMeta;

/// Configuration for scoring search results.
pub struct ScoreConfig {
    /// The original query string (lowercased).
    pub query_lower: String,
}

impl ScoreConfig {
    pub fn from_query(query: &str) -> Self {
        Self {
            query_lower: query.to_lowercase(),
        }
    }
}

/// Score a search result for ranking.
///
/// Higher scores are better.
/// Scoring factors:
/// 1. Depth penalty: deeper paths score lower (-5 per level)
/// 2. Basename match bonus: query appears in filename (+50)
/// 3. Basename prefix/exact match bonus (+30)
/// 4. Filename length penalty: long names are less relevant (-1 per char over 30)
/// 5. Recent modification bonus: modified within 7 days (+10)
pub fn score_result(meta: &FileMeta, config: &ScoreConfig) -> i64 {
    let path_str = meta.path.to_string_lossy();
    let mut score: i64 = 0;

    // 1) Depth penalty: each separator costs -5
    let depth = path_str.matches('/').count() + path_str.matches('\\').count();
    score -= (depth as i64) * 5;

    // 2) & 3) Basename match bonuses
    if let Some(name) = meta.path.file_name() {
        let name_lower = name.to_string_lossy().to_lowercase();
        let q = &config.query_lower;

        if !q.is_empty() {
            // query appears anywhere in basename
            if name_lower.contains(q.as_str()) {
                score += 50;
            }
            // exact or prefix match on basename
            if name_lower == *q || name_lower.starts_with(q.as_str()) {
                score += 30;
            }
        }

        // 4) Filename length penalty
        let len = name.len();
        if len > 30 {
            score -= ((len - 30) as i64).min(30);
        }
    }

    // 5) Recent modification bonus (7 days)
    if let Some(mtime) = meta.mtime {
        if let Ok(elapsed) = mtime.elapsed() {
            if elapsed < std::time::Duration::from_secs(7 * 86400) {
                score += 10;
            }
        }
    }

    score
}

/// Compute highlight byte-ranges for matched portions in a path string.
///
/// Returns a list of `[byte_start, byte_end)` ranges where the query matched.
/// For plain substring matching, finds all case-insensitive occurrences.
/// For path-initials mode, highlights the matched prefix portions of each segment.
pub fn compute_highlights(path: &str, query: &str) -> Vec<[usize; 2]> {
    if query.is_empty() {
        return Vec::new();
    }

    let has_separator = query.contains('\\') || query.contains('/');
    let has_glob = query.contains('*') || query.contains('?');

    if has_separator && !has_glob {
        // Path-initials mode: highlight matched segment prefixes
        compute_path_initials_highlights(path, query)
    } else {
        // Substring mode: find all case-insensitive occurrences
        compute_substring_highlights(path, query)
    }
}

/// Find all case-insensitive occurrences of `needle` in `haystack`.
fn compute_substring_highlights(haystack: &str, needle: &str) -> Vec<[usize; 2]> {
    let mut highlights = Vec::new();
    let h_lower = haystack.to_lowercase();
    let n_lower = needle.to_lowercase();

    if n_lower.is_empty() {
        return highlights;
    }

    let mut start = 0;
    while let Some(pos) = h_lower[start..].find(&n_lower) {
        let abs_pos = start + pos;
        highlights.push([abs_pos, abs_pos + n_lower.len()]);
        start = abs_pos + 1;
        if start >= h_lower.len() {
            break;
        }
    }
    highlights
}

/// Highlight matched portions for path-initials queries.
///
/// For `query = "tmp/tes/new"`, `path = "/tmp/test_data/newfile.txt"`:
/// highlights the "tmp", "tes" and "new" portions in their matching segments.
fn compute_path_initials_highlights(path: &str, query: &str) -> Vec<[usize; 2]> {
    let mut highlights = Vec::new();

    // Split query by separators
    let query_segments: Vec<&str> = query.split(|c: char| c == '/' || c == '\\')
        .filter(|s| !s.is_empty())
        .collect();

    if query_segments.is_empty() {
        return highlights;
    }

    // Collect path segments with their byte offsets
    let path_segments = split_path_with_offsets(path);

    let mut qi = 0; // query segment index
    let mut pi = 0; // path segment index

    while qi < query_segments.len() && pi < path_segments.len() {
        let qs_lower = query_segments[qi].to_lowercase();
        let is_last_query_seg = qi == query_segments.len() - 1;

        // Scan forward in path segments to find a match
        while pi < path_segments.len() {
            let (seg_text, seg_offset) = &path_segments[pi];
            let seg_lower = seg_text.to_lowercase();

            let matched = if is_last_query_seg {
                // Last query segment: prefix or substring match
                seg_lower.starts_with(&qs_lower) || seg_lower.contains(&qs_lower)
            } else {
                // Non-last segments: prefix match only
                seg_lower.starts_with(&qs_lower)
            };

            if matched {
                // Determine highlight range
                if is_last_query_seg && !seg_lower.starts_with(&qs_lower) {
                    // Substring match in last segment
                    if let Some(pos) = seg_lower.find(&qs_lower) {
                        let abs_start = seg_offset + pos;
                        highlights.push([abs_start, abs_start + qs_lower.len()]);
                    }
                } else {
                    // Prefix match
                    highlights.push([*seg_offset, seg_offset + qs_lower.len()]);
                }
                pi += 1;
                break;
            }
            pi += 1;
        }
        qi += 1;
    }

    // If not all query segments matched, return empty highlights
    if qi > query_segments.len() {
        return Vec::new();
    }

    highlights
}

/// Split a path into segments, returning each segment's text and byte offset.
fn split_path_with_offsets(path: &str) -> Vec<(&str, usize)> {
    let mut segments = Vec::new();
    let mut start = 0;
    let bytes = path.as_bytes();

    for (i, &b) in bytes.iter().enumerate() {
        if b == b'/' || b == b'\\' {
            if i > start {
                segments.push((&path[start..i], start));
            }
            start = i + 1;
        }
    }
    // Last segment
    if start < path.len() {
        segments.push((&path[start..], start));
    }

    segments
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::{FileKey, FileMeta};
    use std::path::PathBuf;
    use std::time::SystemTime;

    fn meta(path: &str, size: u64, mtime: Option<SystemTime>) -> FileMeta {
        FileMeta {
            file_key: FileKey { dev: 0, ino: 0 },
            path: PathBuf::from(path),
            size,
            mtime,
            ctime: None,
            atime: None,
        }
    }

    #[test]
    fn score_shallow_path_higher_than_deep() {
        let config = ScoreConfig::from_query("test");
        let shallow = meta("/home/test.txt", 100, None);
        let deep = meta("/home/user/projects/subdir/test.txt", 100, None);
        assert!(score_result(&shallow, &config) > score_result(&deep, &config));
    }

    #[test]
    fn score_basename_match_bonus() {
        let config = ScoreConfig::from_query("test");
        let matched = meta("/a/test.txt", 100, None);
        let not_matched = meta("/a/other.txt", 100, None);
        assert!(score_result(&matched, &config) > score_result(&not_matched, &config));
    }

    #[test]
    fn score_prefix_match_bonus() {
        let config = ScoreConfig::from_query("test");
        let prefix = meta("/a/test_file.txt", 100, None);
        let contains = meta("/a/mytest.txt", 100, None);
        assert!(score_result(&prefix, &config) > score_result(&contains, &config));
    }

    #[test]
    fn score_long_filename_penalty() {
        let config = ScoreConfig::from_query("test");
        let short = meta("/a/test.txt", 100, None);
        let long = meta("/a/test_with_a_very_long_filename_that_goes_on_forever.txt", 100, None);
        assert!(score_result(&short, &config) > score_result(&long, &config));
    }

    #[test]
    fn score_recent_file_bonus() {
        let config = ScoreConfig::from_query("test");
        let recent = meta("/a/test.txt", 100, Some(SystemTime::now()));
        let old = meta("/a/test.txt", 100, None);
        assert!(score_result(&recent, &config) > score_result(&old, &config));
    }

    #[test]
    fn highlight_substring_basic() {
        let h = compute_highlights("/home/user/test.txt", "test");
        assert_eq!(h, vec![[11, 15]]);
    }

    #[test]
    fn highlight_substring_case_insensitive() {
        let h = compute_highlights("/home/user/TEST.txt", "test");
        assert_eq!(h, vec![[11, 15]]);
    }

    #[test]
    fn highlight_multiple_occurrences() {
        let h = compute_highlights("/test/data/test.txt", "test");
        assert_eq!(h, vec![[1, 5], [11, 15]]);
    }

    #[test]
    fn highlight_path_initials() {
        let h = compute_highlights("/tmp/test_data/newfile.txt", "tmp/tes/new");
        assert_eq!(h.len(), 3);
        assert_eq!(h[0], [1, 4]);   // "tmp"
        assert_eq!(h[1], [5, 8]);   // "tes"
        assert_eq!(h[2], [15, 18]); // "new"
    }

    #[test]
    fn highlight_empty_query() {
        let h = compute_highlights("/home/user/test.txt", "");
        assert!(h.is_empty());
    }
}
