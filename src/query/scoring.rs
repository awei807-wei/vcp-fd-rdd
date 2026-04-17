use crate::core::FileMeta;

// ─── Constants ───────────────────────────────────────────────────────────────

/// 构建/依赖/缓存等"噪声"目录名（小写匹配）。
/// 搜索源码时这些目录下的文件几乎不需要，权重大幅下降。
const JUNK_DIR_NAMES: &[&str] = &[
    "node_modules",
    ".node_modules",
    "target",
    "cache",
    ".cache",
    "__pycache__",
    ".tox",
    "dist",
    "build",
    ".build",
    "vendor",
    ".gradle",
    ".mvn",
    ".cargo",
    "bower_components",
    ".npm",
    ".yarn",
    ".pnpm-store",
    ".next",
    ".nuxt",
    "coverage",
    ".coverage",
    ".pytest_cache",
    ".mypy_cache",
    ".ruff_cache",
    "venv",
    ".venv",
    "env",
    ".eggs",
];

/// 含 `node` 关键字的 Low Priority Zone 目录名。
/// 当 query 不含 "node" 时，这些目录下的文件权重直接乘以 0.1。
const NODE_ZONE_NAMES: &[&str] = &["node_modules", ".node_modules"];

// ─── Scoring weights (tunable) ──────────────────────────────────────────────

/// 当匹配命中在 basename（文件名）中时，整体匹配质量乘以此系数。
const BASENAME_MULTIPLIER: f64 = 2.5;

/// 匹配字符紧跟在边界字符 (/, ., -, _) 之后的每字符奖励。
const BOUNDARY_BONUS_PER_CHAR: f64 = 12.0;

/// CamelCase 边界（小写→大写过渡）的每字符奖励。
const CAMEL_BOUNDARY_BONUS_PER_CHAR: f64 = 8.0;

/// 字符串首字母匹配的奖励。
const STRING_START_BONUS: f64 = 15.0;

/// 精确匹配 basename stem 的额外奖励。
const EXACT_STEM_BONUS: f64 = 60.0;

/// query 作为 basename 前缀的额外奖励。
const PREFIX_BONUS: f64 = 40.0;

/// 文件名长度惩罚系数：score -= filename.length * LENGTH_PENALTY_FACTOR。
const LENGTH_PENALTY_FACTOR: f64 = 0.1;

/// 近期修改奖励（7 天内）。
const RECENT_MTIME_BONUS: f64 = 15.0;

/// 隐藏目录惩罚分。
const HIDDEN_DIR_PENALTY: f64 = 30.0;

/// 噪声目录惩罚分（non-node junk dirs）。
const JUNK_DIR_PENALTY: f64 = 200.0;

/// node_modules 的 Low Priority Zone 乘子——近乎屏蔽，但依然可搜。
const NODE_ZONE_MULTIPLIER: f64 = 0.1;

/// 匹配起始位紧跟 `.` 或 `/` 时的匹配质量翻倍系数。
/// 例：搜索 `config.json` 命中 `.config.json` 中 `.` 之后的位置，分数 ×2。
const PERFECT_BOUNDARY_MULTIPLIER: f64 = 2.0;

/// 深度作为平分仲裁时的微小系数（每层 -0.5 分）。
const DEPTH_TIEBREAKER_FACTOR: f64 = 0.5;

/// 基础分。
const BASE_SCORE: f64 = 100.0;

// ─── ScoreConfig ────────────────────────────────────────────────────────────

/// Configuration for scoring search results.
///
/// 预解析 query 的多维特征，避免每次打分时重复分析。
pub struct ScoreConfig {
    /// The original query string (lowercased).
    pub query_lower: String,
    /// query 的 basename 部分（去掉路径前缀，小写）。
    pub query_basename: String,
    /// 用户是否在 query 中指定了路径（包含 / 或 \），此时不应惩罚深度和噪声目录。
    pub has_path_hint: bool,
    /// query 中是否包含 '.'（点号），用于 Smart Dot-file 处理。
    pub query_has_dot: bool,
    /// query 中是否包含 "node" 子串，用于 node_modules 物理隔离逻辑。
    pub query_has_node: bool,
}

impl ScoreConfig {
    pub fn from_query(query: &str) -> Self {
        let query_lower = query.to_lowercase();
        let has_path_hint = query.contains('/') || query.contains('\\');
        let query_has_dot = query.contains('.');
        let query_has_node = query_lower.contains("node");

        let query_basename = query_lower
            .rsplit(['/', '\\'])
            .next()
            .unwrap_or(&query_lower)
            .to_string();

        Self {
            query_lower,
            query_basename,
            has_path_hint,
            query_has_dot,
            query_has_node,
        }
    }
}

// ─── Helper: hidden / junk / node zone detection ────────────────────────────

/// 隐藏**目录**（.开头的目录段，不含文件名本身）是否出现在路径中。
/// 注意：文件名本身以 `.` 开头不惩罚（如 `.config.json`、`.eslintrc` 是常用文件）。
fn path_has_hidden_dir(path: &str) -> bool {
    let segments: Vec<&str> = path.split(['/', '\\']).collect();
    // 最后一段是文件名，不检查
    for seg in segments.iter().take(segments.len().saturating_sub(1)) {
        if seg.starts_with('.') && seg.len() > 1 {
            return true;
        }
    }
    false
}

/// 路径是否经过"噪声目录"。
fn path_in_junk_dir(path: &str) -> bool {
    let lower = path.to_lowercase();
    for seg in lower.split(['/', '\\']) {
        if seg.is_empty() {
            continue;
        }
        for junk in JUNK_DIR_NAMES {
            if seg == *junk {
                return true;
            }
        }
    }
    false
}

/// 路径是否经过 node_modules 等 "Low Priority Zone"。
fn path_in_node_zone(path: &str) -> bool {
    let lower = path.to_lowercase();
    for seg in lower.split(['/', '\\']) {
        if seg.is_empty() {
            continue;
        }
        for nz in NODE_ZONE_NAMES {
            if seg == *nz {
                return true;
            }
        }
    }
    false
}

// ─── Helper: word boundary detection ────────────────────────────────────────

/// 判断字符是否为"完美边界"分隔符（`.` 或 `/`/`\`）。
/// 当匹配起始位紧跟此类字符时，整体匹配质量翻倍。
#[inline]
fn is_perfect_boundary(c: u8) -> bool {
    matches!(c, b'.' | b'/' | b'\\')
}

/// 判断字符是否为"单词边界分隔符"。
#[inline]
fn is_boundary_char(c: u8) -> bool {
    matches!(c, b'/' | b'\\' | b'.' | b'-' | b'_' | b' ')
}

/// 为字符串中每个字节位置标记"位格"分数。
///
/// 高位格条件：
/// - 字符串首字母
/// - 紧跟在 `/`, `\`, `.`, `-`, `_`, ` ` 之后的首字母
/// - CamelCase 中的大写字母（前一个字符为小写）
///
/// 返回 `(boundary_score, camel_score)` 的数组（与 haystack 字节等长）。
fn compute_position_bonuses(haystack: &[u8]) -> Vec<(f64, f64)> {
    let len = haystack.len();
    let mut bonuses = vec![(0.0_f64, 0.0_f64); len];

    for i in 0..len {
        // 字符串首字母
        if i == 0 {
            bonuses[i].0 = STRING_START_BONUS;
            continue;
        }
        let prev = haystack[i - 1];
        let cur = haystack[i];

        // 紧跟在边界字符之后
        if is_boundary_char(prev) {
            bonuses[i].0 = BOUNDARY_BONUS_PER_CHAR;
        }

        // CamelCase: 前一字符小写，当前字符大写
        if prev.is_ascii_lowercase() && cur.is_ascii_uppercase() {
            bonuses[i].1 = CAMEL_BOUNDARY_BONUS_PER_CHAR;
        }
    }
    bonuses
}

/// 计算 query 在 haystack 中的"边界感知匹配质量"。
///
/// 扫描 haystack 中所有 case-insensitive 出现的 needle，对每个匹配位置：
/// - 累加每个匹配字符的 boundary + camel 位格分数
/// - 取最高分的匹配实例
///
/// 返回 `(best_boundary_bonus, match_found_in_basename, has_perfect_boundary)`。
/// `has_perfect_boundary` 为 true 当最佳匹配位置的前一个字符是 `.` 或 `/`。
fn boundary_aware_match(path: &str, basename: &str, needle: &str) -> (f64, bool, bool) {
    if needle.is_empty() {
        return (0.0, false, false);
    }

    let path_bytes = path.as_bytes();
    let needle_lower: Vec<u8> = needle.bytes().map(|b| b.to_ascii_lowercase()).collect();
    let path_lower: Vec<u8> = path_bytes.iter().map(|b| b.to_ascii_lowercase()).collect();

    let bonuses = compute_position_bonuses(path_bytes);

    let mut best_score = 0.0_f64;
    let mut best_perfect = false;
    let mut found_in_basename = false;

    // 找到 basename 在 path 中的字节起始位置
    let basename_start = if path.len() >= basename.len() {
        path.len() - basename.len()
    } else {
        0
    };

    // 扫描所有 case-insensitive substring 匹配
    let nlen = needle_lower.len();
    if nlen > path_lower.len() {
        return (0.0, false, false);
    }

    for start in 0..=(path_lower.len() - nlen) {
        let mut matched = true;
        for j in 0..nlen {
            if path_lower[start + j] != needle_lower[j] {
                matched = false;
                break;
            }
        }
        if !matched {
            continue;
        }

        // 累加此匹配实例的位格分数
        let mut instance_score = 0.0;
        for j in 0..nlen {
            let (bnd, camel) = bonuses[start + j];
            // 只有匹配首字符命中高位格时才给高分
            if j == 0 {
                instance_score += bnd + camel;
            }
        }

        // 检测"完美边界"：匹配前一个字符是 `.` 或 `/`
        let perfect = start > 0 && is_perfect_boundary(path_bytes[start - 1]);

        if instance_score > best_score {
            best_score = instance_score;
            best_perfect = perfect;
        } else if instance_score == best_score && perfect && !best_perfect {
            // 相同分数时优先选择完美边界的实例
            best_perfect = perfect;
        }

        // 检查是否命中在 basename 中
        if start >= basename_start {
            found_in_basename = true;
        }
    }

    (best_score, found_in_basename, best_perfect)
}

// ─── Main scoring function ──────────────────────────────────────────────────

/// 多维启发式评分 (Multi-factor Heuristics)。
///
/// 核心公式：
///   FinalScore = (MatchQuality * BasenameMultiplier) + BoundaryBonus
///                - LengthPenalty - ContextPenalty
///
/// 评分维度：
///
/// | 维度              | 逻辑描述                                                                 |
/// |:------------------|:-------------------------------------------------------------------------|
/// | Basename 权重     | 匹配在文件名中时，权重 × 2.5                                            |
/// | 边界加成          | 匹配首字符紧跟 `/`, `.`, `-`, `_` → 高额加分                            |
/// | CamelCase 加成    | 小写→大写过渡位置匹配 → 额外加分                                        |
/// | 精确 stem 匹配    | query == basename_stem → +60                                            |
/// | 前缀匹配          | basename.starts_with(query) → +40                                       |
/// | Smart Dot-file    | query 含 `.` 或匹配在文件名中 → 取消隐藏目录降权                        |
/// | 长度惩罚          | score -= filename.length × 0.1（短文件优先）                            |
/// | node_modules 隔离 | query 不含 "node" 时，node_modules 下文件权重 × 0.1                     |
/// | 噪声目录惩罚      | target/cache/vendor 等 → -200                                           |
/// | 近期修改加分      | 7 天内修改 → +15                                                        |
/// | 深度仲裁          | 仅作为 Tie-breaker，每层 -0.5                                           |
///
/// 当用户指定了路径+文件名（query 含 / 或 \）时，跳过深度和噪声目录惩罚。
pub fn score_result(meta: &FileMeta, config: &ScoreConfig) -> i64 {
    let path_str = meta.path.to_string_lossy();
    let mut score: f64 = BASE_SCORE;

    // ── 提取 basename ──
    let basename = meta
        .path
        .file_name()
        .map(|n| n.to_string_lossy().into_owned())
        .unwrap_or_default();
    let basename_lower = basename.to_lowercase();

    let q = &config.query_basename;

    // ── 1) 匹配质量 + 边界加成 + Basename 乘子 ──
    if !q.is_empty() {
        // 边界感知匹配：扫描整个路径，找到最佳匹配位置的位格分数
        let (boundary_bonus, matched_in_basename, has_perfect_boundary) =
            boundary_aware_match(&path_str, &basename, q);

        // 基础匹配质量：basename 中是否包含 query
        let mut match_quality: f64 = 0.0;

        if basename_lower.contains(q.as_str()) {
            match_quality += 60.0;
        }

        // 精确 stem 匹配
        let name_stem = basename_lower
            .rsplit_once('.')
            .map(|(s, _)| s)
            .unwrap_or(&basename_lower);
        if name_stem == q.as_str() {
            match_quality += EXACT_STEM_BONUS;
        }

        // 前缀匹配
        if basename_lower.starts_with(q.as_str()) {
            match_quality += PREFIX_BONUS;
        }

        // Basename 乘子：匹配命中在文件名中时，质量 × 2.5
        if matched_in_basename || basename_lower.contains(q.as_str()) {
            match_quality *= BASENAME_MULTIPLIER;
        }

        // "完美边界"翻倍：匹配前一个字符是 `.` 或 `/` 时，整体质量 × 2
        // 效果：搜 `config.json` 在 `.config.json` 中命中 `.` 后位置，分数翻倍；
        //       在 `some_folder/config/readme.txt` 中的路径段匹配也因 `/` 而翻倍。
        if has_perfect_boundary {
            match_quality *= PERFECT_BOUNDARY_MULTIPLIER;
        }

        score += match_quality + boundary_bonus;
    }

    // ── 2) 文件名长度惩罚（Tie-break 用途，短文件优先）──
    let name_len = basename.len() as f64;
    score -= name_len * LENGTH_PENALTY_FACTOR;

    // ── 3) 近期修改加分（7 天内）──
    if let Some(mtime) = meta.mtime {
        if let Ok(elapsed) = mtime.elapsed() {
            if elapsed < std::time::Duration::from_secs(7 * 86400) {
                score += RECENT_MTIME_BONUS;
            }
        }
    }

    // ── 4) 上下文惩罚（仅当 query 不含路径提示时生效）──
    if !config.has_path_hint {
        // 4a) Smart Dot-file 处理
        //   - 若 query 含 '.' → 取消所有隐藏目录降权
        //   - 若匹配在文件名中 → 取消隐藏目录降权
        //   - 只有 .file 作为路径"背景板"时才降权
        let basename_has_match = !q.is_empty() && basename_lower.contains(q.as_str());
        let suppress_hidden_penalty = config.query_has_dot || basename_has_match;

        if !suppress_hidden_penalty && path_has_hidden_dir(&path_str) {
            score -= HIDDEN_DIR_PENALTY;
        }

        // 4b) node_modules "物理隔离"
        //   除非 query 含 "node"，否则 node_modules 下的权重直接 × 0.1
        if path_in_node_zone(&path_str) && !config.query_has_node {
            // 将当前累积的正向分数压缩到 10%
            if score > 0.0 {
                score *= NODE_ZONE_MULTIPLIER;
            }
        }
        // 4c) 其他噪声目录惩罚（非 node_modules 的 junk dirs）
        else if path_in_junk_dir(&path_str) && !path_in_node_zone(&path_str) {
            score -= JUNK_DIR_PENALTY;
        }

        // 4d) 深度仅作为"最后的仲裁"（Tie-breaker）
        let depth =
            path_str.matches('/').count() + path_str.matches('\\').count();
        score -= (depth as f64) * DEPTH_TIEBREAKER_FACTOR;
    }

    score.round() as i64
}

// ─── Highlight computation (unchanged) ──────────────────────────────────────

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
        // Advance by at least one UTF-8 character to avoid landing inside a multi-byte char
        let advance = h_lower[abs_pos..].chars().next().map(|c| c.len_utf8()).unwrap_or(1);
        start = abs_pos + advance;
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
    let query_segments: Vec<&str> = query
        .split(['/', '\\'])
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

// ─── Tests ──────────────────────────────────────────────────────────────────

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

    // ── 基础行为：浅层 > 深层（深度作为 tiebreaker）──

    #[test]
    fn score_shallow_path_higher_than_deep() {
        let config = ScoreConfig::from_query("test");
        let shallow = meta("/home/test.txt", 100, None);
        let deep = meta("/home/user/projects/subdir/test.txt", 100, None);
        assert!(score_result(&shallow, &config) > score_result(&deep, &config));
    }

    // ── Basename 匹配加分 ──

    #[test]
    fn score_basename_match_bonus() {
        let config = ScoreConfig::from_query("test");
        let matched = meta("/a/test.txt", 100, None);
        let not_matched = meta("/a/other.txt", 100, None);
        assert!(score_result(&matched, &config) > score_result(&not_matched, &config));
    }

    // ── 前缀匹配 > 中间包含 ──

    #[test]
    fn score_prefix_match_bonus() {
        let config = ScoreConfig::from_query("test");
        let prefix = meta("/a/test_file.txt", 100, None);
        let contains = meta("/a/mytest.txt", 100, None);
        assert!(score_result(&prefix, &config) > score_result(&contains, &config));
    }

    // ── 短文件名优先 ──

    #[test]
    fn score_long_filename_penalty() {
        let config = ScoreConfig::from_query("test");
        let short = meta("/a/test.txt", 100, None);
        let long = meta(
            "/a/test_with_a_very_long_filename_that_goes_on_forever.txt",
            100,
            None,
        );
        assert!(score_result(&short, &config) > score_result(&long, &config));
    }

    // ── 近期修改加分 ──

    #[test]
    fn score_recent_file_bonus() {
        let config = ScoreConfig::from_query("test");
        let recent = meta("/a/test.txt", 100, Some(SystemTime::now()));
        let old = meta("/a/test.txt", 100, None);
        assert!(score_result(&recent, &config) > score_result(&old, &config));
    }

    // ── node_modules 智能隔离 ──

    #[test]
    fn score_node_modules_penalized() {
        let config = ScoreConfig::from_query("index");
        let normal = meta("/home/project/src/index.ts", 100, None);
        let node = meta("/home/project/node_modules/lib/index.js", 100, None);
        let s_normal = score_result(&normal, &config);
        let s_node = score_result(&node, &config);
        assert!(
            s_normal > s_node + 100,
            "normal={s_normal} should be >> node={s_node}"
        );
    }

    #[test]
    fn score_node_modules_not_penalized_when_query_contains_node() {
        // 当 query 含 "node" 时，node_modules 不应被 × 0.1
        let config = ScoreConfig::from_query("node");
        assert!(config.query_has_node);
        let nm_file = meta("/home/project/node_modules/express/index.js", 100, None);
        let s = score_result(&nm_file, &config);
        // 不应被压缩到极低分
        assert!(s > 50, "node_modules file with node query should not be suppressed, got {s}");
    }

    // ── 其他噪声目录惩罚 ──

    #[test]
    fn score_target_dir_penalized() {
        let config = ScoreConfig::from_query("main");
        let src = meta("/home/project/src/main.rs", 100, None);
        let target = meta("/home/project/target/debug/build/main.rs", 100, None);
        assert!(score_result(&src, &config) > score_result(&target, &config));
    }

    // ── Smart Dot-file 处理 ──

    #[test]
    fn score_hidden_dir_penalized() {
        // 当 query 不匹配文件名时，隐藏目录应被降权
        let config = ScoreConfig::from_query("data");
        let visible = meta("/home/project/data.csv", 100, None);
        let hidden_dir = meta("/home/.hidden/data.csv", 100, None);
        let s_visible = score_result(&visible, &config);
        let s_hidden = score_result(&hidden_dir, &config);
        // 两者都匹配 basename，smart dot-file 豁免了隐藏目录惩罚（这是正确行为）。
        // 用 query 不命中 basename 的场景验证隐藏目录降权仍然生效：
        let _s_visible = s_visible;
        let _s_hidden = s_hidden;
        let config2 = ScoreConfig::from_query("xyz");
        let v2 = meta("/home/project/other.txt", 100, None);
        let h2 = meta("/home/.secret/other.txt", 100, None);
        let sv2 = score_result(&v2, &config2);
        let sh2 = score_result(&h2, &config2);
        assert!(
            sv2 > sh2,
            "visible={sv2} should > hidden_dir={sh2} when query doesn't match basename"
        );
    }

    #[test]
    fn score_hidden_filename_not_penalized() {
        // 文件名以.开头（如 .eslintrc）不应因文件名而被惩罚
        let config = ScoreConfig::from_query("eslintrc");
        let hidden_file = meta("/home/project/.eslintrc", 100, None);
        let s = score_result(&hidden_file, &config);
        // 应该和同深度、无隐藏目录的文件得分一致（basename匹配加分）
        assert!(s > 100, "hidden filename score={s} should be positive");
    }

    #[test]
    fn score_hidden_dir_not_penalized_when_query_has_dot() {
        // query 含 '.' 时，隐藏目录不应降权
        let config = ScoreConfig::from_query(".config");
        assert!(config.query_has_dot);
        let hidden_dir_file = meta("/home/.config/settings.json", 100, None);
        let s = score_result(&hidden_dir_file, &config);
        assert!(
            s >= 90,
            "hidden dir with dot-query should not be penalized, got {s}"
        );
    }

    #[test]
    fn score_hidden_dir_not_penalized_when_match_in_basename() {
        // 当匹配在文件名中时，路径中的隐藏目录不应降权
        let config = ScoreConfig::from_query("settings");
        let hidden_path = meta("/home/.config/settings.json", 100, None);
        let s = score_result(&hidden_path, &config);
        // basename 匹配到了 settings，所以不应受 .config 目录惩罚
        assert!(
            s > 100,
            "basename match should suppress hidden dir penalty, got {s}"
        );
    }

    // ── 深度作为 Tiebreaker（不再是主评分因素）──

    #[test]
    fn score_deep_nesting_uses_depth_as_tiebreaker() {
        let config = ScoreConfig::from_query("test");
        let depth3 = meta("/a/b/c/test.txt", 100, None);
        let depth8 = meta("/a/b/c/d/e/f/g/h/test.txt", 100, None);
        let s3 = score_result(&depth3, &config);
        let s8 = score_result(&depth8, &config);
        // 深度差 5 层，每层 -0.5，差值约 2-3 分（而非旧方案的 50+ 分）
        assert!(
            s3 > s8,
            "depth3={s3} should be > depth8={s8} (tiebreaker)"
        );
        // 差距不应太大——证明深度是 tiebreaker 而非主因
        let gap = s3 - s8;
        assert!(
            gap < 20,
            "depth gap should be small (tiebreaker), got {gap}"
        );
    }

    // ── 路径提示跳过惩罚 ──

    #[test]
    fn score_path_hint_skips_depth_penalty() {
        // 当用户指定了路径+文件名时，不应受深度惩罚
        let config = ScoreConfig::from_query("src/main");
        assert!(config.has_path_hint);
        let deep = meta("/home/project/src/main.rs", 100, None);
        let s = score_result(&deep, &config);
        // 不应有大幅的深度惩罚
        assert!(s > 50, "score with path hint should be high, got {s}");
    }

    // ── 边界加成测试 ──

    #[test]
    fn score_boundary_match_after_dot_gets_bonus() {
        // 搜索 "json" 时，`.config.json` 的 `j` 命中 `.` 后高位格
        let config = ScoreConfig::from_query("json");
        let dotfile = meta("/home/project/.config.json", 100, None);
        let embedded = meta("/home/project/myjsonfile.txt", 100, None);
        let s_dot = score_result(&dotfile, &config);
        let s_emb = score_result(&embedded, &config);
        assert!(
            s_dot > s_emb,
            ".config.json={s_dot} should rank higher than myjsonfile={s_emb}"
        );
    }

    #[test]
    fn score_boundary_match_after_underscore() {
        // 搜索 "config" 时，`app_config.toml` 中的 `config` 紧跟 `_`
        let config = ScoreConfig::from_query("config");
        let underscore = meta("/a/app_config.toml", 100, None);
        let embedded = meta("/a/appconfigurator.toml", 100, None);
        let s_under = score_result(&underscore, &config);
        let s_emb = score_result(&embedded, &config);
        assert!(
            s_under > s_emb,
            "app_config={s_under} should rank higher than appconfigurator={s_emb}"
        );
    }

    #[test]
    fn score_camel_case_boundary_bonus() {
        // 搜索 "Config" 时，CamelCase 中的 C 命中小写→大写过渡
        let config = ScoreConfig::from_query("Config");
        let camel = meta("/a/AppConfig.ts", 100, None);
        let flat = meta("/a/appconfig.ts", 100, None);
        let s_camel = score_result(&camel, &config);
        let s_flat = score_result(&flat, &config);
        assert!(
            s_camel > s_flat,
            "AppConfig={s_camel} should rank higher than appconfig={s_flat}"
        );
    }

    #[test]
    fn score_perfect_boundary_dot_doubles_quality() {
        // 搜索 "config.json" 时，`.config.json` 的匹配紧跟 `.`，分数翻倍
        let config = ScoreConfig::from_query("config.json");
        let dot_before = meta("/home/project/.config.json", 100, None);
        let no_dot = meta("/home/project/myconfig.json", 100, None);
        let s_dot = score_result(&dot_before, &config);
        let s_no = score_result(&no_dot, &config);
        assert!(
            s_dot > s_no,
            ".config.json={s_dot} should rank higher than myconfig.json={s_no} (perfect boundary × 2)"
        );
    }

    #[test]
    fn score_perfect_boundary_slash_doubles_quality() {
        // 匹配紧跟 `/` 时也触发翻倍（路径段起始位置）
        let config = ScoreConfig::from_query("readme");
        let at_slash = meta("/a/readme.md", 100, None);
        let embedded = meta("/a/myreadme.md", 100, None);
        let s_slash = score_result(&at_slash, &config);
        let s_emb = score_result(&embedded, &config);
        assert!(
            s_slash > s_emb,
            "readme.md={s_slash} should rank higher than myreadme.md={s_emb} (perfect boundary after /)"
        );
    }

    // ── ScoreConfig 感知测试 ──

    #[test]
    fn score_config_detects_dot_query() {
        let c = ScoreConfig::from_query(".eslintrc");
        assert!(c.query_has_dot);
        assert_eq!(c.query_basename, ".eslintrc");
    }

    #[test]
    fn score_config_detects_node_query() {
        let c = ScoreConfig::from_query("node_modules");
        assert!(c.query_has_node);
    }

    #[test]
    fn score_config_extracts_basename_from_path_query() {
        let c = ScoreConfig::from_query("src/components/Button");
        assert!(c.has_path_hint);
        assert_eq!(c.query_basename, "button");
    }

    // ── Highlight 测试（行为不变）──

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
        assert_eq!(h[0], [1, 4]); // "tmp"
        assert_eq!(h[1], [5, 8]); // "tes"
        assert_eq!(h[2], [15, 18]); // "new"
    }

    #[test]
    fn highlight_empty_query() {
        let h = compute_highlights("/home/user/test.txt", "");
        assert!(h.is_empty());
    }

    #[test]
    fn highlight_chinese_substring() {
        let h = compute_highlights("/tmp/中文文档.txt", "文档");
        assert_eq!(h, vec![[11, 17]], "文档 is 6 bytes, should highlight at correct byte positions");
    }

    #[test]
    fn highlight_chinese_multiple_occurrences() {
        let h = compute_highlights("/中文文档/中文文件.txt", "中文");
        assert_eq!(h, vec![[1, 7], [14, 20]], "中文 is 6 bytes each");
    }

    #[test]
    fn highlight_chinese_path_initials() {
        let h = compute_highlights("/tmp/中文目录/文档备份.txt", "tmp/中文/文档");
        assert_eq!(h.len(), 3);
        assert_eq!(h[0], [1, 4]);   // "tmp"
        assert_eq!(h[1], [5, 11]);  // "中文"
        assert_eq!(h[2], [18, 24]); // "文档"
    }
}