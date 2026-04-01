use std::path::Path;
use std::sync::Arc;

/// Glob 匹配模式
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum GlobMode {
    /// 对完整路径字符串做 glob 匹配（用户模式含路径分隔符时）
    FullPath,
    /// 按文件名/路径段匹配（用户模式不含路径分隔符时）
    Segment,
}

/// Matcher 抽象接口，定义匹配行为
pub trait Matcher: Send + Sync {
    /// 判断路径是否匹配
    fn matches(&self, path: &str) -> bool;
    /// 匹配是否大小写敏感（Smart-Case 支持）
    fn case_sensitive(&self) -> bool {
        true
    }
    /// 获取用于前缀过滤的固定前缀（如果有）
    fn prefix(&self) -> Option<&str> {
        None
    }
    /// 获取可用于 trigram 候选预过滤的 literal 提示（可选）。
    ///
    /// 约束：
    /// - 必须不包含路径分隔符（`/`、`\\`），否则会破坏“组件索引”的超集性质
    /// - 允许返回长度 < 3 的 hint；调用方应做长度门槛并回退全扫
    fn literal_hint(&self) -> Option<&[u8]> {
        None
    }
    /// 获取 glob 模式（非 glob 匹配器返回 None）
    fn glob_mode(&self) -> Option<GlobMode> {
        None
    }
}

/// 精确包含匹配 (contains)
pub struct ExactMatcher {
    pattern: String,
    case_sensitive: bool,
}

impl ExactMatcher {
    pub fn new(pattern: &str, case_sensitive: bool) -> Self {
        Self {
            pattern: pattern.to_string(),
            case_sensitive,
        }
    }
}

impl Matcher for ExactMatcher {
    fn matches(&self, path: &str) -> bool {
        if self.case_sensitive {
            return path.contains(&self.pattern);
        }
        contains_ascii_insensitive(path, &self.pattern)
    }

    fn case_sensitive(&self) -> bool {
        self.case_sensitive
    }

    fn literal_hint(&self) -> Option<&[u8]> {
        // contains 语义可能跨组件（例如 "/a/b"），无法保证组件 trigram 的超集过滤；
        // 因此当 pattern 含分隔符时显式禁用 hint，回退全扫。
        if contains_path_separator(&self.pattern) {
            return None;
        }
        Some(self.pattern.as_bytes())
    }
}

/// 通配符匹配 (Glob)
pub struct GlobMatcher {
    pattern: Vec<char>,
    prefix: Option<String>,
    mode: GlobMode,
    literal_hint: Option<Vec<u8>>,
    case_sensitive: bool,
}

impl GlobMatcher {
    pub fn new(pattern: &str, mode: GlobMode, case_sensitive: bool) -> Self {
        // 提取通配符前的固定前缀
        let prefix = pattern
            .split(|c| c == '*' || c == '?')
            .next()
            .filter(|s| !s.is_empty())
            .map(|s| s.to_string());

        let literal_hint = extract_longest_literal_hint(pattern);

        Self {
            pattern: simplify_glob_pattern(pattern),
            prefix,
            mode,
            literal_hint,
            case_sensitive,
        }
    }
}

impl Matcher for GlobMatcher {
    fn matches(&self, path: &str) -> bool {
        match self.mode {
            GlobMode::FullPath => glob_matches(&self.pattern, path, self.case_sensitive),
            GlobMode::Segment => {
                let p = Path::new(path);
                // 优先匹配 basename（命中率高、成本低）
                if let Some(name) = p.file_name().and_then(|n| n.to_str()) {
                    if glob_matches(&self.pattern, name, self.case_sensitive) {
                        return true;
                    }
                }
                // 再遍历其余路径段
                for component in p.components() {
                    let seg = component.as_os_str().to_string_lossy();
                    if glob_matches(&self.pattern, &seg, self.case_sensitive) {
                        return true;
                    }
                }
                false
            }
        }
    }

    fn case_sensitive(&self) -> bool {
        self.case_sensitive
    }

    fn prefix(&self) -> Option<&str> {
        self.prefix.as_deref()
    }

    fn literal_hint(&self) -> Option<&[u8]> {
        self.literal_hint.as_deref()
    }

    fn glob_mode(&self) -> Option<GlobMode> {
        Some(self.mode)
    }
}

/// 判断模式是否包含路径分隔符
pub fn contains_path_separator(pattern: &str) -> bool {
    pattern.contains('/') || pattern.contains('\\')
}

fn is_literal_delim(b: u8) -> bool {
    matches!(b, b'/' | b'\\' | b'*' | b'?')
}

fn fold_ascii_char(c: char) -> char {
    c.to_ascii_lowercase()
}

fn chars_equal(a: char, b: char, case_sensitive: bool) -> bool {
    if case_sensitive {
        a == b
    } else {
        fold_ascii_char(a) == fold_ascii_char(b)
    }
}

fn simplify_glob_pattern(pattern: &str) -> Vec<char> {
    let mut out: Vec<char> = Vec::with_capacity(pattern.len());
    let mut prev_star = false;
    for c in pattern.chars() {
        if c == '*' {
            if prev_star {
                continue;
            }
            prev_star = true;
            out.push(c);
            continue;
        }
        prev_star = false;
        out.push(c);
    }
    out
}

/// Glob 匹配：`*`/`?`，匹配整个字符串（与 wildmatch 的语义一致）。
fn glob_matches(pattern: &[char], input: &str, case_sensitive: bool) -> bool {
    if pattern.is_empty() {
        return input.is_empty();
    }

    let mut input_chars = input.chars();

    let mut pattern_idx = 0usize;
    if let Some(mut input_char) = input_chars.next() {
        const NONE: usize = usize::MAX;
        let mut start_idx = NONE;
        let mut matched = "".chars();

        loop {
            if pattern_idx < pattern.len() && pattern[pattern_idx] == '*' {
                start_idx = pattern_idx;
                matched = input_chars.clone();
                pattern_idx += 1;
            } else if pattern_idx < pattern.len()
                && (pattern[pattern_idx] == '?'
                    || chars_equal(pattern[pattern_idx], input_char, case_sensitive))
            {
                pattern_idx += 1;
                if let Some(next_char) = input_chars.next() {
                    input_char = next_char;
                } else {
                    break;
                }
            } else if start_idx != NONE {
                pattern_idx = start_idx + 1;
                if let Some(next_char) = matched.next() {
                    input_char = next_char;
                } else {
                    break;
                }
                input_chars = matched.clone();
            } else {
                return false;
            }
        }
    }

    while pattern_idx < pattern.len() && pattern[pattern_idx] == '*' {
        pattern_idx += 1;
    }

    pattern_idx == pattern.len()
}

fn fold_ascii_byte(b: u8) -> u8 {
    b.to_ascii_lowercase()
}

fn contains_ascii_insensitive(haystack: &str, needle: &str) -> bool {
    if needle.is_empty() {
        return true;
    }
    let h = haystack.as_bytes();
    let n = needle.as_bytes();
    if n.len() > h.len() {
        return false;
    }
    for i in 0..=h.len() - n.len() {
        let mut ok = true;
        for j in 0..n.len() {
            if fold_ascii_byte(h[i + j]) != fold_ascii_byte(n[j]) {
                ok = false;
                break;
            }
        }
        if ok {
            return true;
        }
    }
    false
}

/// 安全 literal 提取：把 `/`、`\\`、`*`、`?` 统一视为分隔符，提取最长连续片段（长度 >= 3）。
fn extract_longest_literal_hint(pattern: &str) -> Option<Vec<u8>> {
    let bytes = pattern.as_bytes();
    let mut best: Option<(usize, usize)> = None; // (start, len)
    let mut start = 0usize;

    for i in 0..=bytes.len() {
        let at_end = i == bytes.len();
        if at_end || is_literal_delim(bytes[i]) {
            let len = i.saturating_sub(start);
            if len >= 3 {
                match best {
                    Some((_, best_len)) if best_len >= len => {}
                    _ => best = Some((start, len)),
                }
            }
            start = i.saturating_add(1);
        }
    }

    best.map(|(s, len)| bytes[s..s + len].to_vec())
}

/// 匹配器工厂与自动识别
pub fn create_matcher(pattern: &str, case_sensitive: bool) -> Arc<dyn Matcher> {
    if pattern.contains('*') || pattern.contains('?') {
        let mode = if contains_path_separator(pattern) {
            GlobMode::FullPath
        } else {
            GlobMode::Segment
        };
        Arc::new(GlobMatcher::new(pattern, mode, case_sensitive))
    } else {
        Arc::new(ExactMatcher::new(pattern, case_sensitive))
    }
}

/// 路径作用域：用于 wfn/regex 等“basename vs fullpath”感应。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PathScope {
    FullPath,
    Basename,
}

/// wfn: 完整文件名匹配（默认 basename；含分隔符则 fullpath）
pub struct WfnMatcher {
    pattern: String,
    scope: PathScope,
    case_sensitive: bool,
}

impl WfnMatcher {
    pub fn new(pattern: &str, scope: PathScope, case_sensitive: bool) -> Self {
        Self {
            pattern: pattern.to_string(),
            scope,
            case_sensitive,
        }
    }
}

impl Matcher for WfnMatcher {
    fn matches(&self, path: &str) -> bool {
        let target: std::borrow::Cow<'_, str> = match self.scope {
            PathScope::FullPath => std::borrow::Cow::Borrowed(path),
            PathScope::Basename => {
                let p = Path::new(path);
                let Some(name) = p.file_name().and_then(|n| n.to_str()) else {
                    return false;
                };
                std::borrow::Cow::Borrowed(name)
            }
        };

        if self.case_sensitive {
            target.as_ref() == self.pattern
        } else {
            target.as_ref().eq_ignore_ascii_case(&self.pattern)
        }
    }

    fn case_sensitive(&self) -> bool {
        self.case_sensitive
    }

    fn literal_hint(&self) -> Option<&[u8]> {
        if contains_path_separator(&self.pattern) {
            return None;
        }
        Some(self.pattern.as_bytes())
    }
}

/// regex: 正则匹配（默认 basename；含分隔符则 fullpath）
pub struct RegexMatcher {
    re: regex::Regex,
    scope: PathScope,
    case_sensitive: bool,
}

impl RegexMatcher {
    pub fn new(re: regex::Regex, scope: PathScope, case_sensitive: bool) -> Self {
        Self {
            re,
            scope,
            case_sensitive,
        }
    }
}

impl Matcher for RegexMatcher {
    fn matches(&self, path: &str) -> bool {
        match self.scope {
            PathScope::FullPath => self.re.is_match(path),
            PathScope::Basename => {
                let p = Path::new(path);
                let Some(name) = p.file_name().and_then(|n| n.to_str()) else {
                    return false;
                };
                self.re.is_match(name)
            }
        }
    }

    fn case_sensitive(&self) -> bool {
        self.case_sensitive
    }
}

/// ext: / doc:/pic:/video: 过滤器的 anchor matcher（按扩展名匹配）。
pub struct ExtMatcher {
    exts_lc: Vec<Vec<u8>>,
    case_sensitive: bool,
}

impl ExtMatcher {
    pub fn new(exts_lc: Vec<Vec<u8>>, case_sensitive: bool) -> Self {
        Self {
            exts_lc,
            // 扩展名过滤通常应忽略大小写；这里仍按 query 的 case_sensitive 记录，以便 L1 启发式一致。
            case_sensitive,
        }
    }
}

impl Matcher for ExtMatcher {
    fn matches(&self, path: &str) -> bool {
        let p = Path::new(path);
        let Some(ext) = p.extension() else {
            return false;
        };
        let ext = ext.to_string_lossy();
        let ext_lc: Vec<u8> = ext
            .as_bytes()
            .iter()
            .map(|b| b.to_ascii_lowercase())
            .collect();
        self.exts_lc.iter().any(|e| *e == ext_lc)
    }

    fn case_sensitive(&self) -> bool {
        self.case_sensitive
    }

    fn literal_hint(&self) -> Option<&[u8]> {
        if self.exts_lc.len() == 1 {
            let e = &self.exts_lc[0];
            if e.len() >= 3 {
                return Some(e.as_slice());
            }
        }
        None
    }
}

/// Match-all：用于无正向 anchor 的退化扫描。
pub struct MatchAllMatcher;

impl Matcher for MatchAllMatcher {
    fn matches(&self, _path: &str) -> bool {
        true
    }

    fn case_sensitive(&self) -> bool {
        false
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // ── create_matcher 路由判定 ──

    #[test]
    fn segment_mode_when_no_separator() {
        let m = create_matcher("test_*", true);
        assert_eq!(m.glob_mode(), Some(GlobMode::Segment));
    }

    #[test]
    fn fullpath_mode_when_slash_present() {
        let m = create_matcher("/tmp/vcptest/test_*", true);
        assert_eq!(m.glob_mode(), Some(GlobMode::FullPath));
    }

    #[test]
    fn fullpath_mode_when_backslash_present() {
        let m = create_matcher("C:\\tmp\\vcptest\\test_*", true);
        assert_eq!(m.glob_mode(), Some(GlobMode::FullPath));
    }

    #[test]
    fn exact_matcher_no_glob_mode() {
        let m = create_matcher("hello", true);
        assert_eq!(m.glob_mode(), None);
    }

    // ── Segment 匹配语义 ──

    #[test]
    fn segment_matches_basename() {
        // test_* 应命中 basename 为 test_123 的路径
        let m = create_matcher("test_*", true);
        assert!(m.matches("/tmp/vcptest/test_123"));
    }

    #[test]
    fn segment_rejects_partial_basename() {
        // test_* 不应命中 basename 为 attest_123 的路径（不以 test_ 开头）
        let m = create_matcher("test_*", true);
        assert!(!m.matches("/tmp/vcptest/attest_123"));
    }

    #[test]
    fn segment_matches_question_mark() {
        let m = create_matcher("test_12?", true);
        assert!(m.matches("/tmp/vcptest/test_123"));
        assert!(!m.matches("/tmp/vcptest/test_1234"));
    }

    #[test]
    fn segment_matches_directory_component() {
        // 段匹配也能命中中间目录段
        let m = create_matcher("vcp*", true);
        assert!(m.matches("/tmp/vcptest/somefile.txt"));
    }

    // ── FullPath 匹配语义 ──

    #[test]
    fn fullpath_matches_absolute() {
        let m = create_matcher("/tmp/vcptest/test_*", true);
        assert!(m.matches("/tmp/vcptest/test_123"));
    }

    #[test]
    fn fullpath_rejects_different_dir() {
        let m = create_matcher("/tmp/vcptest/test_*", true);
        assert!(!m.matches("/home/user/test_123"));
    }

    #[test]
    fn fullpath_with_structure() {
        let m = create_matcher("tmp/*/test_??", true);
        // 这是 FullPath 模式（含 /），对完整路径做 glob
        // wildmatch 默认 * 不跨 /，所以需要完整路径匹配
        assert!(m.matches("tmp/vcptest/test_12"));
    }

    // ── ExactMatcher（contains）语义不变 ──

    #[test]
    fn exact_contains_match() {
        let m = create_matcher("vcptest", true);
        assert!(m.matches("/tmp/vcptest/test_123"));
    }

    #[test]
    fn exact_contains_no_match() {
        let m = create_matcher("nonexistent", true);
        assert!(!m.matches("/tmp/vcptest/test_123"));
    }

    // ── literal_hint（trigram 候选预过滤提示）──

    #[test]
    fn exact_literal_hint_disabled_when_contains_separator() {
        let m = create_matcher("/some/deep/path", true);
        assert_eq!(m.literal_hint(), None);
    }

    #[test]
    fn glob_literal_hint_extracts_longest_and_never_includes_separator() {
        let m = create_matcher("/some/deep/path/*回忆录*", true);
        assert_eq!(m.literal_hint(), Some("回忆录".as_bytes()));
    }

    #[test]
    fn glob_literal_hint_none_when_all_literals_too_short() {
        let m = create_matcher("*ab*", true);
        assert_eq!(m.literal_hint(), None);
    }

    #[test]
    fn smart_case_insensitive_contains() {
        let m = create_matcher("vcp", false);
        assert!(m.matches("/tmp/VCPTest/main.rs"));
    }
}
