use std::path::Path;
use std::sync::Arc;
use wildmatch::WildMatch;

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
    /// 获取用于前缀过滤的固定前缀（如果有）
    fn prefix(&self) -> Option<&str> {
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
}

impl ExactMatcher {
    pub fn new(pattern: &str) -> Self {
        Self {
            pattern: pattern.to_string(),
        }
    }
}

impl Matcher for ExactMatcher {
    fn matches(&self, path: &str) -> bool {
        path.contains(&self.pattern)
    }
}

/// 通配符匹配 (Glob)
pub struct GlobMatcher {
    wild: WildMatch,
    prefix: Option<String>,
    mode: GlobMode,
}

impl GlobMatcher {
    pub fn new(pattern: &str, mode: GlobMode) -> Self {
        // 提取通配符前的固定前缀
        let prefix = pattern
            .split(|c| c == '*' || c == '?')
            .next()
            .filter(|s| !s.is_empty())
            .map(|s| s.to_string());

        Self {
            wild: WildMatch::new(pattern),
            prefix,
            mode,
        }
    }
}

impl Matcher for GlobMatcher {
    fn matches(&self, path: &str) -> bool {
        match self.mode {
            GlobMode::FullPath => self.wild.matches(path),
            GlobMode::Segment => {
                let p = Path::new(path);
                // 优先匹配 basename（命中率高、成本低）
                if let Some(name) = p.file_name().and_then(|n| n.to_str()) {
                    if self.wild.matches(name) {
                        return true;
                    }
                }
                // 再遍历其余路径段
                for component in p.components() {
                    let seg = component.as_os_str().to_string_lossy();
                    if self.wild.matches(&seg) {
                        return true;
                    }
                }
                false
            }
        }
    }

    fn prefix(&self) -> Option<&str> {
        self.prefix.as_deref()
    }

    fn glob_mode(&self) -> Option<GlobMode> {
        Some(self.mode)
    }
}

/// 判断模式是否包含路径分隔符
fn contains_path_separator(pattern: &str) -> bool {
    pattern.contains('/') || pattern.contains('\\')
}

/// 匹配器工厂与自动识别
pub fn create_matcher(pattern: &str) -> Arc<dyn Matcher> {
    if pattern.contains('*') || pattern.contains('?') {
        let mode = if contains_path_separator(pattern) {
            GlobMode::FullPath
        } else {
            GlobMode::Segment
        };
        Arc::new(GlobMatcher::new(pattern, mode))
    } else {
        Arc::new(ExactMatcher::new(pattern))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // ── create_matcher 路由判定 ──

    #[test]
    fn segment_mode_when_no_separator() {
        let m = create_matcher("test_*");
        assert_eq!(m.glob_mode(), Some(GlobMode::Segment));
    }

    #[test]
    fn fullpath_mode_when_slash_present() {
        let m = create_matcher("/tmp/vcptest/test_*");
        assert_eq!(m.glob_mode(), Some(GlobMode::FullPath));
    }

    #[test]
    fn fullpath_mode_when_backslash_present() {
        let m = create_matcher("C:\\tmp\\vcptest\\test_*");
        assert_eq!(m.glob_mode(), Some(GlobMode::FullPath));
    }

    #[test]
    fn exact_matcher_no_glob_mode() {
        let m = create_matcher("hello");
        assert_eq!(m.glob_mode(), None);
    }

    // ── Segment 匹配语义 ──

    #[test]
    fn segment_matches_basename() {
        // test_* 应命中 basename 为 test_123 的路径
        let m = create_matcher("test_*");
        assert!(m.matches("/tmp/vcptest/test_123"));
    }

    #[test]
    fn segment_rejects_partial_basename() {
        // test_* 不应命中 basename 为 attest_123 的路径（不以 test_ 开头）
        let m = create_matcher("test_*");
        assert!(!m.matches("/tmp/vcptest/attest_123"));
    }

    #[test]
    fn segment_matches_question_mark() {
        let m = create_matcher("test_12?");
        assert!(m.matches("/tmp/vcptest/test_123"));
        assert!(!m.matches("/tmp/vcptest/test_1234"));
    }

    #[test]
    fn segment_matches_directory_component() {
        // 段匹配也能命中中间目录段
        let m = create_matcher("vcp*");
        assert!(m.matches("/tmp/vcptest/somefile.txt"));
    }

    // ── FullPath 匹配语义 ──

    #[test]
    fn fullpath_matches_absolute() {
        let m = create_matcher("/tmp/vcptest/test_*");
        assert!(m.matches("/tmp/vcptest/test_123"));
    }

    #[test]
    fn fullpath_rejects_different_dir() {
        let m = create_matcher("/tmp/vcptest/test_*");
        assert!(!m.matches("/home/user/test_123"));
    }

    #[test]
    fn fullpath_with_structure() {
        let m = create_matcher("tmp/*/test_??");
        // 这是 FullPath 模式（含 /），对完整路径做 glob
        // wildmatch 默认 * 不跨 /，所以需要完整路径匹配
        assert!(m.matches("tmp/vcptest/test_12"));
    }

    // ── ExactMatcher（contains）语义不变 ──

    #[test]
    fn exact_contains_match() {
        let m = create_matcher("vcptest");
        assert!(m.matches("/tmp/vcptest/test_123"));
    }

    #[test]
    fn exact_contains_no_match() {
        let m = create_matcher("nonexistent");
        assert!(!m.matches("/tmp/vcptest/test_123"));
    }
}
