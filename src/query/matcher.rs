use wildmatch::WildMatch;
use std::sync::Arc;

/// Matcher 抽象接口，定义匹配行为
pub trait Matcher: Send + Sync {
    /// 判断路径是否匹配
    fn matches(&self, path: &str) -> bool;
    /// 获取用于前缀过滤的固定前缀（如果有）
    fn prefix(&self) -> Option<&str> {
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
}

impl GlobMatcher {
    pub fn new(pattern: &str) -> Self {
        // 提取通配符前的固定前缀
        let prefix = pattern
            .split(|c| c == '*' || c == '?')
            .next()
            .filter(|s| !s.is_empty())
            .map(|s| s.to_string());

        Self {
            wild: WildMatch::new(pattern),
            prefix,
        }
    }
}

impl Matcher for GlobMatcher {
    fn matches(&self, path: &str) -> bool {
        self.wild.matches(path)
    }

    fn prefix(&self) -> Option<&str> {
        self.prefix.as_deref()
    }
}

/// 匹配器工厂与自动识别
pub fn create_matcher(pattern: &str) -> Arc<dyn Matcher> {
    if pattern.contains('*') || pattern.contains('?') {
        Arc::new(GlobMatcher::new(pattern))
    } else {
        Arc::new(ExactMatcher::new(pattern))
    }
}