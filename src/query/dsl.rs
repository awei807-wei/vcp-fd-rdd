use crate::core::FileMeta;
use crate::query::filter::{
    doc_exts, normalize_ext_list, pic_exts, video_exts, Filter,
};
use crate::query::dsl_parser::{tokenize, Parser};
use crate::query::matcher::{
    contains_path_separator, create_matcher, ExtMatcher, MatchAllMatcher, Matcher,
    PathInitialsMatcher, PathScope, RegexMatcher, WfnMatcher,
};
use regex::{Regex, RegexBuilder};
use std::sync::Arc;

#[derive(Debug, Clone)]
pub enum Expr {
    Or(Vec<Expr>),
    And(Vec<Expr>),
    True,
    Atom(Atom),
}

#[derive(Debug, Clone)]
pub enum Atom {
    /// 默认：无通配符 => contains；带 `*`/`?` => glob（并沿用 matcher.rs 的 FullPath/Segment 判定）
    Text(String),
    /// wfn: 完整文件名匹配（默认 basename；含分隔符则 fullpath）
    Wfn(String),
    /// regex: 正则匹配（默认 basename；含分隔符则 fullpath）
    Regex(String),
    /// ext:js;py
    Ext(Vec<String>),
    /// doc:/pic:/video:
    Type(MediaKind),
    /// dm:today / dm:YYYY-MM-DD
    DateModified(DateRange),
    /// dc:today / dc:YYYY-MM-DD (date created / ctime)
    DateCreated(DateRange),
    /// da:today / da:YYYY-MM-DD (date accessed / atime)
    DateAccessed(DateRange),
    /// size:>10mb
    Size(SizeFilter),
    /// parent:/home/user  (parent directory equals path)
    Parent(String),
    /// depth:3 / depth:>2 (path separator count)
    Depth(CmpOp, usize),
    /// len:>30 (filename byte length)
    NameLen(CmpOp, usize),
    /// type:file / type:folder
    EntryType(EntryKind),
    /// content:keyword (全文搜索，占位)
    Content(String),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EntryKind {
    File,
    Folder,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MediaKind {
    Doc,
    Pic,
    Video,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CmpOp {
    Lt,
    Le,
    Eq,
    Ge,
    Gt,
}

#[derive(Debug, Clone, Copy)]
pub struct SizeFilter {
    pub op: CmpOp,
    pub bytes: u64,
}

#[derive(Debug, Clone, Copy)]
pub struct DateRange {
    pub start: std::time::SystemTime,
    pub end: std::time::SystemTime,
}

#[derive(Clone)]
pub struct CompiledQuery {
    pub case_sensitive: bool,
    anchors: Vec<Arc<dyn Matcher>>,
    include: CompiledExpr,
    excludes: Vec<CompiledExpr>,
}

impl CompiledQuery {
    pub fn anchors(&self) -> &[Arc<dyn Matcher>] {
        &self.anchors
    }

    pub fn matches(&self, meta: &FileMeta) -> bool {
        if !self.include.matches(meta) {
            return false;
        }
        for ex in &self.excludes {
            if ex.matches(meta) {
                return false;
            }
        }
        true
    }
}

#[derive(Clone)]
enum CompiledExpr {
    Or(Vec<CompiledExpr>),
    And(Vec<CompiledExpr>),
    True,
    Path(Arc<dyn Matcher>),
    Filter(Filter),
}

impl CompiledExpr {
    fn matches(&self, meta: &FileMeta) -> bool {
        match self {
            CompiledExpr::Or(v) => v.iter().any(|e| e.matches(meta)),
            CompiledExpr::And(v) => v.iter().all(|e| e.matches(meta)),
            CompiledExpr::True => true,
            CompiledExpr::Path(m) => {
                let s = meta.path.to_string_lossy();
                m.matches(&s)
            }
            CompiledExpr::Filter(f) => f.matches(meta),
        }
    }
}

#[derive(thiserror::Error, Debug)]
pub enum QueryCompileError {
    #[error("invalid query syntax: {0}")]
    Syntax(String),
    #[error("invalid filter: {0}")]
    Filter(String),
}

fn is_path_initials_query(input: &str) -> bool {
    let has_separator = input.contains('\\') || input.contains('/');
    let has_glob = input.contains('*') || input.contains('?');
    let has_special_prefix = input.starts_with("ext:")
        || input.starts_with("regex:")
        || input.starts_with("wfn:")
        || input.starts_with("pic:")
        || input.starts_with("doc:")
        || input.starts_with("dm:")
        || input.starts_with("dc:")
        || input.starts_with("da:")
        || input.starts_with("size:")
        || input.starts_with("parent:")
        || input.starts_with("infolder:")
        || input.starts_with("depth:")
        || input.starts_with("len:")
        || input.starts_with("type:")
        || input.starts_with("case:")
        || input.starts_with("content:");
    has_separator && !has_glob && !has_special_prefix
}

pub fn compile_query(input: &str) -> Result<CompiledQuery, QueryCompileError> {
    let tokens = tokenize(input)?;
    let mut p = Parser::new(tokens);

    // Smart-Case：默认不敏感；显式 case: 或包含大写字母则敏感
    let mut case_sensitive = false;
    let (include_expr, exclude_exprs) = p.parse_query(&mut case_sensitive)?;
    if p.has_remaining_tokens() {
        return Err(QueryCompileError::Syntax(
            "unexpected trailing tokens".into(),
        ));
    }

    if !case_sensitive && input.chars().any(|c| c.is_uppercase()) {
        case_sensitive = true;
    }

    // 编译表达式
    let mut include = compile_expr(&include_expr, case_sensitive)?;
    let excludes = exclude_exprs
        .iter()
        .map(|e| compile_expr(e, case_sensitive))
        .collect::<Result<Vec<_>, _>>()?;

    // 选 anchor：每个 OR 分支选 1 个；若任一分支无法选出 anchor，则退化为 MatchAll
    let mut anchors = select_anchors(&include_expr, case_sensitive)?;

    // 路径段首匹配：自动检测并追加 PathInitialsMatcher 作为 OR 分支
    if is_path_initials_query(input) {
        let pim: Arc<dyn Matcher> = Arc::new(PathInitialsMatcher::new(input));
        // Wrap include as OR with PathInitialsMatcher
        include = CompiledExpr::Or(vec![include, CompiledExpr::Path(Arc::clone(&pim))]);
        // Add as additional anchor so index scan covers path-initials matches
        anchors.push(pim);
    }

    Ok(CompiledQuery {
        case_sensitive,
        anchors,
        include,
        excludes,
    })
}

fn compile_expr(expr: &Expr, case_sensitive: bool) -> Result<CompiledExpr, QueryCompileError> {
    match expr {
        Expr::Or(v) => Ok(CompiledExpr::Or(
            v.iter()
                .map(|e| compile_expr(e, case_sensitive))
                .collect::<Result<Vec<_>, _>>()?,
        )),
        Expr::And(v) => Ok(CompiledExpr::And(
            v.iter()
                .map(|e| compile_expr(e, case_sensitive))
                .collect::<Result<Vec<_>, _>>()?,
        )),
        Expr::True => Ok(CompiledExpr::True),
        Expr::Atom(a) => compile_atom(a, case_sensitive),
    }
}

fn compile_atom(atom: &Atom, case_sensitive: bool) -> Result<CompiledExpr, QueryCompileError> {
    match atom {
        Atom::Text(pat) => Ok(CompiledExpr::Path(create_matcher(pat, case_sensitive))),
        Atom::Wfn(pat) => {
            let scope = if contains_path_separator(pat) {
                PathScope::FullPath
            } else {
                PathScope::Basename
            };
            Ok(CompiledExpr::Path(Arc::new(WfnMatcher::new(
                pat,
                scope,
                case_sensitive,
            ))))
        }
        Atom::Regex(pat) => {
            let scope = if regex_has_path_separator(pat) {
                PathScope::FullPath
            } else {
                PathScope::Basename
            };
            let re = match compile_regex(pat, case_sensitive) {
                Ok(r) => r,
                Err(e) => {
                    tracing::warn!("regex compile failed, fallback to text matcher: {}", e);
                    return Ok(CompiledExpr::Path(create_matcher(pat, case_sensitive)));
                }
            };
            Ok(CompiledExpr::Path(Arc::new(RegexMatcher::new(
                re,
                scope,
                case_sensitive,
            ))))
        }
        Atom::Ext(exts) => {
            let exts = normalize_ext_list(exts);
            Ok(CompiledExpr::Filter(Filter::ExtAny(exts)))
        }
        Atom::Type(kind) => {
            let exts = match kind {
                MediaKind::Doc => doc_exts(),
                MediaKind::Pic => pic_exts(),
                MediaKind::Video => video_exts(),
            };
            Ok(CompiledExpr::Filter(Filter::ExtAny(exts)))
        }
        Atom::DateModified(dr) => Ok(CompiledExpr::Filter(Filter::DateModified(*dr))),
        Atom::DateCreated(dr) => Ok(CompiledExpr::Filter(Filter::DateCreated(*dr))),
        Atom::DateAccessed(dr) => Ok(CompiledExpr::Filter(Filter::DateAccessed(*dr))),
        Atom::Size(sf) => Ok(CompiledExpr::Filter(Filter::Size(*sf))),
        Atom::Parent(p) => Ok(CompiledExpr::Filter(Filter::Parent(p.clone()))),
        Atom::Depth(op, n) => Ok(CompiledExpr::Filter(Filter::Depth(*op, *n))),
        Atom::NameLen(op, n) => Ok(CompiledExpr::Filter(Filter::NameLen(*op, *n))),
        Atom::EntryType(k) => Ok(CompiledExpr::Filter(Filter::EntryType(*k))),
        Atom::Content(s) => {
            tracing::warn!(
                "Content filter is not yet implemented. Keyword '{}' will not match any files.",
                s
            );
            Ok(CompiledExpr::Filter(Filter::Content(s.clone())))
        }
    }
}

fn select_anchors(
    expr: &Expr,
    case_sensitive: bool,
) -> Result<Vec<Arc<dyn Matcher>>, QueryCompileError> {
    if matches!(expr, Expr::True) {
        return Ok(vec![Arc::new(MatchAllMatcher)]);
    }
    let branches = match expr {
        Expr::Or(v) => v.as_slice(),
        _ => std::slice::from_ref(expr),
    };

    let mut out: Vec<Arc<dyn Matcher>> = Vec::new();

    for b in branches {
        let Some(anchor) = best_anchor_in_branch(b, case_sensitive)? else {
            return Ok(vec![Arc::new(MatchAllMatcher)]);
        };
        out.push(anchor);
    }

    Ok(out)
}

fn best_anchor_in_branch(
    expr: &Expr,
    case_sensitive: bool,
) -> Result<Option<Arc<dyn Matcher>>, QueryCompileError> {
    match expr {
        Expr::Or(v) => {
            let _ = v;
            Ok(None)
        }
        Expr::And(v) => {
            let mut best: Option<(i64, Arc<dyn Matcher>)> = None;
            for e in v {
                if let Some((score, m)) = best_anchor_in_branch_scored(e, case_sensitive)? {
                    match &best {
                        Some((s, _)) if *s >= score => {}
                        _ => best = Some((score, m)),
                    }
                }
            }
            Ok(best.map(|(_, m)| m))
        }
        Expr::True => Ok(None),
        Expr::Atom(a) => best_anchor_for_atom(a, case_sensitive),
    }
}

type AnchorScoreResult = Result<Option<(i64, Arc<dyn Matcher>)>, QueryCompileError>;

fn best_anchor_in_branch_scored(
    expr: &Expr,
    case_sensitive: bool,
) -> AnchorScoreResult {
    match expr {
        Expr::Or(_) => Ok(None),
        Expr::True => Ok(None),
        Expr::And(_) | Expr::Atom(_) => {
            let Some(m) = best_anchor_in_branch(expr, case_sensitive)? else {
                return Ok(None);
            };
            let mut score: i64 = 0;
            if let Some(h) = m.literal_hint() {
                score += (h.len() as i64).min(1024);
            }
            if let Some(p) = m.prefix() {
                score += (p.len() as i64).min(1024) / 2;
            }
            Ok(Some((score, m)))
        }
    }
}

fn best_anchor_for_atom(
    atom: &Atom,
    case_sensitive: bool,
) -> Result<Option<Arc<dyn Matcher>>, QueryCompileError> {
    match atom {
        Atom::Text(pat) => Ok(Some(create_matcher(pat, case_sensitive))),
        Atom::Wfn(pat) => {
            let scope = if contains_path_separator(pat) {
                PathScope::FullPath
            } else {
                PathScope::Basename
            };
            Ok(Some(Arc::new(WfnMatcher::new(pat, scope, case_sensitive))))
        }
        Atom::Regex(pat) => {
            let scope = if regex_has_path_separator(pat) {
                PathScope::FullPath
            } else {
                PathScope::Basename
            };
            match compile_regex(pat, case_sensitive) {
                Ok(re) => Ok(Some(Arc::new(RegexMatcher::new(re, scope, case_sensitive)))),
                Err(e) => {
                    tracing::warn!("regex compile failed, anchor fallback to MatchAll: {}", e);
                    Ok(None)
                }
            }
        }
        Atom::Ext(exts) => {
            let exts = normalize_ext_list(exts);
            Ok(Some(Arc::new(ExtMatcher::new(exts, case_sensitive))))
        }
        Atom::Type(kind) => {
            let exts = match kind {
                MediaKind::Doc => doc_exts(),
                MediaKind::Pic => pic_exts(),
                MediaKind::Video => video_exts(),
            };
            Ok(Some(Arc::new(ExtMatcher::new(exts, case_sensitive))))
        }
        Atom::DateModified(_)
        | Atom::DateCreated(_)
        | Atom::DateAccessed(_)
        | Atom::Size(_)
        | Atom::Parent(_)
        | Atom::Depth(_, _)
        | Atom::NameLen(_, _)
        | Atom::EntryType(_)
        | Atom::Content(_) => Ok(None),
    }
}

fn compile_regex(pat: &str, case_sensitive: bool) -> Result<Regex, regex::Error> {
    RegexBuilder::new(pat)
        .case_insensitive(!case_sensitive)
        .build()
}

fn regex_has_path_separator(pat: &str) -> bool {
    pat.contains('/') || pat.contains("\\\\")
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::{FileKey, FileMeta};
    use crate::query::dsl_parser::parse_dm;
    use std::path::PathBuf;
    use std::time::{Duration, SystemTime};

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
    fn and_or_not_phrase() {
        let q = compile_query("VCP server|plugin !node_modules \"New Folder\"").unwrap();
        let m1 = meta("/work/VCP_server/New Folder/readme.md", 10, None);
        assert!(q.matches(&m1));
        let m2 = meta(
            "/work/node_modules/VCP_server/New Folder/readme.md",
            10,
            None,
        );
        assert!(!q.matches(&m2));
    }

    #[test]
    fn smart_case_default_insensitive_and_uppercase_sensitive() {
        let q1 = compile_query("vcp").unwrap();
        let m = meta("/work/VCP/main.rs", 1, None);
        assert!(q1.matches(&m));

        let q2 = compile_query("VCP").unwrap();
        let m2 = meta("/work/vcp/main.rs", 1, None);
        assert!(!q2.matches(&m2));
    }

    #[test]
    fn case_directive_forces_sensitive() {
        let q = compile_query("case: vcp").unwrap();
        let m = meta("/work/VCP/main.rs", 1, None);
        assert!(!q.matches(&m));
    }

    #[test]
    fn ext_and_type_filters() {
        let q = compile_query("ext:jpg;png pic:十一").unwrap();
        let m1 = meta("/a/十一.jpg", 1, None);
        assert!(q.matches(&m1));
        let m2 = meta("/a/十一.txt", 1, None);
        assert!(!q.matches(&m2));
    }

    #[test]
    fn size_filter_basic() {
        let q = compile_query("size:<10b").unwrap();
        let m1 = meta("/a/x.txt", 9, None);
        let m2 = meta("/a/y.txt", 10, None);
        assert!(q.matches(&m1));
        assert!(!q.matches(&m2));
    }

    #[test]
    fn dm_fixed_date_range_includes_start_excludes_end() {
        // 仅验证区间逻辑（不依赖具体 epoch）
        let dr = parse_dm("today").unwrap_or(DateRange {
            start: SystemTime::now(),
            end: SystemTime::now() + Duration::from_secs(1),
        });
        let q = compile_query("dm:today").unwrap();
        let inside = meta("/a/x.txt", 1, Some(dr.start + Duration::from_secs(1)));
        let outside = meta("/a/y.txt", 1, Some(dr.end));
        assert!(q.matches(&inside));
        assert!(!q.matches(&outside));
    }

    #[test]
    fn wfn_and_regex_scope_by_separator() {
        let q1 = compile_query("wfn:server.js").unwrap();
        assert!(q1.matches(&meta("/a/b/server.js", 1, None)));
        assert!(!q1.matches(&meta("/a/b/myserver.js", 1, None)));

        let q2 = compile_query(r#"regex:"^VCP.*\\.js$""#).unwrap();
        assert!(q2.matches(&meta("/a/VCPPlugin.js", 1, None)));
        assert!(q2.matches(&meta("/a/x/VCPPlugin.js", 1, None))); // basename 模式，不看目录

        let q3 = compile_query(r#"regex:"/a/x/.*\\.js$""#).unwrap();
        assert!(q3.matches(&meta("/a/x/VCPPlugin.js", 1, None))); // fullpath 模式
    }

    #[test]
    fn chinese_text_query_contains() {
        let q = compile_query("文档").unwrap();
        assert!(q.matches(&meta("/tmp/中文文档.txt", 1, None)));
        assert!(!q.matches(&meta("/tmp/中文文件.txt", 1, None)));
    }

    #[test]
    fn chinese_quoted_phrase_query() {
        let q = compile_query("\"中文文档\"").unwrap();
        assert!(q.matches(&meta("/tmp/中文文档.txt", 1, None)));
        assert!(!q.matches(&meta("/tmp/中文文件.txt", 1, None)));
    }

    #[test]
    fn chinese_and_query() {
        let q = compile_query("中文 文档").unwrap();
        assert!(q.matches(&meta("/tmp/中文文档.txt", 1, None)));
        assert!(!q.matches(&meta("/tmp/中文文件.txt", 1, None)));
    }

    #[test]
    fn chinese_with_ext_filter() {
        let q = compile_query("文档 ext:txt").unwrap();
        assert!(q.matches(&meta("/tmp/中文文档.txt", 1, None)));
        assert!(!q.matches(&meta("/tmp/中文文档.jpg", 1, None)));
    }

    #[test]
    fn chinese_wfn_basename_match() {
        let q = compile_query("wfn:中文文档.txt").unwrap();
        assert!(q.matches(&meta("/tmp/中文文档.txt", 1, None)));
        assert!(!q.matches(&meta("/tmp/我的文档.txt", 1, None)));
    }

    #[test]
    fn chinese_mixed_ascii_query() {
        let q = compile_query("vcp文档").unwrap();
        assert!(q.matches(&meta("/tmp/vcp文档.txt", 1, None)));
        assert!(!q.matches(&meta("/tmp/vcp文件.txt", 1, None)));
    }

    #[test]
    fn chinese_case_sensitive_smart_case() {
        // Chinese chars are not uppercase, so smart-case should stay insensitive
        let q = compile_query("文档").unwrap();
        assert!(q.matches(&meta("/tmp/中文文档.txt", 1, None)));
        // Since case is insensitive by default and Chinese has no case,
        // this should still match
        assert!(q.matches(&meta("/tmp/中文文档.txt", 1, None)));
    }

}
