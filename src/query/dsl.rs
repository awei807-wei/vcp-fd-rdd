use crate::core::FileMeta;
use crate::query::matcher::{
    contains_path_separator, create_matcher, ExtMatcher, MatchAllMatcher, Matcher,
    PathInitialsMatcher, PathScope, RegexMatcher, WfnMatcher,
};
use regex::{Regex, RegexBuilder};
use std::sync::Arc;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Op {
    Or,
    Not,
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum Token {
    Op(Op),
    Word(String),
}

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

#[allow(dead_code)]
#[derive(Debug, Clone)]
enum Filter {
    ExtAny(Vec<Vec<u8>>),
    Size(SizeFilter),
    DateModified(DateRange),
    DateCreated(DateRange),
    DateAccessed(DateRange),
    Parent(String),
    Depth(CmpOp, usize),
    NameLen(CmpOp, usize),
    EntryType(EntryKind),
    Content(String),
}

impl Filter {
    fn matches(&self, meta: &FileMeta) -> bool {
        match self {
            Filter::ExtAny(exts) => {
                let Some(ext) = meta.path.extension() else {
                    return false;
                };
                let ext = ext.to_string_lossy();
                let ext_lc = ascii_lower_bytes(ext.as_bytes());
                exts.iter().any(|e| *e == ext_lc)
            }
            Filter::Size(sf) => apply_cmp(sf.op, meta.size, sf.bytes),
            Filter::DateModified(dr) => {
                let Some(t) = meta.mtime else {
                    return false;
                };
                t >= dr.start && t < dr.end
            }
            Filter::DateCreated(dr) => {
                let Some(t) = meta.ctime else {
                    return false;
                };
                t >= dr.start && t < dr.end
            }
            Filter::DateAccessed(dr) => {
                let Some(t) = meta.atime else {
                    return false;
                };
                t >= dr.start && t < dr.end
            }
            Filter::Parent(parent) => meta
                .path
                .parent()
                .map(|p| p.to_string_lossy().eq_ignore_ascii_case(parent))
                .unwrap_or(false),
            Filter::Depth(op, n) => {
                let s = meta.path.to_string_lossy();
                let depth = s.matches('/').count() + s.matches('\\').count();
                apply_cmp(*op, depth as u64, *n as u64)
            }
            Filter::NameLen(op, n) => {
                let len = meta.path.file_name().map(|f| f.len()).unwrap_or(0);
                apply_cmp(*op, len as u64, *n as u64)
            }
            Filter::EntryType(kind) => match kind {
                // We only index files currently; folder matches always false
                EntryKind::File => true,
                EntryKind::Folder => false,
            },
            Filter::Content(_) => {
                // TODO: 接入全文索引后实现真正的内容匹配
                false
            }
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
            // regex 内大量使用 `\` 做转义，不能把单个 `\` 误判为路径分隔符；
            // fullpath 由 `/` 或正则里的字面反斜杠（`\\`）触发。
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
        Atom::Content(s) => Ok(CompiledExpr::Filter(Filter::Content(s.clone()))),
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
            // 分支内部出现 OR：取所有子分支里“最差情况下”都存在的 anchor 非易事；
            // 为保证不漏结果，这里直接要求 MatchAll（由上层处理）。
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

fn best_anchor_in_branch_scored(
    expr: &Expr,
    case_sensitive: bool,
) -> Result<Option<(i64, Arc<dyn Matcher>)>, QueryCompileError> {
    match expr {
        Expr::Or(_) => Ok(None),
        Expr::True => Ok(None),
        Expr::And(_) | Expr::Atom(_) => {
            let Some(m) = best_anchor_in_branch(expr, case_sensitive)? else {
                return Ok(None);
            };
            // score：尽量选更“有区分度”的 matcher
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

struct Parser {
    tokens: Vec<Token>,
    pos: usize,
}

impl Parser {
    fn new(tokens: Vec<Token>) -> Self {
        Self { tokens, pos: 0 }
    }

    fn has_remaining_tokens(&self) -> bool {
        self.pos < self.tokens.len()
    }

    fn peek(&self) -> Option<&Token> {
        self.tokens.get(self.pos)
    }

    fn bump(&mut self) -> Option<Token> {
        let t = self.tokens.get(self.pos).cloned();
        if t.is_some() {
            self.pos += 1;
        }
        t
    }

    fn bump_word(&mut self) -> Option<String> {
        match self.bump() {
            Some(Token::Word(s)) => Some(s),
            _ => None,
        }
    }

    fn parse_query(
        &mut self,
        case_sensitive: &mut bool,
    ) -> Result<(Expr, Vec<Expr>), QueryCompileError> {
        let mut branches: Vec<Vec<Expr>> = vec![Vec::new()];
        let mut excludes: Vec<Expr> = Vec::new();

        while let Some(tok) = self.peek() {
            match tok {
                Token::Op(Op::Or) => {
                    self.bump();
                    branches.push(Vec::new());
                }
                Token::Op(Op::Not) => {
                    self.bump();
                    let Some(word) = self.bump_word() else {
                        return Err(QueryCompileError::Syntax("expected term after '!'".into()));
                    };
                    let ex = parse_atom_expr(&word, case_sensitive)?;
                    if !matches!(ex, Expr::True) {
                        excludes.push(ex);
                    }
                }
                Token::Word(_) => {
                    let Some(word) = self.bump_word() else {
                        break;
                    };
                    let e = parse_atom_expr(&word, case_sensitive)?;
                    if !matches!(e, Expr::True) {
                        branches.last_mut().expect("branches non-empty").push(e);
                    }
                }
            }
        }

        // 构造 include 表达式：只有排除项时 include=TRUE
        let include = build_or_and(branches, !excludes.is_empty())?;
        Ok((include, excludes))
    }
}

fn parse_case_directive(word: &str) -> Option<bool> {
    let w = word.trim();
    if !w.starts_with("case:") {
        return None;
    }
    let v = w.strip_prefix("case:").unwrap_or("").trim();
    if v.is_empty() {
        return Some(true);
    }
    match v.to_ascii_lowercase().as_str() {
        "on" | "1" | "true" | "sensitive" => Some(true),
        "off" | "0" | "false" | "insensitive" => Some(false),
        _ => Some(true),
    }
}

fn parse_atom_expr(word: &str, case_sensitive: &mut bool) -> Result<Expr, QueryCompileError> {
    // 允许 token 内嵌引号：prefix:"value with spaces"
    let (head, tail) = split_prefix(word);

    match head {
        Some("wfn") => Ok(Expr::Atom(Atom::Wfn(unquote(tail)?))),
        Some("regex") => Ok(Expr::Atom(Atom::Regex(unquote(tail)?))),
        Some("ext") => {
            let v = unquote(tail)?;
            let list = v
                .split(|c| c == ';' || c == ',')
                .map(str::trim)
                .filter(|s| !s.is_empty())
                .map(|s| s.to_string())
                .collect::<Vec<_>>();
            Ok(Expr::Atom(Atom::Ext(list)))
        }
        Some("doc") => {
            let v = unquote(tail)?;
            if v.is_empty() {
                Ok(Expr::Atom(Atom::Type(MediaKind::Doc)))
            } else {
                Ok(Expr::And(vec![
                    Expr::Atom(Atom::Type(MediaKind::Doc)),
                    Expr::Atom(Atom::Text(v)),
                ]))
            }
        }
        Some("pic") => {
            let v = unquote(tail)?;
            if v.is_empty() {
                Ok(Expr::Atom(Atom::Type(MediaKind::Pic)))
            } else {
                Ok(Expr::And(vec![
                    Expr::Atom(Atom::Type(MediaKind::Pic)),
                    Expr::Atom(Atom::Text(v)),
                ]))
            }
        }
        Some("video") => {
            let v = unquote(tail)?;
            if v.is_empty() {
                Ok(Expr::Atom(Atom::Type(MediaKind::Video)))
            } else {
                Ok(Expr::And(vec![
                    Expr::Atom(Atom::Type(MediaKind::Video)),
                    Expr::Atom(Atom::Text(v)),
                ]))
            }
        }
        Some("dm") => {
            let v = unquote(tail)?;
            let dr = parse_dm(&v)?;
            Ok(Expr::Atom(Atom::DateModified(dr)))
        }
        Some("dc") | Some("datecreated") => {
            let v = unquote(tail)?;
            let dr = parse_dm(&v)?;
            Ok(Expr::Atom(Atom::DateCreated(dr)))
        }
        Some("da") | Some("dateaccessed") => {
            let v = unquote(tail)?;
            let dr = parse_dm(&v)?;
            Ok(Expr::Atom(Atom::DateAccessed(dr)))
        }
        Some("size") => {
            let v = unquote(tail)?;
            let sf = parse_size(&v)?;
            Ok(Expr::Atom(Atom::Size(sf)))
        }
        Some("parent") | Some("infolder") => {
            let v = unquote(tail)?;
            Ok(Expr::Atom(Atom::Parent(v)))
        }
        Some("depth") | Some("parents") => {
            let v = unquote(tail)?;
            let (op, n) = parse_cmp_usize(&v)?;
            Ok(Expr::Atom(Atom::Depth(op, n)))
        }
        Some("len") | Some("namelength") => {
            let v = unquote(tail)?;
            let (op, n) = parse_cmp_usize(&v)?;
            Ok(Expr::Atom(Atom::NameLen(op, n)))
        }
        Some("type") => {
            let v = unquote(tail)?.to_lowercase();
            let kind = match v.as_str() {
                "folder" | "dir" | "directory" => EntryKind::Folder,
                _ => EntryKind::File,
            };
            Ok(Expr::Atom(Atom::EntryType(kind)))
        }
        Some("content") => {
            let v = unquote(tail)?;
            if v.is_empty() {
                return Err(QueryCompileError::Filter("content: empty keyword".into()));
            }
            Ok(Expr::Atom(Atom::Content(v)))
        }
        Some("case") => {
            // 兼容 case: 出现在 split_prefix 分支；不进入 Expr
            if !tail.trim().is_empty() {
                // case:xxx
                if parse_case_directive(word).unwrap_or(true) {
                    *case_sensitive = true;
                }
            } else {
                *case_sensitive = true;
            }
            Ok(Expr::True)
        }
        _ => Ok(Expr::Atom(Atom::Text(unquote(word)?))),
    }
}

fn split_prefix(word: &str) -> (Option<&str>, &str) {
    let Some((k, v)) = word.split_once(':') else {
        return (None, word);
    };
    let k = k.trim();
    if k.is_empty() {
        return (None, word);
    }
    (Some(k), v)
}

fn unquote(s: &str) -> Result<String, QueryCompileError> {
    let s = s.trim();
    if s.len() >= 2 && s.starts_with('"') && s.ends_with('"') {
        return Ok(unescape_quoted(&s[1..s.len() - 1])?);
    }
    Ok(s.to_string())
}

fn unescape_quoted(s: &str) -> Result<String, QueryCompileError> {
    let mut out = String::with_capacity(s.len());
    let mut it = s.chars();
    while let Some(c) = it.next() {
        if c == '\\' {
            let Some(n) = it.next() else {
                return Err(QueryCompileError::Syntax("dangling escape".into()));
            };
            match n {
                '\\' => out.push('\\'),
                '"' => out.push('"'),
                'n' => out.push('\n'),
                't' => out.push('\t'),
                other => {
                    out.push('\\');
                    out.push(other);
                }
            }
        } else {
            out.push(c);
        }
    }
    Ok(out)
}

fn build_or_and(
    branches: Vec<Vec<Expr>>,
    allow_single_empty: bool,
) -> Result<Expr, QueryCompileError> {
    let mut built: Vec<Expr> = Vec::new();

    for b in branches {
        let factors = b
            .into_iter()
            .filter(|e| !matches!(e, Expr::True))
            .collect::<Vec<_>>();

        if factors.is_empty() {
            built.push(Expr::True);
            continue;
        }
        if factors.len() == 1 {
            built.push(factors.into_iter().next().unwrap());
        } else {
            built.push(Expr::And(factors));
        }
    }

    // 若显式使用 OR（多分支），则不允许空分支（避免 `a| !b` 等歧义）
    if built.len() > 1 && built.iter().any(|e| matches!(e, Expr::True)) {
        return Err(QueryCompileError::Syntax("empty OR branch".into()));
    }

    if built.len() == 1 {
        let one = built.into_iter().next().unwrap_or(Expr::True);
        if matches!(one, Expr::True) && !allow_single_empty {
            return Err(QueryCompileError::Syntax("empty expression".into()));
        }
        return Ok(one);
    }

    Ok(Expr::Or(built))
}

fn tokenize(input: &str) -> Result<Vec<Token>, QueryCompileError> {
    let mut out = Vec::new();
    let mut i = 0usize;
    let b = input.as_bytes();

    while i < b.len() {
        // skip ws
        while i < b.len() && b[i].is_ascii_whitespace() {
            i += 1;
        }
        if i >= b.len() {
            break;
        }

        match b[i] {
            b'|' => {
                out.push(Token::Op(Op::Or));
                i += 1;
                continue;
            }
            b'!' => {
                out.push(Token::Op(Op::Not));
                i += 1;
                continue;
            }
            _ => {}
        }

        // word: 允许 token 内出现引号，且引号内允许空格/|/!
        let start = i;
        let mut in_quote = false;
        while i < b.len() {
            let c = b[i];
            if in_quote {
                if c == b'\\' {
                    // skip escaped next byte (best-effort; utf8 仍由后续处理)
                    i += 1;
                    if i < b.len() {
                        i += 1;
                    }
                    continue;
                }
                if c == b'"' {
                    in_quote = false;
                    i += 1;
                    continue;
                }
                i += 1;
                continue;
            }

            if c.is_ascii_whitespace() || c == b'|' || c == b'!' {
                break;
            }
            if c == b'"' {
                in_quote = true;
                i += 1;
                continue;
            }
            i += 1;
        }

        if in_quote {
            return Err(QueryCompileError::Syntax("unclosed quote".into()));
        }

        let s = input[start..i].trim();
        if !s.is_empty() {
            out.push(Token::Word(s.to_string()));
        }
    }

    Ok(out)
}

fn apply_cmp(op: CmpOp, lhs: u64, rhs: u64) -> bool {
    match op {
        CmpOp::Lt => lhs < rhs,
        CmpOp::Le => lhs <= rhs,
        CmpOp::Eq => lhs == rhs,
        CmpOp::Ge => lhs >= rhs,
        CmpOp::Gt => lhs > rhs,
    }
}

fn ascii_lower_bytes(bytes: &[u8]) -> Vec<u8> {
    bytes.iter().map(|b| b.to_ascii_lowercase()).collect()
}

fn normalize_ext_list(exts: &[String]) -> Vec<Vec<u8>> {
    exts.iter()
        .map(|s| s.trim().trim_start_matches('.'))
        .filter(|s| !s.is_empty())
        .map(|s| ascii_lower_bytes(s.as_bytes()))
        .collect()
}

fn doc_exts() -> Vec<Vec<u8>> {
    normalize_ext_list(&[
        "txt".into(),
        "md".into(),
        "markdown".into(),
        "pdf".into(),
        "doc".into(),
        "docx".into(),
        "xls".into(),
        "xlsx".into(),
        "ppt".into(),
        "pptx".into(),
        "rtf".into(),
        "odt".into(),
        "ods".into(),
        "odp".into(),
        "epub".into(),
        "csv".into(),
    ])
}

fn pic_exts() -> Vec<Vec<u8>> {
    normalize_ext_list(&[
        "jpg".into(),
        "jpeg".into(),
        "png".into(),
        "gif".into(),
        "bmp".into(),
        "webp".into(),
        "tif".into(),
        "tiff".into(),
        "heic".into(),
        "heif".into(),
        "svg".into(),
        "ico".into(),
    ])
}

fn video_exts() -> Vec<Vec<u8>> {
    normalize_ext_list(&[
        "mp4".into(),
        "mkv".into(),
        "mov".into(),
        "avi".into(),
        "webm".into(),
        "flv".into(),
        "m4v".into(),
        "mpg".into(),
        "mpeg".into(),
        "wmv".into(),
        "3gp".into(),
        "ts".into(),
    ])
}

fn parse_cmp_usize(s: &str) -> Result<(CmpOp, usize), QueryCompileError> {
    let raw = s.trim();
    let (op, rest) = if let Some(r) = raw.strip_prefix(">=") {
        (CmpOp::Ge, r)
    } else if let Some(r) = raw.strip_prefix("<=") {
        (CmpOp::Le, r)
    } else if let Some(r) = raw.strip_prefix('>') {
        (CmpOp::Gt, r)
    } else if let Some(r) = raw.strip_prefix('<') {
        (CmpOp::Lt, r)
    } else if let Some(r) = raw.strip_prefix('=') {
        (CmpOp::Eq, r)
    } else {
        (CmpOp::Eq, raw)
    };
    let n = rest
        .trim()
        .parse::<usize>()
        .map_err(|_| QueryCompileError::Filter(format!("expected integer, got {:?}", rest)))?;
    Ok((op, n))
}

fn parse_size(s: &str) -> Result<SizeFilter, QueryCompileError> {
    let raw = s.trim();
    if raw.is_empty() {
        return Err(QueryCompileError::Filter("size: empty".into()));
    }

    let (op, rest) = if let Some(r) = raw.strip_prefix(">=") {
        (CmpOp::Ge, r)
    } else if let Some(r) = raw.strip_prefix("<=") {
        (CmpOp::Le, r)
    } else if let Some(r) = raw.strip_prefix(">") {
        (CmpOp::Gt, r)
    } else if let Some(r) = raw.strip_prefix("<") {
        (CmpOp::Lt, r)
    } else if let Some(r) = raw.strip_prefix("=") {
        (CmpOp::Eq, r)
    } else {
        (CmpOp::Ge, raw)
    };

    let rest = rest.trim();
    let bytes = parse_human_bytes(rest)?;
    Ok(SizeFilter { op, bytes })
}

fn parse_human_bytes(s: &str) -> Result<u64, QueryCompileError> {
    let s = s.trim();
    if s.is_empty() {
        return Err(QueryCompileError::Filter("size: empty value".into()));
    }

    let mut num = String::new();
    let mut unit = String::new();
    for c in s.chars() {
        if c.is_ascii_digit() || c == '.' {
            if !unit.is_empty() {
                // "10mb20" 这类输入
                return Err(QueryCompileError::Filter(format!("size: invalid '{}'", s)));
            }
            num.push(c);
        } else if !c.is_whitespace() {
            unit.push(c);
        }
    }

    let v: f64 = num
        .parse()
        .map_err(|_| QueryCompileError::Filter(format!("size: invalid number '{}'", s)))?;
    if v.is_sign_negative() {
        return Err(QueryCompileError::Filter("size: negative".into()));
    }
    let unit = unit.to_ascii_lowercase();
    let mul: f64 = match unit.as_str() {
        "" | "b" | "byte" | "bytes" => 1.0,
        "k" | "kb" | "kib" => 1024.0,
        "m" | "mb" | "mib" => 1024.0 * 1024.0,
        "g" | "gb" | "gib" => 1024.0 * 1024.0 * 1024.0,
        "t" | "tb" | "tib" => 1024.0 * 1024.0 * 1024.0 * 1024.0,
        _ => {
            return Err(QueryCompileError::Filter(format!(
                "size: unknown unit '{}'",
                unit
            )))
        }
    };

    let bytes = (v * mul).round();
    if bytes.is_nan() || bytes.is_infinite() || bytes < 0.0 {
        return Err(QueryCompileError::Filter(format!("size: invalid '{}'", s)));
    }
    Ok(bytes.min(u64::MAX as f64) as u64)
}

fn parse_dm(s: &str) -> Result<DateRange, QueryCompileError> {
    let v = s.trim();
    if v.is_empty() {
        return Err(QueryCompileError::Filter("dm: empty".into()));
    }
    if v.eq_ignore_ascii_case("today") {
        return local_today_range().map_err(|e| QueryCompileError::Filter(format!("dm: {}", e)));
    }
    parse_local_date_range(v).map_err(|e| QueryCompileError::Filter(format!("dm: {}", e)))
}

#[cfg(unix)]
fn local_today_range() -> Result<DateRange, String> {
    use std::mem::MaybeUninit;
    unsafe {
        let now: libc::time_t = libc::time(std::ptr::null_mut());
        if now == (-1 as libc::time_t) {
            return Err("time() failed".into());
        }
        let mut tm = MaybeUninit::<libc::tm>::zeroed();
        if libc::localtime_r(&now, tm.as_mut_ptr()).is_null() {
            return Err("localtime_r failed".into());
        }
        let tm = tm.assume_init();
        local_date_range(tm.tm_year + 1900, tm.tm_mon + 1, tm.tm_mday)
    }
}

#[cfg(not(unix))]
fn local_today_range() -> Result<DateRange, String> {
    // 非 unix：无本地时区转换能力，退化为 UTC day
    let now = std::time::SystemTime::now();
    let dur = now
        .duration_since(std::time::UNIX_EPOCH)
        .map_err(|_| "system time before epoch".to_string())?;
    let secs = dur.as_secs();
    let day = secs / 86_400;
    let start = std::time::UNIX_EPOCH + std::time::Duration::from_secs(day * 86_400);
    let end = start + std::time::Duration::from_secs(86_400);
    Ok(DateRange { start, end })
}

fn parse_local_date_range(s: &str) -> Result<DateRange, String> {
    let parts: Vec<&str> = s.split('-').collect();
    if parts.len() != 3 {
        return Err(format!("invalid date '{}'", s));
    }
    let y: i32 = parts[0]
        .parse()
        .map_err(|_| format!("invalid year '{}'", s))?;
    let m: i32 = parts[1]
        .parse()
        .map_err(|_| format!("invalid month '{}'", s))?;
    let d: i32 = parts[2]
        .parse()
        .map_err(|_| format!("invalid day '{}'", s))?;
    if !(1..=12).contains(&m) || !(1..=31).contains(&d) {
        return Err(format!("invalid date '{}'", s));
    }
    local_date_range(y, m, d)
}

#[cfg(unix)]
fn local_date_range(year: i32, month: i32, day: i32) -> Result<DateRange, String> {
    unsafe fn mktime_local(mut tm: libc::tm) -> Result<libc::time_t, String> {
        tm.tm_isdst = -1;
        let t = libc::mktime(&mut tm as *mut libc::tm);
        if t == (-1 as libc::time_t) {
            Err("mktime failed".into())
        } else {
            Ok(t)
        }
    }

    unsafe {
        let mut tm0: libc::tm = std::mem::zeroed();
        tm0.tm_year = year - 1900;
        tm0.tm_mon = month - 1;
        tm0.tm_mday = day;
        tm0.tm_hour = 0;
        tm0.tm_min = 0;
        tm0.tm_sec = 0;

        let t0 = mktime_local(tm0)?;

        let mut tm1 = tm0;
        tm1.tm_mday = day + 1;
        let t1 = mktime_local(tm1)?;

        Ok(DateRange {
            start: time_t_to_system_time(t0),
            end: time_t_to_system_time(t1),
        })
    }
}

#[cfg(not(unix))]
fn local_date_range(year: i32, month: i32, day: i32) -> Result<DateRange, String> {
    // 非 unix：退化为 UTC
    let _ = (year, month, day);
    Err("local date range unsupported on non-unix".into())
}

#[cfg(unix)]
fn time_t_to_system_time(t: libc::time_t) -> std::time::SystemTime {
    let secs: i64 = t as i64;
    if secs >= 0 {
        std::time::UNIX_EPOCH + std::time::Duration::from_secs(secs as u64)
    } else {
        std::time::UNIX_EPOCH - std::time::Duration::from_secs((-secs) as u64)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::{FileKey, FileMeta};
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
}
