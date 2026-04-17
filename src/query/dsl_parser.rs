use crate::query::dsl::{Atom, CmpOp, DateRange, EntryKind, Expr, MediaKind, QueryCompileError, SizeFilter};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Op {
    Or,
    Not,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum Token {
    Op(Op),
    Word(String),
}

pub(crate) struct Parser {
    tokens: Vec<Token>,
    pos: usize,
}

impl Parser {
    pub(crate) fn new(tokens: Vec<Token>) -> Self {
        Self { tokens, pos: 0 }
    }

    pub(crate) fn has_remaining_tokens(&self) -> bool {
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

    pub(crate) fn parse_query(
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

        let include = build_or_and(branches, !excludes.is_empty())?;
        Ok((include, excludes))
    }
}

pub(crate) fn tokenize(input: &str) -> Result<Vec<Token>, QueryCompileError> {
    let mut out = Vec::new();
    let mut i = 0usize;
    let b = input.as_bytes();

    while i < b.len() {
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

        let start = i;
        let mut in_quote = false;
        while i < b.len() {
            let c = b[i];
            if in_quote {
                if c == b'\\' {
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

pub(crate) fn build_or_and(
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

pub(crate) fn parse_atom_expr(word: &str, case_sensitive: &mut bool) -> Result<Expr, QueryCompileError> {
    let (head, tail) = split_prefix(word);

    match head {
        Some("wfn") => Ok(Expr::Atom(Atom::Wfn(unquote(tail)?))),
        Some("regex") => Ok(Expr::Atom(Atom::Regex(unquote(tail)?))),
        Some("ext") => {
            let v = unquote(tail)?;
            let list = v
                .split([';', ','])
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
            if !tail.trim().is_empty() {
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
        return unescape_quoted(&s[1..s.len() - 1]);
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

pub(crate) fn parse_case_directive(word: &str) -> Option<bool> {
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

pub(crate) fn parse_cmp_usize(s: &str) -> Result<(CmpOp, usize), QueryCompileError> {
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

pub(crate) fn parse_size(s: &str) -> Result<SizeFilter, QueryCompileError> {
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
    } else if let Some(r) = raw.strip_prefix('=') {
        (CmpOp::Eq, r)
    } else {
        (CmpOp::Ge, raw)
    };

    let rest = rest.trim();
    let bytes = parse_human_bytes(rest)?;
    Ok(SizeFilter { op, bytes })
}

pub(crate) fn parse_human_bytes(s: &str) -> Result<u64, QueryCompileError> {
    let s = s.trim();
    if s.is_empty() {
        return Err(QueryCompileError::Filter("size: empty value".into()));
    }

    let mut num = String::new();
    let mut unit = String::new();
    for c in s.chars() {
        if c.is_ascii_digit() || c == '.' {
            if !unit.is_empty() {
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

pub(crate) fn parse_dm(s: &str) -> Result<DateRange, QueryCompileError> {
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
    // Fallback: compute UTC day range using Julian Day Number arithmetic.
    let jdn = |y: i32, m: i32, d: i32| -> i64 {
        let a = (14 - m) / 12;
        let yy = y + 4800 - a;
        let mm = m + 12 * a - 3;
        d as i64 + (153 * mm as i64 + 2) / 5 + 365 * yy as i64 + yy as i64 / 4 - yy as i64 / 100 + yy as i64 / 400 - 32045
    };

    let days = jdn(year, month, day);
    let epoch_jdn = jdn(1970, 1, 1);
    let start_day = days - epoch_jdn;
    let start_secs = start_day * 86_400;
    let end_secs = (start_day + 1) * 86_400;

    let start = if start_secs >= 0 {
        std::time::UNIX_EPOCH + std::time::Duration::from_secs(start_secs as u64)
    } else {
        std::time::UNIX_EPOCH - std::time::Duration::from_secs((-start_secs) as u64)
    };
    let end = if end_secs >= 0 {
        std::time::UNIX_EPOCH + std::time::Duration::from_secs(end_secs as u64)
    } else {
        std::time::UNIX_EPOCH - std::time::Duration::from_secs((-end_secs) as u64)
    };

    Ok(DateRange { start, end })
}

#[cfg(unix)]
fn time_t_to_system_time(t: libc::time_t) -> std::time::SystemTime {
    let secs: i64 = t;
    if secs >= 0 {
        std::time::UNIX_EPOCH + std::time::Duration::from_secs(secs as u64)
    } else {
        std::time::UNIX_EPOCH - std::time::Duration::from_secs((-secs) as u64)
    }
}
