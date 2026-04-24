use crate::core::FileMeta;
use crate::index::TieredIndex;
use crate::query::scoring::{score_result, ScoreConfig};
use fuzzy_matcher::skim::SkimMatcherV2;
use fuzzy_matcher::FuzzyMatcher;

const FUZZY_CANDIDATE_MULTIPLIER: usize = 20;
const FUZZY_MIN_CANDIDATES: usize = 512;
const FUZZY_MAX_CANDIDATES: usize = 20_000;

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum QueryMode {
    #[default]
    Exact,
    Fuzzy,
}

impl QueryMode {
    pub fn parse_label(value: Option<&str>) -> Result<Self, &'static str> {
        match value.map(str::trim).filter(|s| !s.is_empty()) {
            None | Some("exact") | Some("dsl") => Ok(Self::Exact),
            Some("fuzzy") | Some("fzf") => Ok(Self::Fuzzy),
            Some(_) => Err("expected one of: exact, fuzzy"),
        }
    }

    pub fn as_str(self) -> &'static str {
        match self {
            Self::Exact => "exact",
            Self::Fuzzy => "fuzzy",
        }
    }
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum SortColumn {
    /// Relevance score (default)
    #[default]
    Score,
    Name,
    Path,
    Size,
    Ext,
    DateModified,
    DateCreated,
    DateAccessed,
}

impl SortColumn {
    pub fn parse(s: Option<&str>) -> Self {
        match s.map(str::trim).filter(|v| !v.is_empty()) {
            Some("name") => Self::Name,
            Some("path") => Self::Path,
            Some("size") => Self::Size,
            Some("ext" | "extension") => Self::Ext,
            Some("date_modified" | "dm" | "modified") => Self::DateModified,
            Some("date_created" | "dc" | "created") => Self::DateCreated,
            Some("date_accessed" | "da" | "accessed") => Self::DateAccessed,
            _ => Self::Score,
        }
    }
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum SortOrder {
    #[default]
    Asc,
    Desc,
}

impl SortOrder {
    pub fn parse(s: Option<&str>) -> Self {
        match s.map(str::trim) {
            Some("desc") => Self::Desc,
            _ => Self::Asc,
        }
    }
}

pub fn execute_query(
    index: &TieredIndex,
    keyword: &str,
    limit: usize,
    mode: QueryMode,
    sort: SortColumn,
    order: SortOrder,
) -> Vec<FileMeta> {
    let mut results = match mode {
        QueryMode::Exact => index.query_limit(keyword, limit),
        QueryMode::Fuzzy => FzfIntegration::new().query_index(index, keyword, limit),
    };

    sort_results(&mut results, keyword, sort, order);
    results
}

fn sort_results(results: &mut [FileMeta], keyword: &str, sort: SortColumn, order: SortOrder) {
    results.sort_by(|a, b| {
        let cmp = match sort {
            SortColumn::Score => {
                let config = ScoreConfig::from_query(keyword);
                let sa = score_result(a, &config);
                let sb = score_result(b, &config);
                // Score: higher is better, so default desc
                sb.cmp(&sa).then_with(|| a.path.cmp(&b.path))
            }
            SortColumn::Name => {
                let na = a
                    .path
                    .file_name()
                    .map(|f| f.to_string_lossy().to_lowercase())
                    .unwrap_or_default();
                let nb = b
                    .path
                    .file_name()
                    .map(|f| f.to_string_lossy().to_lowercase())
                    .unwrap_or_default();
                na.cmp(&nb).then_with(|| a.path.cmp(&b.path))
            }
            SortColumn::Path => a.path.cmp(&b.path),
            SortColumn::Size => a.size.cmp(&b.size).then_with(|| a.path.cmp(&b.path)),
            SortColumn::Ext => {
                let ea = a
                    .path
                    .extension()
                    .map(|e| e.to_string_lossy().to_lowercase())
                    .unwrap_or_default();
                let eb = b
                    .path
                    .extension()
                    .map(|e| e.to_string_lossy().to_lowercase())
                    .unwrap_or_default();
                ea.cmp(&eb).then_with(|| a.path.cmp(&b.path))
            }
            SortColumn::DateModified => {
                cmp_time(a.mtime, b.mtime).then_with(|| a.path.cmp(&b.path))
            }
            SortColumn::DateCreated => cmp_time(a.ctime, b.ctime).then_with(|| a.path.cmp(&b.path)),
            SortColumn::DateAccessed => {
                cmp_time(a.atime, b.atime).then_with(|| a.path.cmp(&b.path))
            }
        };

        // Score column is already desc by default; all others respect order param
        if sort == SortColumn::Score {
            cmp
        } else if order == SortOrder::Desc {
            cmp.reverse()
        } else {
            cmp
        }
    });
}

fn cmp_time(
    a: Option<std::time::SystemTime>,
    b: Option<std::time::SystemTime>,
) -> std::cmp::Ordering {
    match (a, b) {
        (Some(ta), Some(tb)) => ta.cmp(&tb),
        (Some(_), None) => std::cmp::Ordering::Greater,
        (None, Some(_)) => std::cmp::Ordering::Less,
        (None, None) => std::cmp::Ordering::Equal,
    }
}

pub struct FzfIntegration {
    matcher: SkimMatcherV2,
}

impl Default for FzfIntegration {
    fn default() -> Self {
        Self::new()
    }
}

impl FzfIntegration {
    pub fn new() -> Self {
        Self {
            matcher: SkimMatcherV2::default(),
        }
    }

    pub fn match_query(&self, keyword: &str, entries: Vec<FileMeta>) -> Vec<(FileMeta, i64)> {
        let config = ScoreConfig::from_query(keyword);
        let mut results = Vec::new();
        for entry in entries {
            if let Some(fuzzy_score) = self
                .matcher
                .fuzzy_match(&entry.path.to_string_lossy(), keyword)
            {
                // 综合评分：fuzzy matcher 分数 + 排序权重（深度/噪声目录/隐藏文件等）
                let rank_score = score_result(&entry, &config);
                let combined = fuzzy_score + rank_score;
                results.push((entry, combined));
            }
        }
        results.sort_by_key(|k| std::cmp::Reverse(k.1));
        results
    }

    pub fn query_index(&self, index: &TieredIndex, keyword: &str, limit: usize) -> Vec<FileMeta> {
        if limit == 0 {
            return Vec::new();
        }

        let keyword = keyword.trim();
        if keyword.is_empty() {
            return index.query_limit(keyword, limit);
        }

        let candidate_limit = fuzzy_candidate_limit(index.file_count(), limit);
        let mut candidates = index.query_limit(keyword, candidate_limit);
        if candidates.is_empty() {
            candidates = index.collect_all_live_metas();
        }

        self.match_query(keyword, candidates)
            .into_iter()
            .take(limit)
            .map(|(meta, _)| meta)
            .collect()
    }
}

fn fuzzy_candidate_limit(file_count: usize, limit: usize) -> usize {
    let scaled = limit.saturating_mul(FUZZY_CANDIDATE_MULTIPLIER);
    let bounded = scaled.clamp(FUZZY_MIN_CANDIDATES, FUZZY_MAX_CANDIDATES);
    bounded.max(limit.max(1)).min(file_count.max(1))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::{EventRecord, EventType, FileIdentifier};
    use std::fs;
    use std::path::PathBuf;
    use std::time::{SystemTime, UNIX_EPOCH};

    fn unique_tmp_dir(prefix: &str) -> PathBuf {
        let ns = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos();
        let mut p = std::env::temp_dir();
        p.push(format!(
            "fd-rdd-fuzzy-{}-{}-{}",
            prefix,
            std::process::id(),
            ns
        ));
        fs::create_dir_all(&p).unwrap();
        p
    }

    fn ev(seq: u64, path: PathBuf) -> EventRecord {
        EventRecord {
            seq,
            timestamp: SystemTime::now(),
            event_type: EventType::Create,
            id: FileIdentifier::Path(path),
            path_hint: None,
        }
    }

    #[test]
    fn query_mode_parse_defaults_to_exact() {
        assert_eq!(QueryMode::parse_label(None).unwrap(), QueryMode::Exact);
        assert_eq!(
            QueryMode::parse_label(Some("exact")).unwrap(),
            QueryMode::Exact
        );
        assert_eq!(
            QueryMode::parse_label(Some("fuzzy")).unwrap(),
            QueryMode::Fuzzy
        );
        assert!(QueryMode::parse_label(Some("bogus")).is_err());
    }

    #[test]
    fn fuzzy_query_can_fallback_to_match_all_candidates() -> anyhow::Result<()> {
        let root = unique_tmp_dir("fallback");
        let wanted = root.join("main_document.txt");
        let other = root.join("beta.rs");
        fs::write(&wanted, b"a")?;
        fs::write(&other, b"b")?;

        let index = TieredIndex::empty(vec![root.clone()]);
        index.apply_events(&[ev(1, wanted.clone()), ev(2, other)]);

        let exact = execute_query(
            &index,
            "mdt",
            10,
            QueryMode::Exact,
            SortColumn::default(),
            SortOrder::default(),
        );
        assert!(exact.is_empty());

        let fuzzy = execute_query(
            &index,
            "mdt",
            1,
            QueryMode::Fuzzy,
            SortColumn::default(),
            SortOrder::default(),
        );
        assert_eq!(fuzzy.len(), 1);
        assert_eq!(fuzzy[0].path, wanted);
        Ok(())
    }

    #[test]
    fn fuzzy_fallback_scans_beyond_truncated_match_all_candidates() -> anyhow::Result<()> {
        let root = unique_tmp_dir("fallback-wide");
        let wanted = root.join("main_document_target.txt");
        fs::create_dir_all(&root)?;

        let index = TieredIndex::empty(vec![root.clone()]);
        let mut events = Vec::new();
        for i in 0..700u64 {
            let noise = root.join(format!("noise_{:04}.txt", i));
            fs::write(&noise, b"x")?;
            events.push(ev(i + 1, noise));
        }
        fs::write(&wanted, b"wanted")?;
        events.push(ev(10_000, wanted.clone()));
        index.apply_events(&events);

        let exact = execute_query(
            &index,
            "maindoctarget",
            10,
            QueryMode::Exact,
            SortColumn::default(),
            SortOrder::default(),
        );
        assert!(exact.is_empty());

        let fuzzy = execute_query(
            &index,
            "maindoctarget",
            1,
            QueryMode::Fuzzy,
            SortColumn::default(),
            SortOrder::default(),
        );
        assert_eq!(fuzzy.len(), 1);
        assert_eq!(fuzzy[0].path, wanted);
        Ok(())
    }
}
