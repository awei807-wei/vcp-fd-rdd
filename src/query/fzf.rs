use crate::core::FileMeta;
use crate::index::TieredIndex;
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

pub fn execute_query(
    index: &TieredIndex,
    keyword: &str,
    limit: usize,
    mode: QueryMode,
) -> Vec<FileMeta> {
    match mode {
        QueryMode::Exact => index.query_limit(keyword, limit),
        QueryMode::Fuzzy => FzfIntegration::new().query_index(index, keyword, limit),
    }
}

pub struct FzfIntegration {
    matcher: SkimMatcherV2,
}

impl FzfIntegration {
    pub fn new() -> Self {
        Self {
            matcher: SkimMatcherV2::default(),
        }
    }

    pub fn match_query(&self, keyword: &str, entries: Vec<FileMeta>) -> Vec<(FileMeta, i64)> {
        let mut results = Vec::new();
        for entry in entries {
            if let Some(score) = self
                .matcher
                .fuzzy_match(&entry.path.to_string_lossy(), keyword)
            {
                results.push((entry, score));
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
            candidates = index.query_limit("", candidate_limit);
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
    let bounded = scaled.max(FUZZY_MIN_CANDIDATES).min(FUZZY_MAX_CANDIDATES);
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

        let exact = execute_query(&index, "mdt", 10, QueryMode::Exact);
        assert!(exact.is_empty());

        let fuzzy = execute_query(&index, "mdt", 1, QueryMode::Fuzzy);
        assert_eq!(fuzzy.len(), 1);
        assert_eq!(fuzzy[0].path, wanted);
        Ok(())
    }
}
