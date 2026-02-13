use fuzzy_matcher::FuzzyMatcher;
use fuzzy_matcher::skim::SkimMatcherV2;
use crate::core::FileEntry;

pub struct FzfIntegration {
    matcher: SkimMatcherV2,
}

impl FzfIntegration {
    pub fn new() -> Self {
        Self {
            matcher: SkimMatcherV2::default(),
        }
    }

    pub fn match_query(&self, keyword: &str, entries: Vec<FileEntry>) -> Vec<(FileEntry, i64)> {
        let mut results = Vec::new();
        for entry in entries {
            if let Some(score) = self.matcher.fuzzy_match(&entry.path.to_string_lossy(), keyword) {
                results.push((entry, score));
            }
        }
        results.sort_by_key(|k| std::cmp::Reverse(k.1));
        results
    }
}