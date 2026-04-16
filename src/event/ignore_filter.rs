//! .gitignore-based event filtering for the incremental event pipeline.
//!
//! Wraps the `ignore` crate's gitignore matcher so that file-change events
//! matching .gitignore rules are silently dropped before reaching the index.

use std::path::{Path, PathBuf};

use ignore::gitignore::{Gitignore, GitignoreBuilder};

/// Filters file paths against .gitignore rules loaded from one or more root directories.
///
/// Each root gets its own matcher built from:
/// - `<root>/.gitignore`
/// - `<root>/.ignore`
/// - `<root>/.git/info/exclude`
///
/// In addition, git's global ignore file (`core.excludesFile` / XDG fallback)
/// is loaded once and applied to every path.
#[derive(Clone)]
pub struct IgnoreFilter {
    matchers: Vec<(PathBuf, Gitignore)>,
    global: Gitignore,
}

impl IgnoreFilter {
    /// Load ignore rules from each root directory plus git global ignore.
    pub fn from_roots(roots: &[PathBuf]) -> Self {
        let mut matchers = Vec::new();
        for root in roots {
            let mut builder = GitignoreBuilder::new(root);
            let mut loaded_any = false;
            for path in [
                root.join(".gitignore"),
                root.join(".ignore"),
                root.join(".git").join("info").join("exclude"),
            ] {
                if !path.exists() {
                    continue;
                }
                loaded_any = true;
                if let Some(err) = builder.add(&path) {
                    tracing::warn!(
                        "Failed to load ignore rules from {}: {}",
                        path.display(),
                        err
                    );
                }
            }
            if !loaded_any {
                continue;
            }
            match builder.build() {
                Ok(gi) => matchers.push((root.clone(), gi)),
                Err(e) => {
                    tracing::warn!(
                        "Failed to parse ignore rules under {}: {}",
                        root.display(),
                        e
                    );
                }
            }
        }
        matchers.sort_by_key(|(root, _)| std::cmp::Reverse(root.components().count()));

        let (global, global_err) = Gitignore::global();
        if let Some(err) = global_err {
            tracing::warn!("Failed to load global gitignore rules: {}", err);
        }

        IgnoreFilter { matchers, global }
    }

    /// Returns `true` if the given path should be ignored according to .gitignore rules.
    ///
    /// The path is matched against the gitignore of the root it falls under.
    /// If the path doesn't belong to any known root, it is *not* ignored.
    pub fn is_ignored(&self, path: &Path) -> bool {
        let is_dir = path.is_dir();

        if !self.global.is_empty() {
            let global_root = self.global.path();
            if path.starts_with(global_root) {
                let rel = path.strip_prefix(global_root).unwrap_or(path);
                if self.global.matched_path_or_any_parents(rel, is_dir).is_ignore() {
                    return true;
                }
            }
        }

        for (root, gi) in &self.matchers {
            if path.starts_with(root) {
                let rel = path.strip_prefix(root).unwrap_or(path);
                if gi.matched_path_or_any_parents(rel, is_dir).is_ignore() {
                    return true;
                }
            }
        }
        false
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use std::time::{SystemTime, UNIX_EPOCH};

    fn unique_tmp_dir(tag: &str) -> PathBuf {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos();
        std::env::temp_dir().join(format!("fd-rdd-ignore-{}-{}", tag, nanos))
    }

    #[test]
    fn ignores_matching_paths() {
        let root = unique_tmp_dir("ignore-match");
        fs::create_dir_all(&root).unwrap();
        fs::write(root.join(".gitignore"), "*.log\ntarget/\n").unwrap();
        fs::write(root.join("app.log"), "log").unwrap();

        let filter = IgnoreFilter::from_roots(&[root.clone()]);
        assert!(filter.is_ignored(&root.join("app.log")));
        assert!(!filter.is_ignored(&root.join("main.rs")));

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn no_gitignore_means_nothing_ignored() {
        let root = unique_tmp_dir("ignore-none");
        fs::create_dir_all(&root).unwrap();

        let filter = IgnoreFilter::from_roots(&[root.clone()]);
        assert!(!filter.is_ignored(&root.join("anything.txt")));

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn path_outside_roots_not_ignored() {
        let root = unique_tmp_dir("ignore-outside");
        fs::create_dir_all(&root).unwrap();
        fs::write(root.join(".gitignore"), "*.log\n").unwrap();

        let filter = IgnoreFilter::from_roots(&[root.clone()]);
        // Path outside the root should not be ignored
        assert!(!filter.is_ignored(Path::new("/some/other/path/app.log")));

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn supports_dot_ignore_and_git_info_exclude() {
        let root = unique_tmp_dir("ignore-extra");
        fs::create_dir_all(root.join(".git").join("info")).unwrap();
        fs::write(root.join(".ignore"), "*.tmp\n").unwrap();
        fs::write(root.join(".git").join("info").join("exclude"), "cache/\n").unwrap();

        let filter = IgnoreFilter::from_roots(&[root.clone()]);
        assert!(filter.is_ignored(&root.join("foo.tmp")));
        assert!(filter.is_ignored(&root.join("cache").join("x.txt")));
        assert!(!filter.is_ignored(&root.join("keep.rs")));

        let _ = fs::remove_dir_all(root);
    }
}
