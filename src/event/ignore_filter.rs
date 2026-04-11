//! .gitignore-based event filtering for the incremental event pipeline.
//!
//! Wraps the `ignore` crate's gitignore matcher so that file-change events
//! matching .gitignore rules are silently dropped before reaching the index.

use std::path::{Path, PathBuf};

use ignore::gitignore::{Gitignore, GitignoreBuilder};

/// Filters file paths against .gitignore rules loaded from one or more root directories.
///
/// Each root gets its own `Gitignore` matcher built from `<root>/.gitignore`.
/// A path is considered ignored if it falls under a root whose .gitignore matches it.
#[derive(Clone)]
pub struct IgnoreFilter {
    matchers: Vec<(PathBuf, Gitignore)>,
}

impl IgnoreFilter {
    /// Load .gitignore rules from each root directory.
    ///
    /// Roots that don't contain a `.gitignore` file are silently skipped.
    pub fn from_roots(roots: &[PathBuf]) -> Self {
        let mut matchers = Vec::new();
        for root in roots {
            let gitignore_path = root.join(".gitignore");
            if !gitignore_path.exists() {
                continue;
            }
            let mut builder = GitignoreBuilder::new(root);
            // add() returns Option<Error> for parse warnings; we ignore them.
            let _ = builder.add(&gitignore_path);
            match builder.build() {
                Ok(gi) => matchers.push((root.clone(), gi)),
                Err(e) => {
                    tracing::warn!("Failed to parse {}: {}", gitignore_path.display(), e);
                }
            }
        }
        IgnoreFilter { matchers }
    }

    /// Returns `true` if the given path should be ignored according to .gitignore rules.
    ///
    /// The path is matched against the gitignore of the root it falls under.
    /// If the path doesn't belong to any known root, it is *not* ignored.
    pub fn is_ignored(&self, path: &Path) -> bool {
        for (root, gi) in &self.matchers {
            if path.starts_with(root) {
                // Use metadata to determine if path is a directory; default to false (file).
                let is_dir = path.is_dir();
                if gi.matched(path, is_dir).is_ignore() {
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
}
