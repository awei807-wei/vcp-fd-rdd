use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use parking_lot::Mutex;

use crate::config::ContentIndexConfig;
use crate::core::FileMeta;
use crate::fs_policy::FsPolicy;
use crate::index::content_filter::ContentFilter;
use crate::util::path_has_excluded_component;

use super::TieredIndex;

/// 内容索引与内容哈希去重的运行时状态子结构。
pub(super) struct ContentIndexState {
    pub enabled: AtomicBool,
    pub config: Mutex<ContentIndexConfig>,
    pub docs: Mutex<HashMap<PathBuf, String>>,
    pub indexed_paths: AtomicU64,
    pub indexed_bytes: AtomicU64,
    pub last_elapsed_ms: AtomicU64,
    pub hash_queue_pending: AtomicU64,
    pub hash_candidate_count: AtomicU64,
    pub hash_confirmed_groups: AtomicU64,
    pub hash_skipped_count: AtomicU64,
    pub hash_last_elapsed_ms: AtomicU64,
    pub hash_last_skip_reason: Mutex<String>,
}

impl Default for ContentIndexState {
    fn default() -> Self {
        Self {
            enabled: AtomicBool::new(false),
            config: Mutex::new(ContentIndexConfig::default()),
            docs: Mutex::new(HashMap::new()),
            indexed_paths: AtomicU64::new(0),
            indexed_bytes: AtomicU64::new(0),
            last_elapsed_ms: AtomicU64::new(0),
            hash_queue_pending: AtomicU64::new(0),
            hash_candidate_count: AtomicU64::new(0),
            hash_confirmed_groups: AtomicU64::new(0),
            hash_skipped_count: AtomicU64::new(0),
            hash_last_elapsed_ms: AtomicU64::new(0),
            hash_last_skip_reason: Mutex::new(String::new()),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(super) enum ContentReadEligibility {
    Eligible { size: u64 },
    Skip { reason: String },
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct ContentIndexReport {
    pub indexed_paths: usize,
    pub indexed_bytes: u64,
    pub skipped_paths: usize,
    pub elapsed_ms: u64,
}

#[derive(Clone, Debug, Default)]
pub(crate) struct ContentQueryContext {
    matches_by_term: HashMap<String, HashSet<PathBuf>>,
}

impl ContentQueryContext {
    pub(crate) fn matches(&self, path: &Path, term: &str) -> bool {
        let term = term.to_lowercase();
        self.matches_by_term
            .get(&term)
            .is_some_and(|paths| paths.contains(path))
    }
}

impl TieredIndex {
    pub fn apply_content_index_config(&self, config: ContentIndexConfig) {
        self.content.enabled.store(config.enable, Ordering::Relaxed);
        *self.content.config.lock() = normalize_content_config(config.clone());
        if !config.enable {
            self.content.docs.lock().clear();
            self.content.indexed_paths.store(0, Ordering::Relaxed);
            self.content.indexed_bytes.store(0, Ordering::Relaxed);
            self.content.last_elapsed_ms.store(0, Ordering::Relaxed);
        }
    }

    pub fn spawn_content_index_worker(self: &Arc<Self>, interval: Duration) {
        if !self.content.enabled.load(Ordering::Relaxed) {
            return;
        }
        let index = self.clone();
        let interval = interval.max(Duration::from_secs(1));
        std::thread::spawn(move || loop {
            index.set_current_thread_idle_io_priority_for_scan();
            let report = index.rebuild_content_index_now();
            tracing::debug!(
                "content index refresh complete: paths={} bytes={} skipped={} elapsed_ms={}",
                report.indexed_paths,
                report.indexed_bytes,
                report.skipped_paths,
                report.elapsed_ms
            );
            std::thread::sleep(interval);
        });
    }

    pub fn rebuild_content_index_now(&self) -> ContentIndexReport {
        let started = Instant::now();
        if !self.content.enabled.load(Ordering::Relaxed) {
            self.content.docs.lock().clear();
            return ContentIndexReport::default();
        }

        let config = self.content.config.lock().clone();
        let mut docs: HashMap<PathBuf, String> = HashMap::new();
        let mut report = ContentIndexReport::default();
        let fs_policy = FsPolicy::current_with_shared_config(self.shared_fs_policy_config());

        for meta in self.collect_live_metas_for_diagnostics() {
            let Some((path, text, bytes)) = self.read_content_index_doc(&meta, &config, &fs_policy)
            else {
                report.skipped_paths = report.skipped_paths.saturating_add(1);
                continue;
            };
            report.indexed_paths = report.indexed_paths.saturating_add(1);
            report.indexed_bytes = report.indexed_bytes.saturating_add(bytes);
            docs.insert(path, text);
        }

        report.elapsed_ms = started.elapsed().as_millis().min(u128::from(u64::MAX)) as u64;
        *self.content.docs.lock() = docs;
        self.content
            .indexed_paths
            .store(report.indexed_paths as u64, Ordering::Relaxed);
        self.content
            .indexed_bytes
            .store(report.indexed_bytes, Ordering::Relaxed);
        self.content
            .last_elapsed_ms
            .store(report.elapsed_ms, Ordering::Relaxed);
        report
    }

    pub(crate) fn content_query_context(&self, terms: &[String]) -> Option<ContentQueryContext> {
        if !self.content.enabled.load(Ordering::Relaxed) {
            return None;
        }
        let mut normalized_terms = terms
            .iter()
            .map(|term| term.to_lowercase())
            .filter(|term| !term.is_empty())
            .collect::<Vec<_>>();
        normalized_terms.sort();
        normalized_terms.dedup();

        let docs = self.content.docs.lock();
        let mut matches_by_term = HashMap::new();
        for term in normalized_terms {
            let paths = docs
                .iter()
                .filter(|(_, text)| text.contains(&term))
                .map(|(path, _)| path.clone())
                .collect::<HashSet<_>>();
            matches_by_term.insert(term, paths);
        }
        Some(ContentQueryContext { matches_by_term })
    }

    fn read_content_index_doc(
        &self,
        meta: &FileMeta,
        config: &ContentIndexConfig,
        fs_policy: &Option<FsPolicy>,
    ) -> Option<(PathBuf, String, u64)> {
        match self.content_read_eligibility(meta, config, fs_policy, true) {
            ContentReadEligibility::Eligible { .. } => {}
            ContentReadEligibility::Skip { .. } => return None,
        };

        self.io_governor.before_io();
        let bytes = std::fs::read(&meta.path).ok()?;
        if ContentFilter::is_binary(&bytes) {
            return None;
        }
        Some((
            meta.path.clone(),
            String::from_utf8_lossy(&bytes).to_lowercase(),
            bytes.len() as u64,
        ))
    }

    pub(super) fn content_read_eligibility(
        &self,
        meta: &FileMeta,
        config: &ContentIndexConfig,
        fs_policy: &Option<FsPolicy>,
        require_allowed_extension: bool,
    ) -> ContentReadEligibility {
        if !meta.kind.is_file() {
            return content_read_skip("not_file_entry");
        }
        if self.path_is_frozen(meta.path.as_path()) {
            return content_read_skip("frozen_path");
        }
        if path_has_excluded_component(&meta.path, &self.exclude_dirs) {
            return content_read_skip("excluded_dir");
        }
        if require_allowed_extension && !content_extension_allowed(&meta.path, config) {
            return content_read_skip("extension_not_allowed");
        }

        let root = self.roots.iter().find(|root| meta.path.starts_with(root));
        if let Some(policy) = fs_policy.as_ref() {
            let decision = policy.check_path_counted(
                meta.path.as_path(),
                root.map(|path| path.as_path()),
                self.mount_policy_counters.as_ref(),
            );
            if let crate::fs_policy::FsPolicyDecision::Deny { reason } = decision {
                return ContentReadEligibility::Skip {
                    reason: format!("mount_policy:{reason}"),
                };
            }
        }

        self.io_governor.before_io();
        let fs_meta = match std::fs::metadata(&meta.path) {
            Ok(fs_meta) => fs_meta,
            Err(err) => {
                return ContentReadEligibility::Skip {
                    reason: format!("metadata_error:{}", err.kind()),
                };
            }
        };
        if !fs_meta.is_file() {
            return content_read_skip("not_file");
        }
        let size = fs_meta.len();
        if size > config.max_file_size {
            return ContentReadEligibility::Skip {
                reason: format!("too_large>{}", config.max_file_size),
            };
        }

        ContentReadEligibility::Eligible { size }
    }
}

fn content_read_skip(reason: &str) -> ContentReadEligibility {
    ContentReadEligibility::Skip {
        reason: reason.to_string(),
    }
}

fn normalize_content_config(mut config: ContentIndexConfig) -> ContentIndexConfig {
    config.include_ext = normalize_ext_list(config.include_ext);
    config.exclude_ext = normalize_ext_list(config.exclude_ext);
    config
}

fn normalize_ext_list(exts: Vec<String>) -> Vec<String> {
    let mut out = exts
        .into_iter()
        .map(|ext| ext.trim().trim_start_matches('.').to_ascii_lowercase())
        .filter(|ext| !ext.is_empty())
        .collect::<Vec<_>>();
    out.sort();
    out.dedup();
    out
}

fn content_extension_allowed(path: &Path, config: &ContentIndexConfig) -> bool {
    let Some(ext) = path.extension() else {
        return false;
    };
    let ext = ext.to_string_lossy().to_ascii_lowercase();
    if config.exclude_ext.iter().any(|blocked| blocked == &ext) {
        return false;
    }
    !config.include_ext.is_empty() && config.include_ext.iter().any(|allowed| allowed == &ext)
}
