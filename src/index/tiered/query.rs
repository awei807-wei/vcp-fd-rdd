use std::{
    collections::{HashMap, HashSet},
    io::Read,
    path::{Path, PathBuf},
    sync::{atomic::Ordering, Arc},
    time::Instant,
};

use crate::core::{EventRecord, FileKey, FileKind, FileMeta};
use crate::event::sync::DirtyReason;
use crate::fs_policy::FsPolicy;
use crate::index::base_index::BaseIndexData;
use crate::index::content_filter::ContentFilter;
use crate::index::l2_partition::{mtime_to_ns, PersistentIndex};
use crate::index::IndexLayer;
use crate::query::dsl::{compile_query, QueryCompileError};
use crate::query::matcher::create_matcher;
use xxhash_rust::xxh3::Xxh3;

use super::arena::{path_deleted_by_any, PathArenaSet};
use super::content::ContentReadEligibility;
use super::query_plan::QueryPlan;
use super::{QueryResultFreshness, QueryResultIndexTier, QueryResultMeta, TieredIndex};

const QUERY_GUARD_SLOW_THRESHOLD_US: u64 = 50_000;
const HARDLINK_DUPE_REASON: &str = "hardlink_same_file_key";
const HARDLINK_DUPE_CONFIDENCE: f32 = 1.0;
const CONTENT_DUPE_REASON: &str = "content_hash_match";
const CONTENT_DUPE_CONFIDENCE: f32 = 0.99;
const CONTENT_DUPE_PARTIAL_BYTES: usize = 4096;
const CONTENT_INDEX_UNSUPPORTED: &str =
    "content index is disabled; enable content_index before using content:/text:";
type ContentMatcher<'a> = dyn Fn(&Path, &str) -> bool + 'a;

struct QueryVerifyBudget {
    remaining: usize,
    deadline: Instant,
    exhausted: bool,
}

impl QueryVerifyBudget {
    fn new(index: &TieredIndex) -> Self {
        let max_verify = index
            .query_max_verify_per_query
            .load(Ordering::Relaxed)
            .max(1) as usize;
        let timeout_ms = index.query_verify_timeout_ms.load(Ordering::Relaxed).max(1);
        Self {
            remaining: max_verify,
            deadline: Instant::now() + std::time::Duration::from_millis(timeout_ms),
            exhausted: false,
        }
    }

    fn try_consume(&mut self) -> bool {
        if self.remaining == 0 || Instant::now() >= self.deadline {
            self.exhausted = true;
            return false;
        }
        self.remaining -= 1;
        true
    }

    fn exhausted(&self) -> bool {
        self.exhausted
    }
}

impl TieredIndex {
    /// 查询入口：L1 → L2 → DiskSegments（mmap），不扫真实文件系统
    pub fn query(&self, keyword: &str) -> Vec<FileMeta> {
        self.query_limit(keyword, usize::MAX)
    }

    /// 查询入口（带 limit）：用于 IPC/HTTP 等"结果集可能很大"的场景，避免一次性聚合造成内存峰值。
    pub fn query_limit(&self, keyword: &str, limit: usize) -> Vec<FileMeta> {
        self.query_limit_detailed(keyword, limit)
            .into_iter()
            .map(|r| r.meta)
            .collect()
    }

    /// 严格查询入口：DSL 编译失败时返回错误，供公开 API 拒绝非法过滤器。
    pub fn query_limit_strict(
        &self,
        keyword: &str,
        limit: usize,
    ) -> Result<Vec<FileMeta>, QueryCompileError> {
        self.query_limit_detailed_strict(keyword, limit)
            .map(|results| results.into_iter().map(|r| r.meta).collect())
    }

    /// 查询入口（带冷层校验元数据）：HTTP/API 使用它返回 result freshness 与 index tier。
    pub fn query_limit_detailed(&self, keyword: &str, limit: usize) -> Vec<QueryResultMeta> {
        self.query_limit_detailed_legacy(keyword, limit)
    }

    /// 查询入口（严格 DSL）：编译失败时不回退到 legacy 文本匹配。
    pub fn query_limit_detailed_strict(
        &self,
        keyword: &str,
        limit: usize,
    ) -> Result<Vec<QueryResultMeta>, QueryCompileError> {
        self.query_limit_detailed_inner(keyword, limit, true)
    }

    fn query_limit_detailed_legacy(&self, keyword: &str, limit: usize) -> Vec<QueryResultMeta> {
        self.query_limit_detailed_inner(keyword, limit, false)
            .unwrap_or_else(|e| {
                tracing::warn!("query failed unexpectedly, returning empty result: {}", e);
                Vec::new()
            })
    }

    fn query_limit_detailed_inner(
        &self,
        keyword: &str,
        limit: usize,
        strict: bool,
    ) -> Result<Vec<QueryResultMeta>, QueryCompileError> {
        if limit == 0 {
            return Ok(Vec::new());
        }

        let plan = match compile_query(keyword) {
            Ok(compiled) => QueryPlan::compiled(compiled),
            Err(e) => {
                tracing::warn!(
                    "query dsl compile failed, fallback to legacy matcher: {}",
                    e
                );
                if strict || is_removed_size_filter_error(&e) {
                    return Err(e);
                }
                let case_sensitive =
                    keyword.contains("case:") || keyword.chars().any(|c| c.is_uppercase());
                let matcher = create_matcher(keyword, case_sensitive);

                if let Some(results) = self.l1.query(matcher.as_ref()) {
                    tracing::debug!("L1 hit: {} results", results.len());
                    return Ok(results
                        .into_iter()
                        .filter(|meta| !self.path_is_frozen(meta.path.as_path()))
                        .take(limit)
                        .map(QueryResultMeta::hot)
                        .collect());
                }

                QueryPlan::legacy(matcher)
            }
        };

        if !plan.has_trigram_hint() {
            self.record_query_no_trigram_hint_metric();
        }
        let content_query_context = if plan.requires_content_index() {
            let Some(context) = self.content_query_context(plan.content_terms()) else {
                return Err(QueryCompileError::Filter(CONTENT_INDEX_UNSUPPORTED.into()));
            };
            Some(context)
        } else {
            None
        };

        let results = if let Some(context) = content_query_context.as_ref() {
            let content_matches = |path: &Path, term: &str| context.matches(path, term);
            self.execute_query_plan(&plan, limit, Some(&content_matches as &ContentMatcher<'_>))
        } else {
            self.execute_query_plan(&plan, limit, None)
        };
        if !results.is_empty() {
            tracing::debug!("Query hit: {} results", results.len());
            for meta in results.iter().take(10) {
                self.l1.insert(meta.meta.clone());
            }
            return Ok(results);
        }

        self.l2.load_full().maybe_schedule_repair();
        self.enqueue_query_miss(keyword);
        Ok(Vec::new())
    }

    pub(crate) fn annotate_query_results(&self, metas: Vec<FileMeta>) -> Vec<QueryResultMeta> {
        let mut budget = QueryVerifyBudget::new(self);
        let mut results = Vec::with_capacity(metas.len());
        for meta in metas {
            if let Some(result) = self.annotate_query_result(meta, &mut budget) {
                results.push(result);
            }
            if budget.exhausted() {
                break;
            }
        }
        results
    }

    pub(crate) fn collect_all_live_metas(&self) -> Vec<FileMeta> {
        let _guard = QueryGenerationGuard::new(self);
        self.collect_live_metas_for_diagnostics()
    }

    pub fn collect_fast_scan_known_dirs(&self, limit: usize) -> Vec<PathBuf> {
        self.collect_fast_scan_known_dirs_excluding(limit, &[])
    }

    pub fn collect_fast_scan_known_dirs_excluding(
        &self,
        limit: usize,
        excluded_roots: &[PathBuf],
    ) -> Vec<PathBuf> {
        let limit = limit.max(1);
        let mut dirs = HashSet::new();
        for root in &self.roots {
            if !path_is_under_any_root(root.as_path(), excluded_roots) && root.is_dir() {
                dirs.insert(root.clone());
            }
            if dirs.len() >= limit {
                return sorted_limited_dirs(dirs, limit);
            }
        }

        let live_events = {
            let db = self.delta_buffer.lock();
            db.live_records().cloned().collect::<Vec<_>>()
        };
        for ev in &live_events {
            if ev.best_path().is_some_and(|path| self.path_is_frozen(path)) {
                continue;
            }
            let Some(meta) = self.overlay_meta_for_event(ev) else {
                continue;
            };
            if self.path_is_frozen(meta.path.as_path()) {
                continue;
            }
            collect_parent_dirs_for_fast_scan(&meta, &self.roots, excluded_roots, &mut dirs, limit);
            if dirs.len() >= limit {
                return sorted_limited_dirs(dirs, limit);
            }
        }

        let base = self.base.load_full();
        base.for_each_live_meta_until(|meta| {
            if !self.path_is_frozen(meta.path.as_path()) {
                collect_parent_dirs_for_fast_scan(
                    &meta,
                    &self.roots,
                    excluded_roots,
                    &mut dirs,
                    limit,
                );
            }
            dirs.len() < limit
        });

        if dirs.len() < limit && base.file_count() == 0 {
            self.l2.load_full().for_each_live_meta(|meta| {
                if dirs.len() >= limit || self.path_is_frozen(meta.path.as_path()) {
                    return;
                }
                collect_parent_dirs_for_fast_scan(
                    &meta,
                    &self.roots,
                    excluded_roots,
                    &mut dirs,
                    limit,
                );
            });
        }

        sorted_limited_dirs(dirs, limit)
    }

    pub(crate) fn collect_live_metas_for_diagnostics(&self) -> Vec<FileMeta> {
        // Lock the overlay before loading base for a consistent (base, overlay)
        // snapshot vs. finish_rebuild's atomic publish (see execute_query_plan).
        let db = self.delta_buffer.lock();
        let base = self.base.load_full();
        let mut del = PathArenaSet::default();
        for p in db.deleted_paths() {
            let _ = del.insert(p);
        }
        let live_events: Vec<EventRecord> = db.live_records().cloned().collect();
        drop(db);
        let overlay_deleted = Arc::new(del);
        let mut blocked_paths = PathArenaSet::default();
        let deleted_sources: Vec<Arc<PathArenaSet>> = vec![overlay_deleted];
        let mut results: Vec<FileMeta> = Vec::with_capacity(base.file_count().saturating_add(256));

        for ev in &live_events {
            if ev.best_path().is_some_and(|path| self.path_is_frozen(path)) {
                continue;
            }
            let Some(meta) = self.overlay_meta_for_event(ev) else {
                continue;
            };
            let path_bytes = meta.path.as_os_str().as_encoded_bytes();
            if self.path_is_frozen(meta.path.as_path()) {
                continue;
            }
            if blocked_paths.contains(path_bytes)
                || path_deleted_by_any(path_bytes, deleted_sources.as_slice())
            {
                continue;
            }
            let _ = blocked_paths.insert(path_bytes);
            results.push(meta);
        }

        base.for_each_live_meta(|meta| {
            if self.path_is_frozen(meta.path.as_path()) {
                return;
            }
            collect_live_meta(
                meta,
                None,
                deleted_sources.as_slice(),
                &mut blocked_paths,
                &mut results,
            );
        });

        if base.file_count() == 0 {
            self.l2.load_full().for_each_live_meta(|meta| {
                if self.path_is_frozen(meta.path.as_path()) {
                    return;
                }
                collect_live_meta(
                    meta,
                    None,
                    deleted_sources.as_slice(),
                    &mut blocked_paths,
                    &mut results,
                );
            });
        }

        results
    }

    pub(crate) fn materialize_snapshot_base(&self) -> anyhow::Result<Arc<BaseIndexData>> {
        let mut db = self.delta_buffer.lock();
        let mut del = PathArenaSet::default();
        for p in db.deleted_paths() {
            let _ = del.insert(p);
        }
        let deleted_paths = db.deleted_paths().count();
        let live_events: Vec<EventRecord> = db.live_records().cloned().collect();

        let base = self.base.load_full();
        let base_count_before = base.file_count();
        let overlay_deleted = Arc::new(del);
        let mut blocked_paths = PathArenaSet::default();
        let deleted_sources: Vec<Arc<PathArenaSet>> = vec![overlay_deleted];
        let mut metas: Vec<FileMeta> = Vec::with_capacity(base.file_count().saturating_add(256));

        for ev in &live_events {
            let Some(meta) = self.overlay_meta_for_event(ev) else {
                continue;
            };
            let path_bytes = meta.path.as_os_str().as_encoded_bytes();
            if blocked_paths.contains(path_bytes)
                || path_deleted_by_any(path_bytes, deleted_sources.as_slice())
            {
                continue;
            }
            let _ = blocked_paths.insert(path_bytes);
            metas.push(meta);
        }

        base.for_each_live_meta(|meta| {
            collect_live_meta(
                meta,
                None,
                deleted_sources.as_slice(),
                &mut blocked_paths,
                &mut metas,
            );
        });

        let compact = PersistentIndex::new_with_roots(self.roots.clone());
        for meta in metas {
            compact.upsert_path_alias(meta);
        }
        let new_base = Arc::new(compact.to_base_index_data());
        validate_snapshot_materialization(
            base_count_before,
            deleted_paths,
            live_events.len(),
            new_base.file_count(),
        )?;
        db.clear();
        self.base.store(new_base.clone());
        self.l2.store(Arc::new(PersistentIndex::new_with_roots(
            self.roots.clone(),
        )));
        self.invalidate_memory_report_cache();
        Ok(new_base)
    }

    fn execute_query_plan(
        &self,
        plan: &QueryPlan,
        limit: usize,
        content_matches: Option<&ContentMatcher<'_>>,
    ) -> Vec<QueryResultMeta> {
        let requires_hardlink_dupe = plan.requires_hardlink_dupe();
        let requires_content_dupe = plan.requires_content_dupe();
        let scan_limit = if requires_hardlink_dupe || requires_content_dupe {
            usize::MAX
        } else {
            limit
        };
        let (results, hardlink_dupe_keys, content_dupe_metas) = {
            let _guard = QueryGenerationGuard::new(self);
            // Capture `base` while holding the delta_buffer lock so the (base, overlay)
            // pair is consistent with finish_rebuild / materialize_snapshot_base, which
            // publish `base` and clear the overlay atomically under this same lock.
            // Loading base *before* locking races with that publish: a query can read
            // the pre-publish (empty) base together with the post-publish (cleared)
            // overlay and return [] even though the index is fully populated — the
            // transient-empty-result race seen in the large-scale CI query test.
            let db = self.delta_buffer.lock();
            let base = self.base.load_full();
            let mut del = PathArenaSet::default();
            for p in db.deleted_paths() {
                let _ = del.insert(p);
            }
            let live_events: Vec<EventRecord> = db.live_records().cloned().collect();
            drop(db);
            let overlay_deleted = Arc::new(del);
            let mut blocked_paths = PathArenaSet::default();
            let deleted_sources: Vec<Arc<PathArenaSet>> = vec![overlay_deleted];
            let mut overlay_live_metas: Vec<FileMeta> = Vec::with_capacity(live_events.len());
            let mut verify_budget = QueryVerifyBudget::new(self);
            for ev in &live_events {
                if ev.best_path().is_some_and(|path| self.path_is_frozen(path)) {
                    continue;
                }
                let Some(meta) = self.overlay_meta_for_event(ev) else {
                    continue;
                };
                let path_bytes = meta.path.as_os_str().as_encoded_bytes();
                if blocked_paths.contains(path_bytes)
                    || path_deleted_by_any(path_bytes, deleted_sources.as_slice())
                {
                    continue;
                }
                overlay_live_metas.push(meta);
            }
            let mut results: Vec<QueryResultMeta> = Vec::with_capacity(limit.min(128));
            let dupe_metas = if requires_hardlink_dupe || requires_content_dupe {
                Some(self.collect_live_metas_for_diagnostics())
            } else {
                None
            };
            let hardlink_dupe_keys = if requires_hardlink_dupe {
                let keys = hardlink_duplicate_keys(
                    dupe_metas
                        .as_ref()
                        .into_iter()
                        .flat_map(|metas| metas.iter().cloned()),
                );
                if keys.is_empty() {
                    return Vec::new();
                }
                Some(keys)
            } else {
                None
            };
            let content_dupe_metas = if requires_content_dupe {
                dupe_metas
            } else {
                None
            };

            'collect_results: {
                // Overlay upserts take precedence over the immutable base. This keeps
                // delete+recreate and rename windows correct while base is only
                // materialized at snapshot/rebuild boundaries.
                for meta in &overlay_live_metas {
                    if results.len() >= scan_limit {
                        break;
                    }
                    let path_str = meta.path.to_string_lossy();
                    if self.path_is_frozen(meta.path.as_path()) {
                        continue;
                    }
                    let matches_anchor = plan.anchors().iter().any(|a| a.matches(&path_str));
                    if !matches_anchor {
                        continue;
                    }
                    let path_bytes = meta.path.as_os_str().as_encoded_bytes();
                    if blocked_paths.contains(path_bytes)
                        || path_deleted_by_any(path_bytes, deleted_sources.as_slice())
                    {
                        continue;
                    }
                    let _ = blocked_paths.insert(path_bytes);
                    if self.plan_matches(plan, meta, content_matches) {
                        results.push(QueryResultMeta::hot(meta.clone()));
                    }
                }

                if results.len() >= scan_limit {
                    break 'collect_results;
                }

                // ParentIndex fast path: if query has a parent filter, get exact candidates from base
                if let Some(ref parent_path) = plan.parent_filter() {
                    for hit in base.parent_query_metas(parent_path) {
                        let meta = hit.meta;
                        let path_bytes = meta.path.as_os_str().as_encoded_bytes();
                        if self.path_is_frozen(meta.path.as_path()) {
                            continue;
                        }
                        if self.path_blocked_by_runtime_subtree_tombstone(meta.path.as_path()) {
                            self.stats.record_query_stale_hits(1);
                            continue;
                        }
                        let blocked = blocked_paths.contains(path_bytes)
                            || path_deleted_by_any(path_bytes, deleted_sources.as_slice());
                        if blocked {
                            self.stats.record_query_stale_hits(1);
                            continue;
                        }
                        let _ = blocked_paths.insert(path_bytes);
                        if self.plan_matches(plan, &meta, content_matches) {
                            let index_tier = if hit.manifest_only {
                                QueryResultIndexTier::FrozenManifestOnly
                            } else {
                                QueryResultIndexTier::ColdMmap
                            };
                            if let Some(result) =
                                self.validate_cold_result(meta, index_tier, &mut verify_budget)
                            {
                                results.push(result);
                                if results.len() >= scan_limit {
                                    break 'collect_results;
                                }
                            } else if verify_budget.exhausted() {
                                break 'collect_results;
                            }
                        }
                    }
                }

                if self.query_layer(
                    plan,
                    base.as_ref(),
                    None,
                    deleted_sources.as_slice(),
                    &mut blocked_paths,
                    &mut results,
                    scan_limit,
                    content_matches,
                    &mut verify_budget,
                ) {
                    break 'collect_results;
                }
                if verify_budget.exhausted() {
                    break 'collect_results;
                }

                if base.file_count() == 0 && !self.rebuild_in_progress() {
                    let l2 = self.l2.load_full();
                    if self.query_l2_layer(
                        plan,
                        l2.as_ref(),
                        deleted_sources.as_slice(),
                        &mut blocked_paths,
                        &mut results,
                        scan_limit,
                        content_matches,
                        &mut verify_budget,
                    ) {
                        break 'collect_results;
                    }
                }
            }

            (results, hardlink_dupe_keys, content_dupe_metas)
        };

        let content_dupe_paths = if let Some(metas) = content_dupe_metas {
            let outcome = content_duplicate_paths(metas, self);
            self.record_content_dupe_outcome(&outcome);
            if outcome.paths.is_empty() {
                return Vec::new();
            }
            Some(outcome.paths)
        } else {
            None
        };

        filter_dupe_results(
            results,
            hardlink_dupe_keys.as_ref(),
            content_dupe_paths.as_ref(),
            limit,
        )
    }

    fn overlay_meta_for_event(&self, ev: &EventRecord) -> Option<FileMeta> {
        let path = ev.best_path().map(super::normalize_path)?;
        if let Ok(m) = std::fs::metadata(&path) {
            let file_key = FileKey::from_path_and_metadata(&path, &m)?;
            return Some(FileMeta {
                file_key,
                path,
                size: m.len(),
                mtime: m.modified().ok(),
                ctime: m.created().ok(),
                atime: m.accessed().ok(),
                kind: FileKind::from_metadata(&m),
            });
        }

        let fk = ev.id.as_file_key()?;
        self.l2.load_full().get_meta(fk)
    }

    #[allow(clippy::too_many_arguments)]
    fn query_layer(
        &self,
        plan: &QueryPlan,
        layer: &BaseIndexData,
        layer_deleted: Option<&PathArenaSet>,
        deleted_sources: &[Arc<PathArenaSet>],
        blocked_paths: &mut PathArenaSet,
        results: &mut Vec<QueryResultMeta>,
        limit: usize,
        content_matches: Option<&ContentMatcher<'_>>,
        verify_budget: &mut QueryVerifyBudget,
    ) -> bool {
        for anchor in plan.anchors() {
            for hit in layer.query_metas(anchor.as_ref()) {
                if verify_budget.exhausted() {
                    return true;
                }
                let meta = hit.meta;
                let path_bytes = meta.path.as_os_str().as_encoded_bytes();
                if self.path_is_frozen(meta.path.as_path()) {
                    continue;
                }
                if self.path_blocked_by_runtime_subtree_tombstone(meta.path.as_path()) {
                    self.stats.record_query_stale_hits(1);
                    continue;
                }
                let blocked = blocked_paths.contains(path_bytes)
                    || layer_deleted.is_some_and(|paths| paths.contains(path_bytes))
                    || path_deleted_by_any(path_bytes, deleted_sources);
                if blocked {
                    self.stats.record_query_stale_hits(1);
                    continue;
                }

                let _ = blocked_paths.insert(path_bytes);
                if self.plan_matches(plan, &meta, content_matches) {
                    let index_tier = if hit.manifest_only {
                        QueryResultIndexTier::FrozenManifestOnly
                    } else {
                        QueryResultIndexTier::ColdMmap
                    };
                    if let Some(result) = self.validate_cold_result(meta, index_tier, verify_budget)
                    {
                        results.push(result);
                        if results.len() >= limit {
                            return true;
                        }
                    } else if verify_budget.exhausted() {
                        return true;
                    }
                }
            }
        }

        false
    }

    #[allow(clippy::too_many_arguments)]
    fn query_l2_layer(
        &self,
        plan: &QueryPlan,
        layer: &PersistentIndex,
        deleted_sources: &[Arc<PathArenaSet>],
        blocked_paths: &mut PathArenaSet,
        results: &mut Vec<QueryResultMeta>,
        limit: usize,
        content_matches: Option<&ContentMatcher<'_>>,
        verify_budget: &mut QueryVerifyBudget,
    ) -> bool {
        for anchor in plan.anchors() {
            for meta in layer.query(anchor.as_ref(), limit.saturating_sub(results.len())) {
                if verify_budget.exhausted() {
                    return true;
                }
                let path_bytes = meta.path.as_os_str().as_encoded_bytes();
                if self.path_is_frozen(meta.path.as_path()) {
                    continue;
                }
                if self.path_blocked_by_runtime_subtree_tombstone(meta.path.as_path()) {
                    self.stats.record_query_stale_hits(1);
                    continue;
                }
                let blocked = blocked_paths.contains(path_bytes)
                    || path_deleted_by_any(path_bytes, deleted_sources);
                if blocked {
                    self.stats.record_query_stale_hits(1);
                    continue;
                }

                let _ = blocked_paths.insert(path_bytes);
                if self.plan_matches(plan, &meta, content_matches) {
                    if let Some(result) = self.validate_l2_warm_result(
                        meta,
                        QueryResultIndexTier::WarmMemory,
                        verify_budget,
                    ) {
                        results.push(result);
                        if results.len() >= limit {
                            return true;
                        }
                    } else if verify_budget.exhausted() {
                        return true;
                    }
                }
            }
        }

        false
    }

    fn validate_l2_warm_result(
        &self,
        meta: FileMeta,
        index_tier: QueryResultIndexTier,
        verify_budget: &mut QueryVerifyBudget,
    ) -> Option<QueryResultMeta> {
        if has_non_filesystem_file_key(&meta) {
            return Some(QueryResultMeta::cold(
                meta,
                QueryResultFreshness::Unknown,
                index_tier,
                false,
            ));
        }
        self.validate_cold_result(meta, index_tier, verify_budget)
    }

    fn plan_matches(
        &self,
        plan: &QueryPlan,
        meta: &FileMeta,
        content_matches: Option<&ContentMatcher<'_>>,
    ) -> bool {
        match content_matches {
            Some(content_matches) => plan.matches_with_content(meta, content_matches),
            None => plan.matches(meta),
        }
    }

    fn annotate_query_result(
        &self,
        meta: FileMeta,
        verify_budget: &mut QueryVerifyBudget,
    ) -> Option<QueryResultMeta> {
        if self.path_is_frozen(meta.path.as_path()) {
            return None;
        }
        if self.path_blocked_by_runtime_subtree_tombstone(meta.path.as_path()) {
            self.stats.record_query_stale_hits(1);
            return None;
        }
        let path_bytes = meta.path.as_os_str().as_encoded_bytes();
        if self.delta_buffer.lock().is_live(path_bytes) {
            return Some(QueryResultMeta::hot(meta));
        }
        let index_tier = if self.base.load().key_is_manifest_only(meta.file_key) {
            QueryResultIndexTier::FrozenManifestOnly
        } else {
            QueryResultIndexTier::ColdMmap
        };
        self.validate_cold_result(meta, index_tier, verify_budget)
    }

    fn validate_cold_result(
        &self,
        meta: FileMeta,
        index_tier: QueryResultIndexTier,
        verify_budget: &mut QueryVerifyBudget,
    ) -> Option<QueryResultMeta> {
        if self.path_is_frozen(meta.path.as_path()) {
            return None;
        }
        if has_non_filesystem_file_key(&meta) {
            return Some(QueryResultMeta::cold(
                meta,
                QueryResultFreshness::Unknown,
                index_tier,
                false,
            ));
        }

        if self.lazy_validation_is_enabled() {
            self.try_enqueue_lazy_validation(meta.clone());
            return Some(QueryResultMeta::cold(
                meta,
                QueryResultFreshness::Unknown,
                index_tier,
                false,
            ));
        }

        if !verify_budget.try_consume() {
            return None;
        }

        self.stats.record_cold_validate(1);

        let fs_meta = match std::fs::metadata(&meta.path) {
            Ok(m) if m.is_file() || m.is_dir() => m,
            Ok(_) => {
                self.apply_query_delete(meta.path.as_path());
                self.enqueue_dirty_parent(meta.path.as_path(), DirtyReason::QueryHitStale);
                self.stats.record_query_stale_hits(1);
                return None;
            }
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                self.apply_query_delete(meta.path.as_path());
                self.enqueue_dirty_parent(meta.path.as_path(), DirtyReason::QueryHitStale);
                self.stats.record_query_stale_hits(1);
                return None;
            }
            Err(e) => {
                if e.kind() == std::io::ErrorKind::PermissionDenied {
                    self.stats.record_query_permission_denied(1);
                }
                tracing::debug!(
                    "cold query validation skipped unreadable path {}: {}",
                    meta.path.display(),
                    e
                );
                return Some(QueryResultMeta::cold(
                    meta,
                    QueryResultFreshness::Unknown,
                    index_tier,
                    false,
                ));
            }
        };

        let current_key = FileKey::from_path_and_metadata(&meta.path, &fs_meta);
        let current_mtime = fs_meta.modified().ok();
        let old_mtime_ns = mtime_to_ns(meta.mtime);
        let current_mtime_ns = mtime_to_ns(current_mtime);
        let changed =
            current_key.is_some_and(|key| key != meta.file_key) || old_mtime_ns != current_mtime_ns;

        if changed {
            self.stats.record_query_stale_hits(1);
            self.enqueue_dirty_parent(meta.path.as_path(), DirtyReason::QueryHitStale);

            let current = FileMeta {
                file_key: current_key.unwrap_or(meta.file_key),
                path: meta.path,
                size: fs_meta.len(),
                mtime: current_mtime,
                ctime: fs_meta.created().ok(),
                atime: fs_meta.accessed().ok(),
                kind: FileKind::from_metadata(&fs_meta),
            };
            return Some(QueryResultMeta::cold(
                current,
                QueryResultFreshness::Changed,
                index_tier,
                true,
            ));
        }

        Some(QueryResultMeta::cold(
            meta,
            QueryResultFreshness::StaleChecked,
            index_tier,
            true,
        ))
    }

    fn apply_query_delete(&self, path: &std::path::Path) {
        let ev = EventRecord {
            seq: 0,
            timestamp: std::time::SystemTime::now(),
            event_type: crate::core::EventType::Delete,
            id: crate::core::FileIdentifier::Path(path.to_path_buf()),
            path_hint: Some(path.to_path_buf()),
        };
        self.apply_events(&[ev]);
    }

    fn enqueue_dirty_parent(&self, path: &std::path::Path, reason: DirtyReason) {
        let Some(parent) = path.parent() else {
            return;
        };
        if reason == DirtyReason::QueryHitStale {
            self.record_recent_stale_hit_dir(parent.to_path_buf());
        }
        self.enqueue_dirty_dirs(vec![parent.to_path_buf()], reason);
    }

    fn record_recent_stale_hit_dir(&self, dir: PathBuf) {
        let mut dirs = self.recent_stale_hit_dirs.lock();
        if !dirs.iter().any(|existing| existing == &dir) {
            dirs.push(dir);
        }
        if dirs.len() > 256 {
            let overflow = dirs.len().saturating_sub(256);
            dirs.drain(0..overflow);
        }
    }

    pub fn drain_recent_stale_hit_dirs(&self) -> Vec<PathBuf> {
        let mut dirs = self.recent_stale_hit_dirs.lock();
        let mut out = std::mem::take(&mut *dirs);
        out.sort();
        out.dedup();
        out
    }

    fn enqueue_query_miss(&self, keyword: &str) {
        let Some(query_path) = query_miss_path_candidate(keyword) else {
            return;
        };
        let mut dirs = Vec::new();
        if query_path.is_absolute() {
            if self
                .roots
                .iter()
                .any(|root| query_path.starts_with(root.as_path()))
            {
                if let Some(parent) = query_path.parent() {
                    dirs.push(parent.to_path_buf());
                }
            }
        } else {
            let Some(parent) = query_path.parent() else {
                return;
            };
            for root in &self.roots {
                dirs.push(root.join(parent));
            }
        }
        if dirs.is_empty() {
            return;
        }
        self.enqueue_dirty_dirs(dirs, DirtyReason::QueryMiss);
        tracing::debug!("query miss enqueued dirty compensation for {}", keyword);
    }

    fn record_content_dupe_outcome(&self, outcome: &ContentDupeOutcome) {
        self.content_hash_queue_pending.store(0, Ordering::Relaxed);
        self.content_hash_candidate_count
            .store(outcome.candidate_count as u64, Ordering::Relaxed);
        self.content_hash_confirmed_groups
            .store(outcome.confirmed_groups as u64, Ordering::Relaxed);
        self.content_hash_skipped_count
            .store(outcome.skipped_count as u64, Ordering::Relaxed);
        self.content_hash_last_elapsed_ms
            .store(outcome.elapsed_ms, Ordering::Relaxed);
        *self.content_hash_last_skip_reason.lock() = outcome.last_skip_reason.clone();
    }
}

struct QueryGenerationGuard<'a> {
    index: &'a TieredIndex,
    started: Instant,
}

impl<'a> QueryGenerationGuard<'a> {
    fn new(index: &'a TieredIndex) -> Self {
        index.begin_query_guard_metric();
        Self {
            index,
            started: Instant::now(),
        }
    }
}

impl Drop for QueryGenerationGuard<'_> {
    fn drop(&mut self) {
        let elapsed_us = self.started.elapsed().as_micros().min(u128::from(u64::MAX)) as u64;
        if self
            .index
            .finish_query_guard_metric(elapsed_us, QUERY_GUARD_SLOW_THRESHOLD_US)
        {
            tracing::warn!(
                "query generation guard held for {}us (threshold={}us)",
                elapsed_us,
                QUERY_GUARD_SLOW_THRESHOLD_US
            );
        }
    }
}

fn is_removed_size_filter_error(err: &QueryCompileError) -> bool {
    matches!(
        err,
        QueryCompileError::Filter(message)
            if message.contains("size: is no longer supported")
    )
}

fn query_miss_path_candidate(keyword: &str) -> Option<std::path::PathBuf> {
    let mut s = keyword.trim();
    for prefix in ["exact:", "anchor:", "icase:"] {
        if let Some(rest) = s.strip_prefix(prefix) {
            s = rest;
        }
    }
    if s.len() < 3 || (!s.contains('/') && !s.contains('\\')) {
        return None;
    }
    if s.contains('*') || s.starts_with("re:") {
        return None;
    }
    Some(std::path::PathBuf::from(s))
}

fn has_non_filesystem_file_key(meta: &FileMeta) -> bool {
    // Several regression tests build synthetic in-memory indexes with dev=1
    // and no backing file. Do not convert those fixtures into tombstones.
    meta.file_key.dev <= 1 && meta.file_key.generation == 0
}

fn hardlink_duplicate_keys(metas: impl IntoIterator<Item = FileMeta>) -> HashSet<FileKey> {
    let mut counts: HashMap<FileKey, usize> = HashMap::new();
    for meta in metas {
        if meta.kind.is_file() {
            *counts.entry(meta.file_key).or_default() += 1;
        }
    }
    counts
        .into_iter()
        .filter_map(|(key, count)| (count >= 2).then_some(key))
        .collect()
}

fn filter_dupe_results(
    results: Vec<QueryResultMeta>,
    hardlink_dupe_keys: Option<&HashSet<FileKey>>,
    content_dupe_paths: Option<&HashSet<PathBuf>>,
    limit: usize,
) -> Vec<QueryResultMeta> {
    if hardlink_dupe_keys.is_none() && content_dupe_paths.is_none() {
        return results;
    }
    results
        .into_iter()
        .filter_map(|mut result| {
            if let Some(keys) = hardlink_dupe_keys {
                if !keys.contains(&result.meta.file_key) {
                    return None;
                }
                result.reason = Some(HARDLINK_DUPE_REASON.to_string());
                result.confidence = Some(HARDLINK_DUPE_CONFIDENCE);
            }
            if let Some(paths) = content_dupe_paths {
                if !paths.contains(&result.meta.path) {
                    return None;
                }
                result.reason = Some(CONTENT_DUPE_REASON.to_string());
                result.confidence = Some(CONTENT_DUPE_CONFIDENCE);
            }
            Some(result)
        })
        .take(limit)
        .collect()
}

#[derive(Default)]
struct ContentDupeOutcome {
    paths: HashSet<PathBuf>,
    candidate_count: usize,
    confirmed_groups: usize,
    skipped_count: usize,
    last_skip_reason: String,
    elapsed_ms: u64,
}

#[derive(Clone)]
struct ContentDupeCandidate {
    meta: FileMeta,
    size: u64,
}

fn content_duplicate_paths(
    metas: impl IntoIterator<Item = FileMeta>,
    index: &TieredIndex,
) -> ContentDupeOutcome {
    let started = Instant::now();
    let mut outcome = ContentDupeOutcome::default();
    let mut by_size: HashMap<u64, Vec<ContentDupeCandidate>> = HashMap::new();
    let config = index.content_index_config.lock().clone();
    let fs_policy = FsPolicy::current_with_config(index.fs_policy_config());

    for meta in metas {
        let Some(candidate) =
            content_dupe_candidate(meta, index, &config, &fs_policy, &mut outcome)
        else {
            continue;
        };
        by_size.entry(candidate.size).or_default().push(candidate);
    }

    let mut partial_groups: HashMap<(u64, u64), Vec<ContentDupeCandidate>> = HashMap::new();
    for (size, candidates) in by_size {
        if candidates.len() < 2 {
            continue;
        }
        outcome.candidate_count += candidates.len();
        for candidate in candidates {
            match file_partial_hash(&candidate.meta.path, index) {
                Ok(partial_hash) => partial_groups
                    .entry((size, partial_hash))
                    .or_default()
                    .push(candidate),
                Err(reason) => record_content_skip(&mut outcome, reason),
            }
        }
    }

    let mut full_groups: HashMap<(u64, u64), Vec<PathBuf>> = HashMap::new();
    for ((size, _partial_hash), candidates) in partial_groups {
        if candidates.len() < 2 {
            continue;
        }
        for candidate in candidates {
            match file_full_hash(&candidate.meta.path, index) {
                Ok(full_hash) => full_groups
                    .entry((size, full_hash))
                    .or_default()
                    .push(candidate.meta.path),
                Err(reason) => record_content_skip(&mut outcome, reason),
            }
        }
    }

    for mut paths in full_groups.into_values() {
        if paths.len() < 2 {
            continue;
        }
        paths.sort();
        paths.dedup();
        if paths.len() < 2 {
            continue;
        }
        outcome.confirmed_groups += 1;
        outcome.paths.extend(paths);
    }

    outcome.elapsed_ms = started.elapsed().as_millis().min(u128::from(u64::MAX)) as u64;
    outcome
}

fn content_dupe_candidate(
    meta: FileMeta,
    index: &TieredIndex,
    config: &crate::config::ContentIndexConfig,
    fs_policy: &Option<FsPolicy>,
    outcome: &mut ContentDupeOutcome,
) -> Option<ContentDupeCandidate> {
    let size = match index.content_read_eligibility(&meta, config, fs_policy, false) {
        ContentReadEligibility::Eligible { size } => size,
        ContentReadEligibility::Skip { reason } => {
            record_content_skip(outcome, reason);
            return None;
        }
    };
    Some(ContentDupeCandidate { meta, size })
}

fn file_partial_hash(path: &Path, index: &TieredIndex) -> Result<u64, String> {
    let mut file = std::fs::File::open(path).map_err(|err| format!("open_error:{}", err.kind()))?;
    let mut buf = vec![0u8; CONTENT_DUPE_PARTIAL_BYTES];
    let mut read_total = 0usize;
    while read_total < CONTENT_DUPE_PARTIAL_BYTES {
        index.io_governor.before_io();
        let read = file
            .read(&mut buf[read_total..CONTENT_DUPE_PARTIAL_BYTES])
            .map_err(|err| format!("read_error:{}", err.kind()))?;
        if read == 0 {
            break;
        }
        read_total += read;
    }
    buf.truncate(read_total);
    Ok(ContentFilter::content_hash(&buf))
}

fn file_full_hash(path: &Path, index: &TieredIndex) -> Result<u64, String> {
    let mut file = std::fs::File::open(path).map_err(|err| format!("open_error:{}", err.kind()))?;
    let mut hasher = Xxh3::new();
    let mut buf = [0u8; 64 * 1024];
    loop {
        index.io_governor.before_io();
        let read = file
            .read(&mut buf)
            .map_err(|err| format!("read_error:{}", err.kind()))?;
        if read == 0 {
            break;
        }
        hasher.update(&buf[..read]);
    }
    Ok(hasher.digest())
}

fn record_content_skip(outcome: &mut ContentDupeOutcome, reason: String) {
    outcome.skipped_count += 1;
    outcome.last_skip_reason = reason;
}

fn sorted_limited_dirs(dirs: HashSet<PathBuf>, limit: usize) -> Vec<PathBuf> {
    let mut dirs = dirs.into_iter().collect::<Vec<_>>();
    dirs.sort();
    dirs.truncate(limit);
    dirs
}

fn collect_parent_dirs_for_fast_scan(
    meta: &FileMeta,
    roots: &[PathBuf],
    excluded_roots: &[PathBuf],
    dirs: &mut HashSet<PathBuf>,
    limit: usize,
) {
    let mut current = if meta.kind.is_directory() {
        Some(meta.path.as_path())
    } else {
        meta.path.parent()
    };
    while let Some(dir) = current {
        let under_index_root = roots
            .iter()
            .any(|root| dir == root.as_path() || dir.starts_with(root.as_path()));
        if under_index_root && !path_is_under_any_root(dir, excluded_roots) {
            dirs.insert(dir.to_path_buf());
        }
        if dirs.len() >= limit {
            return;
        }
        if roots.iter().any(|root| dir == root.as_path()) {
            break;
        }
        current = dir.parent();
    }
}

fn path_is_under_any_root(path: &Path, roots: &[PathBuf]) -> bool {
    roots
        .iter()
        .any(|root| path == root.as_path() || path.starts_with(root.as_path()))
}

fn validate_snapshot_materialization(
    base_count_before: usize,
    deleted_paths: usize,
    upserted_paths: usize,
    candidate_count: usize,
) -> anyhow::Result<()> {
    if base_count_before < 10_000 || candidate_count >= base_count_before {
        return Ok(());
    }

    let allowed_loss = (base_count_before / 10)
        .max(10_000)
        .max(deleted_paths.saturating_mul(1024));
    let min_expected = base_count_before.saturating_sub(allowed_loss);
    if candidate_count < min_expected {
        anyhow::bail!(
            "snapshot materialization guard refused to shrink base from {} to {} entries \
             (deleted_paths={}, upserted_paths={}, min_expected={})",
            base_count_before,
            candidate_count,
            deleted_paths,
            upserted_paths,
            min_expected
        );
    }

    Ok(())
}

fn collect_live_meta(
    meta: FileMeta,
    layer_deleted: Option<&PathArenaSet>,
    deleted_sources: &[Arc<PathArenaSet>],
    blocked_paths: &mut PathArenaSet,
    results: &mut Vec<FileMeta>,
) {
    let path_bytes = meta.path.as_os_str().as_encoded_bytes();
    if blocked_paths.contains(path_bytes)
        || layer_deleted.is_some_and(|paths| paths.contains(path_bytes))
        || path_deleted_by_any(path_bytes, deleted_sources)
    {
        return;
    }

    let _ = blocked_paths.insert(path_bytes);
    results.push(meta);
}
