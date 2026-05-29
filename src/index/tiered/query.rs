use std::sync::Arc;

use crate::core::{EventRecord, FileKey, FileMeta};
use crate::event::sync::DirtyReason;
use crate::index::base_index::BaseIndexData;
use crate::index::l2_partition::{mtime_to_ns, PersistentIndex};
use crate::index::IndexLayer;
use crate::query::dsl::{compile_query, QueryCompileError};
use crate::query::matcher::create_matcher;

use super::arena::{path_deleted_by_any, PathArenaSet};
use super::query_plan::QueryPlan;
use super::{QueryResultFreshness, QueryResultIndexTier, QueryResultMeta, TieredIndex};

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

        if self.base.load().file_count() == 0
            && !self.rebuild_in_progress()
            && self.l2.load().file_count() > 0
        {
            self.refresh_base();
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
                        .take(limit)
                        .map(QueryResultMeta::hot)
                        .collect());
                }

                QueryPlan::legacy(matcher)
            }
        };

        let results = self.execute_query_plan(&plan, limit);
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
        metas
            .into_iter()
            .filter_map(|meta| self.annotate_query_result(meta))
            .collect()
    }

    pub(crate) fn collect_all_live_metas(&self) -> Vec<FileMeta> {
        let base = self.base.load_full();
        let db = self.delta_buffer.lock();
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
            results.push(meta);
        }

        base.for_each_live_meta(|meta| {
            collect_live_meta(
                meta,
                None,
                deleted_sources.as_slice(),
                &mut blocked_paths,
                &mut results,
            );
        });

        results
    }

    pub(crate) fn materialize_snapshot_base(&self) -> Arc<BaseIndexData> {
        let mut db = self.delta_buffer.lock();
        let mut del = PathArenaSet::default();
        for p in db.deleted_paths() {
            let _ = del.insert(p);
        }
        let live_events: Vec<EventRecord> = db.live_records().cloned().collect();
        db.clear();

        let base = self.base.load_full();
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
        self.base.store(new_base.clone());
        self.l2.store(Arc::new(PersistentIndex::new_with_roots(
            self.roots.clone(),
        )));
        new_base
    }

    fn execute_query_plan(&self, plan: &QueryPlan, limit: usize) -> Vec<QueryResultMeta> {
        let base = self.base.load_full();
        let db = self.delta_buffer.lock();
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
            overlay_live_metas.push(meta);
        }
        let mut results: Vec<QueryResultMeta> = Vec::with_capacity(limit.min(128));

        // Overlay upserts take precedence over the immutable base. This keeps
        // delete+recreate and rename windows correct while base is only
        // materialized at snapshot/rebuild boundaries.
        for meta in &overlay_live_metas {
            if results.len() >= limit {
                break;
            }
            let path_str = meta.path.to_string_lossy();
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
            if plan.matches(meta) {
                results.push(QueryResultMeta::hot(meta.clone()));
            }
        }

        if results.len() >= limit {
            return results;
        }

        // ParentIndex fast path: if query has a parent filter, get exact candidates from base
        if let Some(ref parent_path) = plan.parent_filter() {
            for hit in base.parent_query_metas(parent_path) {
                let meta = hit.meta;
                let path_bytes = meta.path.as_os_str().as_encoded_bytes();
                let blocked = blocked_paths.contains(path_bytes)
                    || path_deleted_by_any(path_bytes, deleted_sources.as_slice());
                if blocked {
                    self.stats.record_query_stale_hits(1);
                    continue;
                }
                let _ = blocked_paths.insert(path_bytes);
                if plan.matches(&meta) {
                    let index_tier = if hit.manifest_only {
                        QueryResultIndexTier::FrozenManifestOnly
                    } else {
                        QueryResultIndexTier::ColdMmap
                    };
                    if let Some(result) = self.validate_cold_result(meta, index_tier) {
                        results.push(result);
                        if results.len() >= limit {
                            return results;
                        }
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
            limit,
        ) {
            return results;
        }

        results
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
    ) -> bool {
        for anchor in plan.anchors() {
            for hit in layer.query_metas(anchor.as_ref()) {
                let meta = hit.meta;
                let path_bytes = meta.path.as_os_str().as_encoded_bytes();
                let blocked = blocked_paths.contains(path_bytes)
                    || layer_deleted.is_some_and(|paths| paths.contains(path_bytes))
                    || path_deleted_by_any(path_bytes, deleted_sources);
                if blocked {
                    self.stats.record_query_stale_hits(1);
                    continue;
                }

                let _ = blocked_paths.insert(path_bytes);
                if plan.matches(&meta) {
                    let index_tier = if hit.manifest_only {
                        QueryResultIndexTier::FrozenManifestOnly
                    } else {
                        QueryResultIndexTier::ColdMmap
                    };
                    if let Some(result) = self.validate_cold_result(meta, index_tier) {
                        results.push(result);
                        if results.len() >= limit {
                            return true;
                        }
                    }
                }
            }
        }

        false
    }

    fn annotate_query_result(&self, meta: FileMeta) -> Option<QueryResultMeta> {
        let path_bytes = meta.path.as_os_str().as_encoded_bytes();
        if self.delta_buffer.lock().is_live(path_bytes) {
            return Some(QueryResultMeta::hot(meta));
        }
        let index_tier = if self.base.load().key_is_manifest_only(meta.file_key) {
            QueryResultIndexTier::FrozenManifestOnly
        } else {
            QueryResultIndexTier::ColdMmap
        };
        self.validate_cold_result(meta, index_tier)
    }

    fn validate_cold_result(
        &self,
        meta: FileMeta,
        index_tier: QueryResultIndexTier,
    ) -> Option<QueryResultMeta> {
        if meta.mtime.is_none() {
            return Some(QueryResultMeta::cold(
                meta,
                QueryResultFreshness::Unknown,
                index_tier,
                false,
            ));
        }

        self.stats.record_cold_validate(1);

        let fs_meta = match std::fs::metadata(&meta.path) {
            Ok(m) if m.is_file() => m,
            Ok(_) => {
                self.apply_query_delete(meta.path.as_path());
                self.enqueue_dirty_parent(meta.path.as_path(), DirtyReason::QueryHitStale);
                self.stats.record_query_stale_hits(1);
                return None;
            }
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                if has_non_filesystem_file_key(&meta) {
                    return Some(QueryResultMeta::cold(
                        meta,
                        QueryResultFreshness::Unknown,
                        index_tier,
                        false,
                    ));
                }
                self.apply_query_delete(meta.path.as_path());
                self.enqueue_dirty_parent(meta.path.as_path(), DirtyReason::QueryHitStale);
                self.stats.record_query_stale_hits(1);
                return None;
            }
            Err(e) => {
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
        self.enqueue_dirty_dirs(vec![parent.to_path_buf()], reason);
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
