use std::sync::Arc;

use crate::core::{EventRecord, FileKey, FileMeta};
use crate::index::base_index::BaseIndexData;
use crate::index::l2_partition::PersistentIndex;
use crate::index::IndexLayer;
use crate::query::dsl::compile_query;
use crate::query::matcher::create_matcher;

use super::arena::{path_deleted_by_any, PathArenaSet};
use super::query_plan::QueryPlan;
use super::TieredIndex;

impl TieredIndex {
    /// 查询入口：L1 → L2 → DiskSegments（mmap），不扫真实文件系统
    pub fn query(&self, keyword: &str) -> Vec<FileMeta> {
        self.query_limit(keyword, usize::MAX)
    }

    /// 查询入口（带 limit）：用于 IPC/HTTP 等"结果集可能很大"的场景，避免一次性聚合造成内存峰值。
    pub fn query_limit(&self, keyword: &str, limit: usize) -> Vec<FileMeta> {
        if limit == 0 {
            return Vec::new();
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
                let case_sensitive =
                    keyword.contains("case:") || keyword.chars().any(|c| c.is_uppercase());
                let matcher = create_matcher(keyword, case_sensitive);

                if let Some(results) = self.l1.query(matcher.as_ref()) {
                    tracing::debug!("L1 hit: {} results", results.len());
                    return results.into_iter().take(limit).collect();
                }

                QueryPlan::legacy(matcher)
            }
        };

        let results = self.execute_query_plan(&plan, limit);
        if !results.is_empty() {
            tracing::debug!("Query hit: {} results", results.len());
            for meta in results.iter().take(10) {
                self.l1.insert(meta.clone());
            }
            return results;
        }

        self.l2.load_full().maybe_schedule_repair();
        Vec::new()
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
        let mut seen: std::collections::HashSet<FileKey> =
            std::collections::HashSet::with_capacity(base.file_count().saturating_add(256));
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
            if !seen.insert(meta.file_key) {
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
                &mut seen,
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
        let mut seen: std::collections::HashSet<FileKey> =
            std::collections::HashSet::with_capacity(base.file_count().saturating_add(256));
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
            if !seen.insert(meta.file_key) {
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
                &mut seen,
                &mut blocked_paths,
                &mut metas,
            );
        });

        let compact = PersistentIndex::new_with_roots(self.roots.clone());
        for meta in metas {
            compact.upsert_rename(meta);
        }
        let new_base = Arc::new(compact.to_base_index_data());
        self.base.store(new_base.clone());
        self.l2.store(Arc::new(PersistentIndex::new_with_roots(
            self.roots.clone(),
        )));
        new_base
    }

    fn execute_query_plan(&self, plan: &QueryPlan, limit: usize) -> Vec<FileMeta> {
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
        let mut seen: std::collections::HashSet<FileKey> =
            std::collections::HashSet::with_capacity(base.file_count().saturating_add(256));
        let mut results: Vec<FileMeta> = Vec::with_capacity(limit.min(128));

        // Overlay upserts take precedence over the immutable base. This keeps
        // delete+recreate and rename windows correct while base is only
        // materialized at snapshot/rebuild boundaries.
        for ev in &live_events {
            if results.len() >= limit {
                break;
            }
            let Some(meta) = self.overlay_meta_for_event(ev) else {
                continue;
            };
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
            if !seen.insert(meta.file_key) {
                continue;
            }
            let _ = blocked_paths.insert(path_bytes);
            if plan.matches(&meta) {
                results.push(meta);
            }
        }

        if results.len() >= limit {
            return results;
        }

        // ParentIndex fast path: if query has a parent filter, get exact candidates from base
        if let Some(ref parent_path) = plan.parent_filter() {
            let candidates = base.parent_candidates(parent_path);
            for key in candidates {
                if !seen.insert(key) {
                    continue;
                }
                let Some(meta) = base.get_meta(key) else {
                    continue;
                };
                let path_bytes = meta.path.as_os_str().as_encoded_bytes();
                if blocked_paths.contains(path_bytes)
                    || path_deleted_by_any(path_bytes, deleted_sources.as_slice())
                {
                    continue;
                }
                let _ = blocked_paths.insert(path_bytes);
                if plan.matches(&meta) {
                    results.push(meta);
                    if results.len() >= limit {
                        return results;
                    }
                }
            }
        }

        if self.query_layer(
            plan,
            base.as_ref(),
            None,
            deleted_sources.as_slice(),
            &mut seen,
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
        layer: &dyn IndexLayer,
        layer_deleted: Option<&PathArenaSet>,
        deleted_sources: &[Arc<PathArenaSet>],
        seen: &mut std::collections::HashSet<FileKey>,
        blocked_paths: &mut PathArenaSet,
        results: &mut Vec<FileMeta>,
        limit: usize,
    ) -> bool {
        for anchor in plan.anchors() {
            for key in layer.query_keys(anchor.as_ref()) {
                if !seen.insert(key) {
                    continue;
                }

                let Some(meta) = layer.get_meta(key) else {
                    continue;
                };
                let path_bytes = meta.path.as_os_str().as_encoded_bytes();
                if blocked_paths.contains(path_bytes)
                    || layer_deleted.is_some_and(|paths| paths.contains(path_bytes))
                    || path_deleted_by_any(path_bytes, deleted_sources)
                {
                    continue;
                }

                let _ = blocked_paths.insert(path_bytes);
                if plan.matches(&meta) {
                    results.push(meta);
                    if results.len() >= limit {
                        return true;
                    }
                }
            }
        }

        false
    }
}

fn collect_live_meta(
    meta: FileMeta,
    layer_deleted: Option<&PathArenaSet>,
    deleted_sources: &[Arc<PathArenaSet>],
    seen: &mut std::collections::HashSet<FileKey>,
    blocked_paths: &mut PathArenaSet,
    results: &mut Vec<FileMeta>,
) {
    if !seen.insert(meta.file_key) {
        return;
    }

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
