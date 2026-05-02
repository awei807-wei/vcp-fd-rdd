use std::sync::Arc;

use crate::core::{FileKey, FileMeta};
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

        let base_count = self.base.load().file_count();
        let l2_count = self.l2.load().file_count();
        if base_count != l2_count {
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
        let mut ups = PathArenaSet::default();
        for p in db.upserted_paths() {
            let _ = ups.insert(p);
        }
        drop(db);
        let overlay_deleted = Arc::new(del);
        let overlay_upserted = Arc::new(ups);
        let mut blocked_paths = PathArenaSet::default();
        let deleted_sources: Vec<Arc<PathArenaSet>> = vec![overlay_deleted];
        let mut seen: std::collections::HashSet<FileKey> =
            std::collections::HashSet::with_capacity(base.file_count().saturating_add(256));
        let mut results: Vec<FileMeta> = Vec::with_capacity(base.file_count().saturating_add(256));

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

        overlay_upserted.for_each_bytes(|bytes| {
            let path = super::pathbuf_from_bytes(bytes);
            let path = super::normalize_path(&path);
            let path_bytes = path.as_os_str().as_encoded_bytes();
            if blocked_paths.contains(path_bytes)
                || path_deleted_by_any(path_bytes, deleted_sources.as_slice())
            {
                return;
            }

            let meta = match std::fs::metadata(&path) {
                Ok(m) => {
                    let file_key = match FileKey::from_path_and_metadata(&path, &m) {
                        Some(fk) => fk,
                        None => return,
                    };
                    if !seen.insert(file_key) {
                        return;
                    }
                    FileMeta {
                        file_key,
                        path: path.clone(),
                        size: m.len(),
                        mtime: m.modified().ok(),
                        ctime: m.created().ok(),
                        atime: m.accessed().ok(),
                    }
                }
                Err(_) => return,
            };
            let _ = blocked_paths.insert(path_bytes);
            results.push(meta);
        });

        results
    }

    fn execute_query_plan(&self, plan: &QueryPlan, limit: usize) -> Vec<FileMeta> {
        let base = self.base.load_full();
        let db = self.delta_buffer.lock();
        let mut del = PathArenaSet::default();
        for p in db.deleted_paths() {
            let _ = del.insert(p);
        }
        let mut ups = PathArenaSet::default();
        for p in db.upserted_paths() {
            let _ = ups.insert(p);
        }
        drop(db);
        let overlay_deleted = Arc::new(del);
        let overlay_upserted = Arc::new(ups);
        let mut blocked_paths = PathArenaSet::default();
        let deleted_sources: Vec<Arc<PathArenaSet>> = vec![overlay_deleted];
        let mut seen: std::collections::HashSet<FileKey> =
            std::collections::HashSet::with_capacity(base.file_count().saturating_add(256));
        let mut results: Vec<FileMeta> = Vec::with_capacity(limit.min(128));

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

        // Supplement results from overlay upserted paths (e.g., during rebuild)
        if results.len() < limit {
            overlay_upserted.for_each_bytes(|bytes| {
                if results.len() >= limit {
                    return;
                }
                let path = super::pathbuf_from_bytes(bytes);
                let path = super::normalize_path(&path);
                let path_str = path.to_string_lossy();
                let matches_anchor = plan.anchors().iter().any(|a| a.matches(&path_str));
                if !matches_anchor {
                    return;
                }
                let meta = match std::fs::metadata(&path) {
                    Ok(m) => {
                        let file_key = match FileKey::from_path_and_metadata(&path, &m) {
                            Some(fk) => fk,
                            None => return,
                        };
                        if !seen.insert(file_key) {
                            return;
                        }
                        FileMeta {
                            file_key,
                            path: path.clone(),
                            size: m.len(),
                            mtime: m.modified().ok(),
                            ctime: m.created().ok(),
                            atime: m.accessed().ok(),
                        }
                    }
                    Err(_) => {
                        // File may have been deleted after upsert; skip.
                        return;
                    }
                };
                if plan.matches(&meta) {
                    results.push(meta);
                }
            });
        }

        results
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
