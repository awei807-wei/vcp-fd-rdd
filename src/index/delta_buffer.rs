use crate::core::{EventRecord, EventType, FileKind, FileMeta};
use crate::index::case_policy::unicode_case_fold_lookup;
use roaring::RoaringBitmap;
use std::collections::{hash_map::Entry, BTreeSet, HashMap};
use std::sync::Arc;

#[derive(Debug, Clone, Default)]
pub(crate) struct SubtreeInvalidationSnapshot {
    prefixes: BTreeSet<Vec<u8>>,
    epoch: u64,
}

impl SubtreeInvalidationSnapshot {
    pub(crate) fn covers(&self, path: &std::path::Path) -> bool {
        path.ancestors().any(|ancestor| {
            self.prefixes
                .contains(ancestor.as_os_str().as_encoded_bytes())
        })
    }

    pub(crate) fn epoch(&self) -> u64 {
        self.epoch
    }
}

/// 查询 overlay 的稳态物化缓存：元数据数组与同代 trigram posting 原子发布。
const OVERLAY_TRIGRAM_MAX_METAS: usize = 16 * 1024;
const OVERLAY_TRIGRAM_MAX_PATH_BYTES: usize = 8 * 1024 * 1024;
const OVERLAY_TRIGRAM_MAX_DISTINCT: usize = 64 * 1024;
const OVERLAY_TRIGRAM_MAX_POSTINGS: usize = 1_000_000;

#[derive(Debug)]
pub struct OverlayMetaCache {
    metas: Arc<Vec<FileMeta>>,
    trigram_index: Option<HashMap<[u8; 3], RoaringBitmap>>,
}

impl OverlayMetaCache {
    pub fn new(metas: Arc<Vec<FileMeta>>) -> Self {
        let path_bytes = metas.iter().fold(0usize, |total, meta| {
            total.saturating_add(meta.path.as_os_str().as_encoded_bytes().len())
        });
        if metas.len() > OVERLAY_TRIGRAM_MAX_METAS || path_bytes > OVERLAY_TRIGRAM_MAX_PATH_BYTES {
            return Self {
                metas,
                trigram_index: None,
            };
        }
        let trigram_index = Self::build_trigram_index(
            metas.as_slice(),
            OVERLAY_TRIGRAM_MAX_DISTINCT,
            OVERLAY_TRIGRAM_MAX_POSTINGS,
        );
        Self {
            metas,
            trigram_index,
        }
    }

    fn build_trigram_index(
        metas: &[FileMeta],
        max_distinct: usize,
        max_postings: usize,
    ) -> Option<HashMap<[u8; 3], RoaringBitmap>> {
        let mut trigram_index: HashMap<[u8; 3], RoaringBitmap> = HashMap::new();
        let mut posting_count = 0usize;
        for (doc_id, meta) in metas.iter().enumerate() {
            let folded = unicode_case_fold_lookup(&meta.path.to_string_lossy());
            for tri in folded.as_bytes().windows(3) {
                let key = [tri[0], tri[1], tri[2]];
                let distinct_count = trigram_index.len();
                let inserted = match trigram_index.entry(key) {
                    Entry::Occupied(mut entry) => entry.get_mut().insert(doc_id as u32),
                    Entry::Vacant(entry) => {
                        if distinct_count >= max_distinct {
                            return None;
                        }
                        entry.insert(RoaringBitmap::new()).insert(doc_id as u32)
                    }
                };
                if inserted {
                    posting_count = posting_count.saturating_add(1);
                    if posting_count > max_postings {
                        return None;
                    }
                }
            }
        }
        Some(trigram_index)
    }

    pub fn metas(&self) -> &[FileMeta] {
        self.metas.as_slice()
    }

    pub fn metas_arc(&self) -> Arc<Vec<FileMeta>> {
        Arc::clone(&self.metas)
    }

    pub fn estimated_bytes(&self) -> usize {
        use std::mem::size_of;
        let meta_bytes = self.metas.capacity() * size_of::<FileMeta>()
            + self
                .metas
                .iter()
                .map(|meta| meta.path.as_os_str().as_encoded_bytes().len())
                .sum::<usize>();
        let trigram_bytes = self
            .trigram_index
            .as_ref()
            .map(|index| {
                index.capacity() * (size_of::<([u8; 3], RoaringBitmap)>() + 1)
                    + index
                        .values()
                        .map(|posting| {
                            size_of::<RoaringBitmap>()
                                + posting
                                    .serialized_size()
                                    .max(posting.len() as usize * size_of::<u32>())
                        })
                        .sum::<usize>()
            })
            .unwrap_or(0);
        size_of::<Self>() + meta_bytes + trigram_bytes
    }

    /// 返回 literal hint 的安全候选交集；短于 3 字节时无法收窄。
    pub fn trigram_candidates(&self, hint: &[u8]) -> Option<RoaringBitmap> {
        let index = self.trigram_index.as_ref()?;
        let folded = unicode_case_fold_lookup(&String::from_utf8_lossy(hint));
        let mut trigrams = folded.as_bytes().windows(3);
        let first = trigrams.next()?;
        let first_key = [first[0], first[1], first[2]];
        let Some(first_posting) = index.get(&first_key) else {
            return Some(RoaringBitmap::new());
        };
        let mut candidates = first_posting.clone();
        for tri in trigrams {
            let key = [tri[0], tri[1], tri[2]];
            let Some(posting) = index.get(&key) else {
                return Some(RoaringBitmap::new());
            };
            candidates &= posting;
            if candidates.is_empty() {
                break;
            }
        }
        Some(candidates)
    }
}

/// 统一增量缓冲区，替代 overlay_state + pending_events
#[derive(Debug, Clone)]
pub struct DeltaBuffer {
    /// 路径(bytes) → 最新增量状态（按路径去重）
    entries: HashMap<Vec<u8>, DeltaState>,
    /// 硬容量上限（默认 256K 条）
    max_capacity: usize,
    /// Normal operating limit restored after a rebuild/snapshot generation is
    /// durably closed.
    base_max_capacity: usize,
    /// Prefixes whose pre-event descendants are invalid for this generation.
    /// This survives a later Live state at the same path so delete→recreate
    /// cannot resurrect cold children.
    subtree_invalidations: std::collections::HashSet<Vec<u8>>,
    subtree_invalidation_epoch: u64,
    /// Recursively scanned subtree roots whose descendants were stable at the
    /// scan boundary. Direct v7 persistence may use these proofs to distinguish
    /// a complete subtree move/recreate from a lone directory event.
    complete_subtree_scans: std::collections::HashSet<Vec<u8>>,
    /// Rename targets and delete-then-recreate targets that may contain an
    /// unobserved subtree. This association survives later scan upserts at the
    /// same path until the generation is durably closed.
    structural_upsert_targets: std::collections::HashSet<Vec<u8>>,
    /// Once a path has been rejected, this generation is no longer a complete
    /// description of the mutable L2. A direct snapshot must fail closed and
    /// rebuild from the filesystem instead of persisting a partial delta.
    overflowed: bool,
    /// A directory rename/recreate crossed the active scan boundary. Replaying
    /// one directory record cannot prove that every descendant was scanned.
    structural_replay_unproven: bool,
    /// 记录集（entries）的变更代数：任何 insert/delete/clear 递增。
    mutation_epoch: u64,
    /// 按 mutation_epoch 失效的 overlay 物化 meta 缓存（查询路径填充）。
    overlay_meta_cache: Option<Arc<OverlayMetaCache>>,
}

#[derive(Debug, Clone)]
pub enum DeltaState {
    /// 文件存在（创建/修改/重命名到）
    Live(EventRecord),
    /// 文件已删除
    Deleted,
}

impl DeltaBuffer {
    pub fn with_capacity(cap: usize) -> Self {
        Self {
            entries: HashMap::with_capacity(cap.min(1024)),
            max_capacity: cap,
            base_max_capacity: cap,
            subtree_invalidations: std::collections::HashSet::new(),
            subtree_invalidation_epoch: 0,
            complete_subtree_scans: std::collections::HashSet::new(),
            structural_upsert_targets: std::collections::HashSet::new(),
            overflowed: false,
            structural_replay_unproven: false,
            mutation_epoch: 0,
            overlay_meta_cache: None,
        }
    }

    #[cfg(test)]
    fn with_capacity_and_limit(cap: usize, max_capacity: usize) -> Self {
        Self {
            entries: HashMap::with_capacity(cap.min(1024)),
            max_capacity,
            base_max_capacity: max_capacity,
            subtree_invalidations: std::collections::HashSet::new(),
            subtree_invalidation_epoch: 0,
            complete_subtree_scans: std::collections::HashSet::new(),
            structural_upsert_targets: std::collections::HashSet::new(),
            overflowed: false,
            structural_replay_unproven: false,
            mutation_epoch: 0,
            overlay_meta_cache: None,
        }
    }

    /// 记录集发生变更：推进代数并失效 overlay 物化缓存。
    fn note_mutation(&mut self) {
        self.mutation_epoch = self.mutation_epoch.wrapping_add(1);
        self.overlay_meta_cache = None;
    }

    /// 使依赖外部可见性状态（例如 freeze gate）的查询缓存失效。
    /// 推进 epoch 可阻止并发中的旧构建结果重新发布。
    pub fn invalidate_overlay_meta_cache(&mut self) {
        self.note_mutation();
    }

    pub fn mutation_epoch(&self) -> u64 {
        self.mutation_epoch
    }

    pub fn overlay_meta_cache(&self) -> Option<Arc<OverlayMetaCache>> {
        self.overlay_meta_cache.as_ref().map(Arc::clone)
    }

    /// 回写物化缓存；仅在 epoch 未被并发 mutation 推进时接受。
    pub fn store_overlay_meta_cache(&mut self, epoch: u64, cache: Arc<OverlayMetaCache>) -> bool {
        if epoch != self.mutation_epoch {
            return false;
        }
        self.overlay_meta_cache = Some(cache);
        true
    }

    /// 应用一批事件，按路径去重保留最新状态。
    /// 返回 `true` 表示所有事件均已应用；`false` 表示容量已满，后续事件被丢弃。
    pub fn apply_events(&mut self, events: &[EventRecord]) -> bool {
        for ev in events {
            if !self.insert(ev.clone()) {
                self.overflowed = true;
                return false;
            }
        }
        true
    }

    /// 单个事件插入（内部去重逻辑）。
    /// 返回 `true` 表示已插入/更新；`false` 表示容量已满且为新路径，无法插入。
    fn insert(&mut self, event: EventRecord) -> bool {
        let Some(path) = event.best_path() else {
            // FID-only 且无路径：保守跳过 overlay 更新（后续 fanotify 反查完善）。
            return true;
        };
        let path_bytes = path.as_os_str().as_encoded_bytes().to_vec();

        match &event.event_type {
            EventType::Delete => {
                if self.entries.len() >= self.max_capacity
                    && !self.entries.contains_key(&path_bytes)
                {
                    return false;
                }
                self.note_subtree_invalidation(path_bytes.clone());
                self.entries.insert(path_bytes, DeltaState::Deleted);
                self.note_mutation();
                true
            }
            EventType::Create | EventType::Modify => {
                if self.entries.len() >= self.max_capacity
                    && !self.entries.contains_key(&path_bytes)
                {
                    return false;
                }
                if matches!(&event.event_type, EventType::Create)
                    || matches!(&event.event_type, EventType::Modify)
                        && self
                            .subtree_invalidations
                            .iter()
                            .any(|prefix| encoded_path_is_same_or_descendant(&path_bytes, prefix))
                {
                    self.structural_upsert_targets.insert(path_bytes.clone());
                }
                self.entries.insert(path_bytes, DeltaState::Live(event));
                self.note_mutation();
                true
            }
            EventType::Rename {
                from,
                from_path_hint,
            } => {
                let from_best = from_path_hint.as_deref().or_else(|| from.as_path());
                let from_bytes = from_best.map(|p| p.as_os_str().as_encoded_bytes().to_vec());

                // 计算本次 Rename 事件是否会净增新条目。
                let mut net_new = 0usize;
                if let Some(ref fb) = from_bytes {
                    if !self.entries.contains_key(fb) {
                        net_new += 1;
                    }
                }
                if !self.entries.contains_key(&path_bytes) {
                    net_new += 1;
                }

                if self.entries.len().saturating_add(net_new) > self.max_capacity {
                    return false;
                }

                if let Some(fb) = from_bytes {
                    self.note_subtree_invalidation(fb.clone());
                    self.entries.insert(fb, DeltaState::Deleted);
                }
                self.structural_upsert_targets.insert(path_bytes.clone());
                self.entries.insert(path_bytes, DeltaState::Live(event));
                self.note_mutation();
                true
            }
        }
    }

    /// 查询时：返回所有 Live 状态的记录（替代 pending_events）
    pub fn live_records(&self) -> impl Iterator<Item = &EventRecord> {
        self.entries.values().filter_map(|state| match state {
            DeltaState::Live(rec) => Some(rec),
            DeltaState::Deleted => None,
        })
    }

    /// 查询时：返回所有被删除的路径 bytes（替代 deleted_paths）
    pub fn deleted_paths(&self) -> impl Iterator<Item = &[u8]> {
        self.entries.iter().filter_map(|(path, state)| match state {
            DeltaState::Deleted => Some(path.as_slice()),
            DeltaState::Live(_) => None,
        })
    }

    /// Prefix deletions that must be persisted even if a later Live state at
    /// the same path replaced the exact Deleted state.
    pub fn snapshot_deleted_paths(&self) -> impl Iterator<Item = &[u8]> {
        self.deleted_paths().chain(
            self.subtree_invalidations
                .iter()
                .map(std::vec::Vec::as_slice),
        )
    }

    pub fn has_subtree_invalidations(&self) -> bool {
        !self.subtree_invalidations.is_empty()
    }

    pub fn snapshot_subtree_invalidations(&self) -> BTreeSet<Vec<u8>> {
        self.subtree_invalidations.iter().cloned().collect()
    }

    pub(crate) fn invalidation_snapshot(&self) -> SubtreeInvalidationSnapshot {
        SubtreeInvalidationSnapshot {
            prefixes: self.snapshot_subtree_invalidations(),
            epoch: self.subtree_invalidation_epoch,
        }
    }

    pub(crate) fn invalidation_epoch(&self) -> u64 {
        self.subtree_invalidation_epoch
    }

    /// Return the most specific delete/rename source that covers `path`.
    ///
    /// Snapshot capture uses this only after a Live record can no longer be
    /// resolved from either the filesystem or L2. Coverage is the evidence
    /// that the Live record is a stale pre-invalidation fact rather than an
    /// unexplained missing upsert; callers must still fail closed when this
    /// returns `None`.
    pub(crate) fn invalidation_covering_path(&self, path: &std::path::Path) -> Option<&[u8]> {
        let path = path.as_os_str().as_encoded_bytes();
        self.subtree_invalidations
            .iter()
            .filter(|prefix| encoded_path_is_same_or_descendant(path, prefix.as_slice()))
            .max_by_key(|prefix| prefix.len())
            .map(Vec::as_slice)
    }

    pub fn clear_subtree_invalidations(&mut self) {
        self.clear_subtree_invalidations_inner();
    }

    pub(crate) fn note_complete_subtree_scan(&mut self, path: Vec<u8>) {
        if self
            .complete_subtree_scans
            .iter()
            .any(|existing| encoded_path_is_same_or_descendant(&path, existing))
        {
            return;
        }
        self.complete_subtree_scans
            .retain(|existing| !encoded_path_is_same_or_descendant(existing, &path));
        self.complete_subtree_scans.insert(path);
    }

    pub(crate) fn has_complete_subtree_scans(&self) -> bool {
        !self.complete_subtree_scans.is_empty()
    }

    pub(crate) fn snapshot_complete_subtree_scans(&self) -> BTreeSet<Vec<u8>> {
        self.complete_subtree_scans.iter().cloned().collect()
    }

    pub(crate) fn snapshot_structural_upsert_targets(&self) -> BTreeSet<Vec<u8>> {
        self.structural_upsert_targets.iter().cloned().collect()
    }

    pub(crate) fn invalidate_complete_subtree_scans_for_event(
        &mut self,
        event: &EventRecord,
        target_kind: Option<FileKind>,
    ) {
        let invalidating_target = match &event.event_type {
            EventType::Rename { .. } | EventType::Create
                if !matches!(target_kind, Some(FileKind::File)) =>
            {
                event.best_path()
            }
            EventType::Modify if !matches!(target_kind, Some(FileKind::File)) => event.best_path(),
            _ => None,
        };
        let Some(target) = invalidating_target else {
            return;
        };
        let target = target.as_os_str().as_encoded_bytes();
        self.complete_subtree_scans.retain(|proof| {
            !encoded_path_is_same_or_descendant(target, proof)
                && !encoded_path_is_same_or_descendant(proof, target)
        });
    }

    /// Record the resolved target type for an event that crossed an active
    /// full-scan boundary. Directory renames are always structural. A
    /// directory create/modify is structural only when the same path was
    /// invalidated earlier in this generation (delete then recreate).
    /// Unresolved rename/recreate targets fail closed because they may have
    /// been directories before another concurrent event removed them.
    pub(crate) fn note_rebuild_event_target(
        &mut self,
        event: &EventRecord,
        target_kind: Option<FileKind>,
    ) {
        let Some(path) = event.best_path() else {
            if matches!(&event.event_type, EventType::Rename { .. }) {
                self.structural_replay_unproven = true;
            }
            return;
        };
        let path_bytes = path.as_os_str().as_encoded_bytes();
        match &event.event_type {
            EventType::Rename { .. } => {
                if !matches!(target_kind, Some(FileKind::File)) {
                    self.structural_replay_unproven = true;
                }
            }
            EventType::Create | EventType::Modify => {
                if self.subtree_invalidations.contains(path_bytes)
                    && !matches!(target_kind, Some(FileKind::File))
                {
                    self.structural_replay_unproven = true;
                }
            }
            EventType::Delete => {}
        }
    }

    pub(crate) fn structural_replay_unproven(&self) -> bool {
        self.structural_replay_unproven
    }

    /// 查询时：返回所有 upserted 路径 bytes（替代 upserted_paths）
    pub fn upserted_paths(&self) -> impl Iterator<Item = &[u8]> {
        self.entries.iter().filter_map(|(path, state)| match state {
            DeltaState::Live(_) => Some(path.as_slice()),
            DeltaState::Deleted => None,
        })
    }

    /// 检查某路径是否被删除
    pub fn is_deleted(&self, path: &[u8]) -> bool {
        matches!(self.entries.get(path), Some(DeltaState::Deleted))
    }

    /// 检查某路径是否处于 Live 状态
    pub fn is_live(&self, path: &[u8]) -> bool {
        matches!(self.entries.get(path), Some(DeltaState::Live(_)))
    }

    /// 当前条目数
    pub fn len(&self) -> usize {
        self.entries.len()
    }

    /// 是否为空
    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    /// Whether this generation contains every event applied to the mutable L2.
    pub fn is_complete(&self) -> bool {
        !self.overflowed
    }

    /// 清空（flush 后调用）
    pub fn clear(&mut self) {
        self.entries.clear();
        self.note_mutation();
        if self.entries.capacity() > 4096 {
            self.entries.shrink_to(1024);
        }
    }

    /// Start a new filesystem-rebuild boundary without hiding previously
    /// represented paths from queries. Missing pre-boundary events are covered
    /// by the subsequent full scan; events after the boundary get additional,
    /// still-bounded headroom and must all fit or the rebuild is retried.
    pub fn begin_full_rebuild_generation(&mut self) {
        const MAX_RECOVERY_CAPACITY: usize = 1_048_576;
        self.overflowed = false;
        self.structural_replay_unproven = false;
        // The full scan starts after this boundary and therefore proves all
        // pre-boundary subtree state from the filesystem itself.
        self.clear_subtree_invalidations_inner();
        self.complete_subtree_scans.clear();
        self.structural_upsert_targets.clear();
        let ceiling = self.base_max_capacity.max(MAX_RECOVERY_CAPACITY);
        let with_headroom = self.entries.len().saturating_add(self.base_max_capacity);
        self.max_capacity = self
            .max_capacity
            .saturating_mul(2)
            .max(with_headroom)
            .min(ceiling);
    }

    /// Close a proven-complete generation after durable snapshot publication
    /// or a complete full rebuild.
    pub fn reset_complete_generation(&mut self) {
        self.clear();
        self.clear_subtree_invalidations_inner();
        self.complete_subtree_scans.clear();
        self.structural_upsert_targets.clear();
        self.overflowed = false;
        self.structural_replay_unproven = false;
        self.max_capacity = self.base_max_capacity;
    }

    /// Restore the normal capacity after a rebuild whose replay buffer drained
    /// completely. The caller must have checked `is_complete()` first.
    pub fn finish_full_rebuild_generation(&mut self) {
        debug_assert!(self.is_complete());
        self.complete_subtree_scans.clear();
        self.structural_upsert_targets.clear();
        self.max_capacity = self.base_max_capacity;
    }

    #[cfg(test)]
    pub(crate) fn set_max_capacity_for_test(&mut self, max_capacity: usize) {
        self.max_capacity = max_capacity;
    }

    /// 提取需要写入 seg-*.del 的删除路径（Deleted 状态）
    pub fn drain_deleted_for_flush(&mut self) -> Vec<Vec<u8>> {
        let mut deleted = Vec::new();
        for (path, state) in &self.entries {
            if matches!(state, DeltaState::Deleted) {
                deleted.push(path.clone());
            }
        }
        self.clear();
        deleted
    }

    /// 估算内存占用（字节数）
    pub fn estimated_bytes(&self) -> usize {
        use std::mem::size_of;
        // HashMap 条目开销 + key/value 本身
        let entry_overhead = size_of::<(Vec<u8>, DeltaState)>() + 16;
        self.entries.len() * entry_overhead
            + self.entries.capacity().saturating_sub(self.entries.len())
                * size_of::<(Vec<u8>, DeltaState)>()
            + self
                .subtree_invalidations
                .iter()
                .map(|path| path.capacity() + size_of::<Vec<u8>>() + 16)
                .sum::<usize>()
            + self
                .complete_subtree_scans
                .iter()
                .map(|path| path.capacity() + size_of::<Vec<u8>>() + 16)
                .sum::<usize>()
            + self
                .structural_upsert_targets
                .iter()
                .map(|path| path.capacity() + size_of::<Vec<u8>>() + 16)
                .sum::<usize>()
            + self
                .overlay_meta_cache
                .as_ref()
                .map(|cache| cache.estimated_bytes())
                .unwrap_or(0)
    }

    fn note_subtree_invalidation(&mut self, path: Vec<u8>) {
        if self.subtree_invalidations.insert(path) {
            self.subtree_invalidation_epoch = self.subtree_invalidation_epoch.wrapping_add(1);
        }
    }

    fn clear_subtree_invalidations_inner(&mut self) {
        if !self.subtree_invalidations.is_empty() {
            self.subtree_invalidations.clear();
            self.subtree_invalidation_epoch = self.subtree_invalidation_epoch.wrapping_add(1);
        }
    }
}

fn encoded_path_is_same_or_descendant(path: &[u8], prefix: &[u8]) -> bool {
    if path == prefix {
        return true;
    }
    if prefix.is_empty() || !path.starts_with(prefix) {
        return false;
    }
    if prefix
        .last()
        .is_some_and(|byte| std::path::is_separator(char::from(*byte)))
    {
        return true;
    }
    path.get(prefix.len())
        .is_some_and(|byte| std::path::is_separator(char::from(*byte)))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::{EventRecord, EventType, FileIdentifier};
    use std::path::PathBuf;

    fn make_event(seq: u64, event_type: EventType, path: &str) -> EventRecord {
        EventRecord {
            seq,
            timestamp: std::time::SystemTime::UNIX_EPOCH,
            event_type,
            id: FileIdentifier::Path(PathBuf::from(path)),
            path_hint: None,
        }
    }

    fn make_meta(ino: u64, path: &str) -> FileMeta {
        FileMeta {
            file_key: crate::core::FileKey {
                dev: 1,
                ino,
                generation: 0,
            },
            path: PathBuf::from(path),
            size: 0,
            mtime: None,
            ctime: None,
            atime: None,
            kind: FileKind::File,
        }
    }

    #[test]
    fn overlay_meta_cache_trigram_candidates_narrow_without_false_negatives() {
        let cache = OverlayMetaCache::new(Arc::new(vec![
            make_meta(1, "/tmp/AlphaTarget.txt"),
            make_meta(2, "/tmp/beta.txt"),
            make_meta(3, "/tmp/alpha-other.txt"),
        ]));

        let alpha = cache.trigram_candidates(b"ALPHA").unwrap();
        assert_eq!(alpha.iter().collect::<Vec<_>>(), vec![0, 2]);
        assert!(cache.trigram_candidates(b"missing").unwrap().is_empty());
        assert!(cache.trigram_candidates(b"ab").is_none());
    }

    #[test]
    fn overlay_meta_cache_disables_trigrams_over_meta_budget() {
        let metas = (0..=OVERLAY_TRIGRAM_MAX_METAS)
            .map(|index| make_meta(index as u64 + 1, &format!("/tmp/target-{index}.txt")))
            .collect::<Vec<_>>();
        let cache = OverlayMetaCache::new(Arc::new(metas));

        assert!(
            cache.trigram_candidates(b"target").is_none(),
            "oversized overlays must fall back to the bounded-memory linear path"
        );
    }

    #[test]
    fn overlay_meta_cache_disables_trigrams_over_posting_budget() {
        let metas = vec![
            make_meta(1, "/tmp/alpha-target.txt"),
            make_meta(2, "/tmp/beta-target.txt"),
        ];

        assert!(OverlayMetaCache::build_trigram_index(&metas, 64, 1).is_none());
        assert!(OverlayMetaCache::build_trigram_index(&metas, 1, 64).is_none());
    }

    #[test]
    fn test_create_then_delete() {
        let mut db = DeltaBuffer::with_capacity(1024);
        db.apply_events(&[make_event(1, EventType::Create, "/tmp/a")]);
        assert!(db.is_live(b"/tmp/a"));
        db.apply_events(&[make_event(2, EventType::Delete, "/tmp/a")]);
        assert!(db.is_deleted(b"/tmp/a"));
        assert!(!db.is_live(b"/tmp/a"));
    }

    #[test]
    fn test_delete_then_create() {
        let mut db = DeltaBuffer::with_capacity(1024);
        db.apply_events(&[make_event(1, EventType::Delete, "/tmp/a")]);
        db.apply_events(&[make_event(2, EventType::Create, "/tmp/a")]);
        assert!(db.is_live(b"/tmp/a"));
        assert!(!db.is_deleted(b"/tmp/a"));
        assert!(db.snapshot_deleted_paths().any(|path| path == b"/tmp/a"));
    }

    #[test]
    fn test_rename() {
        let mut db = DeltaBuffer::with_capacity(1024);
        let ev = EventRecord {
            seq: 1,
            timestamp: std::time::SystemTime::UNIX_EPOCH,
            event_type: EventType::Rename {
                from: FileIdentifier::Path(PathBuf::from("/tmp/old")),
                from_path_hint: None,
            },
            id: FileIdentifier::Path(PathBuf::from("/tmp/new")),
            path_hint: None,
        };
        db.apply_events(&[ev]);
        assert!(db.is_deleted(b"/tmp/old"));
        assert!(db.is_live(b"/tmp/new"));
    }

    #[test]
    fn stale_live_after_rename_keeps_invalidation_evidence() {
        let mut db = DeltaBuffer::with_capacity(1024);
        let rename = EventRecord {
            seq: 1,
            timestamp: std::time::SystemTime::UNIX_EPOCH,
            event_type: EventType::Rename {
                from: FileIdentifier::Path(PathBuf::from("/tmp/old")),
                from_path_hint: None,
            },
            id: FileIdentifier::Path(PathBuf::from("/tmp/new")),
            path_hint: None,
        };
        db.apply_events(&[rename]);
        db.apply_events(&[make_event(2, EventType::Modify, "/tmp/old")]);

        assert!(db.is_live(b"/tmp/old"));
        assert_eq!(
            db.invalidation_covering_path(std::path::Path::new("/tmp/old")),
            Some(b"/tmp/old".as_slice())
        );
        assert!(db
            .invalidation_covering_path(std::path::Path::new("/tmp/old-child"))
            .is_none());
    }

    #[test]
    fn parent_invalidation_covers_stale_descendant_only() {
        let mut db = DeltaBuffer::with_capacity(1024);
        db.apply_events(&[make_event(1, EventType::Delete, "/tmp/old-tree")]);
        db.apply_events(&[make_event(2, EventType::Modify, "/tmp/old-tree/child.txt")]);

        assert_eq!(
            db.invalidation_covering_path(std::path::Path::new("/tmp/old-tree/child.txt")),
            Some(b"/tmp/old-tree".as_slice())
        );
        assert!(db
            .invalidation_covering_path(std::path::Path::new("/tmp/other-tree/child.txt"))
            .is_none());
    }

    #[test]
    fn modify_below_deleted_subtree_remains_a_structural_target() {
        let mut db = DeltaBuffer::with_capacity(1024);
        db.apply_events(&[make_event(1, EventType::Delete, "/tmp/old-tree")]);
        db.apply_events(&[make_event(2, EventType::Modify, "/tmp/old-tree/recreated")]);

        assert_eq!(
            db.snapshot_structural_upsert_targets(),
            BTreeSet::from([b"/tmp/old-tree/recreated".to_vec()])
        );
    }

    #[test]
    fn exact_delete_preserves_complete_subtree_proof() {
        let mut db = DeltaBuffer::with_capacity(1024);
        db.note_complete_subtree_scan(b"/tmp/tree".to_vec());

        let delete = make_event(1, EventType::Delete, "/tmp/tree/nested");
        db.invalidate_complete_subtree_scans_for_event(&delete, None);

        assert_eq!(
            db.snapshot_complete_subtree_scans(),
            BTreeSet::from([b"/tmp/tree".to_vec()])
        );
    }

    #[test]
    fn unknown_modify_invalidates_complete_subtree_proof() {
        let mut db = DeltaBuffer::with_capacity(1024);
        db.note_complete_subtree_scan(b"/tmp/tree".to_vec());

        let modify = make_event(1, EventType::Modify, "/tmp/tree/nested");
        db.invalidate_complete_subtree_scans_for_event(&modify, None);

        assert!(db.snapshot_complete_subtree_scans().is_empty());
    }

    #[test]
    fn complete_subtree_scan_proof_is_invalidated_by_later_directory_create() {
        let mut db = DeltaBuffer::with_capacity(1024);
        db.note_complete_subtree_scan(b"/tmp/tree".to_vec());
        assert_eq!(
            db.snapshot_complete_subtree_scans(),
            BTreeSet::from([b"/tmp/tree".to_vec()])
        );

        let create = make_event(1, EventType::Create, "/tmp/tree/imported");
        db.invalidate_complete_subtree_scans_for_event(&create, Some(FileKind::Directory));

        assert!(db.snapshot_complete_subtree_scans().is_empty());
    }

    #[test]
    fn complete_subtree_scan_proof_survives_exact_file_changes() {
        let mut db = DeltaBuffer::with_capacity(1024);
        db.note_complete_subtree_scan(b"/tmp/tree".to_vec());

        let modify = make_event(1, EventType::Modify, "/tmp/tree/file.txt");
        db.invalidate_complete_subtree_scans_for_event(&modify, Some(FileKind::File));

        assert_eq!(
            db.snapshot_complete_subtree_scans(),
            BTreeSet::from([b"/tmp/tree".to_vec()])
        );
    }

    #[test]
    fn test_live_records() {
        let mut db = DeltaBuffer::with_capacity(1024);
        db.apply_events(&[
            make_event(1, EventType::Create, "/tmp/a"),
            make_event(2, EventType::Delete, "/tmp/b"),
            make_event(3, EventType::Modify, "/tmp/c"),
        ]);
        let paths: Vec<&str> = db
            .live_records()
            .filter_map(|r| r.best_path())
            .map(|p| p.to_str().unwrap())
            .collect();
        assert_eq!(paths.len(), 2);
        assert!(paths.contains(&"/tmp/a"));
        assert!(paths.contains(&"/tmp/c"));
    }

    #[test]
    fn test_capacity_bound() {
        let mut db = DeltaBuffer::with_capacity_and_limit(2, 2);
        assert!(db.apply_events(&[
            make_event(1, EventType::Create, "/tmp/a"),
            make_event(2, EventType::Create, "/tmp/b"),
        ]));
        assert_eq!(db.len(), 2);
        // 容量超限拒绝新路径
        assert!(!db.apply_events(&[make_event(3, EventType::Create, "/tmp/c")]));
        assert_eq!(db.len(), 2);
        assert!(!db.is_complete());
        // 更新已有路径仍允许
        assert!(db.apply_events(&[make_event(4, EventType::Modify, "/tmp/a")]));
        assert_eq!(db.len(), 2);
        assert!(
            !db.is_complete(),
            "later updates must not hide an earlier rejected path"
        );
        // 删除已有路径仍允许
        assert!(db.apply_events(&[make_event(5, EventType::Delete, "/tmp/a")]));
        assert_eq!(db.len(), 2);
        assert!(db.is_deleted(b"/tmp/a"));
        // 普通 clear 只释放条目，不能把已丢事件的 generation 伪装为完整。
        db.clear();
        assert!(!db.is_complete());
        assert!(db.apply_events(&[make_event(6, EventType::Create, "/tmp/c")]));
        assert_eq!(db.len(), 1);
        db.reset_complete_generation();
        assert!(db.is_complete());
    }

    #[test]
    fn test_hard_capacity_limit_256k() {
        let mut db = DeltaBuffer::with_capacity(256 * 1024);
        for i in 0..(256 * 1024) {
            let path = format!("/tmp/file_{}", i);
            assert!(
                db.apply_events(&[make_event(i as u64, EventType::Create, &path)]),
                "Failed at iteration {}",
                i
            );
        }
        assert_eq!(db.len(), 256 * 1024);

        // 第 256K+1 条被拒绝
        assert!(!db.apply_events(&[make_event(999_999, EventType::Create, "/tmp/overflow")]));
        assert_eq!(db.len(), 256 * 1024);

        // drain_deleted_for_flush 后（内部调用 clear）可以重新插入
        let _ = db.drain_deleted_for_flush();
        assert!(db.is_empty());
        assert!(!db.is_complete());
        assert!(db.apply_events(&[make_event(1, EventType::Create, "/tmp/after_clear")]));
        assert_eq!(db.len(), 1);
    }

    #[test]
    fn clear_releases_large_hashmap_capacity() {
        let mut db = DeltaBuffer::with_capacity(32 * 1024);
        for i in 0..10_000 {
            let path = format!("/tmp/clear_capacity_{i}");
            assert!(db.apply_events(&[make_event(i as u64, EventType::Create, &path)]));
        }
        assert!(db.entries.capacity() > 4096);

        db.clear();

        assert!(db.is_empty());
        assert!(
            db.entries.capacity() <= 4096,
            "clear should shrink large overlay capacity, got {}",
            db.entries.capacity()
        );
    }

    #[test]
    fn overlay_meta_cache_stores_and_invalidates_on_mutation() {
        let mut db = DeltaBuffer::with_capacity(64);
        assert!(db.apply_events(&[make_event(1, EventType::Create, "/tmp/cache_a")]));
        assert!(db.overlay_meta_cache().is_none());

        let epoch = db.mutation_epoch();
        let bytes_without_cache = db.estimated_bytes();
        let cache = Arc::new(OverlayMetaCache::new(Arc::new(vec![make_meta(
            1,
            "/tmp/cache_a",
        )])));
        assert!(db.store_overlay_meta_cache(epoch, Arc::clone(&cache)));
        assert!(db.overlay_meta_cache().is_some());
        assert!(
            db.estimated_bytes() > bytes_without_cache,
            "memory accounting must include the materialized metas and postings"
        );

        // 任何 mutation（insert/delete/clear）都必须失效缓存并推进 epoch。
        assert!(db.apply_events(&[make_event(2, EventType::Delete, "/tmp/cache_a")]));
        assert!(db.overlay_meta_cache().is_none());
        assert_ne!(db.mutation_epoch(), epoch);

        // 陈旧 epoch 的回写必须被拒绝。
        assert!(!db.store_overlay_meta_cache(epoch, cache));
        assert!(db.overlay_meta_cache().is_none());

        let fresh_epoch = db.mutation_epoch();
        assert!(db.store_overlay_meta_cache(
            fresh_epoch,
            Arc::new(OverlayMetaCache::new(Arc::new(Vec::new())))
        ));
        db.clear();
        assert!(db.overlay_meta_cache().is_none());
    }

    #[test]
    fn full_rebuild_boundary_recovers_completeness_with_bounded_headroom() {
        let mut db = DeltaBuffer::with_capacity_and_limit(2, 2);
        assert!(db.apply_events(&[
            make_event(1, EventType::Create, "/tmp/a"),
            make_event(2, EventType::Create, "/tmp/b"),
        ]));
        assert!(!db.apply_events(&[make_event(3, EventType::Create, "/tmp/missed")]));

        db.begin_full_rebuild_generation();

        assert!(db.is_complete());
        assert!(db.apply_events(&[make_event(4, EventType::Create, "/tmp/after-boundary")]));
        assert_eq!(db.len(), 3);
        db.clear();
        db.finish_full_rebuild_generation();
        assert!(db.is_complete());
        assert_eq!(db.max_capacity, 2);
    }

    #[test]
    fn test_drain_deleted_for_flush() {
        let mut db = DeltaBuffer::with_capacity(1024);
        db.apply_events(&[
            make_event(1, EventType::Create, "/tmp/a"),
            make_event(2, EventType::Delete, "/tmp/b"),
            make_event(3, EventType::Modify, "/tmp/c"),
        ]);
        let deleted = db.drain_deleted_for_flush();
        assert_eq!(deleted.len(), 1);
        assert_eq!(deleted[0], b"/tmp/b");
        assert!(db.is_empty());
    }
}
