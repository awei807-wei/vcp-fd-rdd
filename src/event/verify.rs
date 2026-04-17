use crate::core::EventRecord;
use crate::event::recovery::{DirtyScope, DirtyTracker};
use crate::index::TieredIndex;
use std::collections::BTreeSet;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

#[derive(Clone, Debug)]
pub struct GapRecord {
    pub expected_seq: u64,
    pub observed_seq: u64,
    pub missing_count: u64,
    pub scope: DirtyScope,
}

#[derive(Clone, Debug, Default)]
pub struct VerifyReport {
    pub last_verified_seq: u64,
    pub repaired_gap: Option<GapRecord>,
    pub dirs_scanned: usize,
    pub upsert_events: usize,
    pub delete_events: usize,
}

/// 弹性校验：
/// - 维护最近一次已验证的事件序号
/// - 检测批次中的序号缺口
/// - 将缺口降级为 fast-sync repair，而不是直接放任漂移
pub struct ElasticVerifier {
    pub index: Arc<TieredIndex>,
    ignore_prefixes: Vec<PathBuf>,
    targeted_dir_limit: usize,
    last_verified_seq: AtomicU64,
    last_verified_ts_ns: AtomicU64,
}

impl ElasticVerifier {
    pub fn new(index: Arc<TieredIndex>) -> Self {
        Self {
            index,
            ignore_prefixes: Vec::new(),
            targeted_dir_limit: 16,
            last_verified_seq: AtomicU64::new(0),
            last_verified_ts_ns: AtomicU64::new(0),
        }
    }

    pub fn with_ignore_prefixes(mut self, ignore_prefixes: Vec<PathBuf>) -> Self {
        self.ignore_prefixes = ignore_prefixes;
        self
    }

    pub fn with_targeted_dir_limit(mut self, limit: usize) -> Self {
        self.targeted_dir_limit = limit.max(1);
        self
    }

    pub fn last_verified_seq(&self) -> u64 {
        self.last_verified_seq.load(Ordering::Relaxed)
    }

    pub fn verify_gap(&self, events: &[EventRecord]) -> Option<GapRecord> {
        if events.is_empty() {
            return None;
        }

        let mut last_seen = self.last_verified_seq();
        for ev in events {
            if last_seen == 0 {
                last_seen = ev.seq;
                continue;
            }

            if ev.seq <= last_seen {
                continue;
            }

            let expected = last_seen.saturating_add(1);
            if ev.seq > expected {
                return Some(GapRecord {
                    expected_seq: expected,
                    observed_seq: ev.seq,
                    missing_count: ev.seq.saturating_sub(expected),
                    scope: self.scope_from_events(events),
                });
            }

            last_seen = ev.seq;
        }

        None
    }

    pub fn verify_and_repair(&self, events: &[EventRecord]) -> VerifyReport {
        let mut report = VerifyReport::default();

        if let Some(gap) = self.verify_gap(events) {
            tracing::warn!(
                "ElasticVerifier: sequence gap detected (expected={}, observed={}, missing={}), starting fast-sync repair",
                gap.expected_seq,
                gap.observed_seq,
                gap.missing_count
            );
            let sync = self
                .index
                .fast_sync(gap.scope.clone(), &self.ignore_prefixes);
            report.dirs_scanned = sync.dirs_scanned;
            report.upsert_events = sync.upsert_events;
            report.delete_events = sync.delete_events;
            report.repaired_gap = Some(gap);
        }

        self.checkpoint(events);
        report.last_verified_seq = self.last_verified_seq();
        report
    }

    pub fn repair_scope(&self, scope: DirtyScope) -> VerifyReport {
        let sync = self.index.fast_sync(scope.clone(), &self.ignore_prefixes);
        VerifyReport {
            last_verified_seq: self.last_verified_seq(),
            repaired_gap: None,
            dirs_scanned: sync.dirs_scanned,
            upsert_events: sync.upsert_events,
            delete_events: sync.delete_events,
        }
    }

    pub fn spawn_repair(self: &Arc<Self>, scope: DirtyScope, tracker: Arc<DirtyTracker>) {
        let verifier = self.clone();
        std::thread::spawn(move || {
            let report = verifier.repair_scope(scope.clone());
            tracing::warn!(
                "ElasticVerifier repair complete: scope={:?} dirs={} upserts={} deletes={}",
                scope,
                report.dirs_scanned,
                report.upsert_events,
                report.delete_events
            );
            tracker.finish_sync();
        });
    }

    fn checkpoint(&self, events: &[EventRecord]) {
        let Some(max_seq) = events.iter().map(|ev| ev.seq).max() else {
            return;
        };
        let max_ts_ns = events
            .iter()
            .filter_map(|ev| system_time_to_ns(ev.timestamp))
            .max()
            .unwrap_or(0);

        self.last_verified_seq.fetch_max(max_seq, Ordering::Relaxed);
        self.last_verified_ts_ns
            .fetch_max(max_ts_ns, Ordering::Relaxed);
    }

    fn scope_from_events(&self, events: &[EventRecord]) -> DirtyScope {
        let cutoff_ns = self.last_verified_ts_ns.load(Ordering::Relaxed);
        let mut dirs: BTreeSet<PathBuf> = BTreeSet::new();

        for ev in events {
            let Some(parent) = ev.best_path().and_then(|p| p.parent()) else {
                return DirtyScope::All { cutoff_ns };
            };
            dirs.insert(parent.to_path_buf());
            if dirs.len() > self.targeted_dir_limit {
                return DirtyScope::All { cutoff_ns };
            }
        }

        if dirs.is_empty() {
            DirtyScope::All { cutoff_ns }
        } else {
            DirtyScope::Dirs {
                cutoff_ns,
                dirs: dirs.into_iter().collect(),
            }
        }
    }
}

fn system_time_to_ns(ts: SystemTime) -> Option<u64> {
    ts.duration_since(UNIX_EPOCH)
        .ok()
        .map(|dur| dur.as_nanos().min(u128::from(u64::MAX)) as u64)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::{EventType, FileIdentifier};
    use std::fs;
    use std::time::SystemTime;
    use crate::test_util::unique_tmp_dir;


    fn ev(seq: u64, path: PathBuf) -> EventRecord {
        EventRecord {
            seq,
            timestamp: SystemTime::now(),
            event_type: EventType::Modify,
            id: FileIdentifier::Path(path),
            path_hint: None,
        }
    }

    #[test]
    fn verifier_detects_sequence_gap() {
        let root = unique_tmp_dir("gap-detect");
        fs::create_dir_all(&root).unwrap();

        let index = Arc::new(TieredIndex::empty(vec![root.clone()]));
        let verifier = ElasticVerifier::new(index);

        let first = root.join("alpha.txt");
        let second = root.join("beta.txt");
        let initial = vec![ev(1, first.clone())];
        let report = verifier.verify_and_repair(&initial);
        assert_eq!(report.last_verified_seq, 1);
        assert!(report.repaired_gap.is_none());

        let gap = verifier.verify_gap(&[ev(3, second)]);
        assert!(matches!(
            gap.map(|g| g.scope),
            Some(DirtyScope::Dirs { .. })
        ));
    }

    #[test]
    fn verifier_repairs_gap_with_fast_sync() -> anyhow::Result<()> {
        let root = unique_tmp_dir("gap-repair");
        fs::create_dir_all(&root).unwrap();

        let alpha = root.join("alpha_match.txt");
        let beta = root.join("beta_match.txt");
        fs::write(&alpha, b"a")?;
        fs::write(&beta, b"b")?;

        let index = Arc::new(TieredIndex::empty(vec![root.clone()]));
        let verifier = ElasticVerifier::new(index.clone());

        verifier.verify_and_repair(&[ev(1, alpha.clone())]);
        let report = verifier.verify_and_repair(&[ev(3, beta.clone())]);

        assert!(report.repaired_gap.is_some());
        assert!(report.dirs_scanned >= 1);

        let results = index.query_limit("match", 10);
        assert_eq!(results.len(), 2);
        Ok(())
    }
}
