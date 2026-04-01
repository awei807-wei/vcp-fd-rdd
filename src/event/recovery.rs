use parking_lot::Mutex;
use std::collections::HashSet;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

#[derive(Clone, Debug)]
pub enum DirtyScope {
    /// 无法定位具体目录（例如严重风暴/采样上限触发），按全局 dirty 处理。
    All {
        /// 上一次 fast-sync 完成时间（ns since epoch，best-effort）。
        cutoff_ns: u64,
    },
    /// 可定位到“可能丢事件”的目录集合（去重、有上限）。
    Dirs {
        /// 上一次 fast-sync 完成时间（ns since epoch，best-effort）。
        cutoff_ns: u64,
        dirs: Vec<PathBuf>,
    },
}

fn now_ns() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos()
        .min(u128::from(u64::MAX)) as u64
}

#[derive(Debug)]
struct DirtyState {
    dirty_all: bool,
    dirty_dirs: HashSet<PathBuf>,
}

/// overflow 兜底：把“丢事件”转成 dirty region，并由上层在 cooldown/max-staleness 触发 fast-sync。
#[derive(Debug)]
pub struct DirtyTracker {
    max_dirty_dirs: usize,
    state: Mutex<DirtyState>,
    first_dirty_ns: AtomicU64,
    last_activity_ns: AtomicU64,
    sync_in_progress: AtomicBool,
    last_sync_ns: AtomicU64,
}

impl DirtyTracker {
    pub fn new(max_dirty_dirs: usize) -> Arc<Self> {
        Arc::new(Self {
            max_dirty_dirs: max_dirty_dirs.max(1),
            state: Mutex::new(DirtyState {
                dirty_all: false,
                dirty_dirs: HashSet::new(),
            }),
            first_dirty_ns: AtomicU64::new(0),
            last_activity_ns: AtomicU64::new(0),
            sync_in_progress: AtomicBool::new(false),
            last_sync_ns: AtomicU64::new(0),
        })
    }

    pub fn record_activity(&self) {
        self.last_activity_ns.store(now_ns(), Ordering::Relaxed);
    }

    pub fn record_overflow_paths(&self, paths: &[PathBuf]) {
        self.record_activity();
        let t = now_ns();
        self.first_dirty_ns
            .compare_exchange(0, t, Ordering::Relaxed, Ordering::Relaxed)
            .ok();

        let mut st = self.state.lock();
        if st.dirty_all {
            return;
        }

        for p in paths {
            // 以 parent dir 为粒度；路径本身可能是文件或目录，这里不做 stat。
            if let Some(parent) = p.parent() {
                st.dirty_dirs.insert(parent.to_path_buf());
            } else if p.is_absolute() {
                st.dirty_dirs.insert(Path::new("/").to_path_buf());
            }
        }

        if st.dirty_dirs.len() >= self.max_dirty_dirs {
            st.dirty_all = true;
            st.dirty_dirs.clear();
        }
    }

    pub fn mark_dirty_all(&self) {
        self.record_activity();
        let t = now_ns();
        self.first_dirty_ns
            .compare_exchange(0, t, Ordering::Relaxed, Ordering::Relaxed)
            .ok();
        let mut st = self.state.lock();
        st.dirty_all = true;
        st.dirty_dirs.clear();
    }

    pub fn is_dirty(&self) -> bool {
        let st = self.state.lock();
        st.dirty_all || !st.dirty_dirs.is_empty()
    }

    pub fn sync_in_progress(&self) -> bool {
        self.sync_in_progress.load(Ordering::Relaxed)
    }

    pub fn last_sync_ns(&self) -> u64 {
        self.last_sync_ns.load(Ordering::Relaxed)
    }

    /// 根据 cooldown/max-staleness/min-interval 决策是否触发 fast-sync，并在触发时取走 dirty snapshot。
    pub fn try_begin_sync(
        &self,
        cooldown_ns: u64,
        max_staleness_ns: u64,
        min_interval_ns: u64,
    ) -> Option<DirtyScope> {
        if self.sync_in_progress.load(Ordering::Acquire) {
            return None;
        }

        let (dirty_all, dirty_dirs_len) = {
            let st = self.state.lock();
            (st.dirty_all, st.dirty_dirs.len())
        };
        if !dirty_all && dirty_dirs_len == 0 {
            return None;
        }

        let now = now_ns();
        let first = self.first_dirty_ns.load(Ordering::Relaxed);
        let last = self.last_activity_ns.load(Ordering::Relaxed);
        let last_sync = self.last_sync_ns.load(Ordering::Relaxed);

        let quiet = last != 0 && now.saturating_sub(last) >= cooldown_ns;
        let too_stale = first != 0 && now.saturating_sub(first) >= max_staleness_ns;
        if !quiet && !too_stale {
            return None;
        }
        if last_sync != 0 && now.saturating_sub(last_sync) < min_interval_ns {
            return None;
        }

        // 原子抢占：同一时间最多一个 fast-sync。
        if self
            .sync_in_progress
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .is_err()
        {
            return None;
        }

        let cutoff = self.last_sync_ns();
        let scope = {
            let mut st = self.state.lock();
            let scope = if st.dirty_all || st.dirty_dirs.is_empty() {
                DirtyScope::All { cutoff_ns: cutoff }
            } else {
                let dirs = st.dirty_dirs.drain().collect();
                DirtyScope::Dirs {
                    cutoff_ns: cutoff,
                    dirs,
                }
            };
            st.dirty_all = false;
            scope
        };

        self.first_dirty_ns.store(0, Ordering::Relaxed);
        Some(scope)
    }

    pub fn finish_sync(&self) {
        self.last_sync_ns.store(now_ns(), Ordering::Relaxed);
        {
            // fast-sync 完成后，dirty set 往往已 drain/clear；此时可回收 HashSet 的高水位桶数组，
            // 避免“风暴一次把 capacity 顶上去”后常驻不降，从而把 PD plateau 粘住。
            let mut st = self.state.lock();
            let keep = self.max_dirty_dirs.min(2048).max(64);
            if st.dirty_dirs.capacity() > keep.saturating_mul(2) {
                st.dirty_dirs.shrink_to(keep);
            }
        }
        self.sync_in_progress.store(false, Ordering::Release);
    }
}
