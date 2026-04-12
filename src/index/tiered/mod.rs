pub(crate) mod arena;
mod compaction;
mod disk_layer;
pub(crate) mod events;
mod load;
mod memory;
mod query;
mod query_plan;
pub(crate) mod rebuild;
pub(crate) mod sync;
mod snapshot;

#[cfg(test)]
mod tests;

use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, AtomicU64};
use std::sync::Arc;
use std::time::{Duration, Instant};

use arc_swap::ArcSwap;
use parking_lot::{Mutex, RwLock};
use tokio::sync::Notify;

use crate::core::AdaptiveScheduler;
use crate::index::l1_cache::L1Cache;
use crate::index::l2_partition::PersistentIndex;
use crate::index::l3_cold::IndexBuilder;
use crate::storage::traits::WriteAheadLog;

use self::disk_layer::DiskLayer;
use self::events::OverlayState;
use self::rebuild::RebuildState;

const REBUILD_COOLDOWN: Duration = Duration::from_secs(60);
// 更激进的合并阈值：用于百万文件后的"瘦身期"，加速 delta 段收敛。
const COMPACTION_DELTA_THRESHOLD: usize = 2;
// 每次 compaction 最多合并多少个 delta（避免"delta 很多时一次合并过重"导致常驻/临时分配抖动）。
const COMPACTION_MAX_DELTAS_PER_RUN: usize = 2;
// 防抖：避免 flush 高频阶段反复启动 compaction 造成临时大分配抖动。
const COMPACTION_COOLDOWN: Duration = Duration::from_secs(30);

fn pathbuf_from_bytes(bytes: impl AsRef<[u8]>) -> PathBuf {
    let bytes = bytes.as_ref();
    #[cfg(unix)]
    {
        use std::ffi::OsString;
        use std::os::unix::ffi::OsStringExt;
        return PathBuf::from(OsString::from_vec(bytes.to_vec()));
    }
    #[cfg(not(unix))]
    {
        PathBuf::from(String::from_utf8_lossy(bytes).into_owned())
    }
}

/// 三级索引：L1 热缓存 → L2 持久索引（内存常驻）→ L3 构建器（不在查询链路）
pub struct TieredIndex {
    pub l1: L1Cache,
    pub l2: ArcSwap<PersistentIndex>,
    pub(self) disk_layers: RwLock<Vec<DiskLayer>>,
    pub l3: IndexBuilder,
    pub(self) scheduler: Mutex<AdaptiveScheduler>,
    pub(self) wal: Mutex<Option<Arc<dyn WriteAheadLog + Send + Sync>>>,
    pub event_seq: AtomicU64,
    pub(self) rebuild_state: Mutex<RebuildState>,
    pub(self) overlay_state: Mutex<OverlayState>,
    pub(self) apply_gate: RwLock<()>,
    pub(self) compaction_in_progress: AtomicBool,
    pub(self) compaction_last_started_at: Mutex<Option<Instant>>,
    pub(self) flush_requested: AtomicBool,
    pub(self) flush_notify: Notify,
    pub(self) auto_flush_overlay_paths: AtomicU64,
    pub(self) auto_flush_overlay_bytes: AtomicU64,
    pub(self) periodic_flush_min_events: AtomicU64,
    pub(self) periodic_flush_min_bytes: AtomicU64,
    pub(self) pending_flush_events: AtomicU64,
    pub(self) pending_flush_bytes: AtomicU64,
    pub(self) last_snapshot_time: AtomicU64,
    pub roots: Vec<PathBuf>,
    pub include_hidden: bool,
    pub ignore_enabled: bool,
    pub follow_symlinks: bool,
}

impl TieredIndex {
    #[cfg(test)]
    fn rebuild_in_progress(&self) -> bool {
        self.rebuild_state.lock().in_progress
    }
}

// Re-exports
#[cfg(test)]
pub(crate) use self::disk_layer::event_record_estimated_bytes;
