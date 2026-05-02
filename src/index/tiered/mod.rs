pub(crate) mod arena;
pub(crate) mod events;
pub(crate) mod load;
mod memory;
mod query;
mod query_plan;
pub(crate) mod rebuild;
mod snapshot;
pub(crate) mod sync;

#[cfg(test)]
mod tests;

use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, AtomicU64};
use std::sync::Arc;
use std::time::Duration;

use arc_swap::ArcSwap;
use parking_lot::Mutex;
use tokio::sync::Notify;

use crate::core::AdaptiveScheduler;
use crate::index::l1_cache::L1Cache;
use crate::index::l2_partition::PersistentIndex;
use crate::index::l3_cold::IndexBuilder;
use crate::storage::traits::WriteAheadLog;

use self::rebuild::RebuildState;

const REBUILD_COOLDOWN: Duration = Duration::from_secs(60);

pub(crate) fn pathbuf_from_bytes(bytes: impl AsRef<[u8]>) -> PathBuf {
    use unicode_normalization::UnicodeNormalization;
    let s = String::from_utf8_lossy(bytes.as_ref());
    PathBuf::from(s.nfc().collect::<String>())
}

pub(crate) fn normalize_path(path: &std::path::Path) -> PathBuf {
    use unicode_normalization::UnicodeNormalization;
    let s = path.to_string_lossy();
    PathBuf::from(s.nfc().collect::<String>())
}

/// 三级索引：L1 热缓存 → L2 持久索引（内存常驻）→ L3 构建器（不在查询链路）
pub struct TieredIndex {
    pub l1: L1Cache,
    pub l2: ArcSwap<PersistentIndex>,
    pub l3: IndexBuilder,
    pub(self) scheduler: Mutex<AdaptiveScheduler>,
    pub(self) wal: Mutex<Option<Arc<dyn WriteAheadLog + Send + Sync>>>,
    pub event_seq: AtomicU64,
    pub(self) rebuild_state: Mutex<RebuildState>,
    pub(self) delta_buffer: Mutex<crate::index::delta_buffer::DeltaBuffer>,
    pub base: ArcSwap<crate::index::base_index::BaseIndexData>,
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
    pub(self) fast_sync_semaphore: Arc<tokio::sync::Semaphore>,
}

impl TieredIndex {
    pub fn rebuild_in_progress(&self) -> bool {
        self.rebuild_state.lock().in_progress
    }
}

// Re-exports
