pub(crate) mod arena;
mod disk_layer;
pub(crate) mod events;
mod load;
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

pub use self::sync::DirtyScope;

pub(crate) fn pathbuf_from_bytes(bytes: impl AsRef<[u8]>) -> PathBuf {
    let bytes = bytes.as_ref();
    if bytes.is_ascii() {
        // Phase 3：ASCII 字节集对 NFC 是恒等变换；ASCII 又是合法 UTF-8。
        // 直接构造 String/PathBuf，跳过 `nfc().collect()` 中间 String 分配。
        // SAFETY: bytes.is_ascii() 已证明是合法 UTF-8。
        let s = unsafe { std::str::from_utf8_unchecked(bytes) };
        return PathBuf::from(s);
    }
    use unicode_normalization::UnicodeNormalization;
    let s = String::from_utf8_lossy(bytes);
    PathBuf::from(s.nfc().collect::<String>())
}

pub(crate) fn normalize_path(path: &std::path::Path) -> PathBuf {
    // Phase 3：ASCII 字节集对 NFC 是恒等变换。事件管线 99% 路径走这条快速通道，
    // 跳过 `nfc().collect::<String>()` 的中间 String 分配。
    let bytes = path.as_os_str().as_encoded_bytes();
    if bytes.is_ascii() {
        return path.to_path_buf();
    }
    use unicode_normalization::UnicodeNormalization;
    let s = path.to_string_lossy();
    PathBuf::from(s.nfc().collect::<String>())
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
    pub(self) flush_requested: AtomicBool,
    pub(self) flush_notify: Notify,
    pub(self) auto_flush_overlay_paths: AtomicU64,
    pub(self) auto_flush_overlay_bytes: AtomicU64,
    pub(self) periodic_flush_min_events: AtomicU64,
    pub(self) periodic_flush_min_bytes: AtomicU64,
    pub(self) pending_flush_events: AtomicU64,
    pub(self) pending_flush_bytes: AtomicU64,
    pub(self) last_snapshot_time: AtomicU64,
    /// 应用中的事件路径"在 L2 生效前"的临时可见集合。
    ///
    /// Phase 3：从 `Vec<EventRecord>` 紧缩为 `HashSet<PathBuf>`：
    /// - query 路径只关心 path 是否处在 debounce 窗口；不需要 seq/timestamp/file_key。
    /// - 插入端只放 Create/Modify/Rename(to) 的最佳路径——Delete 在 [`tiered::events`]
    ///   入口处就被跳过，HashSet 永远不持有"将被删除"的路径。
    /// - 移除端把 O(K × N) `Vec::retain` 换成 O(K) `HashSet::remove`，事件应用完
    ///   一拉就清。
    /// - 每事件少一份完整 `EventRecord` clone（4 个 PathBuf）。
    pub(self) pending_events: Mutex<std::collections::HashSet<PathBuf>>,
    pub roots: Vec<PathBuf>,
    pub include_hidden: bool,
    pub ignore_enabled: bool,
    pub follow_symlinks: bool,
}

impl TieredIndex {
    pub fn rebuild_in_progress(&self) -> bool {
        self.rebuild_state.lock().in_progress
    }
}

#[cfg(test)]
pub(crate) use self::disk_layer::event_record_estimated_bytes;
