use std::path::PathBuf;
use std::time::Instant;

use crate::core::{EventType, FileIdentifier};

#[derive(Debug, Default)]
pub(super) struct RebuildState {
    pub(super) in_progress: bool,
    pub(super) pending_events: std::collections::HashMap<FileIdentifier, PendingEvent>,
    /// 最近一次 rebuild 开始时间（用于冷却/合并）
    pub(super) last_started_at: Option<Instant>,
    /// 冷却期内收到 rebuild 请求时，设置该标记；在冷却到期后合并执行一次
    pub(super) requested: bool,
    /// 冷却期触发的延迟 rebuild 是否已调度（避免重复 spawn sleep 线程）
    pub(super) scheduled: bool,
}

#[derive(Clone, Debug)]
pub(super) struct PendingEvent {
    pub(super) seq: u64,
    pub(super) timestamp: std::time::SystemTime,
    pub(super) event_type: EventType,
    pub(super) path_hint: Option<PathBuf>,
}
