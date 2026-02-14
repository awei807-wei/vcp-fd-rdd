use serde::{Serialize, Deserialize};
use std::path::PathBuf;

/// 文件系统事件类型
#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum EventType {
    Create,
    Delete,
    Modify,
    Rename { from: PathBuf },
}

/// 事件记录（用于事件管道，非 RDD lineage）
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct EventRecord {
    pub seq: u64,
    pub timestamp: std::time::SystemTime,
    pub event_type: EventType,
    pub path: PathBuf,
}

impl From<notify::event::EventKind> for EventType {
    fn from(kind: notify::event::EventKind) -> Self {
        use notify::event::*;
        match kind {
            EventKind::Create(_) => EventType::Create,
            EventKind::Remove(_) => EventType::Delete,
            EventKind::Modify(_) => EventType::Modify,
            _ => EventType::Modify,
        }
    }
}