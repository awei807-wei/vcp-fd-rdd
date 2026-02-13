use serde::{Serialize, Deserialize};

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct EventRecord {
    pub seq: u64,
    pub timestamp: std::time::SystemTime,
    pub event_type: EventType,
    pub path: std::path::PathBuf,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum EventType {
    Create,
    Delete,
    Modify,
    Rename(std::path::PathBuf),  // from
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