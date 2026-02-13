use serde::{Serialize, Deserialize};
use crate::core::lineage::EventRecord;

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Partition {
    pub id: usize,
    pub root: std::path::PathBuf,
    pub depth: usize,
    pub created_at: std::time::SystemTime,
    pub modified_at: Option<std::time::SystemTime>,
    pub last_event: Option<EventRecord>,
}