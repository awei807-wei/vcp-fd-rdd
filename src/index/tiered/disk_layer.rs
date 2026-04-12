use std::sync::Arc;

use crate::core::{EventRecord, EventType, FileIdentifier};
use crate::index::mmap_index::MmapIndex;

use super::arena::PathArenaSet;

#[derive(Clone)]
pub(super) struct DiskLayer {
    pub(super) id: u64,
    pub(super) idx: Arc<MmapIndex>,
    pub(super) deleted_paths: Arc<PathArenaSet>,
    pub(super) deleted_paths_count: usize,
    pub(super) deleted_paths_bytes: u64,
    pub(super) deleted_paths_estimated_bytes: u64,
}

pub(super) fn file_identifier_estimated_bytes(id: &FileIdentifier) -> u64 {
    match id {
        FileIdentifier::Path(p) => p.as_os_str().as_encoded_bytes().len() as u64,
        FileIdentifier::Fid { .. } => 16,
    }
}

pub(crate) fn event_record_estimated_bytes(ev: &EventRecord) -> u64 {
    let mut bytes = file_identifier_estimated_bytes(&ev.id);
    if let Some(p) = &ev.path_hint {
        bytes = bytes.saturating_add(p.as_os_str().as_encoded_bytes().len() as u64);
    }
    if let EventType::Rename {
        from,
        from_path_hint,
    } = &ev.event_type
    {
        bytes = bytes.saturating_add(file_identifier_estimated_bytes(from));
        if let Some(p) = from_path_hint {
            bytes = bytes.saturating_add(p.as_os_str().as_encoded_bytes().len() as u64);
        }
    }
    bytes
}
