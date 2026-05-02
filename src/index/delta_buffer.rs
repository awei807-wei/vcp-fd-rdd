use std::collections::HashMap;
use crate::core::{EventRecord, EventType};

/// 统一增量缓冲区，替代 overlay_state + pending_events
#[derive(Debug, Clone)]
pub struct DeltaBuffer {
    /// 路径(bytes) → 最新增量状态（按路径去重）
    entries: HashMap<Vec<u8>, DeltaState>,
    /// 容量上限（默认 262_144 = 256K）
    capacity: usize,
}

#[derive(Debug, Clone)]
pub enum DeltaState {
    /// 文件存在（创建/修改/重命名到）
    Live(EventRecord),
    /// 文件已删除
    Deleted,
}

impl DeltaBuffer {
    pub fn with_capacity(cap: usize) -> Self {
        Self {
            entries: HashMap::with_capacity(cap.min(1024)),
            capacity: cap,
        }
    }

    /// 应用一批事件，按路径去重保留最新状态
    pub fn apply_events(&mut self, events: &[EventRecord]) {
        for ev in events {
            self.insert(ev.clone());
        }
    }

    /// 单个事件插入（内部去重逻辑）
    fn insert(&mut self, event: EventRecord) {
        let Some(path) = event.best_path() else {
            // FID-only 且无路径：保守跳过 overlay 更新（后续 fanotify 反查完善）。
            return;
        };
        let path_bytes = path.as_os_str().as_encoded_bytes().to_vec();

        match &event.event_type {
            EventType::Delete => {
                self.entries.insert(path_bytes, DeltaState::Deleted);
            }
            EventType::Create | EventType::Modify => {
                self.entries.insert(path_bytes, DeltaState::Live(event));
            }
            EventType::Rename {
                from,
                from_path_hint,
            } => {
                let from_best = from_path_hint.as_deref().or_else(|| from.as_path());
                if let Some(from_path) = from_best {
                    let from_bytes = from_path.as_os_str().as_encoded_bytes().to_vec();
                    self.entries.insert(from_bytes, DeltaState::Deleted);
                }
                self.entries.insert(path_bytes, DeltaState::Live(event));
            }
        }
    }

    /// 查询时：返回所有 Live 状态的记录（替代 pending_events）
    pub fn live_records(&self) -> impl Iterator<Item = &EventRecord> {
        self.entries.values().filter_map(|state| match state {
            DeltaState::Live(rec) => Some(rec),
            DeltaState::Deleted => None,
        })
    }

    /// 查询时：返回所有被删除的路径 bytes（替代 deleted_paths）
    pub fn deleted_paths(&self) -> impl Iterator<Item = &[u8]> {
        self.entries.iter().filter_map(|(path, state)| match state {
            DeltaState::Deleted => Some(path.as_slice()),
            DeltaState::Live(_) => None,
        })
    }

    /// 查询时：返回所有 upserted 路径 bytes（替代 upserted_paths）
    pub fn upserted_paths(&self) -> impl Iterator<Item = &[u8]> {
        self.entries.iter().filter_map(|(path, state)| match state {
            DeltaState::Live(_) => Some(path.as_slice()),
            DeltaState::Deleted => None,
        })
    }

    /// 检查某路径是否被删除
    pub fn is_deleted(&self, path: &[u8]) -> bool {
        matches!(self.entries.get(path), Some(DeltaState::Deleted))
    }

    /// 检查某路径是否处于 Live 状态
    pub fn is_live(&self, path: &[u8]) -> bool {
        matches!(self.entries.get(path), Some(DeltaState::Live(_)))
    }

    /// 当前条目数
    pub fn len(&self) -> usize {
        self.entries.len()
    }

    /// 是否为空
    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    /// 清空（flush 后调用）
    pub fn clear(&mut self) {
        self.entries.clear();
    }

    /// 提取需要写入 seg-*.del 的删除路径（Deleted 状态）
    pub fn drain_deleted_for_flush(&mut self) -> Vec<Vec<u8>> {
        let mut deleted = Vec::new();
        for (path, state) in &self.entries {
            if matches!(state, DeltaState::Deleted) {
                deleted.push(path.clone());
            }
        }
        self.clear();
        deleted
    }

    /// 估算内存占用（字节数）
    pub fn estimated_bytes(&self) -> usize {
        use std::mem::size_of;
        // HashMap 条目开销 + key/value 本身
        let entry_overhead = size_of::<(Vec<u8>, DeltaState)>() + 16;
        self.entries.len() * entry_overhead
            + self.entries.capacity().saturating_sub(self.entries.len()) * size_of::<(Vec<u8>, DeltaState)>()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::{EventRecord, EventType, FileIdentifier};
    use std::path::PathBuf;

    fn make_event(seq: u64, event_type: EventType, path: &str) -> EventRecord {
        EventRecord {
            seq,
            timestamp: std::time::SystemTime::UNIX_EPOCH,
            event_type,
            id: FileIdentifier::Path(PathBuf::from(path)),
            path_hint: None,
        }
    }

    #[test]
    fn test_create_then_delete() {
        let mut db = DeltaBuffer::with_capacity(1024);
        db.apply_events(&[make_event(1, EventType::Create, "/tmp/a")]);
        assert!(db.is_live(b"/tmp/a"));
        db.apply_events(&[make_event(2, EventType::Delete, "/tmp/a")]);
        assert!(db.is_deleted(b"/tmp/a"));
        assert!(!db.is_live(b"/tmp/a"));
    }

    #[test]
    fn test_delete_then_create() {
        let mut db = DeltaBuffer::with_capacity(1024);
        db.apply_events(&[make_event(1, EventType::Delete, "/tmp/a")]);
        db.apply_events(&[make_event(2, EventType::Create, "/tmp/a")]);
        assert!(db.is_live(b"/tmp/a"));
        assert!(!db.is_deleted(b"/tmp/a"));
    }

    #[test]
    fn test_rename() {
        let mut db = DeltaBuffer::with_capacity(1024);
        let ev = EventRecord {
            seq: 1,
            timestamp: std::time::SystemTime::UNIX_EPOCH,
            event_type: EventType::Rename {
                from: FileIdentifier::Path(PathBuf::from("/tmp/old")),
                from_path_hint: None,
            },
            id: FileIdentifier::Path(PathBuf::from("/tmp/new")),
            path_hint: None,
        };
        db.apply_events(&[ev]);
        assert!(db.is_deleted(b"/tmp/old"));
        assert!(db.is_live(b"/tmp/new"));
    }

    #[test]
    fn test_live_records() {
        let mut db = DeltaBuffer::with_capacity(1024);
        db.apply_events(&[
            make_event(1, EventType::Create, "/tmp/a"),
            make_event(2, EventType::Delete, "/tmp/b"),
            make_event(3, EventType::Modify, "/tmp/c"),
        ]);
        let paths: Vec<&str> = db
            .live_records()
            .filter_map(|r| r.best_path())
            .map(|p| p.to_str().unwrap())
            .collect();
        assert_eq!(paths.len(), 2);
        assert!(paths.contains(&"/tmp/a"));
        assert!(paths.contains(&"/tmp/c"));
    }

    #[test]
    fn test_capacity_bound() {
        let mut db = DeltaBuffer::with_capacity(2);
        db.apply_events(&[
            make_event(1, EventType::Create, "/tmp/a"),
            make_event(2, EventType::Create, "/tmp/b"),
        ]);
        assert_eq!(db.len(), 2);
        // 容量超限仍允许插入（由调用方触发 flush）
        db.apply_events(&[make_event(3, EventType::Create, "/tmp/c")]);
        assert_eq!(db.len(), 3);
    }

    #[test]
    fn test_drain_deleted_for_flush() {
        let mut db = DeltaBuffer::with_capacity(1024);
        db.apply_events(&[
            make_event(1, EventType::Create, "/tmp/a"),
            make_event(2, EventType::Delete, "/tmp/b"),
            make_event(3, EventType::Modify, "/tmp/c"),
        ]);
        let deleted = db.drain_deleted_for_flush();
        assert_eq!(deleted.len(), 1);
        assert_eq!(deleted[0], b"/tmp/b");
        assert!(db.is_empty());
    }
}
