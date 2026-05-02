use std::collections::HashMap;
use crate::core::{EventRecord, EventType};

/// 统一增量缓冲区，替代 overlay_state + pending_events
#[derive(Debug, Clone)]
pub struct DeltaBuffer {
    /// 路径(bytes) → 最新增量状态（按路径去重）
    entries: HashMap<Vec<u8>, DeltaState>,
    /// 硬容量上限（默认 256K 条）
    max_capacity: usize,
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
            max_capacity: cap,
        }
    }

    #[cfg(test)]
    fn with_capacity_and_limit(cap: usize, max_capacity: usize) -> Self {
        Self {
            entries: HashMap::with_capacity(cap.min(1024)),
            max_capacity,
        }
    }

    /// 应用一批事件，按路径去重保留最新状态。
    /// 返回 `true` 表示所有事件均已应用；`false` 表示容量已满，后续事件被丢弃。
    pub fn apply_events(&mut self, events: &[EventRecord]) -> bool {
        for ev in events {
            if !self.insert(ev.clone()) {
                return false;
            }
        }
        true
    }

    /// 单个事件插入（内部去重逻辑）。
    /// 返回 `true` 表示已插入/更新；`false` 表示容量已满且为新路径，无法插入。
    fn insert(&mut self, event: EventRecord) -> bool {
        let Some(path) = event.best_path() else {
            // FID-only 且无路径：保守跳过 overlay 更新（后续 fanotify 反查完善）。
            return true;
        };
        let path_bytes = path.as_os_str().as_encoded_bytes().to_vec();

        match &event.event_type {
            EventType::Delete => {
                if self.entries.len() >= self.max_capacity && !self.entries.contains_key(&path_bytes) {
                    return false;
                }
                self.entries.insert(path_bytes, DeltaState::Deleted);
                true
            }
            EventType::Create | EventType::Modify => {
                if self.entries.len() >= self.max_capacity && !self.entries.contains_key(&path_bytes) {
                    return false;
                }
                self.entries.insert(path_bytes, DeltaState::Live(event));
                true
            }
            EventType::Rename {
                from,
                from_path_hint,
            } => {
                let from_best = from_path_hint.as_deref().or_else(|| from.as_path());
                let from_bytes = from_best.map(|p| p.as_os_str().as_encoded_bytes().to_vec());

                // 计算本次 Rename 事件是否会净增新条目。
                let mut net_new = 0usize;
                if let Some(ref fb) = from_bytes {
                    if !self.entries.contains_key(fb) {
                        net_new += 1;
                    }
                }
                if !self.entries.contains_key(&path_bytes) {
                    net_new += 1;
                }

                if self.entries.len().saturating_add(net_new) > self.max_capacity {
                    return false;
                }

                if let Some(fb) = from_bytes {
                    self.entries.insert(fb, DeltaState::Deleted);
                }
                self.entries.insert(path_bytes, DeltaState::Live(event));
                true
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
        let mut db = DeltaBuffer::with_capacity_and_limit(2, 2);
        assert!(db.apply_events(&[
            make_event(1, EventType::Create, "/tmp/a"),
            make_event(2, EventType::Create, "/tmp/b"),
        ]));
        assert_eq!(db.len(), 2);
        // 容量超限拒绝新路径
        assert!(!db.apply_events(&[make_event(3, EventType::Create, "/tmp/c")]));
        assert_eq!(db.len(), 2);
        // 更新已有路径仍允许
        assert!(db.apply_events(&[make_event(4, EventType::Modify, "/tmp/a")]));
        assert_eq!(db.len(), 2);
        // 删除已有路径仍允许
        assert!(db.apply_events(&[make_event(5, EventType::Delete, "/tmp/a")]));
        assert_eq!(db.len(), 2);
        assert!(db.is_deleted(b"/tmp/a"));
        // clear 后腾出空间
        db.clear();
        assert!(db.apply_events(&[make_event(6, EventType::Create, "/tmp/c")]));
        assert_eq!(db.len(), 1);
    }

    #[test]
    fn test_hard_capacity_limit_256k() {
        let mut db = DeltaBuffer::with_capacity(256 * 1024);
        for i in 0..(256 * 1024) {
            let path = format!("/tmp/file_{}", i);
            assert!(
                db.apply_events(&[make_event(i as u64, EventType::Create, &path)]),
                "Failed at iteration {}",
                i
            );
        }
        assert_eq!(db.len(), 256 * 1024);

        // 第 256K+1 条被拒绝
        assert!(!db.apply_events(&[make_event(999_999, EventType::Create, "/tmp/overflow")]));
        assert_eq!(db.len(), 256 * 1024);

        // drain_deleted_for_flush 后（内部调用 clear）可以重新插入
        let _ = db.drain_deleted_for_flush();
        assert!(db.is_empty());
        assert!(db.apply_events(&[make_event(1, EventType::Create, "/tmp/after_clear")]));
        assert_eq!(db.len(), 1);
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
