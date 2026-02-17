use serde::{Deserialize, Serialize};
use std::path::Path;
use std::path::PathBuf;

use super::rdd::FileKey;

/// 统一文件身份表示：
/// - notify: Path（用户态路径）
/// - fanotify: Fid(dev, ino)（内核 FID）
#[derive(Clone, Debug, Eq, PartialEq, Hash, Serialize, Deserialize)]
pub enum FileIdentifier {
    Path(PathBuf),
    Fid { dev: u64, ino: u64 },
}

impl FileIdentifier {
    pub fn as_path(&self) -> Option<&Path> {
        match self {
            FileIdentifier::Path(p) => Some(p.as_path()),
            FileIdentifier::Fid { .. } => None,
        }
    }

    pub fn as_file_key(&self) -> Option<FileKey> {
        match self {
            FileIdentifier::Path(_) => None,
            FileIdentifier::Fid { dev, ino } => Some(FileKey {
                dev: *dev,
                ino: *ino,
            }),
        }
    }
}

/// 文件系统事件类型
#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum EventType {
    Create,
    Delete,
    Modify,
    Rename {
        from: FileIdentifier,
        from_path_hint: Option<PathBuf>,
    },
}

/// 事件记录（用于事件管道，非 RDD lineage）
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct EventRecord {
    pub seq: u64,
    pub timestamp: std::time::SystemTime,
    pub event_type: EventType,
    /// 事件的主身份（rename-to 的目标身份亦在此）
    pub id: FileIdentifier,
    /// 可选路径提示：用于 fanotify/FID-only 事件在未反查出路径前进行保守处理；
    /// 合并时遵循“最后一次非空覆盖”。
    pub path_hint: Option<PathBuf>,
}

impl EventRecord {
    /// 事件的最佳路径视图：优先 `path_hint`，其次 `FileIdentifier::Path`。
    pub fn best_path(&self) -> Option<&Path> {
        self.path_hint.as_deref().or_else(|| self.id.as_path())
    }
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
