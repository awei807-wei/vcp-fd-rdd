use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::path::Path;

use crate::core::FileKey;

/// 路径 ID（u32 足够支持 40 亿条不同路径）
pub type PathId = u32;

/// 路径去重表：将完整路径字符串压缩为紧凑的 u32 ID。
///
/// 对于百万级文件索引，大量文件共享相同目录前缀；
/// PathTable 通过 intern/deduplicate 消除重复路径的内存占用。
#[derive(Clone, Debug, Default)]
pub struct PathTable {
    /// path_bytes → path_id
    path_to_id: HashMap<Vec<u8>, PathId>,
    /// path_id → path_bytes
    id_to_path: Vec<Vec<u8>>,
}

impl PathTable {
    pub fn new() -> Self {
        Self {
            path_to_id: HashMap::new(),
            id_to_path: Vec::new(),
        }
    }

    /// 插入路径，返回 path_id（已存在则返回已有 id）。
    pub fn intern(&mut self, path: &Path) -> PathId {
        let bytes = path.as_os_str().as_encoded_bytes().to_vec();
        if let Some(&id) = self.path_to_id.get(&bytes) {
            return id;
        }
        let id = self.id_to_path.len() as PathId;
        self.path_to_id.insert(bytes.clone(), id);
        self.id_to_path.push(bytes);
        id
    }

    /// 通过 path_id 获取路径。
    pub fn resolve(&self, id: PathId) -> Option<&Path> {
        let bytes = self.id_to_path.get(id as usize)?;
        #[cfg(unix)]
        {
            use std::ffi::OsStr;
            use std::os::unix::ffi::OsStrExt;
            Some(Path::new(OsStr::from_bytes(bytes)))
        }
        #[cfg(not(unix))]
        {
            let s = std::str::from_utf8(bytes).ok()?;
            Some(Path::new(s))
        }
    }

    /// 当前已存储的不同路径数量。
    pub fn len(&self) -> usize {
        self.id_to_path.len()
    }

    /// 是否为空。
    pub fn is_empty(&self) -> bool {
        self.id_to_path.is_empty()
    }
}

/// 精简版文件元数据（不含完整路径，用 path_id 引用 PathTable）。
///
/// 作为 `FileMeta` 的内存优化替代，用于 L2/L3 索引层。
/// 当前阶段仅提供定义，尚未全面替换 `FileMeta`。
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct FileEntry {
    pub file_key: FileKey,
    pub path_id: PathId,
    pub size: u64,
    pub mtime: Option<std::time::SystemTime>,
    /// 文件创建时间（Linux 上为 ctime/inode-change-time；不持久化到快照）
    #[serde(default, skip_serializing)]
    pub ctime: Option<std::time::SystemTime>,
    /// 最近访问时间（不持久化到快照）
    #[serde(default, skip_serializing)]
    pub atime: Option<std::time::SystemTime>,
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::PathBuf;

    #[test]
    fn test_intern_returns_unique_ids() {
        let mut pt = PathTable::new();
        let p1 = Path::new("/home/user/file.txt");
        let p2 = Path::new("/home/user/other.txt");

        let id1 = pt.intern(p1);
        let id2 = pt.intern(p2);

        assert_ne!(id1, id2);
        assert_eq!(pt.len(), 2);
    }

    #[test]
    fn test_intern_deduplicates_same_path() {
        let mut pt = PathTable::new();
        let p = Path::new("/var/log/syslog");

        let id1 = pt.intern(p);
        let id2 = pt.intern(p);
        let id3 = pt.intern(Path::new("/var/log/syslog"));

        assert_eq!(id1, id2);
        assert_eq!(id1, id3);
        assert_eq!(pt.len(), 1);
    }

    #[test]
    fn test_resolve_roundtrip() {
        let mut pt = PathTable::new();
        let original = Path::new("/tmp/roundtrip_test");

        let id = pt.intern(original);
        let resolved = pt.resolve(id).expect("resolve should succeed");

        assert_eq!(resolved, original);
    }

    #[test]
    fn test_resolve_missing_id() {
        let pt = PathTable::new();
        assert!(pt.resolve(0).is_none());
        assert!(pt.resolve(999).is_none());
    }

    #[test]
    fn test_intern_many_paths() {
        let mut pt = PathTable::new();
        let paths: Vec<PathBuf> = (0..1000)
            .map(|i| PathBuf::from(format!("/data/dir{}/file{}.txt", i % 10, i)))
            .collect();

        let ids: Vec<PathId> = paths.iter().map(|p| pt.intern(p)).collect();

        // 只有 10 个不同目录前缀，但 1000 个不同文件 = 1000 条不同路径
        assert_eq!(pt.len(), 1000);

        // 验证每个 id 都能正确解析回原始路径
        for (orig, id) in paths.iter().zip(ids.iter()) {
            assert_eq!(pt.resolve(*id).unwrap(), orig.as_path());
        }
    }

    #[test]
    fn test_file_entry_serialization_roundtrip() {
        let entry = FileEntry {
            file_key: FileKey {
                dev: 1,
                ino: 42,
                generation: 7,
            },
            path_id: 123,
            size: 4096,
            mtime: Some(std::time::SystemTime::UNIX_EPOCH),
            ctime: None,
            atime: None,
        };

        let json = serde_json::to_string(&entry).unwrap();
        assert!(json.contains("\"path_id\":123"));
        assert!(!json.contains("ctime")); // skip_serializing
        assert!(!json.contains("atime")); // skip_serializing

        let decoded: FileEntry = serde_json::from_str(&json).unwrap();
        assert_eq!(decoded.file_key, entry.file_key);
        assert_eq!(decoded.path_id, entry.path_id);
        assert_eq!(decoded.size, entry.size);
        assert_eq!(decoded.mtime, entry.mtime);
    }
}
