use roaring::RoaringTreemap;
use std::collections::HashMap;
use std::path::PathBuf;

/// ParentIndex: 目录 path -> 该目录下所有文件的 DocId bitmap
///
/// 替代 for_each_live_meta_in_dirs 的 O(N) 全量遍历。
/// 构建复杂度: O(N) 一次性（在 rebuild/snapshot 后构建）
/// 查询复杂度: O(D) + O(bitmap OR)，D=脏目录数
#[derive(Debug, Clone)]
pub struct ParentIndex {
    /// parent directory path (UTF-8 bytes) -> DocIds
    pub(crate) dir_to_files: HashMap<Vec<u8>, RoaringTreemap>,
}

impl ParentIndex {
    pub fn new() -> Self {
        Self {
            dir_to_files: HashMap::new(),
        }
    }

    /// 查询多个目录的并集（fast_sync Phase3 使用）
    pub fn files_in_dirs(
        &self,
        dirs: &std::collections::HashSet<PathBuf>,
    ) -> RoaringTreemap {
        let mut result = RoaringTreemap::new();
        for dir in dirs {
            let dir_bytes = dir.as_os_str().as_encoded_bytes().to_vec();
            if let Some(bitmap) = self.dir_to_files.get(&dir_bytes) {
                result |= bitmap;
            }
        }
        result
    }

    /// 单目录查询
    pub fn files_in_dir(&self, dir: &PathBuf) -> Option<&RoaringTreemap> {
        let dir_bytes = dir.as_os_str().as_encoded_bytes().to_vec();
        self.dir_to_files.get(&dir_bytes)
    }
}

impl Default for ParentIndex {
    fn default() -> Self {
        Self::new()
    }
}
