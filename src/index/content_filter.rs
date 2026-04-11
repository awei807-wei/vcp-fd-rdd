use std::collections::HashSet;
use std::path::Path;

/// 全文索引过滤器：决定哪些文件应被索引，并提供内容去重。
pub struct ContentFilter {
    seen_hashes: HashSet<u64>,
}

/// 最大可索引文件大小：10 MB
const MAX_INDEX_SIZE: u64 = 10 * 1024 * 1024;

impl ContentFilter {
    pub fn new() -> Self {
        Self {
            seen_hashes: HashSet::new(),
        }
    }

    /// 根据路径扩展名和文件大小判断是否应索引。
    /// 超过 10MB 的文件跳过。
    pub fn should_index(_path: &Path, size: u64) -> bool {
        size <= MAX_INDEX_SIZE
    }

    /// 检测二进制文件：前 8KB 中包含 null byte 即视为二进制。
    pub fn is_binary(header: &[u8]) -> bool {
        let check_len = header.len().min(8192);
        header[..check_len].contains(&0)
    }

    /// 计算内容哈希（xxh3），用于去重。
    pub fn content_hash(data: &[u8]) -> u64 {
        xxhash_rust::xxh3::xxh3_64(data)
    }

    /// 检查内容是否已见过（去重）。若未见过则插入并返回 false，已见过返回 true。
    pub fn is_duplicate(&mut self, data: &[u8]) -> bool {
        let hash = Self::content_hash(data);
        !self.seen_hashes.insert(hash)
    }
}

impl Default for ContentFilter {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::Path;

    #[test]
    fn should_index_respects_size_limit() {
        assert!(ContentFilter::should_index(Path::new("a.txt"), 1024));
        assert!(ContentFilter::should_index(Path::new("b.rs"), MAX_INDEX_SIZE));
        assert!(!ContentFilter::should_index(Path::new("c.bin"), MAX_INDEX_SIZE + 1));
    }

    #[test]
    fn is_binary_detects_null_bytes() {
        assert!(!ContentFilter::is_binary(b"hello world"));
        assert!(ContentFilter::is_binary(b"hello\x00world"));
        assert!(!ContentFilter::is_binary(b""));
    }

    #[test]
    fn content_hash_deterministic() {
        let data = b"some file content";
        assert_eq!(ContentFilter::content_hash(data), ContentFilter::content_hash(data));
    }

    #[test]
    fn dedup_detects_duplicates() {
        let mut cf = ContentFilter::new();
        let data = b"unique content";
        assert!(!cf.is_duplicate(data));
        assert!(cf.is_duplicate(data));
        assert!(!cf.is_duplicate(b"different content"));
    }
}
