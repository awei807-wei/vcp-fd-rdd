//! P1 — Query tests.
//!
//! Validates filter effectiveness, fuzzy matching, streaming UDS queries,
//! UDS permission checks, and short query optimization.

use std::path::PathBuf;
use std::sync::Arc;

use fd_rdd::core::{FileKey, FileMeta};
use fd_rdd::index::TieredIndex;

fn unique_tmp_dir(tag: &str) -> PathBuf {
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    std::env::temp_dir().join(format!("fd-rdd-query-{}-{}", tag, nanos))
}

fn build_index_with_files(root: &PathBuf, files: &[(&str, u64)]) -> Arc<TieredIndex> {
    let index = Arc::new(TieredIndex::empty(vec![root.clone()]));
    let l2 = index.l2.load_full();
    for (i, (name, size)) in files.iter().enumerate() {
        let path = root.join(name);
        l2.upsert(FileMeta {
            file_key: FileKey {
                dev: 1,
                ino: (i + 1) as u64,
                generation: 0,
            },
            path,
            size: *size,
            mtime: None,
            ctime: None,
            atime: None,
        });
    }
    index
}

/// 21. 过滤器有效性：基本关键字查询
#[test]
fn basic_keyword_query() {
    let root = unique_tmp_dir("query-basic");
    std::fs::create_dir_all(&root).unwrap();

    let index = build_index_with_files(
        &root,
        &[
            ("readme.md", 100),
            ("main.rs", 200),
            ("config.toml", 50),
            ("readme.txt", 80),
        ],
    );

    let results = index.query("readme");
    assert_eq!(results.len(), 2, "Should find both readme files");
    assert!(results.iter().all(|m| m
        .path
        .to_string_lossy()
        .to_lowercase()
        .contains("readme")));

    let _ = std::fs::remove_dir_all(&root);
}

/// 22. fuzzy 模式匹配和排序
#[test]
fn fuzzy_query_matches() {
    let root = unique_tmp_dir("query-fuzzy");
    std::fs::create_dir_all(&root).unwrap();

    let index = build_index_with_files(
        &root,
        &[
            ("my_document.txt", 100),
            ("my_data.csv", 200),
            ("other_file.rs", 50),
        ],
    );

    // "my" should match files starting with "my"
    let results = index.query("my");
    assert!(
        results.len() >= 2,
        "Should find at least 2 files matching 'my'"
    );

    let _ = std::fs::remove_dir_all(&root);
}

/// 23. 流式 UDS 查询不 OOM — basic smoke test
///     (Full UDS test would require spawning a server; this tests the query path)
#[test]
fn large_result_set_query_does_not_oom() {
    let root = unique_tmp_dir("query-large");
    std::fs::create_dir_all(&root).unwrap();

    let index = Arc::new(TieredIndex::empty(vec![root.clone()]));
    let l2 = index.l2.load_full();

    // Insert 10,000 files with similar names
    for i in 0..10_000u64 {
        let path = root.join(format!("data_{:05}.txt", i));
        l2.upsert(FileMeta {
            file_key: FileKey { dev: 1, ino: i + 1, generation: 0 },
            path,
            size: 100,
            mtime: None,
            ctime: None,
            atime: None,
        });
    }

    // Query that matches all files
    let results = index.query_limit("data", 10_000);
    assert!(
        results.len() > 0,
        "Should return results for broad query"
    );

    let _ = std::fs::remove_dir_all(&root);
}

/// 25. 短查询（1-2 字符）优化
#[test]
fn short_query_works() {
    let root = unique_tmp_dir("query-short");
    std::fs::create_dir_all(&root).unwrap();

    let index = build_index_with_files(
        &root,
        &[
            ("a.txt", 10),
            ("ab.txt", 20),
            ("abc.txt", 30),
            ("xyz.txt", 40),
        ],
    );

    // Single character query
    let results = index.query("a");
    assert!(
        results.len() >= 3,
        "Single char 'a' should match a.txt, ab.txt, abc.txt; got {}",
        results.len()
    );

    // Two character query
    let results = index.query("ab");
    assert!(
        results.len() >= 2,
        "Two char 'ab' should match ab.txt, abc.txt; got {}",
        results.len()
    );

    let _ = std::fs::remove_dir_all(&root);
}
