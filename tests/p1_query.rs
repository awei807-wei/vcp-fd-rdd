//! P1 — Query tests.
//!
//! Validates filter effectiveness, fuzzy matching, streaming UDS queries,
//! UDS permission checks, and short query optimization.

use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, SystemTime};

use fd_rdd::core::{FileKey, FileMeta};
use fd_rdd::index::TieredIndex;
use fd_rdd::query::{execute_query, QueryMode, SortColumn, SortOrder};

fn unique_tmp_dir(tag: &str) -> PathBuf {
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    std::env::temp_dir().join(format!("fd-rdd-query-{}-{}", tag, nanos))
}

fn build_index_with_files(root: &Path, files: &[(&str, u64)]) -> Arc<TieredIndex> {
    let index = Arc::new(TieredIndex::empty(vec![root.to_path_buf()]));
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

fn build_index_with_metas(root: &Path, files: &[FileMeta]) -> Arc<TieredIndex> {
    let index = Arc::new(TieredIndex::empty(vec![root.to_path_buf()]));
    let l2 = index.l2.load_full();
    for meta in files {
        l2.upsert(meta.clone());
    }
    index
}

fn path_depth(path: &Path) -> usize {
    let s = path.to_string_lossy();
    s.matches('/').count() + s.matches('\\').count()
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
    assert!(results
        .iter()
        .all(|m| m.path.to_string_lossy().to_lowercase().contains("readme")));

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

#[test]
fn smart_case_handles_case_distinct_siblings() {
    let root = unique_tmp_dir("query-smart-case-siblings");
    std::fs::create_dir_all(root.join("case")).unwrap();

    let upper = root.join("case").join("VCP_20260424.txt");
    let lower = root.join("case").join("vcp_20260424.txt");
    let index = build_index_with_metas(
        &root,
        &[
            FileMeta {
                file_key: FileKey {
                    dev: 1,
                    ino: 1,
                    generation: 0,
                },
                path: upper.clone(),
                size: 10,
                mtime: None,
                ctime: None,
                atime: None,
            },
            FileMeta {
                file_key: FileKey {
                    dev: 1,
                    ino: 2,
                    generation: 0,
                },
                path: lower.clone(),
                size: 10,
                mtime: None,
                ctime: None,
                atime: None,
            },
        ],
    );

    let insensitive = index.query("vcp_20260424");
    assert!(
        insensitive.iter().any(|m| m.path == upper),
        "lowercase smart-case query should match uppercase sibling"
    );
    assert!(
        insensitive.iter().any(|m| m.path == lower),
        "lowercase smart-case query should match lowercase sibling"
    );

    let sensitive = index.query("VCP_20260424");
    assert!(
        sensitive.iter().any(|m| m.path == upper),
        "uppercase smart-case query should match uppercase sibling"
    );
    assert!(
        sensitive.iter().all(|m| m.path != lower),
        "uppercase smart-case query should exclude lowercase sibling"
    );

    let _ = std::fs::remove_dir_all(&root);
}

#[test]
fn score_sort_prefers_basename_match_over_path_segment_match() {
    let root = unique_tmp_dir("query-score-basename-vs-path");
    std::fs::create_dir_all(&root).unwrap();

    let basename_hit = root.join(".config.json");
    let path_hit = root.join("src").join("config").join("README.md");
    let index = build_index_with_metas(
        &root,
        &[
            FileMeta {
                file_key: FileKey {
                    dev: 1,
                    ino: 1,
                    generation: 0,
                },
                path: basename_hit.clone(),
                size: 10,
                mtime: None,
                ctime: None,
                atime: None,
            },
            FileMeta {
                file_key: FileKey {
                    dev: 1,
                    ino: 2,
                    generation: 0,
                },
                path: path_hit.clone(),
                size: 10,
                mtime: None,
                ctime: None,
                atime: None,
            },
        ],
    );

    let results = execute_query(
        index.as_ref(),
        "config",
        10,
        QueryMode::Exact,
        SortColumn::Score,
        SortOrder::Desc,
    );
    assert_eq!(results[0].path, basename_hit);

    let _ = std::fs::remove_dir_all(&root);
}

#[test]
fn score_sort_prefers_boundary_hit_for_env_style_files() {
    let root = unique_tmp_dir("query-score-boundary");
    std::fs::create_dir_all(&root).unwrap();

    let dot_env = root.join(".env");
    let path_segment_hit = root.join("src").join("env").join("README.md");
    let index = build_index_with_metas(
        &root,
        &[
            FileMeta {
                file_key: FileKey {
                    dev: 1,
                    ino: 1,
                    generation: 0,
                },
                path: dot_env.clone(),
                size: 10,
                mtime: None,
                ctime: None,
                atime: None,
            },
            FileMeta {
                file_key: FileKey {
                    dev: 1,
                    ino: 2,
                    generation: 0,
                },
                path: path_segment_hit.clone(),
                size: 10,
                mtime: None,
                ctime: None,
                atime: None,
            },
        ],
    );

    let results = execute_query(
        index.as_ref(),
        "env",
        10,
        QueryMode::Exact,
        SortColumn::Score,
        SortOrder::Desc,
    );
    assert_eq!(results[0].path, dot_env);

    let _ = std::fs::remove_dir_all(&root);
}

#[test]
fn score_sort_demotes_node_modules_when_query_has_no_node_hint() {
    let root = unique_tmp_dir("query-score-node-zone");
    std::fs::create_dir_all(&root).unwrap();

    let src_hit = root.join("src").join("index.ts");
    let node_hit = root.join("node_modules").join("lib").join("index.js");
    let index = build_index_with_metas(
        &root,
        &[
            FileMeta {
                file_key: FileKey {
                    dev: 1,
                    ino: 1,
                    generation: 0,
                },
                path: src_hit.clone(),
                size: 10,
                mtime: None,
                ctime: None,
                atime: None,
            },
            FileMeta {
                file_key: FileKey {
                    dev: 1,
                    ino: 2,
                    generation: 0,
                },
                path: node_hit.clone(),
                size: 10,
                mtime: None,
                ctime: None,
                atime: None,
            },
        ],
    );

    let results = execute_query(
        index.as_ref(),
        "index",
        10,
        QueryMode::Exact,
        SortColumn::Score,
        SortOrder::Desc,
    );
    assert_eq!(results[0].path, src_hit);

    let _ = std::fs::remove_dir_all(&root);
}

#[test]
fn score_sort_uses_shorter_basename_as_tiebreaker() {
    let root = unique_tmp_dir("query-score-length");
    std::fs::create_dir_all(&root).unwrap();

    let short = root.join("test.txt");
    let long = root.join("test_with_a_significantly_longer_basename.txt");
    let index = build_index_with_metas(
        &root,
        &[
            FileMeta {
                file_key: FileKey {
                    dev: 1,
                    ino: 1,
                    generation: 0,
                },
                path: short.clone(),
                size: 10,
                mtime: None,
                ctime: None,
                atime: None,
            },
            FileMeta {
                file_key: FileKey {
                    dev: 1,
                    ino: 2,
                    generation: 0,
                },
                path: long.clone(),
                size: 10,
                mtime: None,
                ctime: None,
                atime: None,
            },
        ],
    );

    let results = execute_query(
        index.as_ref(),
        "test",
        10,
        QueryMode::Exact,
        SortColumn::Score,
        SortOrder::Desc,
    );
    assert_eq!(results[0].path, short);

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
            file_key: FileKey {
                dev: 1,
                ino: i + 1,
                generation: 0,
            },
            path,
            size: 100,
            mtime: None,
            ctime: None,
            atime: None,
        });
    }

    // Query that matches all files
    let results = index.query_limit("data", 10_000);
    assert!(!results.is_empty(), "Should return results for broad query");

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

#[test]
fn parent_and_infolder_filters_select_exact_parent() {
    let root = unique_tmp_dir("query-parent");
    let target_parent = root.join("target");
    let other_parent = root.join("other");
    std::fs::create_dir_all(&target_parent).unwrap();
    std::fs::create_dir_all(&other_parent).unwrap();

    let index = build_index_with_files(
        &root,
        &[
            ("target/parent_probe.txt", 10),
            ("other/parent_probe.txt", 20),
            ("target/other.txt", 30),
        ],
    );

    let target_parent_str = target_parent.to_string_lossy();
    let parent_results = index.query(&format!("parent:{} parent_probe", target_parent_str));
    assert_eq!(
        parent_results.len(),
        1,
        "parent: should only match exact parent"
    );
    assert!(parent_results[0].path.ends_with("target/parent_probe.txt"));

    let infolder_results = index.query(&format!("infolder:{} parent_probe", target_parent_str));
    assert_eq!(
        infolder_results.len(),
        1,
        "infolder: should behave the same as parent:"
    );
    assert!(infolder_results[0]
        .path
        .ends_with("target/parent_probe.txt"));

    let _ = std::fs::remove_dir_all(&root);
}

#[test]
fn depth_len_and_type_filters_work() {
    let root = unique_tmp_dir("query-filters");
    std::fs::create_dir_all(root.join("alpha/beta/gamma")).unwrap();

    let shallow = root.join("depthprobe_shallow.txt");
    let deep = root.join("alpha/beta/gamma/depthprobe_deep.txt");
    let short_name = root.join("lenprobe.txt");
    let long_name = root.join("lenprobe_filename_with_significantly_long_name.txt");
    let type_file = root.join("typeprobe.txt");

    let metas = vec![
        FileMeta {
            file_key: FileKey {
                dev: 1,
                ino: 1,
                generation: 0,
            },
            path: shallow.clone(),
            size: 10,
            mtime: None,
            ctime: None,
            atime: None,
        },
        FileMeta {
            file_key: FileKey {
                dev: 1,
                ino: 2,
                generation: 0,
            },
            path: deep.clone(),
            size: 20,
            mtime: None,
            ctime: None,
            atime: None,
        },
        FileMeta {
            file_key: FileKey {
                dev: 1,
                ino: 3,
                generation: 0,
            },
            path: short_name.clone(),
            size: 30,
            mtime: None,
            ctime: None,
            atime: None,
        },
        FileMeta {
            file_key: FileKey {
                dev: 1,
                ino: 4,
                generation: 0,
            },
            path: long_name.clone(),
            size: 40,
            mtime: None,
            ctime: None,
            atime: None,
        },
        FileMeta {
            file_key: FileKey {
                dev: 1,
                ino: 5,
                generation: 0,
            },
            path: type_file.clone(),
            size: 50,
            mtime: None,
            ctime: None,
            atime: None,
        },
    ];
    let index = build_index_with_metas(&root, &metas);

    let shallow_depth = path_depth(&shallow);
    let depth_results = index.query(&format!("depthprobe depth:<={}", shallow_depth));
    assert!(
        depth_results.iter().any(|m| m.path == shallow),
        "depth:<= should match the shallow file"
    );
    assert!(
        depth_results.iter().all(|m| m.path != deep),
        "depth:<= should exclude the deeper file"
    );

    let len_results = index.query("lenprobe len:>40");
    assert!(
        len_results.iter().any(|m| m.path == long_name),
        "len:>40 should match the long basename"
    );
    assert!(
        len_results.iter().all(|m| m.path != short_name),
        "len:>40 should exclude the short basename"
    );

    let type_results = index.query("type:file typeprobe");
    assert_eq!(
        type_results.len(),
        1,
        "type:file should match indexed files"
    );
    assert_eq!(type_results[0].path, type_file);

    let _ = std::fs::remove_dir_all(&root);
}

#[test]
fn execute_query_sorts_by_size_and_modified_time() {
    let root = unique_tmp_dir("query-sort");
    std::fs::create_dir_all(&root).unwrap();

    let now = SystemTime::now();
    let older = now.checked_sub(Duration::from_secs(3600)).unwrap();
    let oldest = now.checked_sub(Duration::from_secs(7200)).unwrap();
    let metas = vec![
        FileMeta {
            file_key: FileKey {
                dev: 1,
                ino: 1,
                generation: 0,
            },
            path: root.join("sortprobe_small.txt"),
            size: 10,
            mtime: Some(oldest),
            ctime: Some(older),
            atime: Some(oldest),
        },
        FileMeta {
            file_key: FileKey {
                dev: 1,
                ino: 2,
                generation: 0,
            },
            path: root.join("sortprobe_medium.txt"),
            size: 20,
            mtime: Some(older),
            ctime: Some(oldest),
            atime: Some(older),
        },
        FileMeta {
            file_key: FileKey {
                dev: 1,
                ino: 3,
                generation: 0,
            },
            path: root.join("sortprobe_large.txt"),
            size: 30,
            mtime: Some(now),
            ctime: Some(now),
            atime: Some(now),
        },
    ];
    let index = build_index_with_metas(&root, &metas);

    let size_desc = execute_query(
        index.as_ref(),
        "sortprobe",
        10,
        QueryMode::Exact,
        SortColumn::Size,
        SortOrder::Desc,
    );
    assert_eq!(size_desc[0].path, root.join("sortprobe_large.txt"));

    let modified_desc = execute_query(
        index.as_ref(),
        "sortprobe",
        10,
        QueryMode::Exact,
        SortColumn::DateModified,
        SortOrder::Desc,
    );
    assert_eq!(modified_desc[0].path, root.join("sortprobe_large.txt"));

    let _ = std::fs::remove_dir_all(&root);
}
