//! P1 — Query tests.
//!
//! Validates filter effectiveness, fuzzy matching, streaming UDS queries,
//! UDS permission checks, and short query optimization.

#[allow(dead_code)]
mod common;

use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, SystemTime};

use common::unique_tmp_dir;
use fd_rdd::config::ContentIndexConfig;
use fd_rdd::core::{FileKey, FileKind, FileMeta};
use fd_rdd::diagnostics::{DiagnosticReport, DiagnosticSource};
use fd_rdd::index::{IndexBuilder, TieredIndex};
use fd_rdd::query::{execute_query, QueryMode, SortColumn, SortOrder};

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
            kind: Default::default(),
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

fn file_meta_from_path(path: PathBuf) -> FileMeta {
    let metadata = std::fs::metadata(&path).unwrap();
    FileMeta {
        file_key: FileKey::from_path_and_metadata(&path, &metadata).unwrap(),
        path,
        size: metadata.len(),
        mtime: metadata.modified().ok(),
        ctime: metadata.created().ok(),
        atime: metadata.accessed().ok(),
        kind: FileKind::from_metadata(&metadata),
    }
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
    assert_eq!(
        results.len(),
        2,
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
            common::test_meta(1, upper.clone(), 10),
            common::test_meta(2, lower.clone(), 10),
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
            common::test_meta(1, basename_hit.clone(), 10),
            common::test_meta(2, path_hit.clone(), 10),
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
            common::test_meta(1, dot_env.clone(), 10),
            common::test_meta(2, path_segment_hit.clone(), 10),
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
            common::test_meta(1, src_hit.clone(), 10),
            common::test_meta(2, node_hit.clone(), 10),
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
            common::test_meta(1, short.clone(), 10),
            common::test_meta(2, long.clone(), 10),
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
        l2.upsert(common::test_meta(i + 1, path, 100));
    }

    // Query that matches all files
    let results = index.query_limit("data", 10_000);
    assert_eq!(results.len(), 10_000, "Should return all 10,000 data files");

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
    assert_eq!(
        results.len(),
        3,
        "Single char 'a' should match a.txt, ab.txt, abc.txt; got {}",
        results.len()
    );

    // Two character query
    let results = index.query("ab");
    assert_eq!(
        results.len(),
        2,
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
        common::test_meta(1, shallow.clone(), 10),
        common::test_meta(2, deep.clone(), 20),
        common::test_meta(3, short_name.clone(), 30),
        common::test_meta(4, long_name.clone(), 40),
        common::test_meta(5, type_file.clone(), 50),
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
fn full_build_indexes_directory_entries_for_type_filters() {
    let root = unique_tmp_dir("query-full-build-type-dir");
    let dir = root.join("dirprobe");
    let file = root.join("fileprobe.txt");
    std::fs::create_dir_all(&dir).unwrap();
    std::fs::write(&file, "probe").unwrap();

    let index = Arc::new(TieredIndex::empty(vec![root.clone()]));
    let l2 = index.l2.load_full();
    IndexBuilder::new(vec![root.clone()]).full_build(l2.as_ref());
    index.refresh_base();

    let dirs = index.query("type:dir dirprobe");
    assert_eq!(dirs.len(), 1, "type:dir should match the scanned directory");
    assert_eq!(dirs[0].path, dir);
    assert_eq!(dirs[0].kind, FileKind::Directory);

    let files = index.query("type:file fileprobe");
    assert_eq!(files.len(), 1, "type:file should match the scanned file");
    assert_eq!(files[0].path, file);
    assert_eq!(files[0].kind, FileKind::File);

    let wrong_kind = index.query("type:file dirprobe");
    assert!(
        wrong_kind.iter().all(|meta| meta.path != dir),
        "type:file must not match scanned directories"
    );

    let _ = std::fs::remove_dir_all(&root);
}

#[test]
fn empty_filter_matches_only_real_empty_directories() {
    let root = unique_tmp_dir("query-empty-dir");
    let empty_dir = root.join("emptyprobe");
    let file_child_dir = root.join("filechildprobe");
    let hidden_child_dir = root.join("hiddenchildprobe");
    std::fs::create_dir_all(&empty_dir).unwrap();
    std::fs::create_dir_all(&file_child_dir).unwrap();
    std::fs::create_dir_all(&hidden_child_dir).unwrap();
    std::fs::write(file_child_dir.join("child.txt"), b"child").unwrap();
    std::fs::write(hidden_child_dir.join(".hidden"), b"hidden").unwrap();

    let index = Arc::new(TieredIndex::empty(vec![root.clone()]));
    let l2 = index.l2.load_full();
    IndexBuilder::new(vec![root.clone()]).full_build(l2.as_ref());
    index.refresh_base();

    let empty_results = index.query("type:dir empty: emptyprobe");
    assert_eq!(empty_results.len(), 1);
    assert_eq!(empty_results[0].path, empty_dir);
    assert_eq!(empty_results[0].kind, FileKind::Directory);

    let nonempty_results = index.query("type:dir empty: filechildprobe");
    assert!(
        nonempty_results
            .iter()
            .all(|meta| meta.path != file_child_dir),
        "empty: must reject directories with indexed children"
    );

    let hidden_results = index.query("type:dir empty: hiddenchildprobe");
    assert!(
        hidden_results.iter().all(|meta| meta.path != hidden_child_dir),
        "empty: must reject directories with real hidden children even when hidden entries are not indexed"
    );

    let _ = std::fs::remove_dir_all(&root);
}

#[test]
fn dupe_filter_returns_hardlink_aliases_with_reason() {
    let root = unique_tmp_dir("query-dupe-hardlink");
    std::fs::create_dir_all(&root).unwrap();
    let original = root.join("dupe_original.txt");
    let alias = root.join("dupe_alias.txt");
    let copy = root.join("dupe_copy.txt");
    std::fs::write(&original, b"same").unwrap();
    std::fs::hard_link(&original, &alias).unwrap();
    std::fs::write(&copy, b"same").unwrap();

    let index = Arc::new(TieredIndex::empty(vec![root.clone()]));
    let l2 = index.l2.load_full();
    IndexBuilder::new(vec![root.clone()]).full_build(l2.as_ref());
    index.refresh_base();

    let results = index.query_limit_detailed("dupe: dupe_", 10);
    let paths = results
        .iter()
        .map(|result| result.meta.path.clone())
        .collect::<Vec<_>>();
    assert!(paths.contains(&original));
    assert!(paths.contains(&alias));
    assert!(!paths.contains(&copy));
    assert!(
        results.iter().all(|result| {
            result.reason.as_deref() == Some("hardlink_same_file_key")
                && result.confidence == Some(1.0)
        }),
        "dupe: should annotate reason/confidence: {results:?}"
    );

    let copy_results = index.query("dupe: dupe_copy");
    assert!(
        copy_results.is_empty(),
        "dupe: must reject same-content regular copies"
    );

    let _ = std::fs::remove_dir_all(&root);
}

#[test]
fn dupe_content_filter_returns_same_content_copies_with_reason() {
    let root = unique_tmp_dir("query-dupe-content");
    std::fs::create_dir_all(&root).unwrap();
    let copy_a = root.join("content_dupe_a.txt");
    let copy_b = root.join("content_dupe_b.txt");
    let same_size_diff = root.join("content_dupe_same_size_diff.txt");
    let unique = root.join("content_dupe_unique.txt");
    std::fs::write(&copy_a, b"shared duplicate payload").unwrap();
    std::fs::write(&copy_b, b"shared duplicate payload").unwrap();
    std::fs::write(&same_size_diff, b"different duplicate data").unwrap();
    std::fs::write(&unique, b"unique").unwrap();

    let index = Arc::new(TieredIndex::empty(vec![root.clone()]));
    let l2 = index.l2.load_full();
    IndexBuilder::new(vec![root.clone()]).full_build(l2.as_ref());
    index.refresh_base();

    let results = index.query_limit_detailed("dupe:content content_dupe", 10);
    let paths = results
        .iter()
        .map(|result| result.meta.path.clone())
        .collect::<Vec<_>>();
    assert!(paths.contains(&copy_a));
    assert!(paths.contains(&copy_b));
    assert!(!paths.contains(&same_size_diff));
    assert!(!paths.contains(&unique));
    assert!(
        results.iter().all(|result| {
            result.reason.as_deref() == Some("content_hash_match")
                && result.confidence == Some(0.99)
        }),
        "dupe:content should annotate content hash reason/confidence: {results:?}"
    );

    let mut report = DiagnosticReport::default();
    index.collect(&mut report);
    assert_eq!(report.storage.content_hash_confirmed_groups, 1);
    assert!(report.storage.content_hash_candidate_count >= 3);
    assert_eq!(report.storage.content_hash_queue_pending, 0);

    let hardlink_results = index.query("dupe: content_dupe");
    assert!(
        hardlink_results.is_empty(),
        "plain dupe: must keep hardlink semantics and not match same-content copies"
    );

    let _ = std::fs::remove_dir_all(&root);
}

#[test]
fn dupe_content_filter_skips_excluded_and_oversized_candidates() {
    let root = unique_tmp_dir("query-dupe-content-policy");
    let excluded_dir = root.join("node_modules");
    std::fs::create_dir_all(&excluded_dir).unwrap();

    let allowed_a = root.join("content_policy_allowed_a.txt");
    let allowed_b = root.join("content_policy_allowed_b.txt");
    let excluded_a = excluded_dir.join("content_policy_excluded_a.txt");
    let excluded_b = excluded_dir.join("content_policy_excluded_b.txt");
    let oversized_a = root.join("content_policy_oversized_a.txt");
    let oversized_b = root.join("content_policy_oversized_b.txt");
    std::fs::write(&allowed_a, b"shared-policy").unwrap();
    std::fs::write(&allowed_b, b"shared-policy").unwrap();
    std::fs::write(&excluded_a, b"excluded-policy").unwrap();
    std::fs::write(&excluded_b, b"excluded-policy").unwrap();
    std::fs::write(&oversized_a, vec![b'x'; 64]).unwrap();
    std::fs::write(&oversized_b, vec![b'x'; 64]).unwrap();

    let index = Arc::new(TieredIndex::empty_with_options_follow_and_excludes(
        vec![root.clone()],
        false,
        true,
        false,
        vec!["node_modules".to_string()],
    ));
    index.apply_content_index_config(ContentIndexConfig {
        enable: false,
        max_file_size: 32,
        include_ext: Vec::new(),
        exclude_ext: Vec::new(),
    });
    let l2 = index.l2.load_full();
    for path in [
        allowed_a.clone(),
        allowed_b.clone(),
        excluded_a.clone(),
        excluded_b.clone(),
        oversized_a.clone(),
        oversized_b.clone(),
    ] {
        l2.upsert(file_meta_from_path(path));
    }
    index.refresh_base();

    let results = index.query_limit_detailed("dupe:content content_policy", 20);
    let paths = results
        .iter()
        .map(|result| result.meta.path.clone())
        .collect::<Vec<_>>();
    assert_eq!(paths.len(), 2, "only allowed duplicate files should match");
    assert!(paths.contains(&allowed_a));
    assert!(paths.contains(&allowed_b));
    assert!(!paths.contains(&excluded_a));
    assert!(!paths.contains(&excluded_b));
    assert!(!paths.contains(&oversized_a));
    assert!(!paths.contains(&oversized_b));

    let mut report = DiagnosticReport::default();
    index.collect(&mut report);
    assert_eq!(report.storage.content_hash_confirmed_groups, 1);
    assert!(
        report.storage.content_hash_skipped_count >= 4,
        "excluded and oversized indexed candidates must be diagnosed as skipped"
    );
    assert!(!report.storage.content_hash_last_skip_reason.is_empty());

    let _ = std::fs::remove_dir_all(&root);
}

#[test]
fn execute_query_sorts_by_modified_time() {
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
            kind: Default::default(),
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
            kind: Default::default(),
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
            kind: Default::default(),
        },
    ];
    let index = build_index_with_metas(&root, &metas);

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
