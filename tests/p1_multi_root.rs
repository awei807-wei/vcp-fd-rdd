//! P1 — Multi-root isolation tests.
//!
//! Validates that multiple `--root` arguments produce isolated index partitions.

use std::path::PathBuf;

use fd_rdd::core::{BuildRDD, FileMeta, FsScanRDD};

fn unique_tmp_dir(tag: &str) -> PathBuf {
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    std::env::temp_dir().join(format!("fd-rdd-multi-{}-{}", tag, nanos))
}

/// 8. 多 `--root` 参数隔离测试
#[test]
fn multi_root_scans_all_roots() {
    let root_a = unique_tmp_dir("root-a");
    let root_b = unique_tmp_dir("root-b");
    std::fs::create_dir_all(&root_a).unwrap();
    std::fs::create_dir_all(&root_b).unwrap();

    std::fs::write(root_a.join("a.txt"), b"a").unwrap();
    std::fs::write(root_b.join("b.txt"), b"b").unwrap();

    let rdd = FsScanRDD::from_roots(vec![root_a.clone(), root_b.clone()]);
    let mut seen: Vec<PathBuf> = Vec::new();
    rdd.for_each(|meta: FileMeta| seen.push(meta.path));

    assert!(
        seen.iter().any(|p| p.ends_with("a.txt")),
        "a.txt from root_a should be indexed"
    );
    assert!(
        seen.iter().any(|p| p.ends_with("b.txt")),
        "b.txt from root_b should be indexed"
    );

    let _ = std::fs::remove_dir_all(&root_a);
    let _ = std::fs::remove_dir_all(&root_b);
}

/// Multi-root: files from one root don't appear under the other root's path
#[test]
fn multi_root_files_have_correct_paths() {
    let root_a = unique_tmp_dir("path-a");
    let root_b = unique_tmp_dir("path-b");
    std::fs::create_dir_all(&root_a).unwrap();
    std::fs::create_dir_all(&root_b).unwrap();

    std::fs::write(root_a.join("only_a.txt"), b"a").unwrap();
    std::fs::write(root_b.join("only_b.txt"), b"b").unwrap();

    let rdd = FsScanRDD::from_roots(vec![root_a.clone(), root_b.clone()]);
    let mut seen: Vec<PathBuf> = Vec::new();
    rdd.for_each(|meta: FileMeta| seen.push(meta.path));

    // Verify paths are under the correct root
    for p in &seen {
        if p.ends_with("only_a.txt") {
            assert!(
                p.starts_with(&root_a),
                "only_a.txt should be under root_a"
            );
        }
        if p.ends_with("only_b.txt") {
            assert!(
                p.starts_with(&root_b),
                "only_b.txt should be under root_b"
            );
        }
    }

    let _ = std::fs::remove_dir_all(&root_a);
    let _ = std::fs::remove_dir_all(&root_b);
}
