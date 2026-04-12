//! P1 — Edge case tests.
//!
//! Validates handling of extreme inputs: very long paths, large directories.

use std::path::PathBuf;

use fd_rdd::core::{BuildRDD, FileMeta, FsScanRDD};

fn unique_tmp_dir(tag: &str) -> PathBuf {
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    std::env::temp_dir().join(format!("fd-rdd-edge-{}-{}", tag, nanos))
}

/// 9. 超长路径容错：创建深层嵌套目录，确保扫描不 panic
#[test]
fn very_long_path_does_not_panic() {
    let root = unique_tmp_dir("long-path");
    std::fs::create_dir_all(&root).unwrap();

    // Create a deeply nested directory (each segment ~200 chars, ~20 levels ≈ 4000 chars)
    let mut current = root.clone();
    let segment = "a".repeat(200);
    for _ in 0..20 {
        current = current.join(&segment);
        if std::fs::create_dir_all(&current).is_err() {
            // Some filesystems have path length limits; that's fine
            break;
        }
    }

    // Try to create a file at the deepest level
    let _ = std::fs::write(current.join("deep.txt"), b"deep");

    // Scan should not panic
    let rdd = FsScanRDD::from_roots(vec![root.clone()]);
    let mut count = 0usize;
    rdd.for_each(|_meta: FileMeta| count += 1);

    // We don't assert exact count since filesystem limits vary
    // The key assertion is that we didn't panic

    let _ = std::fs::remove_dir_all(&root);
}

/// 10. 大目录扫描（十万级文件）— 标记 `#[ignore]`
#[test]
#[ignore]
fn large_directory_scan_100k_files() {
    let root = unique_tmp_dir("large-dir");
    std::fs::create_dir_all(&root).unwrap();

    // Create 100,000 files
    for i in 0..100_000u32 {
        let name = format!("file_{:06}.txt", i);
        std::fs::write(root.join(&name), format!("content {}", i).as_bytes()).unwrap();
    }

    let rdd = FsScanRDD::from_roots(vec![root.clone()]);
    let mut count = 0usize;
    rdd.for_each(|_meta: FileMeta| count += 1);

    assert_eq!(count, 100_000, "Should have scanned all 100k files");

    let _ = std::fs::remove_dir_all(&root);
}
