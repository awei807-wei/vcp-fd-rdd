//! P1 — Ignore rules integration tests.
//!
//! Validates that .gitignore / .ignore rules are applied consistently across
//! cold scan, incremental scan, and event filtering.

use std::path::PathBuf;

use fd_rdd::core::{BuildRDD, FileMeta, FsScanRDD};

fn unique_tmp_dir(tag: &str) -> PathBuf {
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    std::env::temp_dir().join(format!("fd-rdd-ignore-{}-{}", tag, nanos))
}

/// 6. ignore 规则贯通：冷扫时 .gitignore 规则生效
#[test]
fn gitignore_rules_applied_during_cold_scan() {
    let root = unique_tmp_dir("gitignore");
    std::fs::create_dir_all(&root).unwrap();

    // Initialize a git repo (needed for .gitignore to be recognized by the `ignore` crate)
    std::fs::create_dir_all(root.join(".git")).unwrap();

    // Create .gitignore
    std::fs::write(root.join(".gitignore"), "*.log\nbuild/\n").unwrap();

    // Create files
    std::fs::write(root.join("main.rs"), b"fn main() {}").unwrap();
    std::fs::write(root.join("debug.log"), b"log data").unwrap();
    std::fs::create_dir_all(root.join("build")).unwrap();
    std::fs::write(root.join("build").join("output.o"), b"binary").unwrap();

    let rdd = FsScanRDD::from_roots(vec![root.clone()])
        .with_hidden(true) // include hidden to see .gitignore itself
        .with_ignore_rules(true);
    let mut seen: Vec<PathBuf> = Vec::new();
    rdd.for_each(|meta: FileMeta| seen.push(meta.path));

    // Should see main.rs and .gitignore
    assert!(
        seen.iter().any(|p| p.ends_with("main.rs")),
        "main.rs should be indexed"
    );

    // Should NOT see debug.log (matched by *.log)
    assert!(
        !seen.iter().any(|p| p.ends_with("debug.log")),
        "debug.log should be ignored by .gitignore"
    );

    // Should NOT see build/output.o (matched by build/)
    assert!(
        !seen.iter().any(|p| p.ends_with("output.o")),
        "build/output.o should be ignored by .gitignore"
    );

    let _ = std::fs::remove_dir_all(&root);
}

/// 6b. ignore 规则禁用时，被忽略的文件应该被扫描到
#[test]
fn ignore_rules_disabled_scans_everything() {
    let root = unique_tmp_dir("no-ignore");
    std::fs::create_dir_all(&root).unwrap();

    std::fs::create_dir_all(root.join(".git")).unwrap();
    std::fs::write(root.join(".gitignore"), "*.log\n").unwrap();
    std::fs::write(root.join("main.rs"), b"fn main() {}").unwrap();
    std::fs::write(root.join("debug.log"), b"log data").unwrap();

    let rdd = FsScanRDD::from_roots(vec![root.clone()])
        .with_hidden(true)
        .with_ignore_rules(false);
    let mut seen: Vec<PathBuf> = Vec::new();
    rdd.for_each(|meta: FileMeta| seen.push(meta.path));

    assert!(
        seen.iter().any(|p| p.ends_with("main.rs")),
        "main.rs should be indexed"
    );
    assert!(
        seen.iter().any(|p| p.ends_with("debug.log")),
        "debug.log should be indexed when ignore rules are disabled"
    );

    let _ = std::fs::remove_dir_all(&root);
}

/// 7. `--include-hidden` 开关：默认跳过隐藏文件，开启后正常扫描
#[test]
fn include_hidden_toggle() {
    let root = unique_tmp_dir("hidden-toggle");
    std::fs::create_dir_all(&root).unwrap();

    std::fs::write(root.join("visible.txt"), b"visible").unwrap();
    std::fs::write(root.join(".hidden.txt"), b"hidden").unwrap();

    // Default: hidden files skipped
    let rdd = FsScanRDD::from_roots(vec![root.clone()]);
    let mut seen: Vec<PathBuf> = Vec::new();
    rdd.for_each(|meta: FileMeta| seen.push(meta.path));
    assert!(seen.iter().any(|p| p.ends_with("visible.txt")));
    assert!(!seen.iter().any(|p| p.ends_with(".hidden.txt")));

    // With include_hidden: hidden files included
    let rdd = FsScanRDD::from_roots(vec![root.clone()]).with_hidden(true);
    let mut seen: Vec<PathBuf> = Vec::new();
    rdd.for_each(|meta: FileMeta| seen.push(meta.path));
    assert!(seen.iter().any(|p| p.ends_with("visible.txt")));
    assert!(seen.iter().any(|p| p.ends_with(".hidden.txt")));

    let _ = std::fs::remove_dir_all(&root);
}
