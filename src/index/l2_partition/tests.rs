use super::*;
use crate::query::matcher::create_matcher;

#[test]
fn roaring_posting_basic_query() {
    let idx = PersistentIndex::new();
    idx.upsert(FileMeta {
        file_key: FileKey {
            dev: 1,
            ino: 1,
            generation: 0,
        },
        path: PathBuf::from("/tmp/alpha_test.txt"),
        size: 1,
        mtime: None,
        ctime: None,
        atime: None,
        kind: Default::default(),
    });
    idx.upsert(FileMeta {
        file_key: FileKey {
            dev: 1,
            ino: 2,
            generation: 0,
        },
        path: PathBuf::from("/tmp/beta_test.txt"),
        size: 1,
        mtime: None,
        ctime: None,
        atime: None,
        kind: Default::default(),
    });

    let m = create_matcher("alpha", true);
    let r = idx.query(m.as_ref(), 100);
    assert_eq!(r.len(), 1);
    assert!(r[0].path.to_string_lossy().contains("alpha_test"));
}

#[test]
fn short_literal_query_uses_short_component_candidates() {
    let idx = PersistentIndex::new();
    idx.upsert(FileMeta {
        file_key: FileKey {
            dev: 1,
            ino: 1,
            generation: 0,
        },
        path: PathBuf::from("/tmp/ab"),
        size: 1,
        mtime: None,
        ctime: None,
        atime: None,
        kind: Default::default(),
    });
    idx.upsert(FileMeta {
        file_key: FileKey {
            dev: 1,
            ino: 2,
            generation: 0,
        },
        path: PathBuf::from("/tmp/cabd.txt"),
        size: 1,
        mtime: None,
        ctime: None,
        atime: None,
        kind: Default::default(),
    });

    let m = create_matcher("ab", true);
    let r = idx.query(m.as_ref(), 100);
    assert_eq!(r.len(), 1);

    let m = create_matcher("a", true);
    let r = idx.query(m.as_ref(), 100);
    assert_eq!(r.len(), 1);
}

#[test]
fn overlong_new_paths_are_indexed_in_runtime_paths_store() {
    let idx = PersistentIndex::new();
    let path = PathBuf::from(format!("/tmp/{}", "a".repeat(u16::MAX as usize + 1)));
    idx.upsert(FileMeta {
        file_key: FileKey {
            dev: 1,
            ino: 1,
            generation: 0,
        },
        path: path.clone(),
        size: 1,
        mtime: None,
        ctime: None,
        atime: None,
        kind: Default::default(),
    });

    assert_eq!(idx.file_count(), 1);
    let m = create_matcher("aaaa", true);
    let results = idx.query(m.as_ref(), 100);
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].path, path);
}

#[test]
fn rename_to_overlong_path_keeps_entry_live() {
    let idx = PersistentIndex::new();
    idx.upsert(FileMeta {
        file_key: FileKey {
            dev: 1,
            ino: 1,
            generation: 0,
        },
        path: PathBuf::from("/tmp/short-name.txt"),
        size: 1,
        mtime: None,
        ctime: None,
        atime: None,
        kind: Default::default(),
    });

    let long_path = PathBuf::from(format!("/tmp/{}", "b".repeat(u16::MAX as usize + 1)));
    idx.upsert_rename(FileMeta {
        file_key: FileKey {
            dev: 1,
            ino: 1,
            generation: 0,
        },
        path: long_path.clone(),
        size: 2,
        mtime: None,
        ctime: None,
        atime: None,
        kind: Default::default(),
    });

    assert_eq!(idx.file_count(), 1);
    let m = create_matcher("short-name", true);
    assert!(idx.query(m.as_ref(), 100).is_empty());
    let m = create_matcher("bbbb", true);
    let results = idx.query(m.as_ref(), 100);
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].path, long_path);
}

#[test]
fn same_filekey_updates_to_new_path_when_old_path_is_missing() {
    let root = std::env::temp_dir().join(format!(
        "fd-rdd-reconcile-{}",
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    let old_project = root.join("old_project");
    let old_dir = old_project.join("node_modules/libA/dist");
    std::fs::create_dir_all(&old_dir).unwrap();

    let old_path = old_dir.join("bundle_1.js");
    std::fs::write(&old_path, b"bundle").unwrap();

    let idx = PersistentIndex::new_with_roots(vec![root.clone()]);
    let file_key = FileKey {
        dev: 1,
        ino: 42,
        generation: 0,
    };

    idx.upsert(FileMeta {
        file_key,
        path: old_path.clone(),
        size: 6,
        mtime: None,
        ctime: None,
        atime: None,
        kind: Default::default(),
    });

    let new_project = root.join("new_project");
    std::fs::rename(&old_project, &new_project).unwrap();
    let new_path = new_project.join("node_modules/libA/dist/bundle_1.js");

    idx.upsert(FileMeta {
        file_key,
        path: new_path.clone(),
        size: 6,
        mtime: None,
        ctime: None,
        atime: None,
        kind: Default::default(),
    });

    let meta = idx.get_meta(file_key).expect("file should remain indexed");
    assert_eq!(meta.path, new_path);

    let matcher = create_matcher("bundle_1", false);
    let results = idx.query(matcher.as_ref(), 10);
    assert!(
        results.iter().any(|m| m.path == new_path),
        "new path should be queryable after reconcile: {results:?}"
    );
    assert!(
        results.iter().all(|m| m.path != old_path),
        "old path should be removed after reconcile: {results:?}"
    );

    let _ = std::fs::remove_dir_all(&root);
}

#[test]
fn chinese_exact_query_via_trigram() {
    let idx = PersistentIndex::new();
    idx.upsert(FileMeta {
        file_key: FileKey {
            dev: 1,
            ino: 1,
            generation: 0,
        },
        path: PathBuf::from("/tmp/中文文件.txt"),
        size: 1,
        mtime: None,
        ctime: None,
        atime: None,
        kind: Default::default(),
    });

    let m = create_matcher("中文", true);
    let r = idx.query(m.as_ref(), 100);
    assert_eq!(r.len(), 1, "expected 1 result for '中文', got {}", r.len());
    assert!(r[0].path.to_string_lossy().contains("中文文件"));

    let m2 = create_matcher("文件", true);
    let r2 = idx.query(m2.as_ref(), 100);
    assert_eq!(
        r2.len(),
        1,
        "expected 1 result for '文件', got {}",
        r2.len()
    );
}
