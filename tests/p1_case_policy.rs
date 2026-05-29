//! P1 — Unicode case-fold lookup behavior.

use fd_rdd::core::{FileKey, FileMeta};
use fd_rdd::index::l2_partition::PersistentIndex;
use fd_rdd::query::matcher::create_matcher;
use std::path::PathBuf;

#[test]
fn unicode_casefold_query_handles_sharp_s_length_jump() {
    let idx = PersistentIndex::new();
    idx.upsert(FileMeta {
        file_key: FileKey {
            dev: 1,
            ino: 1,
            generation: 0,
        },
        path: PathBuf::from("/tmp/Straße-Bericht.txt"),
        size: 1,
        mtime: None,
        ctime: None,
        atime: None,
        kind: Default::default(),
    });

    let matcher = create_matcher("strasse", false);
    let results = idx.query(matcher.as_ref(), 10);
    assert_eq!(results.len(), 1);
    assert!(results[0].path.to_string_lossy().contains("Straße"));
}

#[test]
fn case_sensitive_query_does_not_merge_distinct_sensitive_paths() {
    let idx = PersistentIndex::new();
    idx.upsert(FileMeta {
        file_key: FileKey {
            dev: 1,
            ino: 10,
            generation: 0,
        },
        path: PathBuf::from("/tmp/Test.txt"),
        size: 1,
        mtime: None,
        ctime: None,
        atime: None,
        kind: Default::default(),
    });
    idx.upsert(FileMeta {
        file_key: FileKey {
            dev: 1,
            ino: 11,
            generation: 0,
        },
        path: PathBuf::from("/tmp/test.txt"),
        size: 1,
        mtime: None,
        ctime: None,
        atime: None,
        kind: Default::default(),
    });

    let matcher = create_matcher("Test.txt", true);
    let results = idx.query(matcher.as_ref(), 10);
    assert_eq!(results.len(), 1);
    assert!(results[0].path.ends_with("Test.txt"));
}
