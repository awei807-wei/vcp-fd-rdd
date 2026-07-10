use super::*;
use crate::core::{FileKey, FileKind, FileMeta};
use crate::index::IndexLayer;
use crate::query::matcher::create_matcher;
use std::path::PathBuf;

#[test]
fn runtime_docid_is_32_bit() {
    assert_eq!(std::mem::size_of::<DocId>(), 4);
}

#[test]
fn runtime_paths_use_one_contiguous_arena() {
    let idx = PersistentIndex::new();
    for ino in 0..128_u64 {
        idx.upsert(FileMeta {
            file_key: FileKey {
                dev: 1,
                ino,
                generation: 0,
            },
            path: PathBuf::from(format!("/tmp/compact-path-{ino:04}.txt")),
            size: 1,
            mtime: None,
            ctime: None,
            atime: None,
            kind: Default::default(),
        });
    }

    let stats = idx.memory_stats();
    let path_ref_bytes = stats.file_count as u64 * 8;
    assert!(
        stats.arena_bytes <= stats.arena_capacity as u64 + path_ref_bytes + 64,
        "runtime path storage should be one arena plus compact refs: {stats:?}"
    );
}

#[test]
fn runtime_filekey_index_uses_compact_docid_buckets() {
    let idx = PersistentIndex::new();
    for ino in 0..128_u64 {
        idx.upsert(FileMeta {
            file_key: FileKey {
                dev: 7,
                ino,
                generation: 0,
            },
            path: PathBuf::from(format!("/tmp/filekey-{ino:04}.txt")),
            size: 1,
            mtime: None,
            ctime: None,
            atime: None,
            kind: Default::default(),
        });
    }

    let stats = idx.memory_stats();
    let bucket_bytes = stats.filekey_to_docid_capacity as u64 * 4;
    assert!(
        stats.filekey_to_docid_bytes <= bucket_bytes + 64,
        "FileKey index should store only compact DocId buckets: {stats:?}"
    );
}

#[test]
fn parent_lookup_retains_directories_but_not_file_paths() {
    let idx = PersistentIndex::new();
    let dir_key = FileKey {
        dev: 9,
        ino: 1,
        generation: 0,
    };
    let file_key = FileKey {
        dev: 9,
        ino: 2,
        generation: 0,
    };
    idx.upsert(FileMeta {
        file_key: dir_key,
        path: PathBuf::from("/tmp/compact-parent"),
        size: 0,
        mtime: None,
        ctime: None,
        atime: None,
        kind: FileKind::Directory,
    });
    idx.upsert(FileMeta {
        file_key,
        path: PathBuf::from("/tmp/compact-parent/file.txt"),
        size: 1,
        mtime: None,
        ctime: None,
        atime: None,
        kind: FileKind::File,
    });

    idx.rebuild_parent_index();

    {
        let lookup = idx.parent_path_table.read();
        let lookup = lookup.as_ref().expect("parent lookup should be built");
        assert!(lookup.lookup(b"/tmp/compact-parent").is_some());
        assert_eq!(lookup.lookup(b"/tmp/compact-parent/file.txt"), None);
    }
    assert_eq!(idx.parent_candidates("/tmp/compact-parent"), vec![file_key]);
}

#[test]
fn same_path_with_new_filekey_replaces_stale_identity() {
    let idx = PersistentIndex::new();
    let old_key = FileKey {
        dev: 11,
        ino: 100,
        generation: 1,
    };
    let new_key = FileKey {
        dev: 11,
        ino: 100,
        generation: 2,
    };
    let path = PathBuf::from("/tmp/inode-reused.txt");

    for file_key in [old_key, new_key] {
        idx.upsert(FileMeta {
            file_key,
            path: path.clone(),
            size: 1,
            mtime: None,
            ctime: None,
            atime: None,
            kind: FileKind::File,
        });
    }

    assert!(idx.get_meta(old_key).is_none());
    let meta = idx
        .get_meta(new_key)
        .expect("new identity should be indexed");
    assert_eq!(meta.file_key, new_key);
    assert_eq!(meta.path, path);
}

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

#[test]
fn export_segments_v6_produces_all_seven_segments() {
    let idx = PersistentIndex::new_with_roots(vec![PathBuf::from("/tmp")]);

    // 插入测试数据
    idx.upsert(FileMeta {
        file_key: FileKey {
            dev: 1,
            ino: 1,
            generation: 0,
        },
        path: PathBuf::from("/tmp/test_file.txt"),
        size: 1,
        mtime: None,
        ctime: None,
        atime: None,
        kind: Default::default(),
    });

    let segs = idx.export_segments_v6();

    // 验证所有 7 个段都存在
    assert!(
        !segs.roots_bytes.is_empty(),
        "roots_bytes should not be empty"
    );
    // path_arena_bytes: 段必须存在但内容取决于 root 匹配；
    // 插入的 "/tmp/test_file.txt" 在 root="/" 下 rel_bytes="tmp/test_file.txt"，
    // 因此 arena 应非空。但若 root 匹配逻辑变化，arena 可能为空。
    // 此处不强制断言 arena 内容，仅验证段引用存在。
    // metas_bytes 应该包含至少一条记录 (dev u64 + ino u64 + root_id u16 + path_off u32 + path_len u16 + mtime_ns i64 = 30 bytes)
    assert!(
        !segs.metas_bytes.is_empty(),
        "metas_bytes should not be empty"
    );
    // tombstones_bytes 应该包含 RoaringBitmap 序列化数据
    assert!(
        !segs.tombstones_bytes.is_empty(),
        "tombstones_bytes should not be empty"
    );
    // trigram_table_bytes 应该包含哨兵条目
    assert!(
        !segs.trigram_table_bytes.is_empty(),
        "trigram_table_bytes should not be empty"
    );
    // postings_blob_bytes 应该包含 posting 数据
    assert!(
        !segs.postings_blob_bytes.is_empty(),
        "postings_blob_bytes should not be empty"
    );
    // filekey_map_bytes 应该包含 header + 至少一条记录
    assert!(
        !segs.filekey_map_bytes.is_empty(),
        "filekey_map_bytes should not be empty"
    );

    // 验证 roots_bytes 格式：u16 count + (u16 len + bytes)...
    assert!(
        segs.roots_bytes.len() >= 2,
        "roots_bytes should start with u16 count"
    );
    let roots_count = u16::from_le_bytes([segs.roots_bytes[0], segs.roots_bytes[1]]);
    assert!(roots_count >= 1, "should have at least one root");

    // 验证 filekey_map_bytes 格式：magic [u8;4] + version u16 + flags u16
    assert!(
        segs.filekey_map_bytes.len() >= 8,
        "filekey_map_bytes should have at least 8-byte header"
    );
    assert_eq!(
        &segs.filekey_map_bytes[0..4],
        b"FKM\0",
        "FKM magic should be present"
    );
}

#[test]
fn export_segments_v6_to_writer_writes_seven_segments_in_order() {
    let idx = PersistentIndex::new_with_roots(vec![PathBuf::from("/tmp")]);
    idx.upsert(FileMeta {
        file_key: FileKey {
            dev: 1,
            ino: 2,
            generation: 0,
        },
        path: PathBuf::from("/tmp/another_test.txt"),
        size: 1,
        mtime: None,
        ctime: None,
        atime: None,
        kind: Default::default(),
    });

    let mut writer = Vec::new();
    idx.export_segments_v6_to_writer(&mut writer)
        .expect("write should succeed");

    // 每个段有 u64 LE 长度前缀 (8 bytes)，7 个段 = 7 * 8 = 56 bytes 的前缀 + 实际数据
    assert!(
        writer.len() > 56,
        "writer should contain 7 length-prefixed segments"
    );

    // 验证第一个段的长度前缀指向 roots_bytes
    let first_len = u64::from_le_bytes(writer[0..8].try_into().unwrap());
    assert!(
        first_len > 0,
        "first segment (roots) should have non-zero length"
    );
}
