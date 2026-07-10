use std::path::PathBuf;

use crate::core::{FileKey, FileKind, FileMeta};
use crate::query::ExactMatcher;

use super::*;

fn tmp_path(tag: &str) -> PathBuf {
    let nonce = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    std::env::temp_dir().join(format!("fd-rdd-owned-v7-{tag}-{nonce}"))
}

fn meta(path: impl Into<PathBuf>, ino: u64) -> FileMeta {
    FileMeta {
        file_key: FileKey {
            dev: 7,
            ino,
            generation: 0,
        },
        path: path.into(),
        size: 0,
        mtime: None,
        ctime: None,
        atime: None,
        kind: FileKind::File,
    }
}

#[test]
fn owned_writer_compacts_generation_and_uses_bounded_external_runs() {
    let path = tmp_path("roundtrip");
    let stale_workspace = path.parent().unwrap().join(format!(
        ".{}.owned-v7-{}-0",
        path.file_name().unwrap().to_string_lossy(),
        u32::MAX,
    ));
    std::fs::create_dir(&stale_workspace).unwrap();
    std::fs::write(stale_workspace.join("partial.run"), b"partial").unwrap();
    let active_workspace = path.parent().unwrap().join(format!(
        ".{}.owned-v7-{}-1",
        path.file_name().unwrap().to_string_lossy(),
        std::process::id(),
    ));
    std::fs::create_dir(&active_workspace).unwrap();
    std::fs::write(active_workspace.join("active.run"), b"active").unwrap();
    let index = PersistentIndex::new_with_roots(vec![PathBuf::from("/tmp/owned")]);
    index.upsert_path_alias(meta("/tmp/owned/keep.txt", 1));
    index.upsert_path_alias(meta("/tmp/owned/delete/sub/old.txt", 2));
    let hardlink_key = FileKey {
        dev: 7,
        ino: 3,
        generation: 0,
    };
    index.upsert_path_alias(FileMeta {
        file_key: hardlink_key,
        ..meta("/tmp/owned/links/p1", 3)
    });
    index.upsert_path_alias(FileMeta {
        file_key: hardlink_key,
        ..meta("/tmp/owned/links/p2", 3)
    });
    let old_key = FileKey {
        dev: 7,
        ino: 4,
        generation: 0,
    };
    index.upsert_path_alias(FileMeta {
        file_key: old_key,
        ..meta("/tmp/owned/replace.txt", 4)
    });
    for ordinal in 0..2_000u64 {
        index.upsert_path_alias(meta(
            format!(
                "/tmp/owned/bulk/partition-{}/long-file-name-{ordinal:06}.data",
                ordinal % 17
            ),
            10_000 + ordinal,
        ));
    }

    let new_key = FileKey {
        dev: 7,
        ino: 404,
        generation: 1,
    };
    let overlay = ColdDeltaPlan::new(
        vec![b"/tmp/owned/delete".to_vec()],
        vec![
            FileMeta {
                file_key: new_key,
                ..meta("/tmp/owned/replace.txt", 404)
            },
            meta("/tmp/owned/new/deep/needle.txt", 5),
            FileMeta {
                file_key: hardlink_key,
                ..meta("/tmp/owned/links/p3", 3)
            },
        ],
    );
    let report = write_v7_owned_index_atomic(
        &path,
        index,
        overlay,
        OwnedV7WriteOptions {
            sort_run_bytes: MIN_SORT_RUN_BYTES,
        },
    )
    .unwrap();

    assert_eq!(report.raw_entries, 2_007);
    assert_eq!(report.live_entries, 2_006);
    assert!(report.path_runs > 1);
    assert!(report.trigram_runs > 1);
    assert!(
        report.peak_sort_buffer_bytes
            <= report
                .sort_run_limit_bytes
                .saturating_mul(2)
                .saturating_add(report.largest_sort_record_bytes)
    );

    let loaded = load_v7_from_path(&path).unwrap().unwrap();
    assert!(!stale_workspace.exists());
    assert!(active_workspace.exists());
    loaded.ensure_direct_delta_compatible().unwrap();
    assert!(loaded.get_meta(old_key).unwrap().is_none());
    assert_eq!(loaded.get_meta(new_key).unwrap().unwrap().file_key, new_key);
    assert!(loaded
        .query_keys(&ExactMatcher::new("old.txt", false))
        .unwrap()
        .is_empty());
    assert_eq!(
        loaded
            .query_keys(&ExactMatcher::new("needle", false))
            .unwrap()
            .len(),
        1
    );
    assert_eq!(
        loaded
            .query_metas(&ExactMatcher::new("/tmp/owned/links/p", false))
            .unwrap()
            .len(),
        3
    );
    assert_eq!(loaded.parent_metas("/tmp/owned/new/deep").unwrap().len(), 1);
    assert!(loaded.tombstones().unwrap().is_empty());

    let _ = std::fs::remove_file(path);
    let _ = std::fs::remove_dir_all(active_workspace);
}
