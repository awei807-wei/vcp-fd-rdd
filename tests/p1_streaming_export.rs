//! P1 — Streaming export byte-identical tests.
//!
//! Validates that `export_segments_v6_to_writer` produces byte-identical output
//! to `export_segments_v6`.

use std::path::PathBuf;

use fd_rdd::core::{FileKey, FileMeta};
use fd_rdd::index::PersistentIndex;

fn unique_tmp_dir(tag: &str) -> PathBuf {
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    std::env::temp_dir().join(format!("fd-rdd-streaming-{}-{}", tag, nanos))
}

#[test]
fn streaming_export_byte_identical() {
    let root = unique_tmp_dir("export");
    std::fs::create_dir_all(&root).unwrap();
    let idx = PersistentIndex::new_with_roots(vec![root.clone()]);

    // Insert a few FileMeta entries with varied paths
    for i in 0..5u64 {
        let meta = FileMeta {
            file_key: FileKey {
                dev: 1,
                ino: i + 1,
                generation: 0,
            },
            path: root.join(format!("file_{}.txt", i)),
            size: 100 + i,
            mtime: None,
            ctime: None,
            atime: None,
        };
        idx.upsert(meta);
    }

    // Get in-memory segments
    let segs = idx.export_segments_v6();

    // Stream to writer
    let mut writer_output = Vec::new();
    idx.export_segments_v6_to_writer(&mut writer_output)
        .expect("streaming export should succeed");

    // Build expected output by concatenating segs fields in the same order
    // as export_segments_v6_to_writer writes them, each prefixed with u64 LE length.
    let mut expected = Vec::new();
    for buf in [
        segs.roots_bytes.as_ref(),
        segs.path_arena_bytes.as_ref(),
        segs.metas_bytes.as_ref(),
        segs.tombstones_bytes.as_ref(),
        segs.trigram_table_bytes.as_ref(),
        segs.postings_blob_bytes.as_ref(),
        segs.filekey_map_bytes.as_ref(),
    ] {
        expected.extend_from_slice(&(buf.len() as u64).to_le_bytes());
        expected.extend_from_slice(buf);
    }

    assert_eq!(
        writer_output, expected,
        "streaming export must be byte-identical to in-memory segment concatenation"
    );
}
