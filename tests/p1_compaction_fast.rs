//! P1 — Fast compaction correctness tests.
//!
//! Validates that `export_segments_v6_compacted` produces equivalent output
//! to `export_segments_v6` for live metas (no tombstones).

use std::path::PathBuf;

use fd_rdd::core::{FileKey, FileMeta};
use fd_rdd::index::PersistentIndex;

fn unique_tmp_dir(tag: &str) -> PathBuf {
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    std::env::temp_dir().join(format!("fd-rdd-compaction-{}-{}", tag, nanos))
}

#[test]
fn fast_compaction_equivalence() {
    let root = unique_tmp_dir("fast");
    std::fs::create_dir_all(&root).unwrap();
    let idx = PersistentIndex::new_with_roots(vec![root.clone()]);

    // Populate index normally
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

    let segs_normal = idx.export_segments_v6();
    let segs_compacted = idx.export_segments_v6_compacted();

    // Field-by-field comparison
    assert_eq!(
        segs_normal.roots_bytes.as_ref(),
        segs_compacted.roots_bytes.as_ref(),
        "roots_bytes must match after compaction"
    );
    assert_eq!(
        segs_normal.path_arena_bytes.as_ref(),
        segs_compacted.path_arena_bytes.as_ref(),
        "path_arena_bytes must match after compaction"
    );
    assert_eq!(
        segs_normal.metas_bytes.as_ref(),
        segs_compacted.metas_bytes.as_ref(),
        "metas_bytes must match after compaction"
    );
    // NOTE: trigram_table_bytes and postings_blob_bytes may differ in binary layout
    // because postings blob offsets depend on HashMap iteration order, which is not
    // deterministic across different PersistentIndex instances. We verify logical
    // equivalence instead of byte-identical below.
    assert_trigram_equivalence(
        &segs_normal.trigram_table_bytes,
        &segs_normal.postings_blob_bytes,
        &segs_compacted.trigram_table_bytes,
        &segs_compacted.postings_blob_bytes,
    );
    assert_eq!(
        segs_normal.filekey_map_bytes.as_ref(),
        segs_compacted.filekey_map_bytes.as_ref(),
        "filekey_map_bytes must match after compaction"
    );

    // Compacted version should have no tombstones
    let compacted_tombstones =
        roaring::RoaringBitmap::deserialize_from(&segs_compacted.tombstones_bytes[..])
            .expect("valid roaring bitmap in compacted tombstones_bytes");
    assert!(
        compacted_tombstones.is_empty(),
        "compacted tombstones_bytes must represent an empty set"
    );
}

/// Parse trigram table + postings blob and compare logical equivalence.
fn assert_trigram_equivalence(tri_a: &[u8], blob_a: &[u8], tri_b: &[u8], blob_b: &[u8]) {
    assert_eq!(
        tri_a.len() % 12,
        0,
        "trigram_table_bytes length must be multiple of 12"
    );
    assert_eq!(
        tri_b.len() % 12,
        0,
        "trigram_table_bytes length must be multiple of 12"
    );

    let mut map_a = std::collections::HashMap::new();
    for chunk in tri_a.chunks_exact(12) {
        let tri = [chunk[0], chunk[1], chunk[2]];
        let off = u32::from_le_bytes([chunk[4], chunk[5], chunk[6], chunk[7]]);
        let len = u32::from_le_bytes([chunk[8], chunk[9], chunk[10], chunk[11]]);
        let posting_bytes = &blob_a[off as usize..(off + len) as usize];
        let bitmap = roaring::RoaringBitmap::deserialize_from(posting_bytes)
            .expect("valid roaring bitmap in postings_blob_a");
        map_a.insert(tri, bitmap);
    }

    let mut map_b = std::collections::HashMap::new();
    for chunk in tri_b.chunks_exact(12) {
        let tri = [chunk[0], chunk[1], chunk[2]];
        let off = u32::from_le_bytes([chunk[4], chunk[5], chunk[6], chunk[7]]);
        let len = u32::from_le_bytes([chunk[8], chunk[9], chunk[10], chunk[11]]);
        let posting_bytes = &blob_b[off as usize..(off + len) as usize];
        let bitmap = roaring::RoaringBitmap::deserialize_from(posting_bytes)
            .expect("valid roaring bitmap in postings_blob_b");
        map_b.insert(tri, bitmap);
    }

    assert_eq!(
        map_a, map_b,
        "trigram postings must be logically equivalent"
    );
}
