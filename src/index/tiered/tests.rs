use super::*;
use std::sync::Arc;
use crate::core::{EventRecord, EventType, FileIdentifier};
use crate::event::recovery::DirtyScope;
use crate::stats::EventPipelineStats;
use crate::storage::snapshot::SnapshotStore;
use std::path::PathBuf;

fn mk_event(seq: u64, event_type: EventType, path: PathBuf) -> EventRecord {
    EventRecord {
        seq,
        timestamp: std::time::SystemTime::now(),
        event_type,
        id: FileIdentifier::Path(path.clone()),
        path_hint: Some(path),
    }
}

fn unique_tmp_dir(tag: &str) -> PathBuf {
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    std::env::temp_dir().join(format!("fd-rdd-{}-{}", tag, nanos))
}

#[test]
fn rebuild_with_pending_events_no_loss() {
    let root = unique_tmp_dir("rebuild");
    std::fs::create_dir_all(&root).unwrap();

    let old_path = root.join("old_aaa.txt");
    std::fs::write(&old_path, b"old").unwrap();

    let idx = Arc::new(TieredIndex::empty(vec![root.clone()]));

    // 先让旧索引里有内容（模拟在线服务已有数据）。
    idx.apply_events(&[mk_event(1, EventType::Create, old_path.clone())]);
    assert!(!idx.query("old_aaa").is_empty());

    // 开始 rebuild：此时 apply_events 会进入 pending 缓冲。
    assert!(idx.try_start_rebuild_force());
    assert!(idx.rebuild_in_progress());

    // new L2：模拟 full_build 的结果（这里直接应用一次 Create）。
    let new_l2 = Arc::new(PersistentIndex::new_with_roots(vec![root.clone()]));
    new_l2.apply_events(&[mk_event(2, EventType::Create, old_path.clone())]);

    // rebuild 期间新增文件：必须在切换后仍可查询到。
    let new_path = root.join("new_bbb.txt");
    std::fs::write(&new_path, b"new").unwrap();
    idx.apply_events(&[mk_event(3, EventType::Create, new_path.clone())]);

    // 完成：回放 pending -> 原子切换
    idx.finish_rebuild(new_l2);
    assert!(!idx.rebuild_in_progress());

    assert!(!idx.query("old_aaa").is_empty());
    assert!(!idx.query("new_bbb").is_empty());
}

#[test]
fn overlay_delete_then_recreate_cancels_deleted() {
    let root = unique_tmp_dir("overlay-cancel");
    std::fs::create_dir_all(&root).unwrap();

    let p = root.join("x.txt");
    std::fs::write(&p, b"x").unwrap();

    let idx = TieredIndex::empty(vec![root.clone()]);
    idx.apply_events(&[mk_event(1, EventType::Delete, p.clone())]);
    idx.apply_events(&[mk_event(2, EventType::Create, p.clone())]);

    let r = idx.memory_report(EventPipelineStats::default());
    assert_eq!(r.overlay.deleted_paths, 0);
    assert_eq!(r.overlay.upserted_paths, 1);
}

#[test]
fn overlay_rename_tracks_from_as_delete_and_to_as_upsert() {
    let root = unique_tmp_dir("overlay-rename");
    std::fs::create_dir_all(&root).unwrap();

    let from = root.join("old_aaa.txt");
    std::fs::write(&from, b"old").unwrap();
    let to = root.join("new_bbb.txt");
    std::fs::rename(&from, &to).unwrap();

    let idx = TieredIndex::empty(vec![root.clone()]);
    idx.apply_events(&[mk_event(
        1,
        EventType::Rename {
            from: FileIdentifier::Path(from.clone()),
            from_path_hint: Some(from.clone()),
        },
        to.clone(),
    )]);

    let r = idx.memory_report(EventPipelineStats::default());
    assert_eq!(r.overlay.deleted_paths, 1);
    assert_eq!(r.overlay.upserted_paths, 1);
}

#[test]
fn rebuild_pending_rename_applied_after_switch() {
    let root = unique_tmp_dir("rebuild-rename");
    std::fs::create_dir_all(&root).unwrap();

    let old_path = root.join("old_aaa.txt");
    std::fs::write(&old_path, b"old").unwrap();

    let idx = Arc::new(TieredIndex::empty(vec![root.clone()]));
    idx.apply_events(&[mk_event(1, EventType::Create, old_path.clone())]);
    assert!(!idx.query("old_aaa").is_empty());

    assert!(idx.try_start_rebuild_force());
    let new_l2 = Arc::new(PersistentIndex::new_with_roots(vec![root.clone()]));
    new_l2.apply_events(&[mk_event(2, EventType::Create, old_path.clone())]);

    let new_path = root.join("new_bbb.txt");
    std::fs::rename(&old_path, &new_path).unwrap();
    idx.apply_events(&[mk_event(
        3,
        EventType::Rename {
            from: FileIdentifier::Path(old_path.clone()),
            from_path_hint: Some(old_path.clone()),
        },
        new_path.clone(),
    )]);

    idx.finish_rebuild(new_l2);
    assert!(idx.query("old_aaa").is_empty());
    assert!(!idx.query("new_bbb").is_empty());
}

#[test]
fn fast_sync_reconciles_add_and_delete() {
    let root = unique_tmp_dir("fast-sync");
    std::fs::create_dir_all(&root).unwrap();

    let a = root.join("a_match.txt");
    let b = root.join("b_match.txt");
    std::fs::write(&a, b"a").unwrap();
    std::fs::write(&b, b"b").unwrap();

    let idx = TieredIndex::empty(vec![root.clone()]);
    idx.apply_events(&[
        mk_event(1, EventType::Create, a.clone()),
        mk_event(2, EventType::Create, b.clone()),
    ]);
    assert!(!idx.query("a_match").is_empty());
    assert!(!idx.query("b_match").is_empty());

    // 离线变更：不经过事件管道直接修改文件系统
    std::fs::remove_file(&b).unwrap();
    let c = root.join("c_match.txt");
    std::fs::write(&c, b"c").unwrap();

    let r = idx.fast_sync(
        DirtyScope::Dirs {
            cutoff_ns: 0,
            dirs: vec![root.clone()],
        },
        &[],
    );
    assert!(r.dirs_scanned >= 1);
    assert!(r.upsert_events >= 1);
    assert!(r.delete_events >= 1);

    assert!(!idx.query("a_match").is_empty());
    assert!(idx.query("b_match").is_empty());

    // fast_sync 后底层索引可能有极短异步窗口，poll 等待 c_match 出现
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
    let mut c_found = false;
    while std::time::Instant::now() < deadline {
        if !idx.query("c_match").is_empty() {
            c_found = true;
            break;
        }
        std::thread::sleep(std::time::Duration::from_millis(50));
    }
    assert!(c_found, "c_match should appear after fast_sync");
    assert!(!idx.query("c_match").is_empty());
}

#[tokio::test]
async fn auto_flush_overlay_wakes_snapshot_loop() {
    let root = unique_tmp_dir("auto-flush");
    std::fs::create_dir_all(&root).unwrap();

    let store = Arc::new(SnapshotStore::new(root.join("index.db")));
    let idx = Arc::new(TieredIndex::empty(vec![root.clone()]));
    // 低阈值：1 条路径/1 字节即可触发
    idx.set_auto_flush_limits(1, 1);

    let h = tokio::spawn(idx.clone().snapshot_loop(store.clone(), 3600));

    let p = root.join("a.txt");
    std::fs::write(&p, b"a").unwrap();
    idx.apply_events(&[mk_event(1, EventType::Create, p.clone())]);

    let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(5);
    loop {
        if !idx.disk_layers.read().is_empty() {
            break;
        }
        if tokio::time::Instant::now() >= deadline {
            panic!("auto flush did not produce a disk layer in time");
        }
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    }

    h.abort();
}

#[tokio::test]
async fn query_filters_do_not_leak_old_segment_meta() -> anyhow::Result<()> {
    let root = unique_tmp_dir("query-dsl-no-leak");
    std::fs::create_dir_all(&root)?;

    let store = Arc::new(SnapshotStore::new(root.join("index.db")));
    let idx = Arc::new(TieredIndex::empty(vec![root.clone()]));

    let p = root.join("x.txt");
    std::fs::write(&p, b"x")?;
    idx.apply_events(&[mk_event(1, EventType::Create, p.clone())]);

    // flush: 让旧元数据进入 disk layer
    idx.snapshot_now(store.clone()).await?;
    assert!(!idx.disk_layers.read().is_empty());

    // 修改文件：新元数据进入 L2（size 变大）
    std::fs::write(&p, vec![b'a'; 128])?;
    idx.apply_events(&[mk_event(2, EventType::Modify, p.clone())]);

    // 若未在 miss 时也 block path，旧段的 size 可能会"误命中"并被返回
    let r = idx.query_limit("size:<10b", 100);
    assert!(r.is_empty(), "should not return stale disk meta");

    let _ = std::fs::remove_dir_all(&root);
    Ok(())
}

#[tokio::test]
async fn periodic_flush_batch_threshold_skips_then_flushes() {
    let root = unique_tmp_dir("periodic-batch-events");
    std::fs::create_dir_all(&root).unwrap();

    let store = Arc::new(SnapshotStore::new(root.join("index.db")));
    let idx = Arc::new(TieredIndex::empty(vec![root.clone()]));
    idx.set_auto_flush_limits(0, 0);
    idx.set_periodic_flush_batch_limits(2, 0);

    let h = tokio::spawn(idx.clone().snapshot_loop(store.clone(), 1));

    let p1 = root.join("a.txt");
    std::fs::write(&p1, b"a").unwrap();
    idx.apply_events(&[mk_event(1, EventType::Create, p1.clone())]);

    tokio::time::sleep(std::time::Duration::from_millis(1300)).await;
    assert!(idx.disk_layers.read().is_empty());

    let p2 = root.join("b.txt");
    std::fs::write(&p2, b"b").unwrap();
    idx.apply_events(&[mk_event(2, EventType::Create, p2.clone())]);

    let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(5);
    loop {
        if !idx.disk_layers.read().is_empty() {
            break;
        }
        if tokio::time::Instant::now() >= deadline {
            panic!("periodic flush did not flush after event threshold was met");
        }
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    }

    h.abort();
}

#[tokio::test]
async fn periodic_flush_batch_byte_threshold_skips_then_flushes() {
    let root = unique_tmp_dir("periodic-batch-bytes");
    std::fs::create_dir_all(&root).unwrap();

    let store = Arc::new(SnapshotStore::new(root.join("index.db")));
    let idx = Arc::new(TieredIndex::empty(vec![root.clone()]));
    idx.set_auto_flush_limits(0, 0);

    let p1 = root.join("alpha-long-name.txt");
    let ev1 = mk_event(1, EventType::Create, p1.clone());
    let threshold = event_record_estimated_bytes(&ev1).saturating_add(1);
    idx.set_periodic_flush_batch_limits(0, threshold);

    let h = tokio::spawn(idx.clone().snapshot_loop(store.clone(), 1));

    std::fs::write(&p1, b"a").unwrap();
    idx.apply_events(&[ev1]);

    tokio::time::sleep(std::time::Duration::from_millis(1300)).await;
    assert!(idx.disk_layers.read().is_empty());

    let p2 = root.join("beta-long-name.txt");
    std::fs::write(&p2, b"b").unwrap();
    idx.apply_events(&[mk_event(2, EventType::Create, p2.clone())]);

    let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(5);
    loop {
        if !idx.disk_layers.read().is_empty() {
            break;
        }
        if tokio::time::Instant::now() >= deadline {
            panic!("periodic flush did not flush after byte threshold was met");
        }
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    }

    h.abort();
}

#[tokio::test]
async fn lsm_layering_delete_blocks_base() {
    use crate::core::{FileKey, FileMeta};
    use crate::index::PersistentIndex;

    let root = unique_tmp_dir("lsm-del");
    std::fs::create_dir_all(&root).unwrap();

    let alpha = root.join("alpha_test.txt");
    let gamma = root.join("gamma_test.txt");

    let store = Arc::new(SnapshotStore::new(root.join("index.db")));

    // base: alpha
    let base_idx = PersistentIndex::new_with_roots(vec![root.clone()]);
    base_idx.upsert(FileMeta {
        file_key: FileKey { dev: 1, ino: 1, generation: 0 },
        path: alpha.clone(),
        size: 1,
        mtime: None,
        ctime: None,
        atime: None,
    });
    store
        .lsm_replace_base_v6(&base_idx.export_segments_v6(), None, &[root.clone()], 0)
        .await
        .unwrap();
    store.gc_stale_segments().unwrap();

    // delta seg: gamma + delete(alpha)
    let delta_idx = PersistentIndex::new_with_roots(vec![root.clone()]);
    delta_idx.upsert(FileMeta {
        file_key: FileKey { dev: 1, ino: 2, generation: 0 },
        path: gamma.clone(),
        size: 1,
        mtime: None,
        ctime: None,
        atime: None,
    });
    let deleted = vec![alpha.as_os_str().as_encoded_bytes().to_vec()];
    store
        .lsm_append_delta_v6(
            &delta_idx.export_segments_v6(),
            &deleted,
            &[root.clone()],
            0,
        )
        .await
        .unwrap();

    let idx = TieredIndex::load_or_empty(&store, vec![root.clone()])
        .await
        .unwrap();

    assert!(idx.query("alpha").is_empty());
    assert_eq!(idx.query("gamma").len(), 1);
}

#[tokio::test]
async fn lsm_delete_then_recreate_prefers_newest() {
    use crate::core::{FileKey, FileMeta};
    use crate::index::PersistentIndex;

    let root = unique_tmp_dir("lsm-recreate");
    std::fs::create_dir_all(&root).unwrap();

    let alpha = root.join("alpha_test.txt");
    let store = Arc::new(SnapshotStore::new(root.join("index.db")));

    // base: alpha
    let base_idx = PersistentIndex::new_with_roots(vec![root.clone()]);
    base_idx.upsert(FileMeta {
        file_key: FileKey { dev: 1, ino: 1, generation: 0 },
        path: alpha.clone(),
        size: 1,
        mtime: None,
        ctime: None,
        atime: None,
    });
    store
        .lsm_replace_base_v6(&base_idx.export_segments_v6(), None, &[root.clone()], 0)
        .await
        .unwrap();
    store.gc_stale_segments().unwrap();

    // delta1: delete(alpha)
    let d1 = PersistentIndex::new_with_roots(vec![root.clone()]);
    let deleted = vec![alpha.as_os_str().as_encoded_bytes().to_vec()];
    store
        .lsm_append_delta_v6(&d1.export_segments_v6(), &deleted, &[root.clone()], 0)
        .await
        .unwrap();

    // delta2: recreate(alpha)
    let d2 = PersistentIndex::new_with_roots(vec![root.clone()]);
    d2.upsert(FileMeta {
        file_key: FileKey { dev: 1, ino: 42, generation: 0 },
        path: alpha.clone(),
        size: 2,
        mtime: None,
        ctime: None,
        atime: None,
    });
    store
        .lsm_append_delta_v6(&d2.export_segments_v6(), &[], &[root.clone()], 0)
        .await
        .unwrap();

    let idx = TieredIndex::load_or_empty(&store, vec![root.clone()])
        .await
        .unwrap();

    let r = idx.query("alpha");
    assert_eq!(r.len(), 1);
    assert_eq!(r[0].size, 2);
}

#[tokio::test]
async fn query_same_path_different_filekey_prefers_newest_segment() {
    use crate::core::{FileKey, FileMeta};
    use crate::index::PersistentIndex;

    let root = unique_tmp_dir("q-samepath-newest");
    std::fs::create_dir_all(&root).unwrap();

    let a = root.join("a.txt");
    std::fs::write(&a, b"x").unwrap();

    let store = Arc::new(SnapshotStore::new(root.join("index.db")));

    // seg1 (older): (dev=1, ino=100, path=/a.txt)
    let seg1 = PersistentIndex::new_with_roots(vec![root.clone()]);
    seg1.upsert(FileMeta {
        file_key: FileKey { dev: 1, ino: 100, generation: 0 },
        path: a.clone(),
        size: 1,
        mtime: None,
        ctime: None,
        atime: None,
    });
    store
        .lsm_replace_base_v6(&seg1.export_segments_v6(), None, &[root.clone()], 0)
        .await
        .unwrap();

    // seg2 (newer): (dev=1, ino=200, path=/a.txt) -- no delete sidecar
    let seg2 = PersistentIndex::new_with_roots(vec![root.clone()]);
    seg2.upsert(FileMeta {
        file_key: FileKey { dev: 1, ino: 200, generation: 0 },
        path: a.clone(),
        size: 2,
        mtime: None,
        ctime: None,
        atime: None,
    });
    store
        .lsm_append_delta_v6(&seg2.export_segments_v6(), &[], &[root.clone()], 0)
        .await
        .unwrap();

    let idx = TieredIndex::load_or_empty(&store, vec![root.clone()])
        .await
        .unwrap();

    let r = idx.query("a.txt");
    assert_eq!(r.len(), 1);
    assert_eq!(r[0].file_key.ino, 200);
}

#[tokio::test]
async fn query_rename_from_tombstone_blocks_old_path() {
    use crate::core::{FileKey, FileMeta};
    use crate::index::PersistentIndex;

    let root = unique_tmp_dir("q-rename-tombstone");
    std::fs::create_dir_all(&root).unwrap();

    let old = root.join("old.txt");
    let newp = root.join("new.txt");
    std::fs::write(&old, b"old").unwrap();
    std::fs::write(&newp, b"new").unwrap();

    let store = SnapshotStore::new(root.join("index.db"));

    // seg1 (older): /old.txt
    let seg1 = PersistentIndex::new_with_roots(vec![root.clone()]);
    seg1.upsert(FileMeta {
        file_key: FileKey { dev: 1, ino: 10, generation: 0 },
        path: old.clone(),
        size: 1,
        mtime: None,
        ctime: None,
        atime: None,
    });
    store
        .lsm_replace_base_v6(&seg1.export_segments_v6(), None, &[root.clone()], 0)
        .await
        .unwrap();

    // seg2 (newer): /new.txt + tombstone(/old.txt)
    let seg2 = PersistentIndex::new_with_roots(vec![root.clone()]);
    seg2.upsert(FileMeta {
        file_key: FileKey { dev: 1, ino: 11, generation: 0 },
        path: newp.clone(),
        size: 1,
        mtime: None,
        ctime: None,
        atime: None,
    });
    let deleted = vec![old.as_os_str().as_encoded_bytes().to_vec()];
    store
        .lsm_append_delta_v6(&seg2.export_segments_v6(), &deleted, &[root.clone()], 0)
        .await
        .unwrap();

    let idx = TieredIndex::load_or_empty(&store, vec![root.clone()])
        .await
        .unwrap();

    assert!(idx.query("old.txt").is_empty());
    assert_eq!(idx.query("new.txt").len(), 1);

    // Query wide pattern that would match both paths: old must remain blocked.
    let all = idx.query(".txt");
    assert_eq!(all.len(), 1);
    assert!(all[0].path.to_string_lossy().ends_with("new.txt"));
}

#[tokio::test]
async fn query_same_filekey_multiple_paths_only_returns_newest_path() {
    use crate::core::{FileKey, FileMeta};
    use crate::index::PersistentIndex;

    let root = unique_tmp_dir("q-samekey-newestpath");
    std::fs::create_dir_all(&root).unwrap();

    let p1 = root.join("ghost_v1.txt");
    let p2 = root.join("ghost_v2.txt");
    let p3 = root.join("ghost_v3.txt");
    std::fs::write(&p1, b"1").unwrap();
    std::fs::write(&p2, b"2").unwrap();
    std::fs::write(&p3, b"3").unwrap();

    let store = SnapshotStore::new(root.join("index.db"));

    let k = FileKey { dev: 1, ino: 999, generation: 0 };

    // seg1 (older): k -> p1
    let seg1 = PersistentIndex::new_with_roots(vec![root.clone()]);
    seg1.upsert(FileMeta {
        file_key: k,
        path: p1.clone(),
        size: 1,
        mtime: None,
        ctime: None,
        atime: None,
    });
    store
        .lsm_replace_base_v6(&seg1.export_segments_v6(), None, &[root.clone()], 0)
        .await
        .unwrap();

    // seg2: k -> p2
    let seg2 = PersistentIndex::new_with_roots(vec![root.clone()]);
    seg2.upsert(FileMeta {
        file_key: k,
        path: p2.clone(),
        size: 2,
        mtime: None,
        ctime: None,
        atime: None,
    });
    store
        .lsm_append_delta_v6(&seg2.export_segments_v6(), &[], &[root.clone()], 0)
        .await
        .unwrap();

    // seg3 (newest): k -> p3
    let seg3 = PersistentIndex::new_with_roots(vec![root.clone()]);
    seg3.upsert(FileMeta {
        file_key: k,
        path: p3.clone(),
        size: 3,
        mtime: None,
        ctime: None,
        atime: None,
    });
    store
        .lsm_append_delta_v6(&seg3.export_segments_v6(), &[], &[root.clone()], 0)
        .await
        .unwrap();

    let idx = TieredIndex::load_or_empty(&store, vec![root.clone()])
        .await
        .unwrap();

    // 如果 seen(FileKey) 去重语义回退，这里会返回 3 条（路径不同，blocked 兜不住）。
    let r = idx.query("ghost_");
    assert_eq!(r.len(), 1);
    assert_eq!(r[0].file_key.ino, 999);
    assert!(r[0].path.to_string_lossy().ends_with("ghost_v3.txt"));
}

#[tokio::test]
async fn lsm_offline_dir_mtime_change_skips_disk_segments() {
    use crate::core::{FileKey, FileMeta};
    use crate::index::PersistentIndex;

    let base = unique_tmp_dir("lsm-offline-mtime");
    let content_root = base.join("content");
    let state_root = base.join("state");
    std::fs::create_dir_all(&content_root).unwrap();
    std::fs::create_dir_all(&state_root).unwrap();

    // 预先创建深层目录（用于验证"深层变更不冒泡到 root"的场景）。
    let deep = content_root.join("deep");
    std::fs::create_dir_all(&deep).unwrap();

    let alpha = deep.join("alpha_test.txt");
    std::fs::write(&alpha, b"a").unwrap();

    let store = SnapshotStore::new(state_root.join("index.db"));

    // base: alpha（写入 LSM，生成 last_build_ns）
    let base_idx = PersistentIndex::new_with_roots(vec![content_root.clone()]);
    base_idx.upsert(FileMeta {
        file_key: FileKey { dev: 1, ino: 1, generation: 0 },
        path: alpha.clone(),
        size: 1,
        mtime: None,
        ctime: None,
        atime: None,
    });
    store
        .lsm_replace_base_v6(
            &base_idx.export_segments_v6(),
            None,
            &[content_root.clone()],
            0,
        )
        .await
        .unwrap();
    store.gc_stale_segments().unwrap();

    // 离线变更：在 deep 下新增文件（只会更新 deep 的 mtime，不会更新 content_root 的 mtime）
    std::thread::sleep(std::time::Duration::from_millis(20));
    std::fs::write(deep.join("offline_new.txt"), b"x").unwrap();

    // 重新启动加载：应判定快照不可信，不挂载 disk segments。
    let idx = TieredIndex::load_or_empty(&store, vec![content_root.clone()])
        .await
        .unwrap();
    assert_eq!(idx.disk_layers.read().len(), 0);
}

#[tokio::test]
async fn compaction_prefix_replaces_base_and_keeps_suffix_deltas() {
    use crate::core::{FileKey, FileMeta};
    use crate::index::PersistentIndex;

    let root = unique_tmp_dir("lsm-compact-prefix");
    std::fs::create_dir_all(&root).unwrap();
    let store = Arc::new(SnapshotStore::new(root.join("index.db")));

    let mk_seg = |ino: u64, name: &str| {
        let idx = PersistentIndex::new_with_roots(vec![root.clone()]);
        idx.upsert(FileMeta {
            file_key: FileKey { dev: 1, ino, generation: 0 },
            path: root.join(name),
            size: ino,
            mtime: None,
            ctime: None,
            atime: None,
        });
        idx
    };

    store
        .lsm_replace_base_v6(
            &mk_seg(1, "base.txt").export_segments_v6(),
            None,
            &[root.clone()],
            10,
        )
        .await
        .unwrap();
    store
        .lsm_append_delta_v6(
            &mk_seg(2, "delta-1.txt").export_segments_v6(),
            &[],
            &[root.clone()],
            11,
        )
        .await
        .unwrap();
    store
        .lsm_append_delta_v6(
            &mk_seg(3, "delta-2.txt").export_segments_v6(),
            &[],
            &[root.clone()],
            12,
        )
        .await
        .unwrap();
    store
        .lsm_append_delta_v6(
            &mk_seg(4, "delta-3.txt").export_segments_v6(),
            &[],
            &[root.clone()],
            13,
        )
        .await
        .unwrap();
    store
        .lsm_append_delta_v6(
            &mk_seg(5, "delta-4.txt").export_segments_v6(),
            &[],
            &[root.clone()],
            14,
        )
        .await
        .unwrap();

    let idx = Arc::new(
        TieredIndex::load_or_empty(&store, vec![root.clone()])
            .await
            .unwrap(),
    );
    assert_eq!(idx.disk_layers.read().len(), 5);

    let prefix = idx
        .disk_layers
        .read()
        .iter()
        .take(3)
        .cloned()
        .collect::<Vec<_>>();
    assert_eq!(
        prefix.iter().map(|l| l.id).collect::<Vec<_>>(),
        vec![1, 2, 3]
    );

    idx.compact_layers(store.clone(), prefix).await.unwrap();

    let layer_ids = idx
        .disk_layers
        .read()
        .iter()
        .map(|l| l.id)
        .collect::<Vec<_>>();
    assert_eq!(layer_ids.len(), 3);
    assert_eq!(layer_ids[1..], [4, 5]);
    assert!(layer_ids[0] > 5);

    let loaded = store.load_lsm_if_valid(&[root.clone()]).unwrap().unwrap();
    assert_eq!(loaded.base.as_ref().map(|b| b.id), Some(layer_ids[0]));
    assert_eq!(
        loaded.deltas.iter().map(|d| d.id).collect::<Vec<_>>(),
        vec![4, 5]
    );
    assert_eq!(loaded.wal_seal_id, 14);
}
