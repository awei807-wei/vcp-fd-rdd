//! P1 — Visibility latency tests.
//!
//! Validates that a newly created file becomes query-visible through the real
//! event pipeline within the expected SLA.

#[allow(dead_code)]
mod common;

use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use common::unique_tmp_dir;
use fd_rdd::event::EventPipeline;
use fd_rdd::index::TieredIndex;

async fn wait_until_visible(index: &TieredIndex, query: &str, path: &PathBuf, timeout: Duration) {
    let deadline = tokio::time::Instant::now() + timeout;
    loop {
        if index
            .query(query)
            .iter()
            .any(|m| m.path.as_path() == path.as_path())
        {
            return;
        }
        if tokio::time::Instant::now() >= deadline {
            panic!(
                "file was not query-visible within {:?}: {}",
                timeout,
                path.display()
            );
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

async fn wait_until_gone(index: &TieredIndex, query: &str, path: &PathBuf, timeout: Duration) {
    let deadline = tokio::time::Instant::now() + timeout;
    loop {
        if !index
            .query(query)
            .iter()
            .any(|m| m.path.as_path() == path.as_path())
        {
            return;
        }
        if tokio::time::Instant::now() >= deadline {
            panic!(
                "file was still query-visible after {:?}: {}",
                timeout,
                path.display()
            );
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

#[test]
fn test_new_file_visible_within_2s() {
    let root = unique_tmp_dir("latency");
    std::fs::create_dir_all(&root).unwrap();

    let index = Arc::new(TieredIndex::empty(vec![root.clone()]));
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .unwrap();

    runtime.block_on(async {
        let pipeline = EventPipeline::new_with_config(index.clone(), 25, 1024);
        pipeline.start().await.unwrap();

        tokio::time::sleep(Duration::from_millis(200)).await;

        let probe = root.join("visible_within_2s.txt");
        std::fs::write(&probe, b"hello visibility").unwrap();

        wait_until_visible(&index, "visible_within_2s", &probe, Duration::from_secs(2)).await;
    });

    drop(runtime);
    let _ = std::fs::remove_dir_all(&root);
}

#[test]
fn test_download_rename_final_file_visible_within_2s() {
    let root = unique_tmp_dir("download-rename");
    std::fs::create_dir_all(&root).unwrap();

    let index = Arc::new(TieredIndex::empty(vec![root.clone()]));
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .unwrap();

    runtime.block_on(async {
        let pipeline = EventPipeline::new_with_config(index.clone(), 25, 1024);
        pipeline.start().await.unwrap();

        tokio::time::sleep(Duration::from_millis(200)).await;

        let part = root.join("finished_download_probe.txt.part");
        let final_path = root.join("finished_download_probe.txt");
        std::fs::write(&part, b"partial then complete").unwrap();
        std::fs::rename(&part, &final_path).unwrap();

        wait_until_visible(
            &index,
            "finished_download_probe",
            &final_path,
            Duration::from_secs(2),
        )
        .await;
        wait_until_gone(
            &index,
            "finished_download_probe",
            &part,
            Duration::from_secs(2),
        )
        .await;
    });

    drop(runtime);
    let _ = std::fs::remove_dir_all(&root);
}
