//! P1 — Visibility latency tests.
//!
//! Validates that a newly created file becomes query-visible through the real
//! event pipeline within the expected SLA.

use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use fd_rdd::event::EventPipeline;
use fd_rdd::index::TieredIndex;

fn unique_tmp_dir(tag: &str) -> PathBuf {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    std::env::temp_dir().join(format!("fd-rdd-visibility-{}-{}", tag, nanos))
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

        let deadline = tokio::time::Instant::now() + Duration::from_secs(2);
        loop {
            if index
                .query("visible_within_2s")
                .iter()
                .any(|m| m.path == probe)
            {
                return;
            }
            if tokio::time::Instant::now() >= deadline {
                panic!(
                    "new file was not query-visible within 2s: {}",
                    probe.display()
                );
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    });

    drop(runtime);
    let _ = std::fs::remove_dir_all(&root);
}
