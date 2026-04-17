use std::path::PathBuf;
use std::time::{SystemTime, UNIX_EPOCH};

pub fn unique_tmp_dir(tag: &str) -> PathBuf {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos();
    std::env::temp_dir().join(format!(
        "fd-rdd-test-{}-{}-{}",
        tag,
        std::process::id(),
        nanos
    ))
}
