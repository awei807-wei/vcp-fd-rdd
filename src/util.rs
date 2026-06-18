use std::path::{Component, Path, PathBuf};

// ── RSS trim：主动向 OS 归还空闲堆内存 ──

#[cfg(feature = "mimalloc")]
pub fn maybe_trim_rss() {
    // mimalloc 作为全局分配器时，glibc 的 malloc_trim 无效，需要调用 mimalloc 自己的回收。
    extern "C" {
        fn mi_collect(force: bool);
    }
    // SAFETY: mi_collect is a well-defined mimalloc API that triggers garbage collection.
    // It is safe to call at any time; the `force` parameter requests aggressive collection.
    unsafe { mi_collect(true) };
}

#[cfg(all(not(feature = "mimalloc"), target_os = "linux", target_env = "gnu"))]
pub fn maybe_trim_rss() {
    // glibc malloc 的主动回吐：释放尽可能多的空闲块回 OS。
    // SAFETY: libc::malloc_trim(0) is a glibc extension that releases free memory back to
    // the OS. The argument 0 means "trim as much as possible". It is safe to call at any time.
    unsafe {
        libc::malloc_trim(0);
    }
}

#[cfg(all(
    not(feature = "mimalloc"),
    not(all(target_os = "linux", target_env = "gnu"))
))]
pub fn maybe_trim_rss() {}

// ── 路径工具 ──

/// 从原始字节构造 PathBuf（Unix 上保持无损、非 Unix 上 lossy UTF-8）。
pub fn pathbuf_from_encoded_vec(bytes: Vec<u8>) -> PathBuf {
    #[cfg(unix)]
    {
        use std::ffi::OsString;
        use std::os::unix::ffi::OsStringExt;
        PathBuf::from(OsString::from_vec(bytes))
    }
    #[cfg(not(unix))]
    {
        PathBuf::from(String::from_utf8_lossy(&bytes).into_owned())
    }
}

/// 拼接 root_bytes + separator + rel_bytes 生成绝对路径字节。
pub fn compose_abs_path_bytes(root_bytes: &[u8], rel_bytes: &[u8]) -> Vec<u8> {
    let mut out = Vec::with_capacity(root_bytes.len() + 1 + rel_bytes.len());
    out.extend_from_slice(root_bytes);
    let needs_sep = if cfg!(windows) {
        !out.ends_with(b"/") && !out.ends_with(b"\\")
    } else {
        !out.ends_with(b"/")
    };
    if needs_sep {
        out.push(std::path::MAIN_SEPARATOR as u8);
    }
    out.extend_from_slice(rel_bytes);
    out
}

/// compose_abs_path_bytes 的便捷封装：直接返回 PathBuf。
pub fn compose_abs_path_buf(root_bytes: &[u8], rel_bytes: &[u8]) -> PathBuf {
    let abs = compose_abs_path_bytes(root_bytes, rel_bytes);
    pathbuf_from_encoded_vec(abs)
}

/// 通过 root_id 读取编码后的 root bytes；缺失时回退到 `/`。
pub fn root_bytes_for_id(roots: &[Vec<u8>], root_id: u16) -> &[u8] {
    roots
        .get(root_id as usize)
        .map(|v| v.as_slice())
        .unwrap_or(b"/")
}

pub const DEFAULT_EXCLUDE_DIRS: &[&str] = &[
    ".git",
    ".hg",
    ".svn",
    ".cache",
    ".cargo",
    ".npm",
    ".pnpm-store",
    ".yarn",
    "node_modules",
    "target",
    "dist",
    "build",
    "vendor",
];

pub fn default_exclude_dirs() -> Vec<String> {
    DEFAULT_EXCLUDE_DIRS.iter().map(|s| s.to_string()).collect()
}

pub fn normalize_exclude_dirs(mut dirs: Vec<String>) -> Vec<String> {
    dirs.retain(|s| !s.trim().is_empty());
    for s in &mut dirs {
        *s = s.trim().trim_matches('/').trim_matches('\\').to_string();
    }
    dirs.retain(|s| !s.is_empty());
    dirs.sort();
    dirs.dedup();
    dirs
}

pub fn path_has_excluded_component(path: &Path, exclude_dirs: &[String]) -> bool {
    if exclude_dirs.is_empty() {
        return false;
    }
    path.components().any(|component| {
        let Component::Normal(name) = component else {
            return false;
        };
        exclude_dirs
            .iter()
            .any(|excluded| name == excluded.as_str())
    })
}

/// Current Unix timestamp in seconds. Returns 0 if the system clock is before the epoch.
pub fn unix_secs() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
}

/// Atomically write serializable data as pretty-printed JSON to `path`.
///
/// Creates the parent directory, writes to a `.tmp` sidecar, fsyncs, renames
/// into place, and fsyncs the parent dir. The write closure receives a
/// `&mut std::fs::File` so callers can use `serde_json::to_writer_pretty` or
/// any other serialization method.
pub fn atomic_write_json<T: serde::Serialize>(path: &Path, value: &T) -> anyhow::Result<()> {
    use std::io::Write;

    let dir = path
        .parent()
        .ok_or_else(|| anyhow::anyhow!("path has no parent: {path:?}"))?;
    std::fs::create_dir_all(dir)?;
    let tmp = path.with_extension("json.tmp");
    {
        let mut file = std::fs::File::create(&tmp)?;
        serde_json::to_writer_pretty(&mut file, value)?;
        file.write_all(b"\n")?;
        file.sync_all()?;
    }
    std::fs::rename(&tmp, path)?;
    if let Ok(dir_file) = std::fs::File::open(dir) {
        let _ = dir_file.sync_all();
    }
    Ok(())
}

/// Estimate how many directory watches `notify::RecursiveMode::Recursive` will register.
///
/// This intentionally does not apply fd-rdd's scan/index `exclude_dirs`: notify still
/// installs inotify watches below excluded subtrees when asked to watch a parent
/// recursively, and the tiered watcher budget must model that kernel-facing cost.
///
/// Returns at most `cap + 1`. A result greater than `cap` means the root is too
/// large for the current watch budget and must not be admitted into L0.
pub fn estimate_notify_recursive_watch_count(root: &Path, cap: usize) -> usize {
    fn walk(path: &Path, limit: usize, count: &mut usize) {
        if *count >= limit {
            return;
        }
        *count = (*count).saturating_add(1);
        let Ok(entries) = std::fs::read_dir(path) else {
            return;
        };
        for entry in entries.flatten() {
            if *count >= limit {
                return;
            }
            if entry.file_type().is_ok_and(|ft| ft.is_dir()) {
                walk(entry.path().as_path(), limit, count);
            }
        }
    }

    let mut count = 0usize;
    let limit = cap.max(1).saturating_add(1);
    walk(root, limit, &mut count);
    count.max(1)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn temp_dir(tag: &str) -> PathBuf {
        let nanos = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        std::env::temp_dir().join(format!("fd-rdd-util-{tag}-{}-{nanos}", std::process::id()))
    }

    #[test]
    fn notify_watch_estimate_counts_excluded_subtrees() {
        let root = temp_dir("notify-watch-estimate");
        std::fs::create_dir_all(root.join("node_modules/pkg/a")).unwrap();
        std::fs::create_dir_all(root.join("src")).unwrap();

        let exclude_dirs = vec!["node_modules".to_string()];
        assert!(path_has_excluded_component(
            root.join("node_modules/pkg").as_path(),
            &exclude_dirs
        ));
        assert_eq!(
            estimate_notify_recursive_watch_count(root.as_path(), 100),
            5
        );

        let _ = std::fs::remove_dir_all(root);
    }

    #[test]
    fn notify_watch_estimate_reports_cap_plus_one_on_overflow() {
        let root = temp_dir("notify-watch-estimate-cap");
        std::fs::create_dir_all(root.join("a/b/c")).unwrap();

        assert_eq!(estimate_notify_recursive_watch_count(root.as_path(), 2), 3);

        let _ = std::fs::remove_dir_all(root);
    }
}
