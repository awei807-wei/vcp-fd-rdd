use std::path::PathBuf;

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
