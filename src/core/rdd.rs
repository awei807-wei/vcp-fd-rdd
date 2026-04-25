#[cfg(feature = "rkyv")]
use rkyv::{Archive, Deserialize as RkyvDeserialize, Serialize as RkyvSerialize};
use serde::{Deserialize, Serialize};
use std::path::PathBuf;
use std::sync::Arc;

/// 文件身份：Linux 上用 (dev, ino, generation) 做主键，rename 时 ino 不变，
/// generation 用于区分 inode 复用（ext4 i_generation）。
///
/// 说明：阶段 A 引入 `DocId(u32)` 作为 L2 内部的紧凑主键；
/// `FileKey` 仍用于扫描/事件输入与“同 inode 去重”。
#[derive(
    Clone, Copy, Debug, Default, Eq, PartialEq, Ord, PartialOrd, Hash, Serialize, Deserialize,
)]
#[cfg_attr(feature = "rkyv", derive(Archive, RkyvSerialize, RkyvDeserialize))]
#[cfg_attr(feature = "rkyv", archive(check_bytes))]
pub struct FileKey {
    pub dev: u64,
    pub ino: u64,
    #[serde(default)]
    pub generation: u32,
}

#[cfg(target_os = "linux")]
pub fn get_file_generation(path: &std::path::Path) -> u32 {
    use std::ffi::CString;

    let c_path = match CString::new(path.as_os_str().as_encoded_bytes()) {
        Ok(s) => s,
        Err(_) => return 0,
    };
    let fd = unsafe { libc::open(c_path.as_ptr(), libc::O_PATH | libc::O_CLOEXEC, 0) };
    if fd < 0 {
        return 0;
    }
    let mut generation: i32 = 0;
    const FS_IOC_GETVERSION: libc::c_ulong = 0x8008_7601;
    let ret = unsafe { libc::ioctl(fd, FS_IOC_GETVERSION as _, &mut generation) };
    unsafe { libc::close(fd) };
    if ret == 0 {
        generation as u32
    } else {
        0
    }
}

#[cfg(not(target_os = "linux"))]
pub fn get_file_generation(_path: &std::path::Path) -> u32 {
    0
}

impl FileKey {
    pub fn from_path_and_metadata(
        path: &std::path::Path,
        meta: &std::fs::Metadata,
    ) -> Option<Self> {
        #[cfg(unix)]
        {
            use std::os::unix::fs::MetadataExt;
            let generation = get_file_generation(path);
            Some(Self {
                dev: meta.dev(),
                ino: meta.ino(),
                generation,
            })
        }

        #[cfg(windows)]
        {
            use std::hash::{Hash, Hasher};

            // Windows stable std does not expose a true inode/file-id for rename-stable identity.
            // Fall back to a path-based key to keep the project buildable on Windows. This degrades
            // rename semantics to delete+create (still correct for query results).
            let _ = meta;
            let mut hasher = std::collections::hash_map::DefaultHasher::new();
            path.as_os_str().as_encoded_bytes().hash(&mut hasher);
            let h = hasher.finish();
            Some(Self {
                dev: 0,
                ino: h,
                generation: 0,
            })
        }

        #[cfg(not(any(unix, windows)))]
        {
            let _ = path;
            let _ = meta;
            None
        }
    }
}

/// FileKeyMap 段的条目：稳定身份 -> docid（仅用于磁盘结晶与 mmap 反查）。
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "rkyv", derive(Archive, RkyvSerialize, RkyvDeserialize))]
#[cfg_attr(feature = "rkyv", archive(check_bytes))]
pub struct FileKeyEntry {
    pub key: FileKey,
    pub doc_id: u64,
}

/// 文件元数据
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct FileMeta {
    pub file_key: FileKey,
    pub path: PathBuf,
    pub size: u64,
    pub mtime: Option<std::time::SystemTime>,
    /// 文件创建时间（Linux 上为 ctime/inode-change-time；不持久化到快照）
    #[serde(default, skip_serializing)]
    pub ctime: Option<std::time::SystemTime>,
    /// 最近访问时间（不持久化到快照）
    #[serde(default, skip_serializing)]
    pub atime: Option<std::time::SystemTime>,
}

/// 分区定义（用于构建流水线）
#[derive(Clone, Debug)]
pub struct Partition {
    pub id: usize,
    pub root: PathBuf,
    pub max_depth: usize,
}

/// BuildRDD：面向"构建索引"的数据集抽象
/// - compute 返回迭代器（流式），由下游 builder 消费
/// - 仅用于启动全扫/补扫/重建，不参与在线查询路径
pub trait BuildRDD<T: Send + Sync + 'static>: Send + Sync {
    fn partitions(&self) -> &[Partition];
    fn compute(&self, part: &Partition) -> Box<dyn Iterator<Item = T> + Send>;

    fn for_each(&self, mut sink: impl FnMut(T) + Send) {
        for p in self.partitions() {
            for item in self.compute(p) {
                sink(item);
            }
        }
    }
}

/// FsScanRDD：启动全量扫描/补扫时使用
pub struct FsScanRDD {
    pub parts: Vec<Partition>,
    parallelism: usize,
    include_hidden: bool,
    follow_links: bool,
    ignore_enabled: bool,
}

impl FsScanRDD {
    pub fn from_roots(roots: Vec<PathBuf>) -> Self {
        let parts = roots
            .into_iter()
            .enumerate()
            .map(|(id, root)| Partition {
                id,
                root,
                max_depth: 255,
            })
            .collect();
        Self {
            parts,
            parallelism: 1,
            include_hidden: false,
            follow_links: false,
            ignore_enabled: true,
        }
    }

    /// 设置扫描并行度（仅影响 `for_each_meta` 的并行 walker）。
    pub fn with_parallelism(mut self, parallelism: usize) -> Self {
        self.parallelism = parallelism.max(1);
        self
    }

    /// 控制是否将 `.` 开头的文件/目录纳入扫描。
    pub fn with_hidden(mut self, include_hidden: bool) -> Self {
        self.include_hidden = include_hidden;
        self
    }

    /// 控制是否跟随符号链接（默认 true；`ignore` crate 内置循环检测）。
    pub fn with_follow_links(mut self, follow_links: bool) -> Self {
        self.follow_links = follow_links;
        self
    }

    /// 控制是否启用 `.gitignore` / `.ignore` / git exclude / git global 规则。
    pub fn with_ignore_rules(mut self, ignore_enabled: bool) -> Self {
        self.ignore_enabled = ignore_enabled;
        self
    }

    /// 按指定并行度遍历所有文件元数据（用于冷启动/重建的弹性构建）。
    ///
    /// 注意：这是 FsScanRDD 的专用入口，不改变 `BuildRDD` 的 Iterator 抽象，
    /// 避免牵涉面过大。`parallelism==1` 时行为与原 for_each 等价。
    pub fn for_each_meta(&self, sink: impl Fn(FileMeta) + Send + Sync + 'static) {
        let sink: Arc<dyn Fn(FileMeta) + Send + Sync> = Arc::new(sink);

        if self.parallelism <= 1 {
            for p in self.partitions() {
                for item in self.compute(p) {
                    sink(item);
                }
            }
            return;
        }

        for p in self.partitions() {
            scan_partition_parallel(
                p,
                self.parallelism,
                self.include_hidden,
                self.follow_links,
                self.ignore_enabled,
                sink.clone(),
            );
        }
    }
}

impl BuildRDD<FileMeta> for FsScanRDD {
    fn partitions(&self) -> &[Partition] {
        &self.parts
    }

    fn compute(&self, part: &Partition) -> Box<dyn Iterator<Item = FileMeta> + Send> {
        use ignore::WalkBuilder;

        let mut visited: std::collections::HashSet<FileKey> = std::collections::HashSet::new();

        let mut builder = WalkBuilder::new(&part.root);
        builder
            .max_depth(Some(part.max_depth))
            .hidden(!self.include_hidden)
            .follow_links(self.follow_links)
            .ignore(self.ignore_enabled)
            .git_ignore(self.ignore_enabled)
            .git_global(self.ignore_enabled)
            .git_exclude(self.ignore_enabled);
        let walker = builder.build();

        let iter = walker
            .filter_map(|e| match e {
                Ok(entry) => Some(entry),
                Err(err) => {
                    log_walk_error(&err);
                    None
                }
            })
            .filter(|e| e.file_type().map(|ft| ft.is_file()).unwrap_or(false))
            .filter_map(move |e| {
                let meta = match e.metadata() {
                    Ok(meta) => meta,
                    Err(err) => {
                        log_metadata_error(e.path(), &err);
                        return None;
                    }
                };
                let file_key = FileKey::from_path_and_metadata(e.path(), &meta)?;
                // ino+dev 去重：同一文件可能通过多条符号链接路径到达
                if !visited.insert(file_key) {
                    return None;
                }
                Some(FileMeta {
                    file_key,
                    path: e.path().to_path_buf(),
                    size: meta.len(),
                    mtime: meta.modified().ok(),
                    ctime: meta.created().ok(),
                    atime: meta.accessed().ok(),
                })
            });

        Box::new(iter)
    }
}

fn scan_partition_parallel(
    part: &Partition,
    parallelism: usize,
    include_hidden: bool,
    follow_links: bool,
    ignore_enabled: bool,
    sink: Arc<dyn Fn(FileMeta) + Send + Sync>,
) {
    use ignore::{WalkBuilder, WalkState};

    let visited: Arc<dashmap::DashSet<FileKey>> = Arc::new(dashmap::DashSet::new());

    let mut builder = WalkBuilder::new(&part.root);
    builder
        .max_depth(Some(part.max_depth))
        .hidden(!include_hidden)
        .follow_links(follow_links)
        .ignore(ignore_enabled)
        .git_ignore(ignore_enabled)
        .git_global(ignore_enabled)
        .git_exclude(ignore_enabled)
        .threads(parallelism);
    let walker = builder.build_parallel();

    walker.run(|| {
        let sink = sink.clone();
        let visited = visited.clone();
        Box::new(move |entry| {
            let e = match entry {
                Ok(e) => e,
                Err(err) => {
                    log_walk_error(&err);
                    return WalkState::Continue;
                }
            };
            if !e.file_type().map(|ft| ft.is_file()).unwrap_or(false) {
                return WalkState::Continue;
            }
            let meta = match e.metadata() {
                Ok(meta) => meta,
                Err(err) => {
                    log_metadata_error(e.path(), &err);
                    return WalkState::Continue;
                }
            };
            let Some(file_key) = FileKey::from_path_and_metadata(e.path(), &meta) else {
                return WalkState::Continue;
            };
            // ino+dev 去重：避免符号链接导致同一文件被多次索引
            if !visited.insert(file_key) {
                return WalkState::Continue;
            }

            sink(FileMeta {
                file_key,
                path: e.path().to_path_buf(),
                size: meta.len(),
                mtime: meta.modified().ok(),
                ctime: meta.created().ok(),
                atime: meta.accessed().ok(),
            });

            WalkState::Continue
        })
    });
}

fn log_walk_error(err: &ignore::Error) {
    tracing::warn!("scan walker skipped entry: {}", err);
}

fn log_metadata_error(path: &std::path::Path, err: &ignore::Error) {
    tracing::warn!("scan metadata failed for {}: {}", path.display(), err);
}

/// BuildLineage：仅记录构建流水线的阶段性记录，有硬上限（环形缓冲区）
#[derive(Clone, Debug)]
pub struct BuildLineage {
    pub max_records: usize,
    pub records: std::collections::VecDeque<String>,
}

impl BuildLineage {
    pub fn new(max_records: usize) -> Self {
        Self {
            max_records,
            records: std::collections::VecDeque::with_capacity(max_records),
        }
    }

    pub fn push(&mut self, record: String) {
        if self.records.len() >= self.max_records {
            self.records.pop_front();
        }
        self.records.push_back(record);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use std::path::PathBuf;
    use std::time::{SystemTime, UNIX_EPOCH};

    fn unique_tmp_dir(tag: &str) -> PathBuf {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos();
        std::env::temp_dir().join(format!("fd-rdd-rdd-{}-{}", tag, nanos))
    }

    #[test]
    fn scan_skips_hidden_files_by_default() {
        let root = unique_tmp_dir("hidden-off");
        fs::create_dir_all(&root).expect("create root");
        fs::write(root.join("visible.txt"), b"visible").expect("write visible");
        fs::write(root.join(".hidden.txt"), b"hidden").expect("write hidden");

        let rdd = FsScanRDD::from_roots(vec![root.clone()]);
        let mut seen = Vec::new();
        rdd.for_each(|meta| seen.push(meta.path));

        assert!(seen.iter().any(|p| p.ends_with("visible.txt")));
        assert!(!seen.iter().any(|p| p.ends_with(".hidden.txt")));

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn scan_includes_hidden_files_when_enabled() {
        let root = unique_tmp_dir("hidden-on");
        fs::create_dir_all(&root).expect("create root");
        fs::write(root.join("visible.txt"), b"visible").expect("write visible");
        fs::write(root.join(".hidden.txt"), b"hidden").expect("write hidden");

        let rdd = FsScanRDD::from_roots(vec![root.clone()]).with_hidden(true);
        let mut seen = Vec::new();
        rdd.for_each(|meta| seen.push(meta.path));

        assert!(seen.iter().any(|p| p.ends_with("visible.txt")));
        assert!(seen.iter().any(|p| p.ends_with(".hidden.txt")));

        let _ = fs::remove_dir_all(root);
    }
}
