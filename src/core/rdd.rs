use rkyv::{Archive, Serialize as RkyvSerialize, Deserialize as RkyvDeserialize};
use serde::{Deserialize, Serialize};
use std::path::PathBuf;
use std::sync::Arc;

/// 文件身份：Linux 上用 (dev, ino) 做主键，rename 时 ino 不变。
///
/// 说明：阶段 A 引入 `DocId(u32)` 作为 L2 内部的紧凑主键；
/// `FileKey` 仍用于扫描/事件输入与“同 inode 去重”。
#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash, Serialize, Deserialize, Archive, RkyvSerialize, RkyvDeserialize)
#[archive(check_bytes)]
#[archive_attr(derive(Debug, PartialEq, Eq))]]
#[derive(Archive, RkyvSerialize, RkyvDeserialize, Debug)]
#[archive(check_bytes)]
pub struct FileKeyEntry {
    pub key: FileKey,
    pub doc_id: u32,
}

pub struct FileKey {
    pub dev: u64,
    pub ino: u64,
}

/// 文件元数据
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct FileMeta {
    pub file_key: FileKey,
    pub path: PathBuf,
    pub size: u64,
    pub mtime: Option<std::time::SystemTime>,
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
        }
    }

    /// 设置扫描并行度（仅影响 `for_each_meta` 的并行 walker）。
    pub fn with_parallelism(mut self, parallelism: usize) -> Self {
        self.parallelism = parallelism.max(1);
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
            scan_partition_parallel(p, self.parallelism, sink.clone());
        }
    }
}

impl BuildRDD<FileMeta> for FsScanRDD {
    fn partitions(&self) -> &[Partition] {
        &self.parts
    }

    fn compute(&self, part: &Partition) -> Box<dyn Iterator<Item = FileMeta> + Send> {
        use ignore::WalkBuilder;
        use std::os::unix::fs::MetadataExt;

        let walker = WalkBuilder::new(&part.root)
            .max_depth(Some(part.max_depth))
            .hidden(true)
            .ignore(true)
            .git_ignore(true)
            .build();

        let iter = walker
            .filter_map(|e| e.ok())
            .filter(|e| e.file_type().map(|ft| ft.is_file()).unwrap_or(false))
            .filter_map(|e| {
                let meta = e.metadata().ok()?;
                Some(FileMeta {
                    file_key: FileKey {
                        dev: meta.dev(),
                        ino: meta.ino(),
                    },
                    path: e.path().to_path_buf(),
                    size: meta.len(),
                    mtime: meta.modified().ok(),
                })
            });

        Box::new(iter)
    }
}

fn scan_partition_parallel(
    part: &Partition,
    parallelism: usize,
    sink: Arc<dyn Fn(FileMeta) + Send + Sync>,
) {
    use ignore::{WalkBuilder, WalkState};
    use std::os::unix::fs::MetadataExt;

    let walker = WalkBuilder::new(&part.root)
        .max_depth(Some(part.max_depth))
        .hidden(true)
        .ignore(true)
        .git_ignore(true)
        .threads(parallelism)
        .build_parallel();

    walker.run(|| {
        let sink = sink.clone();
        Box::new(move |entry| {
            let Ok(e) = entry else {
                return WalkState::Continue;
            };
            if !e.file_type().map(|ft| ft.is_file()).unwrap_or(false) {
                return WalkState::Continue;
            }
            let Ok(meta) = e.metadata() else {
                return WalkState::Continue;
            };

            sink(FileMeta {
                file_key: FileKey {
                    dev: meta.dev(),
                    ino: meta.ino(),
                },
                path: e.path().to_path_buf(),
                size: meta.len(),
                mtime: meta.modified().ok(),
            });

            WalkState::Continue
        })
    });
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
