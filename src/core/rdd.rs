use std::path::PathBuf;
use serde::{Serialize, Deserialize};

/// 文件身份：Linux 上用 (dev, ino) 做主键，rename 时 ino 不变
#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash, Serialize, Deserialize)]
pub struct FileId {
    pub dev: u64,
    pub ino: u64,
}

/// 文件元数据
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct FileMeta {
    pub file_id: FileId,
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
        Self { parts }
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
                    file_id: FileId {
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