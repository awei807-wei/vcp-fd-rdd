use std::sync::Arc;
use serde::{Serialize, Deserialize};
use crate::core::partition::Partition;
use crate::core::lineage::EventRecord;

/// RDD 特质：弹性分布式数据集的核心抽象
pub trait RDD<T: Send + Sync + 'static>: Send + Sync {
    /// 获取分区列表
    fn partitions(&self) -> &[Partition];
    
    /// 计算指定分区（惰性）
    fn compute(&self, partition: &Partition) -> Vec<T>;
    
    /// 获取父 RDD（血缘）
    fn dependencies(&self) -> Vec<Arc<dyn RDD<T>>>;
    
    /// 执行操作（触发计算）
    fn collect(&self) -> Vec<T> {
        use rayon::prelude::*;
        self.partitions()
            .par_iter()
            .flat_map(|p| self.compute(p))
            .collect()
    }
}

/// 文件索引专用 RDD
#[derive(Clone, Serialize, Deserialize)]
pub struct FileIndexRDD {
    pub partitions: Vec<Partition>,
    pub lineage: Vec<EventRecord>,  // 不可变事件血缘
    #[serde(skip)]
    pub compute_fn: Option<Arc<dyn Fn(&Partition) -> Vec<FileEntry> + Send + Sync>>,
}

impl RDD<FileEntry> for FileIndexRDD {
    fn partitions(&self) -> &[Partition] {
        &self.partitions
    }
    
    fn compute(&self, partition: &Partition) -> Vec<FileEntry> {
        if let Some(ref f) = self.compute_fn {
            f(partition)
        } else {
            scan_partition(partition)
        }
    }
    
    fn dependencies(&self) -> Vec<Arc<dyn RDD<FileEntry>>> {
        vec![]  // 根 RDD，无父依赖
    }
}

impl FileIndexRDD {
    /// 从目录构建初始 RDD（类 Spark sc.parallelize）
    pub fn from_dirs(dirs: Vec<std::path::PathBuf>) -> Self {
        let partitions = dirs.into_iter()
            .enumerate()
            .map(|(id, root)| Partition {
                id,
                root,
                depth: 255,
                created_at: std::time::SystemTime::now(),
                modified_at: None,
                last_event: None,
            })
            .collect();
        
        Self {
            partitions,
            lineage: Vec::new(),
            compute_fn: Some(Arc::new(|part| scan_partition(part))),
        }
    }
    
    /// 增量更新：应用事件，返回新 RDD（不可变）
    pub fn apply_event(&self, event: EventRecord) -> Self {
        let mut new_partitions = self.partitions.clone();
        let mut new_lineage = self.lineage.clone();
        new_lineage.push(event.clone());
        
        // 定位受影响分区（窄依赖）
        if let Some(part_id) = self.locate_partition(&event.path) {
            // 仅重算该分区（弹性恢复）
            new_partitions[part_id] = self.recompute_partition(part_id, &event);
        }
        
        Self {
            partitions: new_partitions,
            lineage: new_lineage,
            compute_fn: self.compute_fn.clone(),
        }
    }
    
    fn locate_partition(&self, path: &std::path::Path) -> Option<usize> {
        self.partitions.iter()
            .position(|p| path.starts_with(&p.root))
    }
    
    fn recompute_partition(&self, id: usize, event: &EventRecord) -> Partition {
        let mut part = self.partitions[id].clone();
        part.modified_at = Some(std::time::SystemTime::now());
        part.last_event = Some(event.clone());
        part
    }
}

/// 分区扫描实现（调用 ignore 逻辑）
pub fn scan_partition(part: &Partition) -> Vec<FileEntry> {
    use ignore::WalkBuilder;
    
    WalkBuilder::new(&part.root)
        .max_depth(Some(part.depth))
        .hidden(true)
        .build()
        .filter_map(|e| e.ok())
        .filter(|e| e.file_type().map(|ft| ft.is_file()).unwrap_or(false))
        .map(|e| {
            let metadata = e.metadata().ok();
            FileEntry {
                path: e.path().to_path_buf(),
                size: metadata.as_ref().map(|m| m.len()).unwrap_or(0),
                modified: metadata.and_then(|m| m.modified().ok()),
            }
        })
        .collect()
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct FileEntry {
    pub path: std::path::PathBuf,
    pub size: u64,
    pub modified: Option<std::time::SystemTime>,
}