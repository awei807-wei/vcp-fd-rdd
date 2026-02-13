use std::sync::Arc;
use tokio::sync::RwLock;
use crate::core::rdd::{RDD, FileIndexRDD, FileEntry};

/// L2: 温索引（分区 RDD）
pub struct L2Partition {
    pub rdd: Arc<RwLock<FileIndexRDD>>,
}

impl L2Partition {
    pub fn new(rdd: FileIndexRDD) -> Self {
        Self {
            rdd: Arc::new(RwLock::new(rdd)),
        }
    }

    pub async fn query(&self, keyword: &str) -> Vec<FileEntry> {
        let rdd = self.rdd.read().await;
        rdd.collect()
            .into_iter()
            .filter(|e| e.path.to_string_lossy().contains(keyword))
            .collect()
    }
}