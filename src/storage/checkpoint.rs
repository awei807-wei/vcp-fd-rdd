use serde::{Serialize, Deserialize};
use crate::core::{FileIndexRDD, FileEntry};
use std::path::Path;
use tokio::fs;

#[derive(Serialize, Deserialize)]
pub struct Checkpoint {
    pub rdd: FileIndexRDD,
    pub hot_cache: Vec<(std::path::PathBuf, FileEntry)>,
    pub last_seq: u64,
}

impl Checkpoint {
    pub async fn save(&self, path: &Path) -> anyhow::Result<()> {
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent).await?;
        }
        let data = bincode::serialize(self)?;
        fs::write(path, data).await?;
        Ok(())
    }

    pub async fn load(path: &Path) -> anyhow::Result<Self> {
        let data = fs::read(path).await?;
        let checkpoint = bincode::deserialize(&data)?;
        Ok(checkpoint)
    }
}