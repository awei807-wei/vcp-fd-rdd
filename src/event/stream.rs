use std::sync::Arc;
use crate::index::TieredIndex;
use crate::event::watcher::EventWatcher;

/// 事件流（类 Spark Streaming DStream）
pub struct EventStream {
    pub index: Arc<TieredIndex>,
}

impl EventStream {
    pub fn new(index: Arc<TieredIndex>) -> Self {
        Self { index }
    }

    pub async fn start_watcher(&self, roots: Vec<std::path::PathBuf>) -> anyhow::Result<()> {
        let watcher = EventWatcher::new(self.index.clone());
        watcher.watch(roots).await
    }
}