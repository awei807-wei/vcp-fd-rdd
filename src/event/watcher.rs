use notify::{Watcher, RecursiveMode, Config};
use std::sync::Arc;
use crate::index::TieredIndex;

pub struct EventWatcher {
    pub index: Arc<TieredIndex>,
}

impl EventWatcher {
    pub fn new(index: Arc<TieredIndex>) -> Self {
        Self { index }
    }

    pub async fn watch(&self, roots: Vec<std::path::PathBuf>) -> anyhow::Result<()> {
        let index = self.index.clone();
        let (tx, mut rx) = tokio::sync::mpsc::channel(100);

        let mut watcher = notify::RecommendedWatcher::new(
            move |res: notify::Result<notify::Event>| {
                if let Ok(event) = res {
                    let _ = tx.blocking_send(event);
                }
            },
            Config::default(),
        )?;

        for root in roots {
            watcher.watch(&root, RecursiveMode::Recursive)?;
        }

        tokio::spawn(async move {
            while let Some(event) = rx.recv().await {
                index.apply_event(event).await;
            }
        });

        // 保持 watcher 存活
        Box::leak(Box::new(watcher));
        
        Ok(())
    }
}