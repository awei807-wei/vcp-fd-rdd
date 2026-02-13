use crate::core::{FileEntry, EventRecord, EventType};
use crate::index::{L1Cache, L2Partition, L3Cold};
use crate::query::matcher::create_matcher;
use std::sync::atomic::{AtomicU64, Ordering};

/// 三级索引：L1热缓存 → L2分区RDD → L3冷扫描
pub struct TieredIndex {
    pub l1: L1Cache,
    pub l2: L2Partition,
    pub l3: L3Cold,
    pub event_seq: AtomicU64,
    pub roots: Vec<std::path::PathBuf>,
}

impl TieredIndex {
    pub fn new(l1: L1Cache, l2: L2Partition, l3: L3Cold, roots: Vec<std::path::PathBuf>) -> Self {
        Self {
            l1,
            l2,
            l3,
            event_seq: AtomicU64::new(0),
            roots,
        }
    }
    
    /// 查询入口：三级漏斗
    pub async fn query(&self, keyword: &str) -> Vec<FileEntry> {
        let matcher = create_matcher(keyword);

        // L1: 热缓存查询
        if let Some(results) = self.l1.query(matcher.as_ref()) {
            tracing::debug!("L1 hit: {} results", results.len());
            return results;
        }
        
        // L2: 分区RDD查询
        let l2_results = self.l2.query(matcher.as_ref()).await;
        
        if !l2_results.is_empty() {
            tracing::debug!("L2 hit: {} results", l2_results.len());
            // 回填L1
            for entry in &l2_results[..10.min(l2_results.len())] {
                self.l1.insert(entry.path.clone(), entry.clone());
            }
            return l2_results;
        }
        
        // L3: 冷扫描
        tracing::info!("L3 miss, triggering cold scan for: {} in roots: {:?}", keyword, self.roots);
        let l3_results = self.l3.scan(matcher.as_ref(), &self.roots).await;
        
        // 异步更新L2（简化实现：直接回填L1）
        if !l3_results.is_empty() {
            for entry in &l3_results[..10.min(l3_results.len())] {
                self.l1.insert(entry.path.clone(), entry.clone());
            }
        }
        
        l3_results
    }
    
    /// 事件驱动更新
    pub async fn apply_event(&self, event: notify::Event) {
        let seq = self.event_seq.fetch_add(1, Ordering::SeqCst);
        let timestamp = std::time::SystemTime::now();

        // 处理 Rename 事件的特殊逻辑：notify 可能将 Rename 拆分为两个事件，或者在一个事件中包含两个路径
        if matches!(event.kind, notify::EventKind::Modify(notify::event::ModifyKind::Name(_))) && event.paths.len() >= 2 {
            let from = event.paths[0].clone();
            let to = event.paths[1].clone();

            let record = EventRecord {
                seq,
                timestamp,
                event_type: EventType::Rename(from.clone()),
                path: to.clone(),
            };

            // L1 更新
            self.l1.remove(&from);
            self.l1.insert(to.clone(), FileEntry {
                path: to,
                size: 0,
                modified: Some(timestamp),
            });

            // L2 更新
            let rdd_lock = self.l2.rdd.clone();
            tokio::spawn(async move {
                let mut rdd = rdd_lock.write().await;
                *rdd = rdd.apply_event(record);
            });
            return;
        }

        // 处理普通事件
        let record = EventRecord {
            seq,
            timestamp,
            event_type: event.kind.into(),
            path: event.paths.first().cloned().unwrap_or_default(),
        };
        
        // L1立即更新
        match record.event_type {
            EventType::Create | EventType::Modify => {
                self.l1.insert(record.path.clone(), FileEntry {
                    path: record.path.clone(),
                    size: 0,
                    modified: Some(record.timestamp),
                });
            }
            EventType::Delete => {
                self.l1.remove(&record.path);
            }
            EventType::Rename(ref from) => {
                self.l1.remove(from);
                self.l1.insert(record.path.clone(), FileEntry {
                    path: record.path.clone(),
                    size: 0,
                    modified: Some(record.timestamp),
                });
            }
            _ => {}
        }
        
        // L2 RDD增量更新
        let rdd_lock = self.l2.rdd.clone();
        tokio::spawn(async move {
            let mut rdd = rdd_lock.write().await;
            *rdd = rdd.apply_event(record);
        });
    }
}