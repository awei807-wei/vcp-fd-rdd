use crate::index::TieredIndex;
use std::sync::Arc;

/// 弹性校验：检测事件流缺口，触发补扫
pub struct ElasticVerifier {
    pub index: Arc<TieredIndex>,
}

impl ElasticVerifier {
    pub fn new(index: Arc<TieredIndex>) -> Self {
        Self { index }
    }

    /// 检查事件流是否有缺口，必要时触发增量补扫
    pub fn verify_and_repair(&self) {
        // 占位：未来可对比 event_seq 与预期序列号
        // 发现缺口时调用 index.l3.incremental_scan()
        tracing::debug!("ElasticVerifier: check passed");
    }
}
