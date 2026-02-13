use std::sync::Arc;
use crate::core::rdd::RDD;

/// DAG 调度器：管理 RDD 依赖关系与执行计划
pub struct DAGScheduler<T: Send + Sync + 'static> {
    pub final_rdd: Arc<dyn RDD<T>>,
}

impl<T: Send + Sync + 'static> DAGScheduler<T> {
    pub fn new(rdd: Arc<dyn RDD<T>>) -> Self {
        Self { final_rdd: rdd }
    }

    /// 获取执行阶段（简单实现：目前仅支持窄依赖）
    pub fn get_stages(&self) -> Vec<Arc<dyn RDD<T>>> {
        let mut stages = Vec::new();
        self.traverse_dependencies(self.final_rdd.clone(), &mut stages);
        stages
    }

    fn traverse_dependencies(&self, rdd: Arc<dyn RDD<T>>, stages: &mut Vec<Arc<dyn RDD<T>>>) {
        for dep in rdd.dependencies() {
            self.traverse_dependencies(dep, stages);
        }
        stages.push(rdd);
    }
}