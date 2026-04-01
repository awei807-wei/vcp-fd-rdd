use std::collections::{BTreeSet, HashMap};

/// DAG 中的单个阶段。
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DagStage<T> {
    pub id: String,
    pub value: T,
    pub deps: Vec<String>,
}

/// DAG 规划错误。
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum DagError {
    DuplicateStage(String),
    MissingDependency { stage: String, dependency: String },
    Cycle(Vec<String>),
}

/// DAG 调度器：
/// - 维护阶段与依赖关系
/// - 产出稳定、可测试的拓扑顺序与并行执行层
pub struct DAGScheduler<T> {
    stages: HashMap<String, DagStage<T>>,
}

impl<T> Default for DAGScheduler<T> {
    fn default() -> Self {
        Self::new()
    }
}

impl<T> DAGScheduler<T> {
    pub fn new() -> Self {
        Self {
            stages: HashMap::new(),
        }
    }

    pub fn is_empty(&self) -> bool {
        self.stages.is_empty()
    }

    pub fn len(&self) -> usize {
        self.stages.len()
    }

    pub fn add_stage<S, I, D>(&mut self, id: S, value: T, deps: I) -> Result<(), DagError>
    where
        S: Into<String>,
        I: IntoIterator<Item = D>,
        D: Into<String>,
    {
        let id = id.into();
        if self.stages.contains_key(&id) {
            return Err(DagError::DuplicateStage(id));
        }

        let deps: BTreeSet<String> = deps.into_iter().map(Into::into).collect();
        self.stages.insert(
            id.clone(),
            DagStage {
                id,
                value,
                deps: deps.into_iter().collect(),
            },
        );
        Ok(())
    }

    pub fn stage(&self, id: &str) -> Option<&DagStage<T>> {
        self.stages.get(id)
    }

    pub fn topological_order(&self) -> Result<Vec<&DagStage<T>>, DagError> {
        let sorted_ids = self.resolve_order()?;
        Ok(sorted_ids
            .into_iter()
            .filter_map(|id| self.stages.get(&id))
            .collect())
    }

    pub fn execution_layers(&self) -> Result<Vec<Vec<&DagStage<T>>>, DagError> {
        let (mut indegree, reverse_edges) = self.build_graph()?;
        let mut ready: BTreeSet<String> = indegree
            .iter()
            .filter(|(_, degree)| **degree == 0)
            .map(|(id, _)| id.clone())
            .collect();
        let mut layers: Vec<Vec<&DagStage<T>>> = Vec::new();
        let mut resolved = 0usize;

        while !ready.is_empty() {
            let current: Vec<String> = ready.iter().cloned().collect();
            ready.clear();
            resolved += current.len();

            let mut layer: Vec<&DagStage<T>> = Vec::with_capacity(current.len());
            for id in &current {
                if let Some(stage) = self.stages.get(id) {
                    layer.push(stage);
                }
                if let Some(children) = reverse_edges.get(id) {
                    for child in children {
                        if let Some(degree) = indegree.get_mut(child) {
                            *degree = degree.saturating_sub(1);
                            if *degree == 0 {
                                ready.insert(child.clone());
                            }
                        }
                    }
                }
            }
            layers.push(layer);
        }

        if resolved != self.stages.len() {
            return Err(DagError::Cycle(Self::unresolved_nodes(&indegree)));
        }

        Ok(layers)
    }

    fn resolve_order(&self) -> Result<Vec<String>, DagError> {
        let (mut indegree, reverse_edges) = self.build_graph()?;
        let mut ready: BTreeSet<String> = indegree
            .iter()
            .filter(|(_, degree)| **degree == 0)
            .map(|(id, _)| id.clone())
            .collect();
        let mut order: Vec<String> = Vec::with_capacity(self.stages.len());

        while let Some(id) = ready.iter().next().cloned() {
            ready.remove(&id);
            order.push(id.clone());

            if let Some(children) = reverse_edges.get(&id) {
                for child in children {
                    if let Some(degree) = indegree.get_mut(child) {
                        *degree = degree.saturating_sub(1);
                        if *degree == 0 {
                            ready.insert(child.clone());
                        }
                    }
                }
            }
        }

        if order.len() != self.stages.len() {
            return Err(DagError::Cycle(Self::unresolved_nodes(&indegree)));
        }

        Ok(order)
    }

    fn build_graph(
        &self,
    ) -> Result<(HashMap<String, usize>, HashMap<String, Vec<String>>), DagError> {
        let mut indegree: HashMap<String, usize> = HashMap::with_capacity(self.stages.len());
        let mut reverse_edges: HashMap<String, Vec<String>> = HashMap::new();

        for stage in self.stages.values() {
            indegree.insert(stage.id.clone(), stage.deps.len());
        }

        for stage in self.stages.values() {
            for dep in &stage.deps {
                if !self.stages.contains_key(dep) {
                    return Err(DagError::MissingDependency {
                        stage: stage.id.clone(),
                        dependency: dep.clone(),
                    });
                }
                reverse_edges
                    .entry(dep.clone())
                    .or_default()
                    .push(stage.id.clone());
            }
        }

        for children in reverse_edges.values_mut() {
            children.sort();
        }

        Ok((indegree, reverse_edges))
    }

    fn unresolved_nodes(indegree: &HashMap<String, usize>) -> Vec<String> {
        let mut unresolved: Vec<String> = indegree
            .iter()
            .filter(|(_, degree)| **degree > 0)
            .map(|(id, _)| id.clone())
            .collect();
        unresolved.sort();
        unresolved
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn topological_order_is_stable() {
        let mut dag = DAGScheduler::new();
        dag.add_stage("scan", "scan", std::iter::empty::<&str>())
            .unwrap();
        dag.add_stage("hydrate", "hydrate", ["scan"]).unwrap();
        dag.add_stage("verify", "verify", ["scan"]).unwrap();
        dag.add_stage("publish", "publish", ["hydrate", "verify"])
            .unwrap();

        let order = dag
            .topological_order()
            .unwrap()
            .into_iter()
            .map(|s| s.id.as_str())
            .collect::<Vec<_>>();
        assert_eq!(order, vec!["scan", "hydrate", "verify", "publish"]);
    }

    #[test]
    fn execution_layers_group_parallel_stages() {
        let mut dag = DAGScheduler::new();
        dag.add_stage("scan", "scan", std::iter::empty::<&str>())
            .unwrap();
        dag.add_stage("hydrate", "hydrate", ["scan"]).unwrap();
        dag.add_stage("verify", "verify", ["scan"]).unwrap();
        dag.add_stage("publish", "publish", ["hydrate", "verify"])
            .unwrap();

        let layers = dag
            .execution_layers()
            .unwrap()
            .into_iter()
            .map(|layer| {
                layer
                    .into_iter()
                    .map(|stage| stage.id.clone())
                    .collect::<Vec<_>>()
            })
            .collect::<Vec<_>>();
        assert_eq!(
            layers,
            vec![
                vec!["scan".to_string()],
                vec!["hydrate".to_string(), "verify".to_string()],
                vec!["publish".to_string()]
            ]
        );
    }

    #[test]
    fn missing_dependency_returns_error() {
        let mut dag = DAGScheduler::new();
        dag.add_stage("publish", "publish", ["verify"]).unwrap();

        assert_eq!(
            dag.topological_order(),
            Err(DagError::MissingDependency {
                stage: "publish".to_string(),
                dependency: "verify".to_string(),
            })
        );
    }

    #[test]
    fn cycle_returns_error() {
        let mut dag = DAGScheduler::new();
        dag.add_stage("a", "a", ["b"]).unwrap();
        dag.add_stage("b", "b", ["a"]).unwrap();

        assert_eq!(
            dag.execution_layers(),
            Err(DagError::Cycle(vec!["a".to_string(), "b".to_string()]))
        );
    }
}
