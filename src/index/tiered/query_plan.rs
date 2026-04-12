use std::sync::Arc;

use crate::core::FileMeta;
use crate::query::dsl::CompiledQuery;
use crate::query::matcher::Matcher;

pub(super) enum QueryEvaluator {
    Legacy(Arc<dyn Matcher>),
    Compiled(CompiledQuery),
}

pub(super) struct QueryPlan {
    anchors: Vec<Arc<dyn Matcher>>,
    evaluator: QueryEvaluator,
}

impl QueryPlan {
    pub(super) fn compiled(compiled: CompiledQuery) -> Self {
        Self {
            anchors: compiled.anchors().to_vec(),
            evaluator: QueryEvaluator::Compiled(compiled),
        }
    }

    pub(super) fn legacy(matcher: Arc<dyn Matcher>) -> Self {
        Self {
            anchors: vec![matcher.clone()],
            evaluator: QueryEvaluator::Legacy(matcher),
        }
    }

    pub(super) fn anchors(&self) -> &[Arc<dyn Matcher>] {
        &self.anchors
    }

    pub(super) fn matches(&self, meta: &FileMeta) -> bool {
        match &self.evaluator {
            QueryEvaluator::Legacy(matcher) => matcher.matches(&meta.path.to_string_lossy()),
            QueryEvaluator::Compiled(compiled) => compiled.matches(meta),
        }
    }
}
