use serde::{Deserialize, Serialize};

use crate::config::{L3ScanPolicy, DEFAULT_L3_SCAN_INTERVAL_SECS};

use super::world::{DirNode, World};

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "UPPERCASE")]
pub enum SimTier {
    L0,
    L1,
    L2,
    L3,
}

impl SimTier {
    pub fn scan_interval_secs(self, policy: &PolicyParams) -> u64 {
        match self {
            Self::L0 => u64::MAX,
            Self::L1 => policy.l1_scan_interval_secs,
            Self::L2 => policy.l2_scan_interval_secs,
            Self::L3 => {
                if policy.l3_scan_policy.schedules_periodic_scan() {
                    policy.l3_scan_interval_secs
                } else {
                    u64::MAX
                }
            }
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct ScoreWeights {
    pub recent_event_count: f64,
    pub event_recency_decay: f64,
    pub importance: f64,
    pub miss_penalty: f64,
    pub watch_cost: f64,
    pub scan_cost: f64,
}

impl Default for ScoreWeights {
    fn default() -> Self {
        Self {
            recent_event_count: 1.0,
            event_recency_decay: 0.85,
            importance: 2.0,
            miss_penalty: 1.4,
            watch_cost: 0.025,
            scan_cost: 0.004,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct PolicyParams {
    pub name: String,
    pub max_watch_dirs: u32,
    pub sla_secs: u64,
    pub l0_idle_ttl_secs: u64,
    pub l1_scan_interval_secs: u64,
    pub l2_scan_interval_secs: u64,
    #[serde(default = "default_l3_scan_policy")]
    pub l3_scan_policy: L3ScanPolicy,
    #[serde(default = "default_l3_scan_interval_secs")]
    pub l3_scan_interval_secs: u64,
    pub l1_empty_scans_to_l2: u32,
    pub l2_empty_scans_to_l3: u32,
    pub per_round_max_dirs: usize,
    pub per_round_max_files: u64,
    pub per_round_max_ms: u64,
    pub memory_budget_units: u64,
    pub cpu_budget_units: u64,
    pub io_budget_units: u64,
    pub weights: ScoreWeights,
}

fn default_l3_scan_policy() -> L3ScanPolicy {
    L3ScanPolicy::Interval
}

fn default_l3_scan_interval_secs() -> u64 {
    DEFAULT_L3_SCAN_INTERVAL_SECS
}

impl Default for PolicyParams {
    fn default() -> Self {
        Self {
            name: "tiered-default".to_string(),
            max_watch_dirs: 192,
            sla_secs: 30,
            l0_idle_ttl_secs: 600,
            l1_scan_interval_secs: 20,
            l2_scan_interval_secs: 180,
            l3_scan_policy: L3ScanPolicy::Interval,
            l3_scan_interval_secs: DEFAULT_L3_SCAN_INTERVAL_SECS,
            l1_empty_scans_to_l2: 4,
            l2_empty_scans_to_l3: 3,
            per_round_max_dirs: 16,
            per_round_max_files: 4_000,
            per_round_max_ms: 20,
            memory_budget_units: 256,
            cpu_budget_units: 50_000,
            io_budget_units: 250_000,
            weights: ScoreWeights::default(),
        }
    }
}

impl PolicyParams {
    pub fn sanitize(mut self) -> Self {
        self.max_watch_dirs = self.max_watch_dirs.max(1);
        self.sla_secs = self.sla_secs.max(1);
        self.l0_idle_ttl_secs = self.l0_idle_ttl_secs.max(1);
        self.l1_scan_interval_secs = self.l1_scan_interval_secs.max(1);
        self.l2_scan_interval_secs = self
            .l2_scan_interval_secs
            .max(self.l1_scan_interval_secs.saturating_add(1));
        self.l3_scan_interval_secs = self.l3_scan_interval_secs.max(1);
        self.l1_empty_scans_to_l2 = self.l1_empty_scans_to_l2.max(1);
        self.l2_empty_scans_to_l3 = self.l2_empty_scans_to_l3.max(1);
        self.per_round_max_dirs = self.per_round_max_dirs.max(1);
        self.per_round_max_files = self.per_round_max_files.max(1);
        self.per_round_max_ms = self.per_round_max_ms.max(1);
        self.memory_budget_units = self.memory_budget_units.max(1);
        self.cpu_budget_units = self.cpu_budget_units.max(1);
        self.io_budget_units = self.io_budget_units.max(1);
        self
    }

    pub fn score_dir(&self, dir: &DirNode, state: &DirPolicyState, now: u64) -> f64 {
        let recency = if state.last_event_at == 0 {
            0.0
        } else {
            let age = now.saturating_sub(state.last_event_at) as f64;
            (-age / self.l0_idle_ttl_secs.max(1) as f64).exp()
        };
        let miss_penalty = (dir.importance * dir.base_event_rate * self.sla_secs as f64).sqrt();
        self.weights.recent_event_count * state.recent_events as f64
            + self.weights.event_recency_decay * recency
            + self.weights.importance * dir.importance
            + self.weights.miss_penalty * miss_penalty
            - self.weights.watch_cost * dir.watch_cost as f64
            - self.weights.scan_cost * dir.scan_cost as f64
    }

    pub fn initial_score(&self, dir: &DirNode) -> f64 {
        let state = DirPolicyState {
            recent_events: 0,
            last_event_at: 0,
        };
        self.score_dir(dir, &state, 0)
    }
}

#[derive(Debug, Clone, Copy)]
pub struct DirPolicyState {
    pub recent_events: u32,
    pub last_event_at: u64,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn l3_scan_interval_honors_explicit_policy() {
        let interval = PolicyParams {
            l2_scan_interval_secs: 10,
            l3_scan_policy: L3ScanPolicy::Interval,
            l3_scan_interval_secs: 123,
            ..PolicyParams::default()
        }
        .sanitize();
        assert_eq!(SimTier::L3.scan_interval_secs(&interval), 123);

        let validate_on_query = PolicyParams {
            l3_scan_policy: L3ScanPolicy::ValidateOnQuery,
            l3_scan_interval_secs: 123,
            ..PolicyParams::default()
        }
        .sanitize();
        assert_eq!(SimTier::L3.scan_interval_secs(&validate_on_query), u64::MAX);

        let disabled = PolicyParams {
            l3_scan_policy: L3ScanPolicy::Disabled,
            l3_scan_interval_secs: 123,
            ..PolicyParams::default()
        }
        .sanitize();
        assert_eq!(SimTier::L3.scan_interval_secs(&disabled), u64::MAX);
    }
}

pub fn initial_l0_candidates(world: &World, policy: &PolicyParams) -> Vec<usize> {
    let mut scored = world
        .dirs
        .iter()
        .map(|dir| (policy.initial_score(dir), dir.id))
        .collect::<Vec<_>>();
    scored.sort_by(|a, b| b.0.total_cmp(&a.0).then_with(|| a.1.cmp(&b.1)));
    scored.into_iter().map(|(_, id)| id).collect()
}
