use serde::{Deserialize, Serialize};

use super::policy::PolicyParams;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RunMetrics {
    pub events_total: usize,
    pub detected: usize,
    pub missed: usize,
    pub sla_secs: u64,
    pub sla_met: usize,
    pub sla_rate: f64,
    pub avg_detect_secs: f64,
    pub p50_detect_secs: u64,
    pub p95_detect_secs: u64,
    pub p99_detect_secs: u64,
    pub max_detect_secs: u64,
    pub watch_cost_peak: u64,
    pub scan_rounds: u64,
    pub scanned_dirs: u64,
    pub scanned_files: u64,
    pub cpu_units: u64,
    pub io_units: u64,
    pub memory_budget_hit: bool,
    pub cpu_budget_hit: bool,
    pub io_budget_hit: bool,
    pub objective_score: f64,
    pub l0_dirs: u64,
    pub l1_dirs: u64,
    pub l2_dirs: u64,
    pub l3_dirs: u64,
    pub promotion_count: u64,
    pub replacement_count: u64,
    pub budget_blocked_count: u64,
    pub query_hits: u64,
    pub query_stale_hits: u64,
    pub query_misses: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RunReport {
    pub rank: usize,
    pub policy: PolicyParams,
    pub metrics: RunMetrics,
}

pub fn summarize(
    policy: &PolicyParams,
    detected_latencies: &[u64],
    events_total: usize,
    watch_cost_peak: u64,
    scan_rounds: u64,
    scanned_dirs: u64,
    scanned_files: u64,
    tier_counts: (u64, u64, u64, u64),
    promotion_count: u64,
    replacement_count: u64,
    budget_blocked_count: u64,
    query_hits: u64,
    query_stale_hits: u64,
    query_misses: u64,
) -> RunMetrics {
    let mut latencies = detected_latencies.to_vec();
    latencies.sort_unstable();
    let detected = latencies.len();
    let missed = events_total.saturating_sub(detected);
    let sla_met = latencies
        .iter()
        .filter(|latency| **latency <= policy.sla_secs)
        .count();
    let sla_rate = if events_total == 0 {
        1.0
    } else {
        sla_met as f64 / events_total as f64
    };
    let avg_detect_secs = if detected == 0 {
        policy.sla_secs.saturating_mul(10) as f64
    } else {
        latencies.iter().sum::<u64>() as f64 / detected as f64
    };
    let p50_detect_secs = percentile(&latencies, 0.50);
    let p95_detect_secs = percentile(&latencies, 0.95);
    let p99_detect_secs = percentile(&latencies, 0.99);
    let max_detect_secs = latencies.last().copied().unwrap_or(0);
    let cpu_units = scanned_dirs
        .saturating_mul(2)
        .saturating_add(scanned_files / 16);
    let io_units = scanned_files;
    let memory_budget_hit = watch_cost_peak > policy.memory_budget_units;
    let cpu_budget_hit = cpu_units > policy.cpu_budget_units;
    let io_budget_hit = io_units > policy.io_budget_units;
    let resource_penalty = budget_penalty(watch_cost_peak, policy.memory_budget_units)
        + budget_penalty(cpu_units, policy.cpu_budget_units)
        + budget_penalty(io_units, policy.io_budget_units);
    let latency_penalty = avg_detect_secs / policy.sla_secs.max(1) as f64;
    let missed_penalty = if events_total == 0 {
        0.0
    } else {
        missed as f64 / events_total as f64
    };
    let hard_budget_penalty = [memory_budget_hit, cpu_budget_hit, io_budget_hit]
        .into_iter()
        .filter(|hit| *hit)
        .count() as f64
        * 200.0;
    let objective_score = sla_rate * 1_000.0
        - latency_penalty * 25.0
        - resource_penalty * 250.0
        - hard_budget_penalty
        - missed_penalty * 500.0;

    RunMetrics {
        events_total,
        detected,
        missed,
        sla_secs: policy.sla_secs,
        sla_met,
        sla_rate,
        avg_detect_secs,
        p50_detect_secs,
        p95_detect_secs,
        p99_detect_secs,
        max_detect_secs,
        watch_cost_peak,
        scan_rounds,
        scanned_dirs,
        scanned_files,
        cpu_units,
        io_units,
        memory_budget_hit,
        cpu_budget_hit,
        io_budget_hit,
        objective_score,
        l0_dirs: tier_counts.0,
        l1_dirs: tier_counts.1,
        l2_dirs: tier_counts.2,
        l3_dirs: tier_counts.3,
        promotion_count,
        replacement_count,
        budget_blocked_count,
        query_hits,
        query_stale_hits,
        query_misses,
    }
}

fn percentile(sorted: &[u64], p: f64) -> u64 {
    if sorted.is_empty() {
        return 0;
    }
    let idx = ((sorted.len() - 1) as f64 * p.clamp(0.0, 1.0)).round() as usize;
    sorted[idx.min(sorted.len() - 1)]
}

fn budget_penalty(value: u64, budget: u64) -> f64 {
    if value <= budget {
        0.0
    } else {
        (value as f64 / budget.max(1) as f64) - 1.0
    }
}
