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
    pub promotions: u64,
    pub demotions: u64,
    pub replacements: u64,
    pub promotion_budget_blocked: u64,
    pub final_l0_dirs: usize,
    pub final_l1_dirs: usize,
    pub final_l2_dirs: usize,
    pub final_l3_dirs: usize,
    pub cpu_units: u64,
    pub io_units: u64,
    pub memory_budget_hit: bool,
    pub cpu_budget_hit: bool,
    pub io_budget_hit: bool,
    pub objective_score: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RunReport {
    pub rank: usize,
    pub policy: PolicyParams,
    pub metrics: RunMetrics,
}

#[derive(Debug, Clone, Copy, Default)]
pub struct StrategyCounters {
    pub promotions: u64,
    pub demotions: u64,
    pub replacements: u64,
    pub promotion_budget_blocked: u64,
    pub final_l0_dirs: usize,
    pub final_l1_dirs: usize,
    pub final_l2_dirs: usize,
    pub final_l3_dirs: usize,
}

pub fn summarize(
    policy: &PolicyParams,
    detected_latencies: &[u64],
    events_total: usize,
    watch_cost_peak: u64,
    scan_rounds: u64,
    scanned_dirs: u64,
    scanned_files: u64,
    strategy: StrategyCounters,
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
        promotions: strategy.promotions,
        demotions: strategy.demotions,
        replacements: strategy.replacements,
        promotion_budget_blocked: strategy.promotion_budget_blocked,
        final_l0_dirs: strategy.final_l0_dirs,
        final_l1_dirs: strategy.final_l1_dirs,
        final_l2_dirs: strategy.final_l2_dirs,
        final_l3_dirs: strategy.final_l3_dirs,
        cpu_units,
        io_units,
        memory_budget_hit,
        cpu_budget_hit,
        io_budget_hit,
        objective_score,
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
