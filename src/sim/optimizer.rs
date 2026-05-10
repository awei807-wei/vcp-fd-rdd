use std::fs;
use std::path::{Path, PathBuf};

use anyhow::Context;

use serde::{Deserialize, Serialize};

use crate::config::{L3ScanPolicy, DEFAULT_L3_SCAN_INTERVAL_SECS};

use super::metrics::{RunMetrics, RunReport};
use super::policy::PolicyParams;
use super::rng::Rng64;
use super::simulator::run_simulation;
use super::world::{generate_world, WorkloadConfig, WorkloadProfile, WorldSummary};

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct OptimizerConfig {
    pub workload: WorkloadConfig,
    pub policy: PolicyParams,
    pub top_n: usize,
    pub generations: usize,
    pub population: usize,
    pub patience: usize,
    pub min_delta: f64,
    pub robust_profiles: bool,
    pub checkpoint_path: Option<PathBuf>,
    pub resume_path: Option<PathBuf>,
}

impl Default for OptimizerConfig {
    fn default() -> Self {
        Self {
            workload: WorkloadConfig::default(),
            policy: PolicyParams::default(),
            top_n: 10,
            generations: 12,
            population: 24,
            patience: 5,
            min_delta: 0.5,
            robust_profiles: true,
            checkpoint_path: None,
            resume_path: None,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BenchmarkReport {
    pub mode: String,
    pub world: WorldSummary,
    pub best: Option<RunReport>,
    pub baseline: Option<RunReport>,
    pub convergence: Option<ConvergenceReport>,
    pub recommendation: Option<TieredWatchRecommendation>,
    pub runs: Vec<RunReport>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConvergenceReport {
    pub trials: usize,
    pub generations_completed: usize,
    pub current_generation: usize,
    pub current_generation_trials: usize,
    pub phase: String,
    pub converged: bool,
    pub stale_generations: usize,
    pub best_generation: usize,
    pub best_score: f64,
    pub baseline_score: f64,
    pub improvement_pct: f64,
    pub trace: Vec<GenerationTrace>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GenerationTrace {
    pub generation: usize,
    pub trials: usize,
    pub best_score: f64,
    pub best_sla_rate: f64,
    pub best_watch_cost_peak: u64,
    pub stale_generations: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TieredWatchRecommendation {
    pub watch_mode: String,
    pub max_watch_dirs: u32,
    pub scan_items_per_sec: usize,
    pub scan_ms_per_tick: u64,
    pub l0_idle_ttl_secs: u64,
    pub l1_scan_interval_secs: u64,
    pub l2_scan_interval_secs: u64,
    #[serde(default = "default_l3_scan_policy")]
    pub l3_scan_policy: L3ScanPolicy,
    #[serde(default = "default_l3_scan_interval_secs")]
    pub l3_scan_interval_secs: u64,
    pub l1_empty_scans_to_l2: u32,
    pub l2_empty_scans_to_l3: u32,
}

pub const SIM_ONLY_IGNORED_FIELDS: &[&str] = &[
    "weights",
    "per_round_max_dirs",
    "per_round_max_files",
    "per_round_max_ms",
];

fn default_l3_scan_policy() -> L3ScanPolicy {
    L3ScanPolicy::Interval
}

fn default_l3_scan_interval_secs() -> u64 {
    DEFAULT_L3_SCAN_INTERVAL_SECS
}

#[derive(Debug, Clone, Serialize)]
struct TieredWatchConfigPatch {
    watch_mode: String,
    tiered_watch: TieredWatchConfigPatchTable,
}

#[derive(Debug, Clone, Serialize)]
struct TieredWatchConfigPatchTable {
    max_watch_dirs: u32,
    scan_items_per_sec: usize,
    scan_ms_per_tick: u64,
    l0_idle_ttl_secs: u64,
    l1_scan_interval_secs: u64,
    l2_scan_interval_secs: u64,
    l3_scan_policy: L3ScanPolicy,
    l3_scan_interval_secs: u64,
    l1_empty_scans_to_l2: u32,
    l2_empty_scans_to_l3: u32,
}

pub fn tiered_watch_config_patch_toml_from_report(
    report: &BenchmarkReport,
) -> anyhow::Result<String> {
    let recommendation = tiered_watch_recommendation_from_report(report)?;

    tiered_watch_config_patch_toml(&recommendation)
}

pub fn tiered_watch_config_patch_toml_from_reports(
    reports: &[BenchmarkReport],
) -> anyhow::Result<String> {
    let recommendation = conservative_tiered_watch_recommendation_from_reports(reports)?;
    tiered_watch_config_patch_toml(&recommendation)
}

pub fn conservative_tiered_watch_recommendation_from_reports(
    reports: &[BenchmarkReport],
) -> anyhow::Result<TieredWatchRecommendation> {
    if reports.is_empty() {
        anyhow::bail!("at least one benchmark report is required");
    }

    let mut recommendations = reports
        .iter()
        .map(tiered_watch_recommendation_from_report)
        .collect::<anyhow::Result<Vec<_>>>()?;
    let mut aggregate = recommendations
        .pop()
        .expect("reports is non-empty, so recommendations is non-empty");

    for recommendation in recommendations {
        aggregate.watch_mode = "tiered".to_string();
        aggregate.max_watch_dirs = aggregate
            .max_watch_dirs
            .min(recommendation.max_watch_dirs)
            .max(1);
        aggregate.scan_items_per_sec = aggregate
            .scan_items_per_sec
            .min(recommendation.scan_items_per_sec)
            .max(1);
        aggregate.scan_ms_per_tick = aggregate
            .scan_ms_per_tick
            .min(recommendation.scan_ms_per_tick)
            .max(1);
        aggregate.l0_idle_ttl_secs = aggregate
            .l0_idle_ttl_secs
            .min(recommendation.l0_idle_ttl_secs)
            .max(1);
        aggregate.l1_scan_interval_secs = aggregate
            .l1_scan_interval_secs
            .max(recommendation.l1_scan_interval_secs)
            .max(1);
        aggregate.l2_scan_interval_secs = aggregate
            .l2_scan_interval_secs
            .max(recommendation.l2_scan_interval_secs)
            .max(aggregate.l1_scan_interval_secs.saturating_add(1));
        aggregate.l3_scan_policy =
            conservative_l3_scan_policy(aggregate.l3_scan_policy, recommendation.l3_scan_policy);
        aggregate.l3_scan_interval_secs = aggregate
            .l3_scan_interval_secs
            .max(recommendation.l3_scan_interval_secs)
            .max(1);
        aggregate.l1_empty_scans_to_l2 = aggregate
            .l1_empty_scans_to_l2
            .min(recommendation.l1_empty_scans_to_l2)
            .max(1);
        aggregate.l2_empty_scans_to_l3 = aggregate
            .l2_empty_scans_to_l3
            .min(recommendation.l2_empty_scans_to_l3)
            .max(1);
    }

    Ok(aggregate)
}

pub fn tiered_watch_recommendation_from_report(
    report: &BenchmarkReport,
) -> anyhow::Result<TieredWatchRecommendation> {
    report
        .recommendation
        .clone()
        .or_else(|| {
            report
                .best
                .as_ref()
                .map(|run| recommendation_from_policy(&run.policy))
        })
        .or_else(|| {
            report
                .baseline
                .as_ref()
                .map(|run| recommendation_from_policy(&run.policy))
        })
        .ok_or_else(|| anyhow::anyhow!("report has no recommendation, best run, or baseline"))
}

fn conservative_l3_scan_policy(left: L3ScanPolicy, right: L3ScanPolicy) -> L3ScanPolicy {
    match (left, right) {
        (L3ScanPolicy::Disabled, _) | (_, L3ScanPolicy::Disabled) => L3ScanPolicy::Disabled,
        (L3ScanPolicy::ValidateOnQuery, _) | (_, L3ScanPolicy::ValidateOnQuery) => {
            L3ScanPolicy::ValidateOnQuery
        }
        _ => L3ScanPolicy::Interval,
    }
}

pub fn tiered_watch_config_patch_toml(
    recommendation: &TieredWatchRecommendation,
) -> anyhow::Result<String> {
    let patch = TieredWatchConfigPatch {
        watch_mode: recommendation.watch_mode.clone(),
        tiered_watch: TieredWatchConfigPatchTable {
            max_watch_dirs: recommendation.max_watch_dirs,
            scan_items_per_sec: recommendation.scan_items_per_sec,
            scan_ms_per_tick: recommendation.scan_ms_per_tick,
            l0_idle_ttl_secs: recommendation.l0_idle_ttl_secs,
            l1_scan_interval_secs: recommendation.l1_scan_interval_secs,
            l2_scan_interval_secs: recommendation.l2_scan_interval_secs,
            l3_scan_policy: recommendation.l3_scan_policy,
            l3_scan_interval_secs: recommendation.l3_scan_interval_secs,
            l1_empty_scans_to_l2: recommendation.l1_empty_scans_to_l2,
            l2_empty_scans_to_l3: recommendation.l2_empty_scans_to_l3,
        },
    };
    let ignored = SIM_ONLY_IGNORED_FIELDS.join(", ");
    let mut text = String::new();
    text.push_str("# fd-rdd-sim generated runtime config patch.\n");
    text.push_str("# Review before merging into ~/.config/fd-rdd/config.toml.\n");
    text.push_str(&format!("# Sim-only policy fields ignored: {ignored}.\n\n"));
    text.push_str(&toml::to_string_pretty(&patch)?);
    Ok(text)
}

pub fn write_existing_parent(path: &Path, contents: &str) -> anyhow::Result<()> {
    if let Some(parent) = path
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty())
    {
        let metadata = fs::metadata(parent)
            .with_context(|| format!("output directory does not exist: {}", parent.display()))?;
        if !metadata.is_dir() {
            anyhow::bail!("output parent is not a directory: {}", parent.display());
        }
        if !directory_has_write_bit(&metadata) {
            anyhow::bail!("output directory is not writable: {}", parent.display());
        }
    }

    fs::write(path, contents).with_context(|| format!("failed to write {}", path.display()))
}

#[cfg(unix)]
fn directory_has_write_bit(metadata: &fs::Metadata) -> bool {
    use std::os::unix::fs::PermissionsExt;

    metadata.permissions().mode() & 0o222 != 0
}

#[cfg(not(unix))]
fn directory_has_write_bit(metadata: &fs::Metadata) -> bool {
    !metadata.permissions().readonly()
}

pub fn single_report(config: OptimizerConfig) -> BenchmarkReport {
    let world = generate_world(config.workload);
    let policy = config.policy.sanitize();
    let metrics = run_simulation(&world, &policy);
    let run = RunReport {
        rank: 1,
        policy,
        metrics,
    };
    BenchmarkReport {
        mode: "single-baseline".to_string(),
        world: world.summary(),
        best: None,
        baseline: Some(run.clone()),
        convergence: None,
        recommendation: None,
        runs: vec![run],
    }
}

pub fn grid_report(config: OptimizerConfig) -> BenchmarkReport {
    let world = generate_world(config.workload);
    let mut runs = Vec::new();
    for policy in grid_candidates(&config.policy) {
        let metrics = run_simulation(&world, &policy);
        runs.push(RunReport {
            rank: 0,
            policy,
            metrics,
        });
    }
    rank_and_truncate(&mut runs, config.top_n);
    BenchmarkReport {
        mode: "grid".to_string(),
        world: world.summary(),
        best: runs.first().cloned(),
        baseline: None,
        convergence: None,
        recommendation: runs
            .first()
            .map(|run| recommendation_from_policy(&run.policy)),
        runs,
    }
}

pub fn evolve_report(config: OptimizerConfig) -> BenchmarkReport {
    let world = generate_world(config.workload.clone());
    let mut rng = Rng64::new(config.workload.seed ^ 0xa11c_e515_5eed);
    let mut population = seeded_population(&config.policy, config.population.max(4), &mut rng);
    let mut best_seen = Vec::new();

    for _ in 0..config.generations.max(1) {
        let mut generation = population
            .into_iter()
            .map(|policy| {
                let metrics = run_simulation(&world, &policy);
                RunReport {
                    rank: 0,
                    policy,
                    metrics,
                }
            })
            .collect::<Vec<_>>();
        generation.sort_by(rank_order);
        best_seen.extend(generation.iter().take(config.top_n.max(2)).cloned());

        let elite_count = (generation.len() / 4).max(2);
        let elites = generation
            .iter()
            .take(elite_count)
            .map(|run| run.policy.clone())
            .collect::<Vec<_>>();

        population = elites.clone();
        while population.len() < config.population.max(4) {
            let parent = &elites[rng.usize_range(0, elites.len())];
            population.push(mutate_policy(parent, &mut rng));
        }
    }

    rank_and_truncate(&mut best_seen, config.top_n);
    BenchmarkReport {
        mode: "evolve".to_string(),
        world: world.summary(),
        best: best_seen.first().cloned(),
        baseline: None,
        convergence: None,
        recommendation: best_seen
            .first()
            .map(|run| recommendation_from_policy(&run.policy)),
        runs: best_seen,
    }
}

pub fn optimize_report(config: OptimizerConfig) -> anyhow::Result<BenchmarkReport> {
    let summary_world = generate_world(config.workload.clone());
    let resumed = match &config.resume_path {
        Some(path) => Some(read_checkpoint(path)?),
        None => None,
    };
    if resumed.is_none() {
        if let Some(path) = &config.checkpoint_path {
            let report = build_optimize_report(BuildOptimizeReport {
                config: &config,
                world: summary_world.summary(),
                baseline: None,
                trace: Vec::new(),
                converged: false,
                stale_generations: 0,
                best_generation: 0,
                best_score: 0.0,
                trials: 0,
                current_generation: 0,
                current_generation_trials: 0,
                phase: "baseline-started".to_string(),
                best_seen: Vec::new(),
            });
            write_checkpoint(path, &report)?;
        }
    }
    let baseline = resumed
        .as_ref()
        .and_then(|report| report.baseline.clone())
        .unwrap_or_else(|| {
            let baseline_policy = config.policy.clone().sanitize();
            let baseline_metrics = evaluate_policy(&config, &baseline_policy);
            RunReport {
                rank: 0,
                policy: baseline_policy,
                metrics: baseline_metrics,
            }
        });

    let mut rng = Rng64::new(config.workload.seed ^ 0x0f7d_5eed_c0de);
    let mut best_seen = resumed
        .as_ref()
        .map(|report| report.runs.clone())
        .unwrap_or_default();
    let resumed_convergence = resumed
        .as_ref()
        .and_then(|report| report.convergence.clone());
    let mut trace = resumed_convergence
        .as_ref()
        .map(|convergence| convergence.trace.clone())
        .unwrap_or_default();
    let mut best_score = resumed_convergence
        .as_ref()
        .map(|convergence| convergence.best_score)
        .unwrap_or(baseline.metrics.objective_score);
    let mut best_generation = resumed_convergence
        .as_ref()
        .map(|convergence| convergence.best_generation)
        .unwrap_or(0);
    let mut stale_generations = resumed_convergence
        .as_ref()
        .map(|convergence| convergence.stale_generations)
        .unwrap_or(0);
    let mut trials = resumed_convergence
        .as_ref()
        .map(|convergence| convergence.trials)
        .unwrap_or(0);
    let mut converged = false;
    let start_generation = trace.len();
    let mut population = resume_population(&config, &baseline.policy, &best_seen, &mut rng);

    for step in 1..=config.generations.max(1) {
        let generation = start_generation + step;
        let generation_population = population;
        let generation_total = generation_population.len();
        let mut generation_runs = Vec::with_capacity(generation_total);
        for (trial_idx, policy) in generation_population.into_iter().enumerate() {
            if let Some(path) = &config.checkpoint_path {
                let report = build_optimize_report(BuildOptimizeReport {
                    config: &config,
                    world: summary_world.summary(),
                    baseline: Some(baseline.clone()),
                    trace: trace.clone(),
                    converged: false,
                    stale_generations,
                    best_generation,
                    best_score,
                    trials,
                    current_generation: generation,
                    current_generation_trials: trial_idx,
                    phase: "trial-started".to_string(),
                    best_seen: best_seen.clone(),
                });
                write_checkpoint(path, &report)?;
            }
            let metrics = evaluate_policy(&config, &policy);
            trials += 1;
            generation_runs.push(RunReport {
                rank: 0,
                policy,
                metrics,
            });
            generation_runs.sort_by(rank_order);

            let mut partial_best = best_seen.clone();
            partial_best.extend(generation_runs.iter().take(config.top_n.max(4)).cloned());
            rank_and_truncate(&mut partial_best, config.top_n.max(8));
            if let Some(path) = &config.checkpoint_path {
                let report = build_optimize_report(BuildOptimizeReport {
                    config: &config,
                    world: summary_world.summary(),
                    baseline: Some(baseline.clone()),
                    trace: trace.clone(),
                    converged: false,
                    stale_generations,
                    best_generation,
                    best_score: partial_best
                        .first()
                        .map(|run| run.metrics.objective_score.max(best_score))
                        .unwrap_or(best_score),
                    trials,
                    current_generation: generation,
                    current_generation_trials: trial_idx + 1,
                    phase: "trial-finished".to_string(),
                    best_seen: partial_best,
                });
                write_checkpoint(path, &report)?;
            }
        }
        generation_runs.sort_by(rank_order);

        if let Some(best) = generation_runs.first() {
            let delta = best.metrics.objective_score - best_score;
            if delta > config.min_delta {
                best_score = best.metrics.objective_score;
                best_generation = generation;
                stale_generations = 0;
            } else {
                stale_generations += 1;
            }
            trace.push(GenerationTrace {
                generation,
                trials,
                best_score: best.metrics.objective_score,
                best_sla_rate: best.metrics.sla_rate,
                best_watch_cost_peak: best.metrics.watch_cost_peak,
                stale_generations,
            });
        }

        best_seen.extend(generation_runs.iter().take(config.top_n.max(4)).cloned());
        rank_and_truncate(&mut best_seen, config.top_n.max(8));

        if let Some(path) = &config.checkpoint_path {
            let report = build_optimize_report(BuildOptimizeReport {
                config: &config,
                world: summary_world.summary(),
                baseline: Some(baseline.clone()),
                trace: trace.clone(),
                converged,
                stale_generations,
                best_generation,
                best_score,
                trials,
                current_generation: generation,
                current_generation_trials: generation_runs.len(),
                phase: "generation-finished".to_string(),
                best_seen: best_seen.clone(),
            });
            write_checkpoint(path, &report)?;
        }

        if let Some(best) = best_seen.first() {
            tracing::info!(
                "optimize generation {}/{} finished: trials={} best_score={:.2} best_sla={:.3} best_p95={}s watch_peak={} stale={}/{}",
                generation,
                start_generation + config.generations.max(1),
                trials,
                best.metrics.objective_score,
                best.metrics.sla_rate,
                best.metrics.p95_detect_secs,
                best.metrics.watch_cost_peak,
                stale_generations,
                config.patience.max(1),
            );
        }

        if stale_generations >= config.patience.max(1) {
            converged = true;
            break;
        }

        let elite_count = (generation_runs.len() / 4).max(2);
        let elites = generation_runs
            .iter()
            .take(elite_count)
            .map(|run| run.policy.clone())
            .collect::<Vec<_>>();

        population = elites.clone();
        while population.len() < config.population.max(4) {
            let parent = &elites[rng.usize_range(0, elites.len())];
            population.push(mutate_policy(parent, &mut rng));
        }
    }

    rank_and_truncate(&mut best_seen, config.top_n);
    Ok(build_optimize_report(BuildOptimizeReport {
        config: &config,
        world: summary_world.summary(),
        baseline: Some(baseline),
        trace,
        converged,
        stale_generations,
        best_generation,
        best_score,
        trials,
        current_generation: start_generation + config.generations.max(1),
        current_generation_trials: 0,
        phase: "finished".to_string(),
        best_seen,
    }))
}

struct BuildOptimizeReport<'a> {
    config: &'a OptimizerConfig,
    world: WorldSummary,
    baseline: Option<RunReport>,
    trace: Vec<GenerationTrace>,
    converged: bool,
    stale_generations: usize,
    best_generation: usize,
    best_score: f64,
    trials: usize,
    current_generation: usize,
    current_generation_trials: usize,
    phase: String,
    best_seen: Vec<RunReport>,
}

fn build_optimize_report(input: BuildOptimizeReport<'_>) -> BenchmarkReport {
    let BuildOptimizeReport {
        config,
        world,
        baseline,
        trace,
        converged,
        stale_generations,
        best_generation,
        best_score,
        trials,
        current_generation,
        current_generation_trials,
        phase,
        mut best_seen,
    } = input;
    rank_and_truncate(&mut best_seen, config.top_n);
    let baseline_score = baseline
        .as_ref()
        .map(|run| run.metrics.objective_score)
        .unwrap_or(0.0);
    let improvement_pct = if baseline_score.abs() <= f64::EPSILON {
        0.0
    } else {
        ((best_score - baseline_score) / baseline_score.abs()) * 100.0
    };
    let convergence = ConvergenceReport {
        trials,
        generations_completed: trace.len(),
        current_generation,
        current_generation_trials,
        phase,
        converged,
        stale_generations,
        best_generation,
        best_score,
        baseline_score,
        improvement_pct,
        trace,
    };

    BenchmarkReport {
        mode: if config.robust_profiles {
            "optimize-robust".to_string()
        } else {
            "optimize".to_string()
        },
        world,
        best: best_seen.first().cloned(),
        baseline,
        convergence: Some(convergence),
        recommendation: best_seen
            .first()
            .map(|run| recommendation_from_policy(&run.policy)),
        runs: best_seen,
    }
}

fn resume_population(
    config: &OptimizerConfig,
    baseline_policy: &PolicyParams,
    best_seen: &[RunReport],
    rng: &mut Rng64,
) -> Vec<PolicyParams> {
    let target = config.population.max(4);
    let mut population = best_seen
        .iter()
        .take(target / 2)
        .map(|run| run.policy.clone().sanitize())
        .collect::<Vec<_>>();
    if population.is_empty() {
        return seeded_population(baseline_policy, target, rng);
    }
    while population.len() < target {
        let parent = &population[rng.usize_range(0, population.len())].clone();
        population.push(mutate_policy(parent, rng));
    }
    population
}

fn read_checkpoint(path: &Path) -> anyhow::Result<BenchmarkReport> {
    let text = fs::read_to_string(path)?;
    Ok(serde_json::from_str(&text)?)
}

fn write_checkpoint(path: &Path, report: &BenchmarkReport) -> anyhow::Result<()> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)?;
    }
    let tmp_path = path.with_extension("tmp");
    let json = serde_json::to_string_pretty(report)?;
    fs::write(&tmp_path, json)?;
    fs::rename(tmp_path, path)?;
    Ok(())
}

pub fn adversarial_report(mut config: OptimizerConfig) -> BenchmarkReport {
    let base_seed = config.workload.seed;
    let profiles = [
        WorkloadProfile::Developer,
        WorkloadProfile::Burst,
        WorkloadProfile::Dormant,
        WorkloadProfile::Adversarial,
        WorkloadProfile::HomeDesktop,
    ];
    let candidates = grid_candidates(&config.policy);
    let mut aggregate = Vec::new();

    for policy in candidates {
        let mut combined: Option<RunMetrics> = None;
        let mut worst_sla = 1.0;
        for (idx, profile) in profiles.iter().enumerate() {
            config.workload.profile = *profile;
            config.workload.seed = base_seed.saturating_add((idx as u64 + 1) * 97);
            let world = generate_world(config.workload.clone());
            let metrics = run_simulation(&world, &policy);
            worst_sla = f64::min(worst_sla, metrics.sla_rate);
            combined = Some(match combined {
                None => metrics,
                Some(prev) => combine_metrics(prev, metrics),
            });
        }

        let Some(mut metrics) = combined else {
            continue;
        };
        metrics.sla_rate = if metrics.events_total == 0 {
            1.0
        } else {
            metrics.sla_met as f64 / metrics.events_total as f64
        };
        metrics.objective_score =
            metrics.objective_score / profiles.len() as f64 + worst_sla * 150.0;
        aggregate.push(RunReport {
            rank: 0,
            policy,
            metrics,
        });
    }

    rank_and_truncate(&mut aggregate, config.top_n);
    let mut summary_workload = config.workload.clone();
    summary_workload.profile = WorkloadProfile::Adversarial;
    summary_workload.seed = base_seed;
    let world = generate_world(summary_workload);
    BenchmarkReport {
        mode: "adversarial".to_string(),
        world: world.summary(),
        best: aggregate.first().cloned(),
        baseline: None,
        convergence: None,
        recommendation: aggregate
            .first()
            .map(|run| recommendation_from_policy(&run.policy)),
        runs: aggregate,
    }
}

fn evaluate_policy(config: &OptimizerConfig, policy: &PolicyParams) -> RunMetrics {
    if !config.robust_profiles {
        let world = generate_world(config.workload.clone());
        return run_simulation(&world, policy);
    }

    let profiles = [
        WorkloadProfile::Developer,
        WorkloadProfile::Burst,
        WorkloadProfile::Dormant,
        WorkloadProfile::Adversarial,
        WorkloadProfile::HomeDesktop,
    ];
    let mut combined: Option<RunMetrics> = None;
    let mut worst_sla = 1.0;
    for (idx, profile) in profiles.iter().enumerate() {
        let mut workload = config.workload.clone();
        workload.profile = *profile;
        workload.seed = config.workload.seed.saturating_add((idx as u64 + 1) * 131);
        let world = generate_world(workload);
        let metrics = run_simulation(&world, policy);
        worst_sla = f64::min(worst_sla, metrics.sla_rate);
        combined = Some(match combined {
            None => metrics,
            Some(prev) => combine_metrics(prev, metrics),
        });
    }

    let mut metrics = combined.expect("robust profile list is non-empty");
    metrics.sla_rate = if metrics.events_total == 0 {
        1.0
    } else {
        metrics.sla_met as f64 / metrics.events_total as f64
    };
    metrics.objective_score = metrics.objective_score / profiles.len() as f64 + worst_sla * 150.0;
    metrics
}

fn combine_metrics(prev: RunMetrics, metrics: RunMetrics) -> RunMetrics {
    RunMetrics {
        events_total: prev.events_total + metrics.events_total,
        detected: prev.detected + metrics.detected,
        missed: prev.missed + metrics.missed,
        sla_secs: prev.sla_secs,
        sla_met: prev.sla_met + metrics.sla_met,
        sla_rate: 0.0,
        avg_detect_secs: (prev.avg_detect_secs + metrics.avg_detect_secs) / 2.0,
        p50_detect_secs: prev.p50_detect_secs.max(metrics.p50_detect_secs),
        p95_detect_secs: prev.p95_detect_secs.max(metrics.p95_detect_secs),
        p99_detect_secs: prev.p99_detect_secs.max(metrics.p99_detect_secs),
        max_detect_secs: prev.max_detect_secs.max(metrics.max_detect_secs),
        watch_cost_peak: prev.watch_cost_peak.max(metrics.watch_cost_peak),
        scan_rounds: prev.scan_rounds + metrics.scan_rounds,
        scanned_dirs: prev.scanned_dirs + metrics.scanned_dirs,
        scanned_files: prev.scanned_files + metrics.scanned_files,
        promotions: prev.promotions + metrics.promotions,
        demotions: prev.demotions + metrics.demotions,
        replacements: prev.replacements + metrics.replacements,
        promotion_budget_blocked: prev.promotion_budget_blocked + metrics.promotion_budget_blocked,
        final_l0_dirs: prev.final_l0_dirs + metrics.final_l0_dirs,
        final_l1_dirs: prev.final_l1_dirs + metrics.final_l1_dirs,
        final_l2_dirs: prev.final_l2_dirs + metrics.final_l2_dirs,
        final_l3_dirs: prev.final_l3_dirs + metrics.final_l3_dirs,
        cpu_units: prev.cpu_units + metrics.cpu_units,
        io_units: prev.io_units + metrics.io_units,
        memory_budget_hit: prev.memory_budget_hit || metrics.memory_budget_hit,
        cpu_budget_hit: prev.cpu_budget_hit || metrics.cpu_budget_hit,
        io_budget_hit: prev.io_budget_hit || metrics.io_budget_hit,
        objective_score: prev.objective_score + metrics.objective_score,
    }
}

fn grid_candidates(base: &PolicyParams) -> Vec<PolicyParams> {
    let base = base.clone().sanitize();
    let watch_options = spread_u32(base.max_watch_dirs, &[0.35, 0.65, 1.0, 1.5, 2.2]);
    let l1_options = spread_u64(base.l1_scan_interval_secs, &[0.5, 1.0, 1.75]);
    let l2_options = spread_u64(base.l2_scan_interval_secs, &[0.65, 1.0, 1.6]);
    let l3_options = spread_u64(base.l3_scan_interval_secs, &[0.5, 1.0, 2.0]);
    let l3_policy_options = l3_scan_policy_candidates(base.l3_scan_policy);
    let ttl_options = spread_u64(base.l0_idle_ttl_secs, &[0.5, 1.0, 2.0]);

    let mut out = Vec::new();
    for max_watch_dirs in watch_options {
        for l1_scan_interval_secs in &l1_options {
            for l2_scan_interval_secs in &l2_options {
                for l3_scan_policy in &l3_policy_options {
                    for l3_scan_interval_secs in &l3_options {
                        for l0_idle_ttl_secs in &ttl_options {
                            let mut policy = base.clone();
                            policy.name = format!(
                                "grid-w{}-l1{}-l2{}-l3{:?}-{}-ttl{}",
                                max_watch_dirs,
                                l1_scan_interval_secs,
                                l2_scan_interval_secs,
                                l3_scan_policy,
                                l3_scan_interval_secs,
                                l0_idle_ttl_secs
                            );
                            policy.max_watch_dirs = max_watch_dirs;
                            policy.l1_scan_interval_secs = *l1_scan_interval_secs;
                            policy.l2_scan_interval_secs = (*l2_scan_interval_secs)
                                .max(l1_scan_interval_secs.saturating_add(1));
                            policy.l3_scan_policy = *l3_scan_policy;
                            policy.l3_scan_interval_secs = *l3_scan_interval_secs;
                            policy.l0_idle_ttl_secs = *l0_idle_ttl_secs;
                            out.push(policy.sanitize());
                        }
                    }
                }
            }
        }
    }
    out
}

fn l3_scan_policy_candidates(seed: L3ScanPolicy) -> [L3ScanPolicy; 3] {
    let mut out = [
        L3ScanPolicy::Interval,
        L3ScanPolicy::ValidateOnQuery,
        L3ScanPolicy::Disabled,
    ];
    if let Some(pos) = out.iter().position(|policy| *policy == seed) {
        out.swap(0, pos);
    }
    out
}

fn recommendation_from_policy(policy: &PolicyParams) -> TieredWatchRecommendation {
    let policy = policy.clone().sanitize();
    TieredWatchRecommendation {
        watch_mode: "tiered".to_string(),
        max_watch_dirs: policy.max_watch_dirs,
        scan_items_per_sec: policy.per_round_max_files as usize,
        scan_ms_per_tick: policy.per_round_max_ms,
        l0_idle_ttl_secs: policy.l0_idle_ttl_secs,
        l1_scan_interval_secs: policy.l1_scan_interval_secs,
        l2_scan_interval_secs: policy.l2_scan_interval_secs,
        l3_scan_policy: policy.l3_scan_policy,
        l3_scan_interval_secs: policy.l3_scan_interval_secs,
        l1_empty_scans_to_l2: policy.l1_empty_scans_to_l2,
        l2_empty_scans_to_l3: policy.l2_empty_scans_to_l3,
    }
}

fn seeded_population(base: &PolicyParams, size: usize, rng: &mut Rng64) -> Vec<PolicyParams> {
    let mut out = grid_candidates(base);
    out.truncate(size / 2);
    out.push(base.clone().sanitize());
    while out.len() < size {
        out.push(mutate_policy(base, rng));
    }
    out
}

fn mutate_policy(parent: &PolicyParams, rng: &mut Rng64) -> PolicyParams {
    let mut policy = parent.clone();
    policy.name = "evolved".to_string();
    policy.max_watch_dirs = mutate_u32(
        policy.max_watch_dirs,
        8,
        16_384,
        0.45 + rng.next_f64() * 1.4,
    );
    policy.l0_idle_ttl_secs = mutate_u64(
        policy.l0_idle_ttl_secs,
        30,
        86_400,
        0.45 + rng.next_f64() * 1.6,
    );
    policy.l1_scan_interval_secs = mutate_u64(
        policy.l1_scan_interval_secs,
        3,
        900,
        0.45 + rng.next_f64() * 1.5,
    );
    policy.l2_scan_interval_secs = mutate_u64(
        policy.l2_scan_interval_secs,
        10,
        7_200,
        0.50 + rng.next_f64() * 1.8,
    );
    policy.l3_scan_interval_secs = mutate_u64(
        policy.l3_scan_interval_secs,
        1,
        604_800,
        0.45 + rng.next_f64() * 2.0,
    );
    if rng.bool(0.35) {
        let options = l3_scan_policy_candidates(policy.l3_scan_policy);
        policy.l3_scan_policy = options[rng.usize_range(0, options.len())];
    }
    policy.per_round_max_dirs = mutate_usize(
        policy.per_round_max_dirs,
        1,
        256,
        0.55 + rng.next_f64() * 1.5,
    );
    policy.per_round_max_files = mutate_u64(
        policy.per_round_max_files,
        100,
        200_000,
        0.55 + rng.next_f64() * 1.5,
    );
    policy.weights.recent_event_count *= 0.7 + rng.next_f64() * 0.8;
    policy.weights.event_recency_decay *= 0.7 + rng.next_f64() * 0.8;
    policy.weights.importance *= 0.7 + rng.next_f64() * 0.8;
    policy.weights.miss_penalty *= 0.7 + rng.next_f64() * 0.8;
    policy.weights.watch_cost *= 0.7 + rng.next_f64() * 0.8;
    policy.weights.scan_cost *= 0.7 + rng.next_f64() * 0.8;
    policy.sanitize()
}

fn rank_and_truncate(runs: &mut Vec<RunReport>, top_n: usize) {
    runs.sort_by(rank_order);
    runs.dedup_by(|a, b| {
        a.policy.max_watch_dirs == b.policy.max_watch_dirs
            && a.policy.l1_scan_interval_secs == b.policy.l1_scan_interval_secs
            && a.policy.l2_scan_interval_secs == b.policy.l2_scan_interval_secs
            && a.policy.l3_scan_policy == b.policy.l3_scan_policy
            && a.policy.l3_scan_interval_secs == b.policy.l3_scan_interval_secs
            && a.policy.l0_idle_ttl_secs == b.policy.l0_idle_ttl_secs
    });
    runs.truncate(top_n.max(1));
    for (idx, run) in runs.iter_mut().enumerate() {
        run.rank = idx + 1;
    }
}

fn rank_order(a: &RunReport, b: &RunReport) -> std::cmp::Ordering {
    b.metrics
        .objective_score
        .total_cmp(&a.metrics.objective_score)
        .then_with(|| b.metrics.sla_rate.total_cmp(&a.metrics.sla_rate))
}

fn spread_u32(base: u32, factors: &[f64]) -> Vec<u32> {
    unique_sorted(
        factors
            .iter()
            .map(|factor| ((base as f64 * factor).round() as u32).max(1))
            .collect(),
    )
}

fn spread_u64(base: u64, factors: &[f64]) -> Vec<u64> {
    let mut values = factors
        .iter()
        .map(|factor| ((base as f64 * factor).round() as u64).max(1))
        .collect::<Vec<_>>();
    values.sort_unstable();
    values.dedup();
    values
}

fn unique_sorted(mut values: Vec<u32>) -> Vec<u32> {
    values.sort_unstable();
    values.dedup();
    values
}

fn mutate_u32(value: u32, min: u32, max: u32, factor: f64) -> u32 {
    ((value as f64 * factor).round() as u32).clamp(min, max)
}

fn mutate_u64(value: u64, min: u64, max: u64, factor: f64) -> u64 {
    ((value as f64 * factor).round() as u64).clamp(min, max)
}

fn mutate_usize(value: usize, min: usize, max: usize, factor: f64) -> usize {
    ((value as f64 * factor).round() as usize).clamp(min, max)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample_recommendation(max_watch_dirs: u32) -> TieredWatchRecommendation {
        TieredWatchRecommendation {
            watch_mode: "tiered".to_string(),
            max_watch_dirs,
            scan_items_per_sec: 4_000,
            scan_ms_per_tick: 25,
            l0_idle_ttl_secs: 900,
            l1_scan_interval_secs: 15,
            l2_scan_interval_secs: 180,
            l3_scan_policy: L3ScanPolicy::Interval,
            l3_scan_interval_secs: DEFAULT_L3_SCAN_INTERVAL_SECS,
            l1_empty_scans_to_l2: 3,
            l2_empty_scans_to_l3: 2,
        }
    }

    fn sample_report(recommendation: Option<TieredWatchRecommendation>) -> BenchmarkReport {
        let world = generate_world(WorkloadConfig {
            dirs: 4,
            events: 8,
            duration_secs: 60,
            ..WorkloadConfig::default()
        });
        BenchmarkReport {
            mode: "test".to_string(),
            world: world.summary(),
            best: None,
            baseline: None,
            convergence: None,
            recommendation,
            runs: Vec::new(),
        }
    }

    #[test]
    fn grid_report_returns_ranked_runs() {
        let config = OptimizerConfig {
            workload: WorkloadConfig {
                dirs: 80,
                events: 200,
                duration_secs: 180,
                ..WorkloadConfig::default()
            },
            top_n: 3,
            ..OptimizerConfig::default()
        };

        let report = grid_report(config);

        assert_eq!(report.runs.len(), 3);
        assert_eq!(report.runs[0].rank, 1);
        assert!(report.best.is_some());
        assert!(report.recommendation.is_some());
    }

    #[test]
    fn grid_candidates_enumerate_l3_policy_modes() {
        let policies = grid_candidates(&PolicyParams::default());

        assert!(policies
            .iter()
            .any(|policy| policy.l3_scan_policy == L3ScanPolicy::Interval));
        assert!(policies
            .iter()
            .any(|policy| policy.l3_scan_policy == L3ScanPolicy::ValidateOnQuery));
        assert!(policies
            .iter()
            .any(|policy| policy.l3_scan_policy == L3ScanPolicy::Disabled));
    }

    #[test]
    fn mutate_policy_can_change_l3_policy_mode() {
        let base = PolicyParams::default();
        let mut rng = Rng64::new(42);
        let mut seen = Vec::new();

        for _ in 0..128 {
            let mode = mutate_policy(&base, &mut rng).l3_scan_policy;
            if !seen.contains(&mode) {
                seen.push(mode);
            }
        }

        assert!(
            seen.len() > 1,
            "mutation should explore L3 policy modes, seen={seen:?}"
        );
    }

    #[test]
    fn optimize_report_records_trials_and_recommendation() {
        let config = OptimizerConfig {
            workload: WorkloadConfig {
                dirs: 60,
                events: 120,
                duration_secs: 120,
                ..WorkloadConfig::default()
            },
            generations: 3,
            population: 6,
            top_n: 2,
            robust_profiles: false,
            ..OptimizerConfig::default()
        };

        let report = optimize_report(config).expect("optimize should succeed");

        assert!(report.best.is_some());
        assert!(report.baseline.is_some());
        assert!(report.recommendation.is_some());
        let convergence = report.convergence.expect("convergence report");
        assert!(convergence.trials >= 6);
        assert!(!convergence.trace.is_empty());
    }

    #[test]
    fn optimize_checkpoint_can_resume() {
        let checkpoint =
            std::env::temp_dir().join(format!("fd-rdd-sim-checkpoint-{}.json", std::process::id()));
        let _ = std::fs::remove_file(&checkpoint);
        let base = OptimizerConfig {
            workload: WorkloadConfig {
                dirs: 50,
                events: 90,
                duration_secs: 90,
                ..WorkloadConfig::default()
            },
            generations: 2,
            population: 6,
            top_n: 3,
            robust_profiles: false,
            checkpoint_path: Some(checkpoint.clone()),
            ..OptimizerConfig::default()
        };

        let first = optimize_report(base.clone()).expect("first optimize should checkpoint");
        assert!(checkpoint.exists());
        let first_trials = first.convergence.as_ref().unwrap().trials;

        let resumed = optimize_report(OptimizerConfig {
            generations: 2,
            resume_path: Some(checkpoint.clone()),
            checkpoint_path: Some(checkpoint.clone()),
            ..base
        })
        .expect("resume should use checkpoint");

        assert!(
            resumed.convergence.as_ref().unwrap().trials > first_trials,
            "resume should continue trial count"
        );

        let _ = std::fs::remove_file(checkpoint);
    }

    #[test]
    fn config_patch_toml_contains_only_runtime_tiered_fields() {
        let recommendation = sample_recommendation(128);

        let patch = tiered_watch_config_patch_toml(&recommendation)
            .expect("recommendation should serialize as toml");

        assert!(patch.contains("watch_mode = \"tiered\""));
        assert!(patch.contains("[tiered_watch]"));
        assert!(patch.contains("max_watch_dirs = 128"));
        assert!(patch.contains("scan_items_per_sec = 4000"));
        assert!(patch.contains("l3_scan_policy = \"interval\""));
        assert!(patch.contains("l3_scan_interval_secs = 21600"));
        assert!(patch.contains("l2_empty_scans_to_l3 = 2"));
        assert!(patch.contains("Sim-only policy fields ignored: weights"));

        let parsed: toml::Value = toml::from_str(&patch).expect("patch should remain valid TOML");
        assert!(parsed.get("weights").is_none());
        let tiered_watch = parsed
            .get("tiered_watch")
            .and_then(toml::Value::as_table)
            .expect("tiered_watch table");
        assert!(!tiered_watch.contains_key("per_round_max_dirs"));
        assert!(!tiered_watch.contains_key("per_round_max_files"));
        assert!(!tiered_watch.contains_key("per_round_max_ms"));
    }

    #[test]
    fn config_patch_from_reports_uses_conservative_bounds() {
        let mut first = sample_recommendation(256);
        first.scan_items_per_sec = 8_000;
        first.scan_ms_per_tick = 40;
        first.l0_idle_ttl_secs = 1_200;
        first.l1_scan_interval_secs = 20;
        first.l2_scan_interval_secs = 200;
        first.l3_scan_policy = L3ScanPolicy::Interval;
        first.l3_scan_interval_secs = 21_600;
        first.l1_empty_scans_to_l2 = 5;
        first.l2_empty_scans_to_l3 = 4;

        let mut second = sample_recommendation(128);
        second.scan_items_per_sec = 3_000;
        second.scan_ms_per_tick = 15;
        second.l0_idle_ttl_secs = 600;
        second.l1_scan_interval_secs = 45;
        second.l2_scan_interval_secs = 400;
        second.l3_scan_policy = L3ScanPolicy::ValidateOnQuery;
        second.l3_scan_interval_secs = 43_200;
        second.l1_empty_scans_to_l2 = 2;
        second.l2_empty_scans_to_l3 = 2;

        let aggregate = conservative_tiered_watch_recommendation_from_reports(&[
            sample_report(Some(first)),
            sample_report(Some(second)),
        ])
        .expect("reports should aggregate");

        assert_eq!(aggregate.max_watch_dirs, 128);
        assert_eq!(aggregate.scan_items_per_sec, 3_000);
        assert_eq!(aggregate.scan_ms_per_tick, 15);
        assert_eq!(aggregate.l0_idle_ttl_secs, 600);
        assert_eq!(aggregate.l1_scan_interval_secs, 45);
        assert_eq!(aggregate.l2_scan_interval_secs, 400);
        assert_eq!(aggregate.l3_scan_policy, L3ScanPolicy::ValidateOnQuery);
        assert_eq!(aggregate.l3_scan_interval_secs, 43_200);
        assert_eq!(aggregate.l1_empty_scans_to_l2, 2);
        assert_eq!(aggregate.l2_empty_scans_to_l3, 2);
    }

    #[test]
    fn config_patch_errors_without_recommendation_best_or_baseline() {
        let report = sample_report(None);
        let err = tiered_watch_config_patch_toml_from_report(&report)
            .expect_err("report without recommendation, best, or baseline should fail");

        assert!(err
            .to_string()
            .contains("report has no recommendation, best run, or baseline"));
    }

    #[test]
    fn malformed_json_report_is_rejected() {
        let err = serde_json::from_str::<BenchmarkReport>("{not valid json")
            .expect_err("malformed JSON should fail");

        assert!(err.to_string().contains("key must be a string"));
    }

    #[test]
    fn emit_config_output_requires_existing_parent_directory() {
        let root =
            std::env::temp_dir().join(format!("fd-rdd-sim-missing-parent-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&root);
        let path = root.join("missing").join("patch.toml");

        let err = write_existing_parent(&path, "watch_mode = \"tiered\"\n")
            .expect_err("missing output parent should fail");

        assert!(err.to_string().contains("output directory does not exist"));
        let _ = std::fs::remove_dir_all(root);
    }

    #[cfg(unix)]
    #[test]
    fn emit_config_output_rejects_unwritable_parent_directory() {
        use std::os::unix::fs::PermissionsExt;

        let root = std::env::temp_dir().join(format!(
            "fd-rdd-sim-unwritable-parent-{}",
            std::process::id()
        ));
        let _ = std::fs::remove_dir_all(&root);
        std::fs::create_dir_all(&root).expect("create temp dir");
        std::fs::set_permissions(&root, std::fs::Permissions::from_mode(0o555))
            .expect("make temp dir readonly");
        let path = root.join("patch.toml");

        let err = write_existing_parent(&path, "watch_mode = \"tiered\"\n")
            .expect_err("readonly output parent should fail");

        std::fs::set_permissions(&root, std::fs::Permissions::from_mode(0o755))
            .expect("restore temp dir permissions");
        let _ = std::fs::remove_dir_all(root);
        assert!(err.to_string().contains("output directory is not writable"));
    }
}
