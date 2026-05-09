use super::metrics::{summarize, RunMetrics, StrategyCounters};
use super::policy::{initial_l0_candidates, DirPolicyState, PolicyParams, SimTier};
use super::world::World;

#[derive(Debug, Clone)]
struct DirRuntime {
    tier: SimTier,
    recent_events: u32,
    last_event_at: u64,
    next_scan_at: u64,
    empty_scans: u32,
}

impl DirRuntime {
    fn policy_state(&self) -> DirPolicyState {
        DirPolicyState {
            recent_events: self.recent_events,
            last_event_at: self.last_event_at,
        }
    }
}

pub fn run_simulation(world: &World, policy: &PolicyParams) -> RunMetrics {
    tracing::debug!(
        "simulation start: dirs={} events={} duration={}s",
        world.dirs.len(),
        world.events.len(),
        world.config.duration_secs,
    );

    let policy = policy.clone().sanitize();
    let mut dirs = vec![
        DirRuntime {
            tier: SimTier::L1,
            recent_events: 0,
            last_event_at: 0,
            next_scan_at: 0,
            empty_scans: 0,
        };
        world.dirs.len()
    ];
    let mut current_watch_cost = 0u64;
    let mut watch_cost_peak = 0u64;

    for id in initial_l0_candidates(world, &policy) {
        if id == 0 || covered_by_l0(world, &dirs, id) {
            continue;
        }
        let cost = world.dirs[id].watch_cost as u64;
        if current_watch_cost.saturating_add(cost) <= policy.max_watch_dirs as u64 {
            dirs[id].tier = SimTier::L0;
            current_watch_cost = current_watch_cost.saturating_add(cost);
            watch_cost_peak = watch_cost_peak.max(current_watch_cost);
        }
    }

    for (id, state) in dirs.iter_mut().enumerate() {
        if state.tier != SimTier::L0 {
            state.next_scan_at = initial_scan_jitter(id, &policy);
        }
    }

    let mut cursor = 0usize;
    let mut unresolved = Vec::new();
    let mut detected_at = vec![None; world.events.len()];
    let mut scan_rounds = 0u64;
    let mut scanned_dirs = 0u64;
    let mut scanned_files = 0u64;
    let mut promotions = 0u64;
    let mut demotions = 0u64;
    let mut replacements = 0u64;
    let mut promotion_budget_blocked = 0u64;

    let duration = world.config.duration_secs.max(1);
    for now in 0..=world.config.duration_secs {
        if now % (duration / 10 + 1) == 0 {
            tracing::debug!("simulation progress: {}s / {}s", now, duration);
        }

        while cursor < world.events.len() && world.events[cursor].at_secs <= now {
            let event = &world.events[cursor];
            if let Some(root) = covering_l0(world, &dirs, event.dir) {
                detected_at[event.id] = Some(now);
                dirs[root].last_event_at = now;
                dirs[root].recent_events = dirs[root].recent_events.saturating_add(1).min(10_000);
                dirs[root].empty_scans = 0;
            } else {
                unresolved.push(event.id);
            }
            cursor += 1;
        }

        let demote = dirs
            .iter()
            .enumerate()
            .filter_map(|(id, state)| {
                if state.tier == SimTier::L0
                    && state.last_event_at > 0
                    && now.saturating_sub(state.last_event_at) > policy.l0_idle_ttl_secs
                {
                    Some(id)
                } else {
                    None
                }
            })
            .collect::<Vec<_>>();
        for id in demote {
            dirs[id].tier = SimTier::L1;
            dirs[id].next_scan_at = now;
            dirs[id].empty_scans = 0;
            demotions = demotions.saturating_add(1);
            current_watch_cost =
                current_watch_cost.saturating_sub(world.dirs[id].watch_cost as u64);
        }

        let batch = scan_batch(world, &dirs, &policy, now);
        if !batch.is_empty() {
            scan_rounds = scan_rounds.saturating_add(1);
        }

        for dir_id in batch {
            let scan_cost = world.dirs[dir_id].scan_cost as u64;
            scanned_dirs = scanned_dirs.saturating_add(1);
            scanned_files = scanned_files.saturating_add(scan_cost);

            let changed = detect_by_scan(world, &mut unresolved, &mut detected_at, dir_id, now);
            dirs[dir_id].last_event_at = if changed > 0 {
                now
            } else {
                dirs[dir_id].last_event_at
            };

            if changed > 0 {
                dirs[dir_id].recent_events = dirs[dir_id]
                    .recent_events
                    .saturating_add(changed as u32)
                    .min(10_000);
                dirs[dir_id].empty_scans = 0;
                match promote_dir(
                    world,
                    &mut dirs,
                    &policy,
                    dir_id,
                    now,
                    &mut current_watch_cost,
                ) {
                    PromotionOutcome::Promoted => {
                        promotions = promotions.saturating_add(1);
                        watch_cost_peak = watch_cost_peak.max(current_watch_cost);
                    }
                    PromotionOutcome::Replaced => {
                        promotions = promotions.saturating_add(1);
                        replacements = replacements.saturating_add(1);
                        demotions = demotions.saturating_add(1);
                        watch_cost_peak = watch_cost_peak.max(current_watch_cost);
                    }
                    PromotionOutcome::AlreadyCovered => {}
                    PromotionOutcome::BudgetBlocked => {
                        promotion_budget_blocked = promotion_budget_blocked.saturating_add(1);
                        dirs[dir_id].tier = SimTier::L1;
                        dirs[dir_id].next_scan_at =
                            now.saturating_add(dirs[dir_id].tier.scan_interval_secs(&policy));
                    }
                }
            } else {
                dirs[dir_id].recent_events = (dirs[dir_id].recent_events as f64 * 0.82) as u32;
                dirs[dir_id].empty_scans = dirs[dir_id].empty_scans.saturating_add(1);
                maybe_demote_empty(&mut dirs[dir_id], &policy);
                dirs[dir_id].next_scan_at =
                    now.saturating_add(dirs[dir_id].tier.scan_interval_secs(&policy));
            }
        }
    }

    let detected_latencies = world
        .events
        .iter()
        .filter_map(|event| {
            detected_at[event.id].map(|detected| detected.saturating_sub(event.at_secs))
        })
        .collect::<Vec<_>>();

    let mut strategy = StrategyCounters {
        promotions,
        demotions,
        replacements,
        promotion_budget_blocked,
        ..StrategyCounters::default()
    };
    for state in &dirs {
        match state.tier {
            SimTier::L0 => strategy.final_l0_dirs += 1,
            SimTier::L1 => strategy.final_l1_dirs += 1,
            SimTier::L2 => strategy.final_l2_dirs += 1,
            SimTier::L3 => strategy.final_l3_dirs += 1,
        }
    }

    let metrics = summarize(
        &policy,
        &detected_latencies,
        world.events.len(),
        watch_cost_peak,
        scan_rounds,
        scanned_dirs,
        scanned_files,
        strategy,
    );

    tracing::debug!(
        "simulation end: detected={}/{} peak_watch={} score={:.2}",
        metrics.detected,
        metrics.events_total,
        metrics.watch_cost_peak,
        metrics.objective_score,
    );

    metrics
}

fn initial_scan_jitter(id: usize, policy: &PolicyParams) -> u64 {
    let interval = policy.l1_scan_interval_secs.max(1) as usize;
    (id % interval) as u64
}

fn scan_batch(world: &World, dirs: &[DirRuntime], policy: &PolicyParams, now: u64) -> Vec<usize> {
    let mut candidates = dirs
        .iter()
        .enumerate()
        .filter_map(|(id, state)| {
            if state.tier == SimTier::L0
                || state.next_scan_at > now
                || covered_by_l0(world, dirs, id)
            {
                return None;
            }
            let score = policy.score_dir(&world.dirs[id], &state.policy_state(), now);
            Some((state.tier, std::cmp::Reverse(ScoreKey(score)), id))
        })
        .collect::<Vec<_>>();

    candidates.sort_by_key(|(tier, score, id)| (tier_rank(*tier), *score, *id));
    let mut out = Vec::new();
    let mut files = 0u64;
    let mut ms = 0u64;

    for (_, _, id) in candidates {
        let scan_cost = world.dirs[id].scan_cost as u64;
        let scan_ms = (scan_cost / 200).max(1);
        if !out.is_empty()
            && (out.len() >= policy.per_round_max_dirs
                || files.saturating_add(scan_cost) > policy.per_round_max_files
                || ms.saturating_add(scan_ms) > policy.per_round_max_ms)
        {
            break;
        }
        out.push(id);
        files = files.saturating_add(scan_cost);
        ms = ms.saturating_add(scan_ms);
    }

    out
}

#[derive(Debug, Clone, Copy, PartialEq)]
struct ScoreKey(f64);

impl Eq for ScoreKey {}

impl PartialOrd for ScoreKey {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for ScoreKey {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.0.total_cmp(&other.0)
    }
}

fn tier_rank(tier: SimTier) -> u8 {
    match tier {
        SimTier::L1 => 0,
        SimTier::L2 => 1,
        SimTier::L3 => 2,
        SimTier::L0 => 3,
    }
}

fn detect_by_scan(
    world: &World,
    unresolved: &mut Vec<usize>,
    detected_at: &mut [Option<u64>],
    scan_dir: usize,
    now: u64,
) -> usize {
    let mut changed = 0usize;
    unresolved.retain(|event_id| {
        let event = &world.events[*event_id];
        if event.dir == scan_dir {
            detected_at[event.id] = Some(now);
            changed += 1;
            false
        } else {
            true
        }
    });
    changed
}

fn maybe_demote_empty(state: &mut DirRuntime, policy: &PolicyParams) {
    match state.tier {
        SimTier::L1 if state.empty_scans >= policy.l1_empty_scans_to_l2 => {
            state.tier = SimTier::L2;
            state.empty_scans = 0;
        }
        SimTier::L2 if state.empty_scans >= policy.l2_empty_scans_to_l3 => {
            state.tier = SimTier::L3;
            state.empty_scans = 0;
        }
        _ => {}
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum PromotionOutcome {
    AlreadyCovered,
    Promoted,
    Replaced,
    BudgetBlocked,
}

fn promote_dir(
    world: &World,
    dirs: &mut [DirRuntime],
    policy: &PolicyParams,
    dir_id: usize,
    now: u64,
    current_watch_cost: &mut u64,
) -> PromotionOutcome {
    if dir_id == 0 || covered_by_l0(world, dirs, dir_id) {
        return PromotionOutcome::AlreadyCovered;
    }

    let cost = world.dirs[dir_id].watch_cost as u64;
    let budget = policy.max_watch_dirs as u64;
    if cost > budget {
        return PromotionOutcome::BudgetBlocked;
    }

    if current_watch_cost.saturating_add(cost) <= budget {
        dirs[dir_id].tier = SimTier::L0;
        *current_watch_cost = current_watch_cost.saturating_add(cost);
        return PromotionOutcome::Promoted;
    }

    let candidate_score = policy.score_dir(&world.dirs[dir_id], &dirs[dir_id].policy_state(), now);
    let replacement = dirs
        .iter()
        .enumerate()
        .filter(|(_, state)| state.tier == SimTier::L0)
        .min_by(|(left_id, left), (right_id, right)| {
            let left_score = policy.score_dir(&world.dirs[*left_id], &left.policy_state(), now);
            let right_score = policy.score_dir(&world.dirs[*right_id], &right.policy_state(), now);
            left_score
                .total_cmp(&right_score)
                .then_with(|| left_id.cmp(right_id))
        })
        .map(|(id, state)| {
            (
                id,
                policy.score_dir(&world.dirs[id], &state.policy_state(), now),
            )
        });

    let Some((replace_id, replace_score)) = replacement else {
        return PromotionOutcome::BudgetBlocked;
    };
    let replace_cost = world.dirs[replace_id].watch_cost as u64;
    if candidate_score <= replace_score
        || current_watch_cost
            .saturating_sub(replace_cost)
            .saturating_add(cost)
            > budget
    {
        return PromotionOutcome::BudgetBlocked;
    }

    dirs[replace_id].tier = SimTier::L1;
    dirs[replace_id].next_scan_at = now;
    dirs[replace_id].empty_scans = 0;
    dirs[dir_id].tier = SimTier::L0;
    *current_watch_cost = current_watch_cost
        .saturating_sub(replace_cost)
        .saturating_add(cost);
    PromotionOutcome::Replaced
}

fn covering_l0(world: &World, dirs: &[DirRuntime], dir_id: usize) -> Option<usize> {
    world
        .ancestor_ids(dir_id)
        .into_iter()
        .find(|ancestor| dirs[*ancestor].tier == SimTier::L0)
}

fn covered_by_l0(world: &World, dirs: &[DirRuntime], dir_id: usize) -> bool {
    covering_l0(world, dirs, dir_id).is_some()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::event::tiered_watch::{PromotionDecision, TieredWatchRuntime};
    use crate::sim::metrics::RunMetrics;
    use crate::sim::policy::ScoreWeights;
    use crate::sim::world::{generate_world, WorkloadConfig, WorkloadProfile};
    use crate::sim::world::{DirNode, FileEvent, World};
    use crate::stats::WatchStateReport;
    use std::path::{Path, PathBuf};

    fn sim_diag(metrics: &RunMetrics) -> String {
        format!(
            "sim metrics: events={}/{} missed={} sla_rate={:.3} p95={} watch_peak={} scanned_dirs={} scanned_files={} promotions={} demotions={} replacements={} budget_blocked={} final_tiers=L0:{} L1:{} L2:{} L3:{} objective={:.2}",
            metrics.detected,
            metrics.events_total,
            metrics.missed,
            metrics.sla_rate,
            metrics.p95_detect_secs,
            metrics.watch_cost_peak,
            metrics.scanned_dirs,
            metrics.scanned_files,
            metrics.promotions,
            metrics.demotions,
            metrics.replacements,
            metrics.promotion_budget_blocked,
            metrics.final_l0_dirs,
            metrics.final_l1_dirs,
            metrics.final_l2_dirs,
            metrics.final_l3_dirs,
            metrics.objective_score,
        )
    }

    fn runtime_diag(report: &WatchStateReport) -> String {
        format!(
            "runtime report: watched={}/{} util={} tiers=L0:{} L1:{} L2:{} L3:{} backlog={} backlog_by_tier={:?} promotions={} demotions={} replacements={} budget_blocked={} dirty={} cold_validate={} event_score_total={}",
            report.watched_dirs_estimated,
            report.max_watch_dirs,
            report.watch_budget_utilization_pct,
            report.l0_dirs,
            report.l1_dirs,
            report.l2_dirs,
            report.l3_dirs,
            report.scan_backlog,
            report.scan_backlog_by_tier,
            report.promotions,
            report.demotions,
            report.l0_replacements,
            report.promotion_budget_blocked,
            report.dirty_queue_len,
            report.cold_validate_count,
            report.event_score_total,
        )
    }

    fn parity_diag(metrics: &RunMetrics, report: &WatchStateReport) -> String {
        format!("{}\n{}", sim_diag(metrics), runtime_diag(report))
    }

    #[test]
    fn larger_watch_budget_reduces_scan_work() {
        let world = generate_world(WorkloadConfig {
            profile: WorkloadProfile::Developer,
            dirs: 180,
            events: 900,
            duration_secs: 600,
            seed: 7,
        });
        let tight = PolicyParams {
            max_watch_dirs: 16,
            ..PolicyParams::default()
        };
        let roomy = PolicyParams {
            max_watch_dirs: 96,
            ..PolicyParams::default()
        };

        let tight_metrics = run_simulation(&world, &tight);
        let roomy_metrics = run_simulation(&world, &roomy);

        assert!(
            roomy_metrics.watch_cost_peak <= roomy.max_watch_dirs as u64,
            "{}",
            sim_diag(&roomy_metrics)
        );
        assert!(
            tight_metrics.watch_cost_peak <= tight.max_watch_dirs as u64,
            "{}",
            sim_diag(&tight_metrics)
        );
        assert!(
            roomy_metrics.scanned_dirs <= tight_metrics.scanned_dirs,
            "larger L0 budget should not require more scan work\nroomy: {}\ntight: {}",
            sim_diag(&roomy_metrics),
            sim_diag(&tight_metrics)
        );
    }

    #[test]
    fn sim_and_runtime_share_hot_replacement_shape() {
        let world = World {
            config: WorkloadConfig {
                profile: WorkloadProfile::Developer,
                dirs: 3,
                events: 1,
                duration_secs: 0,
                seed: 1,
            },
            dirs: vec![
                DirNode {
                    id: 0,
                    parent: None,
                    depth: 0,
                    importance: 0.0,
                    base_event_rate: 0.0,
                    scan_cost: 1,
                    watch_cost: 1,
                    children: vec![1, 2],
                },
                DirNode {
                    id: 1,
                    parent: Some(0),
                    depth: 1,
                    importance: 0.9,
                    base_event_rate: 0.01,
                    scan_cost: 1,
                    watch_cost: 1,
                    children: Vec::new(),
                },
                DirNode {
                    id: 2,
                    parent: Some(0),
                    depth: 1,
                    importance: 0.5,
                    base_event_rate: 0.01,
                    scan_cost: 1,
                    watch_cost: 1,
                    children: Vec::new(),
                },
            ],
            events: vec![FileEvent {
                id: 0,
                dir: 2,
                at_secs: 0,
            }],
        };
        let policy = PolicyParams {
            max_watch_dirs: 1,
            l1_scan_interval_secs: 1,
            l2_scan_interval_secs: 10,
            per_round_max_dirs: 1,
            per_round_max_files: 100,
            per_round_max_ms: 20,
            weights: ScoreWeights {
                recent_event_count: 10.0,
                event_recency_decay: 0.0,
                importance: 1.0,
                miss_penalty: 0.0,
                watch_cost: 0.0,
                scan_cost: 0.0,
            },
            ..PolicyParams::default()
        };

        let metrics = run_simulation(&world, &policy);
        assert_eq!(metrics.replacements, 1, "{}", sim_diag(&metrics));
        assert_eq!(metrics.promotions, 1, "{}", sim_diag(&metrics));
        assert_eq!(
            metrics.promotion_budget_blocked,
            0,
            "{}",
            sim_diag(&metrics)
        );
        assert_eq!(metrics.final_l0_dirs, 1, "{}", sim_diag(&metrics));

        let runtime = TieredWatchRuntime::new(
            vec![(PathBuf::from("/cold"), 1)],
            vec![(PathBuf::from("/hotter"), 1)],
            1,
            5_000,
            20,
        );
        let hotter = PathBuf::from("/hotter");
        runtime.record_scan(
            hotter.as_path(),
            crate::index::tiered::ScanOutcome {
                scanned: 1,
                changed: 1,
                elapsed_ms: 1,
            },
        );
        let decision = runtime.try_reserve_promotion(hotter.as_path());
        let report = runtime.report();
        assert_eq!(
            decision,
            PromotionDecision::Replace {
                demote: PathBuf::from("/cold"),
                promote: hotter,
            },
            "{}",
            parity_diag(&metrics, &report)
        );
    }

    #[test]
    fn sim_and_runtime_share_empty_scan_l3_shape() {
        let world = World {
            config: WorkloadConfig {
                profile: WorkloadProfile::Dormant,
                dirs: 3,
                events: 0,
                duration_secs: 2,
                seed: 2,
            },
            dirs: vec![
                DirNode {
                    id: 0,
                    parent: None,
                    depth: 0,
                    importance: 0.0,
                    base_event_rate: 0.0,
                    scan_cost: 1,
                    watch_cost: 1,
                    children: vec![1, 2],
                },
                DirNode {
                    id: 1,
                    parent: Some(0),
                    depth: 1,
                    importance: 0.9,
                    base_event_rate: 0.01,
                    scan_cost: 1,
                    watch_cost: 1,
                    children: Vec::new(),
                },
                DirNode {
                    id: 2,
                    parent: Some(0),
                    depth: 1,
                    importance: 0.4,
                    base_event_rate: 0.01,
                    scan_cost: 1,
                    watch_cost: 1,
                    children: Vec::new(),
                },
            ],
            events: Vec::new(),
        };
        let policy = PolicyParams {
            max_watch_dirs: 1,
            l1_scan_interval_secs: 1,
            l2_scan_interval_secs: 1,
            l1_empty_scans_to_l2: 1,
            l2_empty_scans_to_l3: 1,
            per_round_max_dirs: 2,
            per_round_max_files: 100,
            per_round_max_ms: 20,
            ..PolicyParams::default()
        };

        let metrics = run_simulation(&world, &policy);
        assert!(metrics.final_l3_dirs >= 1, "{}", sim_diag(&metrics));

        let runtime =
            TieredWatchRuntime::new(Vec::new(), vec![(PathBuf::from("/warm"), 1)], 1, 5_000, 20);
        let warm = Path::new("/warm");
        for _ in 0..2 {
            runtime.record_scan(
                warm,
                crate::index::tiered::ScanOutcome {
                    scanned: 1,
                    changed: 0,
                    elapsed_ms: 1,
                },
            );
            runtime.apply_scan_policy(warm, 1, 1, 1, 1);
        }
        let report = runtime.report();
        assert_eq!(report.l3_dirs, 1, "{}", parity_diag(&metrics, &report));
    }

    #[test]
    fn fixed_seed_synthetic_workloads_preserve_budget_and_accounting() {
        let policy = PolicyParams {
            max_watch_dirs: 48,
            l1_scan_interval_secs: 8,
            l2_scan_interval_secs: 30,
            l1_empty_scans_to_l2: 2,
            l2_empty_scans_to_l3: 2,
            per_round_max_dirs: 8,
            per_round_max_files: 2_000,
            per_round_max_ms: 16,
            ..PolicyParams::default()
        };
        let cases = [
            WorkloadConfig {
                profile: WorkloadProfile::Developer,
                dirs: 220,
                events: 1_200,
                duration_secs: 900,
                seed: 42,
            },
            WorkloadConfig {
                profile: WorkloadProfile::Burst,
                dirs: 180,
                events: 900,
                duration_secs: 600,
                seed: 99,
            },
            WorkloadConfig {
                profile: WorkloadProfile::Dormant,
                dirs: 160,
                events: 360,
                duration_secs: 1_200,
                seed: 123,
            },
            WorkloadConfig {
                profile: WorkloadProfile::Adversarial,
                dirs: 140,
                events: 420,
                duration_secs: 900,
                seed: 2_026,
            },
        ];

        for config in cases {
            let world = generate_world(config.clone());
            let metrics = run_simulation(&world, &policy);
            let final_dirs = metrics.final_l0_dirs
                + metrics.final_l1_dirs
                + metrics.final_l2_dirs
                + metrics.final_l3_dirs;

            assert!(
                metrics.watch_cost_peak <= policy.max_watch_dirs as u64,
                "watch budget exceeded for {:?} seed {}\n{}",
                config.profile,
                config.seed,
                sim_diag(&metrics)
            );
            assert_eq!(
                final_dirs,
                world.dirs.len(),
                "tier accounting drifted for {:?} seed {}\n{}",
                config.profile,
                config.seed,
                sim_diag(&metrics)
            );
            assert_eq!(
                metrics.detected + metrics.missed,
                world.events.len(),
                "event accounting drifted for {:?} seed {}\n{}",
                config.profile,
                config.seed,
                sim_diag(&metrics)
            );
            assert!(
                metrics.final_l0_dirs <= policy.max_watch_dirs as usize,
                "L0 directory count cannot exceed unit-cost lower bound for {:?} seed {}\n{}",
                config.profile,
                config.seed,
                sim_diag(&metrics)
            );
            assert!(
                metrics.sla_met <= metrics.detected,
                "SLA count cannot exceed detected events for {:?} seed {}\n{}",
                config.profile,
                config.seed,
                sim_diag(&metrics)
            );
        }
    }

    #[test]
    fn sim_distribution_moves_colder_with_aggressive_empty_scan_policy() {
        let world = generate_world(WorkloadConfig {
            profile: WorkloadProfile::Dormant,
            dirs: 220,
            events: 180,
            duration_secs: 1_800,
            seed: 7_777,
        });
        let conservative = PolicyParams {
            max_watch_dirs: 48,
            l1_scan_interval_secs: 12,
            l2_scan_interval_secs: 120,
            l1_empty_scans_to_l2: 8,
            l2_empty_scans_to_l3: 8,
            per_round_max_dirs: 12,
            per_round_max_files: 4_000,
            per_round_max_ms: 20,
            ..PolicyParams::default()
        };
        let aggressive = PolicyParams {
            max_watch_dirs: 48,
            l1_scan_interval_secs: 12,
            l2_scan_interval_secs: 30,
            l1_empty_scans_to_l2: 1,
            l2_empty_scans_to_l3: 1,
            per_round_max_dirs: 12,
            per_round_max_files: 4_000,
            per_round_max_ms: 20,
            ..PolicyParams::default()
        };

        let conservative_metrics = run_simulation(&world, &conservative);
        let aggressive_metrics = run_simulation(&world, &aggressive);

        assert!(
            aggressive_metrics.final_l3_dirs >= conservative_metrics.final_l3_dirs,
            "aggressive empty-scan policy should move at least as many dirs to L3\naggressive: {}\nconservative: {}",
            sim_diag(&aggressive_metrics),
            sim_diag(&conservative_metrics)
        );
        assert!(
            aggressive_metrics.final_l1_dirs + aggressive_metrics.final_l2_dirs
                <= conservative_metrics.final_l1_dirs + conservative_metrics.final_l2_dirs,
            "aggressive empty-scan policy should leave fewer warm/cold-scan dirs\naggressive: {}\nconservative: {}",
            sim_diag(&aggressive_metrics),
            sim_diag(&conservative_metrics)
        );
    }
}
