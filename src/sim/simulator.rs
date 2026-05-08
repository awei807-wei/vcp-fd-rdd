use super::metrics::{summarize, RunMetrics};
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

    for now in 0..=world.config.duration_secs {
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
                if promote_dir(
                    world,
                    &mut dirs,
                    &policy,
                    dir_id,
                    now,
                    &mut current_watch_cost,
                ) {
                    watch_cost_peak = watch_cost_peak.max(current_watch_cost);
                } else {
                    dirs[dir_id].tier = SimTier::L1;
                    dirs[dir_id].next_scan_at =
                        now.saturating_add(dirs[dir_id].tier.scan_interval_secs(&policy));
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

    summarize(
        &policy,
        &detected_latencies,
        world.events.len(),
        watch_cost_peak,
        scan_rounds,
        scanned_dirs,
        scanned_files,
    )
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

fn promote_dir(
    world: &World,
    dirs: &mut [DirRuntime],
    policy: &PolicyParams,
    dir_id: usize,
    now: u64,
    current_watch_cost: &mut u64,
) -> bool {
    if dir_id == 0 || covered_by_l0(world, dirs, dir_id) {
        return true;
    }

    let cost = world.dirs[dir_id].watch_cost as u64;
    let budget = policy.max_watch_dirs as u64;
    if cost > budget {
        return false;
    }

    if current_watch_cost.saturating_add(cost) <= budget {
        dirs[dir_id].tier = SimTier::L0;
        *current_watch_cost = current_watch_cost.saturating_add(cost);
        return true;
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
        return false;
    };
    let replace_cost = world.dirs[replace_id].watch_cost as u64;
    if candidate_score <= replace_score
        || current_watch_cost
            .saturating_sub(replace_cost)
            .saturating_add(cost)
            > budget
    {
        return false;
    }

    dirs[replace_id].tier = SimTier::L1;
    dirs[replace_id].next_scan_at = now;
    dirs[replace_id].empty_scans = 0;
    dirs[dir_id].tier = SimTier::L0;
    *current_watch_cost = current_watch_cost
        .saturating_sub(replace_cost)
        .saturating_add(cost);
    true
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
    use crate::sim::world::{generate_world, WorkloadConfig, WorkloadProfile};

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

        assert!(roomy_metrics.watch_cost_peak <= roomy.max_watch_dirs as u64);
        assert!(tight_metrics.watch_cost_peak <= tight.max_watch_dirs as u64);
        assert!(roomy_metrics.scanned_dirs <= tight_metrics.scanned_dirs);
    }
}
