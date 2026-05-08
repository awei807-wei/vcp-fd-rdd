use serde::{Deserialize, Serialize};

use super::rng::Rng64;

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "kebab-case")]
pub enum WorkloadProfile {
    Developer,
    Burst,
    Dormant,
    Adversarial,
}

impl std::str::FromStr for WorkloadProfile {
    type Err = String;

    fn from_str(input: &str) -> Result<Self, Self::Err> {
        match input {
            "developer" | "dev" => Ok(Self::Developer),
            "burst" => Ok(Self::Burst),
            "dormant" | "cold" => Ok(Self::Dormant),
            "adversarial" | "adv" => Ok(Self::Adversarial),
            other => Err(format!("unknown workload profile: {other}")),
        }
    }
}

impl std::fmt::Display for WorkloadProfile {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let value = match self {
            Self::Developer => "developer",
            Self::Burst => "burst",
            Self::Dormant => "dormant",
            Self::Adversarial => "adversarial",
        };
        f.write_str(value)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WorkloadConfig {
    pub profile: WorkloadProfile,
    pub dirs: usize,
    pub events: usize,
    pub duration_secs: u64,
    pub seed: u64,
}

impl Default for WorkloadConfig {
    fn default() -> Self {
        Self {
            profile: WorkloadProfile::Developer,
            dirs: 1_000,
            events: 10_000,
            duration_secs: 3_600,
            seed: 42,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DirNode {
    pub id: usize,
    pub parent: Option<usize>,
    pub depth: u16,
    pub importance: f64,
    pub base_event_rate: f64,
    pub scan_cost: u32,
    pub watch_cost: u32,
    pub children: Vec<usize>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FileEvent {
    pub id: usize,
    pub dir: usize,
    pub at_secs: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QueryEvent {
    pub id: usize,
    pub dir: usize,
    pub at_secs: u64,
    pub query_depth: u16,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct World {
    pub config: WorkloadConfig,
    pub dirs: Vec<DirNode>,
    pub events: Vec<FileEvent>,
    pub queries: Vec<QueryEvent>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WorldSummary {
    pub profile: WorkloadProfile,
    pub dirs: usize,
    pub events: usize,
    pub duration_secs: u64,
    pub seed: u64,
    pub total_scan_cost: u64,
    pub total_watch_cost: u64,
    pub max_depth: u16,
}

impl World {
    pub fn summary(&self) -> WorldSummary {
        WorldSummary {
            profile: self.config.profile,
            dirs: self.dirs.len(),
            events: self.events.len(),
            duration_secs: self.config.duration_secs,
            seed: self.config.seed,
            total_scan_cost: self.dirs.iter().map(|d| d.scan_cost as u64).sum(),
            total_watch_cost: self.dirs.iter().map(|d| d.watch_cost as u64).sum(),
            max_depth: self.dirs.iter().map(|d| d.depth).max().unwrap_or(0),
        }
    }

    pub fn is_descendant_or_self(&self, child: usize, ancestor: usize) -> bool {
        let mut current = Some(child);
        while let Some(id) = current {
            if id == ancestor {
                return true;
            }
            current = self.dirs[id].parent;
        }
        false
    }

    pub fn ancestor_ids(&self, dir: usize) -> Vec<usize> {
        let mut out = Vec::new();
        let mut current = Some(dir);
        while let Some(id) = current {
            out.push(id);
            current = self.dirs[id].parent;
        }
        out
    }
}

pub fn generate_world(config: WorkloadConfig) -> World {
    let mut rng = Rng64::new(config.seed);
    let dir_count = config.dirs.max(1);
    let mut dirs = Vec::with_capacity(dir_count);

    dirs.push(DirNode {
        id: 0,
        parent: None,
        depth: 0,
        importance: 0.25,
        base_event_rate: 0.01,
        scan_cost: 16,
        watch_cost: 1,
        children: Vec::new(),
    });

    for id in 1..dir_count {
        let parent = choose_parent(&mut rng, &dirs, config.profile, id);
        let depth = dirs[parent].depth.saturating_add(1);
        let hot_band = hot_band(id, dir_count);
        let importance = importance_for(config.profile, hot_band, depth, &mut rng);
        let base_event_rate = event_rate_for(config.profile, hot_band, depth, &mut rng);
        let scan_cost = scan_cost_for(config.profile, hot_band, depth, &mut rng);

        dirs.push(DirNode {
            id,
            parent: Some(parent),
            depth,
            importance,
            base_event_rate,
            scan_cost,
            watch_cost: 1,
            children: Vec::new(),
        });
        dirs[parent].children.push(id);
    }

    let subtree_sizes = subtree_sizes(&dirs);
    for (dir, size) in dirs.iter_mut().zip(subtree_sizes) {
        dir.watch_cost = size.min(u32::MAX as usize) as u32;
    }

    let mut events = generate_events(&config, &dirs, &mut rng);
    events.sort_by_key(|event| (event.at_secs, event.id));
    for (idx, event) in events.iter_mut().enumerate() {
        event.id = idx;
    }

    let mut queries = generate_queries(&config, &dirs, &mut rng);
    queries.sort_by_key(|query| (query.at_secs, query.id));
    for (idx, query) in queries.iter_mut().enumerate() {
        query.id = idx;
    }

    World {
        config,
        dirs,
        events,
        queries,
    }
}

fn choose_parent(
    rng: &mut Rng64,
    dirs: &[DirNode],
    profile: WorkloadProfile,
    next_id: usize,
) -> usize {
    if next_id == 1 {
        return 0;
    }

    match profile {
        WorkloadProfile::Developer => {
            if rng.bool(0.72) {
                rng.usize_range(next_id.saturating_sub(80).max(1), next_id)
            } else {
                rng.usize_range(0, next_id)
            }
        }
        WorkloadProfile::Burst => {
            if rng.bool(0.82) {
                let cluster = (next_id / 32) * 32;
                rng.usize_range(cluster.saturating_sub(24).max(1), next_id)
            } else {
                rng.usize_range(0, next_id)
            }
        }
        WorkloadProfile::Dormant => {
            if rng.bool(0.65) {
                rng.usize_range(0, next_id.min(64).max(1))
            } else {
                rng.usize_range(0, next_id)
            }
        }
        WorkloadProfile::Adversarial => {
            if rng.bool(0.58) {
                let deepest = dirs
                    .iter()
                    .max_by_key(|dir| (dir.depth, dir.scan_cost))
                    .map(|dir| dir.id)
                    .unwrap_or(0);
                if deepest != next_id {
                    return deepest;
                }
            }
            rng.usize_range(0, next_id)
        }
    }
}

fn hot_band(id: usize, total: usize) -> f64 {
    let ratio = id as f64 / total.max(1) as f64;
    if ratio < 0.04 {
        1.0
    } else if ratio < 0.20 {
        0.55
    } else {
        0.12
    }
}

fn importance_for(profile: WorkloadProfile, hot_band: f64, depth: u16, rng: &mut Rng64) -> f64 {
    let jitter = rng.next_f64() * 0.18;
    let depth_bonus = (depth as f64 / 12.0).min(0.35);
    match profile {
        WorkloadProfile::Developer => (0.20 + hot_band * 0.70 + jitter).min(1.0),
        WorkloadProfile::Burst => (0.16 + hot_band * 0.50 + jitter).min(1.0),
        WorkloadProfile::Dormant => (0.12 + hot_band * 0.35 + jitter).min(1.0),
        WorkloadProfile::Adversarial => (0.10 + depth_bonus + jitter).min(1.0),
    }
}

fn event_rate_for(profile: WorkloadProfile, hot_band: f64, depth: u16, rng: &mut Rng64) -> f64 {
    let jitter = 0.5 + rng.next_f64();
    match profile {
        WorkloadProfile::Developer => (0.01 + hot_band * 0.16) * jitter,
        WorkloadProfile::Burst => (0.005 + hot_band * 0.08) * jitter,
        WorkloadProfile::Dormant => (0.001 + hot_band * 0.015) * jitter,
        WorkloadProfile::Adversarial => {
            let depth_factor = (depth as f64 / 10.0).min(1.0);
            (0.002 + depth_factor * 0.04) * jitter
        }
    }
}

fn scan_cost_for(profile: WorkloadProfile, hot_band: f64, depth: u16, rng: &mut Rng64) -> u32 {
    let base = match profile {
        WorkloadProfile::Developer => 16.0 + hot_band * 96.0,
        WorkloadProfile::Burst => 12.0 + hot_band * 64.0,
        WorkloadProfile::Dormant => 8.0 + (1.0 - hot_band) * 36.0,
        WorkloadProfile::Adversarial => 24.0 + depth as f64 * 10.0,
    };
    (base * (0.7 + rng.next_f64())).round().max(1.0) as u32
}

fn subtree_sizes(dirs: &[DirNode]) -> Vec<usize> {
    let mut sizes = vec![1usize; dirs.len()];
    for id in (1..dirs.len()).rev() {
        if let Some(parent) = dirs[id].parent {
            sizes[parent] = sizes[parent].saturating_add(sizes[id]);
        }
    }
    sizes
}

fn generate_events(config: &WorkloadConfig, dirs: &[DirNode], rng: &mut Rng64) -> Vec<FileEvent> {
    let mut events = Vec::with_capacity(config.events);
    let cumulative = cumulative_weights(config.profile, dirs);

    for id in 0..config.events {
        let dir = sample_weighted(&cumulative, rng.next_f64());
        let at_secs = match config.profile {
            WorkloadProfile::Developer => developer_time(config.duration_secs, rng),
            WorkloadProfile::Burst => burst_time(config.duration_secs, id, rng),
            WorkloadProfile::Dormant => dormant_time(config.duration_secs, rng),
            WorkloadProfile::Adversarial => adversarial_time(config.duration_secs, id, rng),
        };
        events.push(FileEvent { id, dir, at_secs });
    }

    events
}

fn generate_queries(config: &WorkloadConfig, dirs: &[DirNode], rng: &mut Rng64) -> Vec<QueryEvent> {
    let query_count = config.events.saturating_mul(2);
    let mut queries = Vec::with_capacity(query_count);

    let cumulative = importance_weights(dirs);

    for id in 0..query_count {
        let dir = sample_weighted(&cumulative, rng.next_f64());
        let at_secs = rng.u64_range(0, config.duration_secs.max(1));
        let max_depth = dirs[dir].depth.saturating_add(1);
        let query_depth = rng.u64_range(1, max_depth as u64 + 1) as u16;
        queries.push(QueryEvent { id, dir, at_secs, query_depth });
    }

    queries
}

fn importance_weights(dirs: &[DirNode]) -> Vec<f64> {
    let mut total = 0.0;
    let mut cumulative = Vec::with_capacity(dirs.len());
    for dir in dirs {
        total += dir.importance.max(0.0);
        cumulative.push(total);
    }
    if total <= f64::EPSILON {
        return (1..=dirs.len()).map(|v| v as f64).collect();
    }
    cumulative
}

fn cumulative_weights(profile: WorkloadProfile, dirs: &[DirNode]) -> Vec<f64> {
    let mut total = 0.0;
    let mut cumulative = Vec::with_capacity(dirs.len());
    for dir in dirs {
        let profile_bias = match profile {
            WorkloadProfile::Developer => 1.0 + dir.importance * 4.0,
            WorkloadProfile::Burst => 1.0 + dir.base_event_rate * 18.0,
            WorkloadProfile::Dormant => {
                if dir.importance < 0.35 {
                    4.0
                } else {
                    0.8
                }
            }
            WorkloadProfile::Adversarial => {
                1.0 + dir.depth as f64 * 0.7 + dir.scan_cost as f64 / 80.0
            }
        };
        total += (dir.base_event_rate + 0.001) * profile_bias;
        cumulative.push(total);
    }
    if total <= f64::EPSILON {
        return (1..=dirs.len()).map(|v| v as f64).collect();
    }
    cumulative
}

fn sample_weighted(cumulative: &[f64], value: f64) -> usize {
    let Some(total) = cumulative.last().copied() else {
        return 0;
    };
    let target = value * total;
    match cumulative.binary_search_by(|probe| probe.total_cmp(&target)) {
        Ok(idx) | Err(idx) => idx.min(cumulative.len().saturating_sub(1)),
    }
}

fn developer_time(duration: u64, rng: &mut Rng64) -> u64 {
    if duration <= 1 {
        return 0;
    }
    if rng.bool(0.70) {
        let active_start = duration / 5;
        let active_end = (duration * 4 / 5).max(active_start + 1);
        rng.u64_range(active_start, active_end)
    } else {
        rng.u64_range(0, duration)
    }
}

fn burst_time(duration: u64, id: usize, rng: &mut Rng64) -> u64 {
    if duration <= 1 {
        return 0;
    }
    let bursts = 8u64;
    let center = ((id as u64 % bursts) * duration / bursts).min(duration - 1);
    let width = (duration / 80).max(5);
    let start = center.saturating_sub(width);
    let end = (center + width).min(duration);
    rng.u64_range(start, end.max(start + 1))
}

fn dormant_time(duration: u64, rng: &mut Rng64) -> u64 {
    if duration <= 1 {
        return 0;
    }
    if rng.bool(0.75) {
        rng.u64_range(duration * 2 / 3, duration)
    } else {
        rng.u64_range(0, duration)
    }
}

fn adversarial_time(duration: u64, id: usize, rng: &mut Rng64) -> u64 {
    if duration <= 1 {
        return 0;
    }
    let scan_edge = 60 + (id as u64 % 17) * 29;
    let at = scan_edge % duration;
    (at + rng.u64_range(1, 12)).min(duration - 1)
}
