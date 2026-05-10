use serde::{Deserialize, Serialize};

use super::rng::Rng64;

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "kebab-case")]
pub enum WorkloadProfile {
    Developer,
    Burst,
    Dormant,
    Adversarial,
    HomeDesktop,
}

impl std::str::FromStr for WorkloadProfile {
    type Err = String;

    fn from_str(input: &str) -> Result<Self, Self::Err> {
        match input {
            "developer" | "dev" => Ok(Self::Developer),
            "burst" => Ok(Self::Burst),
            "dormant" | "cold" => Ok(Self::Dormant),
            "adversarial" | "adv" => Ok(Self::Adversarial),
            "home-desktop" | "home_desktop" | "home" | "desktop" => Ok(Self::HomeDesktop),
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
            Self::HomeDesktop => "home-desktop",
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
pub struct World {
    pub config: WorkloadConfig,
    pub dirs: Vec<DirNode>,
    pub events: Vec<FileEvent>,
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
        let parent = choose_parent(&mut rng, &dirs, config.profile, id, dir_count);
        let depth = dirs[parent].depth.saturating_add(1);
        let hot_band = hot_band(id, dir_count);
        let importance = importance_for(config.profile, id, dir_count, hot_band, depth, &mut rng);
        let base_event_rate =
            event_rate_for(config.profile, id, dir_count, hot_band, depth, &mut rng);
        let scan_cost = scan_cost_for(config.profile, id, dir_count, hot_band, depth, &mut rng);

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

    World {
        config,
        dirs,
        events,
    }
}

fn choose_parent(
    rng: &mut Rng64,
    dirs: &[DirNode],
    profile: WorkloadProfile,
    next_id: usize,
    total_dirs: usize,
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
                rng.usize_range(0, next_id.clamp(1, 64))
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
        WorkloadProfile::HomeDesktop => choose_home_desktop_parent(rng, next_id, total_dirs),
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

fn importance_for(
    profile: WorkloadProfile,
    id: usize,
    total: usize,
    hot_band: f64,
    depth: u16,
    rng: &mut Rng64,
) -> f64 {
    let jitter = rng.next_f64() * 0.18;
    let depth_bonus = (depth as f64 / 12.0).min(0.35);
    match profile {
        WorkloadProfile::Developer => (0.20 + hot_band * 0.70 + jitter).min(1.0),
        WorkloadProfile::Burst => (0.16 + hot_band * 0.50 + jitter).min(1.0),
        WorkloadProfile::Dormant => (0.12 + hot_band * 0.35 + jitter).min(1.0),
        WorkloadProfile::Adversarial => (0.10 + depth_bonus + jitter).min(1.0),
        WorkloadProfile::HomeDesktop => {
            home_desktop_importance(home_desktop_kind_for_id(id, total), depth, rng)
        }
    }
}

fn event_rate_for(
    profile: WorkloadProfile,
    id: usize,
    total: usize,
    hot_band: f64,
    depth: u16,
    rng: &mut Rng64,
) -> f64 {
    let jitter = 0.5 + rng.next_f64();
    match profile {
        WorkloadProfile::Developer => (0.01 + hot_band * 0.16) * jitter,
        WorkloadProfile::Burst => (0.005 + hot_band * 0.08) * jitter,
        WorkloadProfile::Dormant => (0.001 + hot_band * 0.015) * jitter,
        WorkloadProfile::Adversarial => {
            let depth_factor = (depth as f64 / 10.0).min(1.0);
            (0.002 + depth_factor * 0.04) * jitter
        }
        WorkloadProfile::HomeDesktop => {
            home_desktop_event_rate(home_desktop_kind_for_id(id, total), depth, rng)
        }
    }
}

fn scan_cost_for(
    profile: WorkloadProfile,
    id: usize,
    total: usize,
    hot_band: f64,
    depth: u16,
    rng: &mut Rng64,
) -> u32 {
    let base = match profile {
        WorkloadProfile::Developer => 16.0 + hot_band * 96.0,
        WorkloadProfile::Burst => 12.0 + hot_band * 64.0,
        WorkloadProfile::Dormant => 8.0 + (1.0 - hot_band) * 36.0,
        WorkloadProfile::Adversarial => 24.0 + depth as f64 * 10.0,
        WorkloadProfile::HomeDesktop => {
            return home_desktop_scan_cost(home_desktop_kind_for_id(id, total), depth, rng);
        }
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
            WorkloadProfile::HomeDesktop => {
                home_desktop_time(config.duration_secs, dirs, dir, id, rng)
            }
        };
        events.push(FileEvent { id, dir, at_secs });
    }

    events
}

fn cumulative_weights(profile: WorkloadProfile, dirs: &[DirNode]) -> Vec<f64> {
    let mut total = 0.0;
    let mut cumulative = Vec::with_capacity(dirs.len());
    for dir in dirs {
        let weight = match profile {
            WorkloadProfile::Developer => {
                (dir.base_event_rate + 0.001) * (1.0 + dir.importance * 4.0)
            }
            WorkloadProfile::Burst => {
                (dir.base_event_rate + 0.001) * (1.0 + dir.base_event_rate * 18.0)
            }
            WorkloadProfile::Dormant => {
                let profile_bias = if dir.importance < 0.35 { 4.0 } else { 0.8 };
                (dir.base_event_rate + 0.001) * profile_bias
            }
            WorkloadProfile::Adversarial => {
                (dir.base_event_rate + 0.001)
                    * (1.0 + dir.depth as f64 * 0.7 + dir.scan_cost as f64 / 80.0)
            }
            WorkloadProfile::HomeDesktop => home_desktop_sampling_weight(dirs, dir),
        };
        total += weight;
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

const HOME_DESKTOP_ROOT_COUNT: usize = 11;
const HOME_DOWNLOADS_ROOT: usize = 1;
const HOME_DOCUMENTS_ROOT: usize = 2;
const HOME_DESKTOP_ROOT: usize = 3;
const HOME_PICTURES_ROOT: usize = 4;
const HOME_VIDEOS_ROOT: usize = 5;
const HOME_CODE_ROOT: usize = 6;
const HOME_ARCHIVE_ROOT: usize = 7;
const HOME_NAS_ROOT: usize = 8;
const HOME_CACHE_ROOT: usize = 9;
const HOME_NODE_MODULES_ROOT: usize = 10;
const HOME_BUILD_ROOT: usize = 11;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum HomeDesktopKind {
    Root,
    Downloads,
    Documents,
    Desktop,
    Pictures,
    Videos,
    Code,
    Archive,
    Nas,
    ExcludedCache,
    ExcludedNodeModules,
    ExcludedBuild,
}

fn choose_home_desktop_parent(rng: &mut Rng64, next_id: usize, total_dirs: usize) -> usize {
    if next_id <= HOME_DESKTOP_ROOT_COUNT {
        return 0;
    }

    let kind = home_desktop_kind_for_id(next_id, total_dirs);
    let root = home_desktop_root_id(kind);
    if root == 0 || root >= next_id {
        return 0;
    }

    let (start, end) = home_desktop_id_range(kind, total_dirs);
    let upper = next_id.min(end);
    if start < upper && rng.bool(0.68) {
        let recent_start = upper.saturating_sub(48).max(start);
        rng.usize_range(recent_start, upper)
    } else {
        root
    }
}

fn home_desktop_kind_for_id(id: usize, total: usize) -> HomeDesktopKind {
    match id {
        0 => HomeDesktopKind::Root,
        HOME_DOWNLOADS_ROOT => HomeDesktopKind::Downloads,
        HOME_DOCUMENTS_ROOT => HomeDesktopKind::Documents,
        HOME_DESKTOP_ROOT => HomeDesktopKind::Desktop,
        HOME_PICTURES_ROOT => HomeDesktopKind::Pictures,
        HOME_VIDEOS_ROOT => HomeDesktopKind::Videos,
        HOME_CODE_ROOT => HomeDesktopKind::Code,
        HOME_ARCHIVE_ROOT => HomeDesktopKind::Archive,
        HOME_NAS_ROOT => HomeDesktopKind::Nas,
        HOME_CACHE_ROOT => HomeDesktopKind::ExcludedCache,
        HOME_NODE_MODULES_ROOT => HomeDesktopKind::ExcludedNodeModules,
        HOME_BUILD_ROOT => HomeDesktopKind::ExcludedBuild,
        _ => {
            let child_start = HOME_DESKTOP_ROOT_COUNT + 1;
            let child_count = total.saturating_sub(child_start).max(1);
            let child_idx = id.saturating_sub(child_start).min(child_count - 1);
            home_desktop_kind_for_ratio(child_idx as f64 / child_count as f64)
        }
    }
}

fn home_desktop_kind_for_dir(dirs: &[DirNode], dir_id: usize) -> HomeDesktopKind {
    let mut current = dir_id;
    while let Some(parent) = dirs[current].parent {
        if parent == 0 {
            return home_desktop_kind_for_id(current, dirs.len());
        }
        current = parent;
    }
    HomeDesktopKind::Root
}

fn home_desktop_kind_for_ratio(ratio: f64) -> HomeDesktopKind {
    match ratio {
        r if r < 0.13 => HomeDesktopKind::Downloads,
        r if r < 0.23 => HomeDesktopKind::Documents,
        r if r < 0.30 => HomeDesktopKind::Desktop,
        r if r < 0.48 => HomeDesktopKind::Pictures,
        r if r < 0.60 => HomeDesktopKind::Videos,
        r if r < 0.78 => HomeDesktopKind::Code,
        r if r < 0.88 => HomeDesktopKind::Archive,
        r if r < 0.95 => HomeDesktopKind::Nas,
        r if r < 0.97 => HomeDesktopKind::ExcludedCache,
        r if r < 0.99 => HomeDesktopKind::ExcludedNodeModules,
        _ => HomeDesktopKind::ExcludedBuild,
    }
}

fn home_desktop_ratio_range(kind: HomeDesktopKind) -> (f64, f64) {
    match kind {
        HomeDesktopKind::Downloads => (0.00, 0.13),
        HomeDesktopKind::Documents => (0.13, 0.23),
        HomeDesktopKind::Desktop => (0.23, 0.30),
        HomeDesktopKind::Pictures => (0.30, 0.48),
        HomeDesktopKind::Videos => (0.48, 0.60),
        HomeDesktopKind::Code => (0.60, 0.78),
        HomeDesktopKind::Archive => (0.78, 0.88),
        HomeDesktopKind::Nas => (0.88, 0.95),
        HomeDesktopKind::ExcludedCache => (0.95, 0.97),
        HomeDesktopKind::ExcludedNodeModules => (0.97, 0.99),
        HomeDesktopKind::ExcludedBuild => (0.99, 1.00),
        HomeDesktopKind::Root => (0.00, 0.00),
    }
}

fn home_desktop_id_range(kind: HomeDesktopKind, total: usize) -> (usize, usize) {
    let child_start = HOME_DESKTOP_ROOT_COUNT + 1;
    let child_count = total.saturating_sub(child_start);
    if child_count == 0 {
        return (child_start, child_start);
    }

    let (start_ratio, end_ratio) = home_desktop_ratio_range(kind);
    let start = child_start + (child_count as f64 * start_ratio).ceil() as usize;
    let end = child_start + (child_count as f64 * end_ratio).ceil() as usize;
    (start.min(total), end.max(start + 1).min(total))
}

fn home_desktop_root_id(kind: HomeDesktopKind) -> usize {
    match kind {
        HomeDesktopKind::Downloads => HOME_DOWNLOADS_ROOT,
        HomeDesktopKind::Documents => HOME_DOCUMENTS_ROOT,
        HomeDesktopKind::Desktop => HOME_DESKTOP_ROOT,
        HomeDesktopKind::Pictures => HOME_PICTURES_ROOT,
        HomeDesktopKind::Videos => HOME_VIDEOS_ROOT,
        HomeDesktopKind::Code => HOME_CODE_ROOT,
        HomeDesktopKind::Archive => HOME_ARCHIVE_ROOT,
        HomeDesktopKind::Nas => HOME_NAS_ROOT,
        HomeDesktopKind::ExcludedCache => HOME_CACHE_ROOT,
        HomeDesktopKind::ExcludedNodeModules => HOME_NODE_MODULES_ROOT,
        HomeDesktopKind::ExcludedBuild => HOME_BUILD_ROOT,
        HomeDesktopKind::Root => 0,
    }
}

fn home_desktop_importance(kind: HomeDesktopKind, depth: u16, rng: &mut Rng64) -> f64 {
    let jitter = rng.next_f64() * 0.12;
    let depth_penalty = (depth as f64 * 0.015).min(0.12);
    let base = match kind {
        HomeDesktopKind::Downloads => 0.72,
        HomeDesktopKind::Documents => 0.88,
        HomeDesktopKind::Desktop => 0.92,
        HomeDesktopKind::Pictures => 0.46,
        HomeDesktopKind::Videos => 0.38,
        HomeDesktopKind::Code => 0.64,
        HomeDesktopKind::Archive => 0.58,
        HomeDesktopKind::Nas => 0.70,
        HomeDesktopKind::ExcludedCache
        | HomeDesktopKind::ExcludedNodeModules
        | HomeDesktopKind::ExcludedBuild
        | HomeDesktopKind::Root => 0.0,
    };
    (base + jitter - depth_penalty).clamp(0.0, 1.0)
}

fn home_desktop_event_rate(kind: HomeDesktopKind, depth: u16, rng: &mut Rng64) -> f64 {
    let jitter = 0.65 + rng.next_f64() * 0.9;
    let depth_factor = (1.0 - depth as f64 * 0.025).max(0.55);
    let base = match kind {
        HomeDesktopKind::Downloads => 0.145,
        HomeDesktopKind::Documents => 0.030,
        HomeDesktopKind::Desktop => 0.048,
        HomeDesktopKind::Pictures => 0.0024,
        HomeDesktopKind::Videos => 0.0014,
        HomeDesktopKind::Code => 0.052,
        HomeDesktopKind::Archive => 0.0007,
        HomeDesktopKind::Nas => 0.0005,
        HomeDesktopKind::ExcludedCache
        | HomeDesktopKind::ExcludedNodeModules
        | HomeDesktopKind::ExcludedBuild
        | HomeDesktopKind::Root => 0.0,
    };
    base * jitter * depth_factor
}

fn home_desktop_scan_cost(kind: HomeDesktopKind, depth: u16, rng: &mut Rng64) -> u32 {
    let depth_factor = 1.0 + (depth as f64 * 0.08).min(0.7);
    let base = match kind {
        HomeDesktopKind::Downloads => 68.0,
        HomeDesktopKind::Documents => 18.0,
        HomeDesktopKind::Desktop => 14.0,
        HomeDesktopKind::Pictures => 760.0,
        HomeDesktopKind::Videos => 1_800.0,
        HomeDesktopKind::Code => 92.0,
        HomeDesktopKind::Archive => 1_150.0,
        HomeDesktopKind::Nas => 2_200.0,
        HomeDesktopKind::ExcludedCache
        | HomeDesktopKind::ExcludedNodeModules
        | HomeDesktopKind::ExcludedBuild => 2.0,
        HomeDesktopKind::Root => 16.0,
    };
    (base * depth_factor * (0.72 + rng.next_f64() * 0.72))
        .round()
        .max(1.0) as u32
}

fn home_desktop_sampling_weight(dirs: &[DirNode], dir: &DirNode) -> f64 {
    let kind = home_desktop_kind_for_dir(dirs, dir.id);
    match kind {
        HomeDesktopKind::Downloads => dir.base_event_rate * 9.0 + 0.010,
        HomeDesktopKind::Documents | HomeDesktopKind::Desktop => dir.base_event_rate * 4.0 + 0.008,
        HomeDesktopKind::Code => dir.base_event_rate * 5.0 + 0.006,
        HomeDesktopKind::Pictures | HomeDesktopKind::Videos => dir.base_event_rate * 0.8 + 0.001,
        HomeDesktopKind::Archive => dir.base_event_rate * 0.5 + 0.0008,
        HomeDesktopKind::Nas => dir.base_event_rate * 0.4 + 0.0015,
        HomeDesktopKind::ExcludedCache
        | HomeDesktopKind::ExcludedNodeModules
        | HomeDesktopKind::ExcludedBuild
        | HomeDesktopKind::Root => 0.0,
    }
}

fn home_desktop_time(
    duration: u64,
    dirs: &[DirNode],
    dir: usize,
    event_id: usize,
    rng: &mut Rng64,
) -> u64 {
    let kind = home_desktop_kind_for_dir(dirs, dir);
    match kind {
        HomeDesktopKind::Downloads => {
            if rng.bool(0.78) {
                burst_time(duration, event_id, rng)
            } else {
                developer_time(duration, rng)
            }
        }
        HomeDesktopKind::Documents | HomeDesktopKind::Desktop | HomeDesktopKind::Code => {
            developer_time(duration, rng)
        }
        HomeDesktopKind::Pictures | HomeDesktopKind::Videos => {
            if rng.bool(0.70) {
                dormant_time(duration, rng)
            } else {
                developer_time(duration, rng)
            }
        }
        HomeDesktopKind::Archive | HomeDesktopKind::Nas => cold_query_time(duration, event_id, rng),
        HomeDesktopKind::ExcludedCache
        | HomeDesktopKind::ExcludedNodeModules
        | HomeDesktopKind::ExcludedBuild
        | HomeDesktopKind::Root => dormant_time(duration, rng),
    }
}

fn cold_query_time(duration: u64, id: usize, rng: &mut Rng64) -> u64 {
    if duration <= 1 {
        return 0;
    }
    let centers = [duration / 6, duration / 2, duration.saturating_mul(5) / 6];
    let center = centers[id % centers.len()].min(duration - 1);
    let width = (duration / 120).max(3);
    let start = center.saturating_sub(width);
    let end = (center + width).min(duration);
    rng.u64_range(start, end.max(start + 1))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn dirs_for_kind(world: &World, kind: HomeDesktopKind) -> Vec<&DirNode> {
        world
            .dirs
            .iter()
            .filter(|dir| home_desktop_kind_for_dir(&world.dirs, dir.id) == kind)
            .collect()
    }

    fn event_count_for_kind(world: &World, kind: HomeDesktopKind) -> usize {
        world
            .events
            .iter()
            .filter(|event| home_desktop_kind_for_dir(&world.dirs, event.dir) == kind)
            .count()
    }

    fn avg_scan_cost(dirs: &[&DirNode]) -> f64 {
        let total = dirs.iter().map(|dir| dir.scan_cost as u64).sum::<u64>();
        total as f64 / dirs.len().max(1) as f64
    }

    #[test]
    fn home_desktop_profile_parses_and_displays() {
        assert_eq!(
            "home-desktop".parse::<WorkloadProfile>().unwrap(),
            WorkloadProfile::HomeDesktop
        );
        assert_eq!(WorkloadProfile::HomeDesktop.to_string(), "home-desktop");
    }

    #[test]
    fn home_desktop_world_models_hot_roots_exclusions_and_large_cold_dirs() {
        let world = generate_world(WorkloadConfig {
            profile: WorkloadProfile::HomeDesktop,
            dirs: 420,
            events: 12_000,
            duration_secs: 3_600,
            seed: 2_026,
        });

        let downloads_events = event_count_for_kind(&world, HomeDesktopKind::Downloads);
        let code_events = event_count_for_kind(&world, HomeDesktopKind::Code);
        let nas_events = event_count_for_kind(&world, HomeDesktopKind::Nas);
        let excluded_events = event_count_for_kind(&world, HomeDesktopKind::ExcludedCache)
            + event_count_for_kind(&world, HomeDesktopKind::ExcludedNodeModules)
            + event_count_for_kind(&world, HomeDesktopKind::ExcludedBuild);

        assert!(
            downloads_events > code_events,
            "Downloads should dominate event bursts: downloads={downloads_events} code={code_events}"
        );
        assert!(
            code_events > 0,
            "Code should remain capable of becoming hot"
        );
        assert!(
            nas_events > 0,
            "NAS should produce occasional cold-query pressure"
        );
        assert_eq!(
            excluded_events, 0,
            "default excluded trees must not emit workload events"
        );

        let docs = dirs_for_kind(&world, HomeDesktopKind::Documents);
        let desktop = dirs_for_kind(&world, HomeDesktopKind::Desktop);
        let pictures = dirs_for_kind(&world, HomeDesktopKind::Pictures);
        let videos = dirs_for_kind(&world, HomeDesktopKind::Videos);
        let nas = dirs_for_kind(&world, HomeDesktopKind::Nas);
        let excluded = dirs_for_kind(&world, HomeDesktopKind::ExcludedNodeModules);

        let small_high_value_avg = (avg_scan_cost(&docs) + avg_scan_cost(&desktop)) / 2.0;
        let media_avg = (avg_scan_cost(&pictures) + avg_scan_cost(&videos)) / 2.0;
        assert!(
            media_avg > small_high_value_avg * 20.0,
            "media dirs should be much more expensive to scan: media={media_avg:.1} small={small_high_value_avg:.1}"
        );
        assert!(
            avg_scan_cost(&nas) > media_avg,
            "NAS should be among the highest scan-cost cold roots"
        );
        assert!(
            avg_scan_cost(&excluded) <= 5.0,
            "node_modules/build/cache exclusions should be represented as near-zero scan work"
        );
    }
}
