//! Synthetic tiered-watcher simulation framework used by `fd-rdd-sim`.

pub mod metrics;
pub mod optimizer;
pub mod policy;
pub mod rng;
pub mod simulator;
pub mod world;

pub use metrics::{RunMetrics, RunReport};
pub use optimizer::{
    adversarial_report, evolve_report, grid_report, optimize_report, single_report,
    BenchmarkReport, OptimizerConfig,
};
pub use policy::{PolicyParams, ScoreWeights, SimTier};
pub use simulator::run_simulation;
pub use world::{generate_world, WorkloadConfig, WorkloadProfile, World, WorldSummary};