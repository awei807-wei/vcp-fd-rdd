//! Synthetic tiered-watcher simulation framework used by `fd-rdd-sim`.

pub mod metrics;
pub mod optimizer;
pub mod policy;
pub mod regression;
pub mod rng;
pub mod simulator;
pub mod world;

pub use metrics::{RunMetrics, RunReport};
pub use optimizer::{
    adversarial_report, conservative_tiered_watch_recommendation_from_reports, evolve_report,
    grid_report, optimize_report, single_report, tiered_watch_config_patch_toml,
    tiered_watch_config_patch_toml_from_report, tiered_watch_config_patch_toml_from_reports,
    tiered_watch_recommendation_from_report, write_existing_parent, BenchmarkReport,
    OptimizerConfig, TieredWatchRecommendation, SIM_ONLY_IGNORED_FIELDS,
};
pub use policy::{PolicyParams, ScoreWeights, SimTier};
pub use regression::{
    default_regression_policy, sim_regression_markdown, sim_regression_report, SimRegressionReport,
};
pub use simulator::run_simulation;
pub use world::{generate_world, WorkloadConfig, WorkloadProfile, World, WorldSummary};
