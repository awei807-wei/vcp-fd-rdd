use std::collections::HashMap;

use serde::{Deserialize, Serialize};

use super::metrics::RunMetrics;
use super::policy::PolicyParams;
use super::simulator::run_simulation;
use super::world::{generate_world, WorkloadConfig, WorkloadProfile};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SimRegressionReport {
    pub mode: String,
    pub passed: bool,
    pub policy: PolicyParams,
    pub summary: SimRegressionSummary,
    pub cases: Vec<SimRegressionCaseReport>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SimRegressionSummary {
    pub total_cases: usize,
    pub passed_cases: usize,
    pub failed_cases: usize,
    pub worst_p95_discovery_delay_secs: u64,
    pub max_watch_cost_peak: u64,
    pub total_scanned_files: u64,
    pub total_promotion_budget_blocked: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SimRegressionCaseReport {
    pub name: String,
    pub workload: WorkloadConfig,
    pub thresholds: SimRegressionThresholds,
    pub observed: SimRegressionObserved,
    pub metrics: RunMetrics,
    pub passed: bool,
    pub failures: Vec<SimRegressionFailure>,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub struct SimRegressionThresholds {
    pub max_p95_discovery_delay_secs: u64,
    pub max_promotion_budget_blocked: u64,
    pub max_watch_cost_peak: u64,
    pub max_scanned_files: u64,
    pub min_final_l0_dirs: usize,
    pub max_final_l0_dirs: usize,
    pub min_final_l3_dirs: usize,
    pub max_final_l3_dirs: usize,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub struct SimRegressionObserved {
    pub p95_discovery_delay_secs: u64,
    pub promotion_budget_blocked: u64,
    pub watch_cost_peak: u64,
    pub scanned_files: u64,
    pub final_l0_dirs: usize,
    pub final_l3_dirs: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SimRegressionFailure {
    pub metric: String,
    pub actual: String,
    pub expected: String,
}

#[derive(Debug, Clone)]
struct SimRegressionCase {
    name: &'static str,
    workload: WorkloadConfig,
    thresholds: SimRegressionThresholds,
}

pub fn default_regression_policy() -> PolicyParams {
    PolicyParams {
        name: "tiered-regression-ci".to_string(),
        max_watch_dirs: 32,
        l1_scan_interval_secs: 8,
        l2_scan_interval_secs: 45,
        per_round_max_dirs: 8,
        per_round_max_files: 1_600,
        ..PolicyParams::default()
    }
    .sanitize()
}

pub fn sim_regression_report(policy: PolicyParams) -> SimRegressionReport {
    let policy = policy.sanitize();
    let cases = golden_regression_cases()
        .into_iter()
        .map(|case| run_case(case, &policy))
        .collect::<Vec<_>>();
    let passed_cases = cases.iter().filter(|case| case.passed).count();
    let summary = SimRegressionSummary {
        total_cases: cases.len(),
        passed_cases,
        failed_cases: cases.len().saturating_sub(passed_cases),
        worst_p95_discovery_delay_secs: cases
            .iter()
            .map(|case| case.observed.p95_discovery_delay_secs)
            .max()
            .unwrap_or(0),
        max_watch_cost_peak: cases
            .iter()
            .map(|case| case.observed.watch_cost_peak)
            .max()
            .unwrap_or(0),
        total_scanned_files: cases.iter().map(|case| case.observed.scanned_files).sum(),
        total_promotion_budget_blocked: cases
            .iter()
            .map(|case| case.observed.promotion_budget_blocked)
            .sum(),
    };

    SimRegressionReport {
        mode: "sim-regression".to_string(),
        passed: summary.failed_cases == 0,
        policy,
        summary,
        cases,
    }
}

pub fn sim_regression_markdown(
    report: &SimRegressionReport,
    baseline: Option<&SimRegressionReport>,
) -> String {
    let baseline_cases = baseline
        .map(|report| {
            report
                .cases
                .iter()
                .map(|case| (case.name.as_str(), case))
                .collect::<HashMap<_, _>>()
        })
        .unwrap_or_default();
    let mut out = String::new();
    out.push_str("# fd-rdd-sim Strategy Regression\n\n");
    out.push_str(&format!(
        "- Status: {}\n",
        if report.passed { "PASS" } else { "FAIL" }
    ));
    out.push_str(&format!("- Policy: `{}`\n", report.policy.name));
    out.push_str(&format!(
        "- Cases: {}/{} passed\n",
        report.summary.passed_cases, report.summary.total_cases
    ));
    out.push_str(&format!(
        "- Worst p95 discovery delay: {}s\n",
        report.summary.worst_p95_discovery_delay_secs
    ));
    out.push_str(&format!(
        "- Total scanned files: {}\n\n",
        report.summary.total_scanned_files
    ));

    out.push_str("| case | status | p95 discovery delay | promotion budget blocked | watch cost peak | scanned files | final L0 | final L3 |\n");
    out.push_str("|---|---:|---:|---:|---:|---:|---:|---:|\n");
    for case in &report.cases {
        let baseline = baseline_cases.get(case.name.as_str()).copied();
        out.push_str(&format!(
            "| {} | {} | {} | {} | {} | {} | {} | {} |\n",
            case.name,
            if case.passed { "PASS" } else { "FAIL" },
            format_delta_u64(
                case.observed.p95_discovery_delay_secs,
                baseline.map(|case| case.observed.p95_discovery_delay_secs),
                "s"
            ),
            format_delta_u64(
                case.observed.promotion_budget_blocked,
                baseline.map(|case| case.observed.promotion_budget_blocked),
                ""
            ),
            format_delta_u64(
                case.observed.watch_cost_peak,
                baseline.map(|case| case.observed.watch_cost_peak),
                ""
            ),
            format_delta_u64(
                case.observed.scanned_files,
                baseline.map(|case| case.observed.scanned_files),
                ""
            ),
            format_delta_usize(
                case.observed.final_l0_dirs,
                baseline.map(|case| case.observed.final_l0_dirs)
            ),
            format_delta_usize(
                case.observed.final_l3_dirs,
                baseline.map(|case| case.observed.final_l3_dirs)
            ),
        ));
    }

    let failures = report
        .cases
        .iter()
        .flat_map(|case| {
            case.failures
                .iter()
                .map(move |failure| (case.name.as_str(), failure))
        })
        .collect::<Vec<_>>();
    if !failures.is_empty() {
        out.push_str("\n## Threshold Failures\n\n");
        for (name, failure) in failures {
            out.push_str(&format!(
                "- `{name}` `{}`: actual {}, expected {}\n",
                failure.metric, failure.actual, failure.expected
            ));
        }
    }

    out
}

fn golden_regression_cases() -> Vec<SimRegressionCase> {
    vec![
        SimRegressionCase {
            name: "developer-small",
            workload: WorkloadConfig {
                profile: WorkloadProfile::Developer,
                dirs: 120,
                events: 480,
                duration_secs: 480,
                seed: 7_101,
            },
            thresholds: SimRegressionThresholds {
                max_p95_discovery_delay_secs: 70,
                max_promotion_budget_blocked: 400,
                max_watch_cost_peak: 32,
                max_scanned_files: 120_000,
                min_final_l0_dirs: 1,
                max_final_l0_dirs: 32,
                min_final_l3_dirs: 10,
                max_final_l3_dirs: 80,
            },
        },
        SimRegressionCase {
            name: "burst-small",
            workload: WorkloadConfig {
                profile: WorkloadProfile::Burst,
                dirs: 120,
                events: 520,
                duration_secs: 480,
                seed: 7_102,
            },
            thresholds: SimRegressionThresholds {
                max_p95_discovery_delay_secs: 70,
                max_promotion_budget_blocked: 350,
                max_watch_cost_peak: 32,
                max_scanned_files: 80_000,
                min_final_l0_dirs: 1,
                max_final_l0_dirs: 32,
                min_final_l3_dirs: 10,
                max_final_l3_dirs: 80,
            },
        },
        SimRegressionCase {
            name: "dormant-small",
            workload: WorkloadConfig {
                profile: WorkloadProfile::Dormant,
                dirs: 120,
                events: 240,
                duration_secs: 720,
                seed: 7_103,
            },
            thresholds: SimRegressionThresholds {
                max_p95_discovery_delay_secs: 90,
                max_promotion_budget_blocked: 180,
                max_watch_cost_peak: 32,
                max_scanned_files: 90_000,
                min_final_l0_dirs: 1,
                max_final_l0_dirs: 32,
                min_final_l3_dirs: 10,
                max_final_l3_dirs: 90,
            },
        },
        SimRegressionCase {
            name: "adversarial-small",
            workload: WorkloadConfig {
                profile: WorkloadProfile::Adversarial,
                dirs: 120,
                events: 300,
                duration_secs: 600,
                seed: 7_104,
            },
            thresholds: SimRegressionThresholds {
                max_p95_discovery_delay_secs: 140,
                max_promotion_budget_blocked: 300,
                max_watch_cost_peak: 32,
                max_scanned_files: 900_000,
                min_final_l0_dirs: 1,
                max_final_l0_dirs: 32,
                min_final_l3_dirs: 20,
                max_final_l3_dirs: 90,
            },
        },
        SimRegressionCase {
            name: "home-desktop-small",
            workload: WorkloadConfig {
                profile: WorkloadProfile::HomeDesktop,
                dirs: 140,
                events: 560,
                duration_secs: 600,
                seed: 7_105,
            },
            thresholds: SimRegressionThresholds {
                max_p95_discovery_delay_secs: 150,
                max_promotion_budget_blocked: 200,
                max_watch_cost_peak: 32,
                max_scanned_files: 1_100_000,
                min_final_l0_dirs: 1,
                max_final_l0_dirs: 32,
                min_final_l3_dirs: 50,
                max_final_l3_dirs: 120,
            },
        },
    ]
}

fn run_case(case: SimRegressionCase, policy: &PolicyParams) -> SimRegressionCaseReport {
    let world = generate_world(case.workload.clone());
    let metrics = run_simulation(&world, policy);
    let observed = SimRegressionObserved::from_metrics(&metrics);
    let failures = check_thresholds(observed, case.thresholds);
    SimRegressionCaseReport {
        name: case.name.to_string(),
        workload: case.workload,
        thresholds: case.thresholds,
        observed,
        metrics,
        passed: failures.is_empty(),
        failures,
    }
}

impl SimRegressionObserved {
    fn from_metrics(metrics: &RunMetrics) -> Self {
        Self {
            p95_discovery_delay_secs: metrics.p95_detect_secs,
            promotion_budget_blocked: metrics.promotion_budget_blocked,
            watch_cost_peak: metrics.watch_cost_peak,
            scanned_files: metrics.scanned_files,
            final_l0_dirs: metrics.final_l0_dirs,
            final_l3_dirs: metrics.final_l3_dirs,
        }
    }
}

fn check_thresholds(
    observed: SimRegressionObserved,
    thresholds: SimRegressionThresholds,
) -> Vec<SimRegressionFailure> {
    let mut failures = Vec::new();
    check_max_u64(
        &mut failures,
        "p95_discovery_delay_secs",
        observed.p95_discovery_delay_secs,
        thresholds.max_p95_discovery_delay_secs,
    );
    check_max_u64(
        &mut failures,
        "promotion_budget_blocked",
        observed.promotion_budget_blocked,
        thresholds.max_promotion_budget_blocked,
    );
    check_max_u64(
        &mut failures,
        "watch_cost_peak",
        observed.watch_cost_peak,
        thresholds.max_watch_cost_peak,
    );
    check_max_u64(
        &mut failures,
        "scanned_files",
        observed.scanned_files,
        thresholds.max_scanned_files,
    );
    check_range_usize(
        &mut failures,
        "final_l0_dirs",
        observed.final_l0_dirs,
        thresholds.min_final_l0_dirs,
        thresholds.max_final_l0_dirs,
    );
    check_range_usize(
        &mut failures,
        "final_l3_dirs",
        observed.final_l3_dirs,
        thresholds.min_final_l3_dirs,
        thresholds.max_final_l3_dirs,
    );
    failures
}

fn check_max_u64(failures: &mut Vec<SimRegressionFailure>, metric: &str, actual: u64, max: u64) {
    if actual > max {
        failures.push(SimRegressionFailure {
            metric: metric.to_string(),
            actual: actual.to_string(),
            expected: format!("<= {max}"),
        });
    }
}

fn check_range_usize(
    failures: &mut Vec<SimRegressionFailure>,
    metric: &str,
    actual: usize,
    min: usize,
    max: usize,
) {
    if actual < min || actual > max {
        failures.push(SimRegressionFailure {
            metric: metric.to_string(),
            actual: actual.to_string(),
            expected: format!("{min}..={max}"),
        });
    }
}

fn format_delta_u64(value: u64, baseline: Option<u64>, suffix: &str) -> String {
    match baseline {
        Some(baseline) => format!("{value}{suffix} ({:+})", value as i128 - baseline as i128),
        None => format!("{value}{suffix}"),
    }
}

fn format_delta_usize(value: usize, baseline: Option<usize>) -> String {
    match baseline {
        Some(baseline) => format!("{value} ({:+})", value as i128 - baseline as i128),
        None => value.to_string(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn golden_regression_suite_passes_with_default_policy() {
        let report = sim_regression_report(default_regression_policy());

        assert!(report.passed, "{:#?}", report.cases);
        assert_eq!(report.cases.len(), 5);
        assert_eq!(report.summary.failed_cases, 0);
        assert!(report
            .cases
            .iter()
            .any(|case| case.name == "home-desktop-small"));
    }

    #[test]
    fn regression_report_flags_threshold_failures() {
        let policy = PolicyParams {
            max_watch_dirs: 96,
            ..default_regression_policy()
        };

        let report = sim_regression_report(policy);

        assert!(!report.passed);
        assert!(report.cases.iter().any(|case| {
            case.failures
                .iter()
                .any(|failure| failure.metric == "watch_cost_peak")
        }));
    }

    #[test]
    fn markdown_includes_small_workloads_and_metric_columns() {
        let report = sim_regression_report(default_regression_policy());
        let markdown = sim_regression_markdown(&report, None);

        assert!(markdown.contains("developer-small"));
        assert!(markdown.contains("home-desktop-small"));
        assert!(markdown.contains("p95 discovery delay"));
        assert!(markdown.contains("promotion budget blocked"));
    }
}
