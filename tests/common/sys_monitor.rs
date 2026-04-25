//! System resource monitor for CI integration tests.
//!
//! Wraps `sysinfo` to sample CPU / RSS of a specific process and produce
//! threshold-friendly reports.

use std::collections::HashMap;
use std::fs;
use std::path::Path;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant};

use serde::{Deserialize, Serialize};
use sysinfo::{ProcessRefreshKind, System};

/// A single sample of process resource usage.
#[derive(Debug, Clone)]
pub struct ProcessSample {
    pub timestamp_ms: u64,
    pub cpu_percent: f32,
    pub rss_bytes: u64,
}

/// Aggregated statistics for a monitoring session.
#[derive(Debug, Clone)]
pub struct ProcessStats {
    pub max_cpu_percent: f32,
    pub max_rss_bytes: u64,
    pub avg_cpu_percent: f32,
    pub avg_rss_bytes: u64,
    pub samples: Vec<ProcessSample>,
    pub duration_secs: f64,
    /// How long CPU stayed at or above 100 % (milliseconds).
    pub cpu_100pct_duration_ms: u64,
}

/// Metrics for a single test phase, in the shape CI expects.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct PhaseMetrics {
    pub cpu_peak_percent: u32,
    pub cpu_100_duration_ms: u64,
    pub rss_peak_kb: u64,
}

impl PhaseMetrics {
    pub fn from_stats(stats: &ProcessStats) -> Self {
        Self {
            cpu_peak_percent: stats.max_cpu_percent as u32,
            cpu_100_duration_ms: stats.cpu_100pct_duration_ms,
            rss_peak_kb: stats.max_rss_bytes / 1024,
        }
    }
}

/// Full metrics report written for CI consumption.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct MetricsReport {
    pub phases: HashMap<String, PhaseMetrics>,
    #[serde(flatten)]
    pub overall: OverallMetrics,
}

/// Overall / flat metrics (also written at root level via flatten).
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct OverallMetrics {
    pub rss_peak_kb: u64,
    pub cpu_100_duration_ms: u64,
}

/// Collects per-phase metrics and writes the final JSON report.
#[derive(Debug, Clone, Default)]
pub struct MetricsCollector {
    report: MetricsReport,
}

impl MetricsCollector {
    pub fn new() -> Self {
        Self::default()
    }

    /// Record metrics for a named phase.
    pub fn record(&mut self, phase: &str, stats: &ProcessStats) {
        let phase_metrics = PhaseMetrics::from_stats(stats);
        self.report
            .phases
            .insert(phase.to_string(), phase_metrics.clone());

        // Update overall peak values.
        self.report.overall.rss_peak_kb = self
            .report
            .overall
            .rss_peak_kb
            .max(phase_metrics.rss_peak_kb);
        self.report.overall.cpu_100_duration_ms = self
            .report
            .overall
            .cpu_100_duration_ms
            .max(phase_metrics.cpu_100_duration_ms);
    }

    /// Write the report to the canonical CI metrics file.
    pub fn write_to_file(&self, path: &Path) {
        let json = serde_json::to_string_pretty(&self.report).expect("serialize metrics");
        if let Some(parent) = path.parent() {
            let _ = fs::create_dir_all(parent);
        }
        fs::write(path, json).unwrap_or_else(|e| {
            eprintln!(
                "[WARN] Failed to write metrics to {}: {}",
                path.display(),
                e
            );
        });
        println!("[METRICS-FILE] Written to {}", path.display());
    }

    /// Convenience: write to the CI-agreed default path.
    pub fn write_default(&self) {
        self.write_to_file(Path::new("/tmp/fd-rdd-hybrid-metrics.json"));
    }

    pub fn report(&self) -> &MetricsReport {
        &self.report
    }
}

/// Monitors a specific process (by PID) for CPU and RAM usage.
pub struct ProcessMonitor {
    pid: u32,
    interval_ms: u64,
    samples: Arc<Mutex<Vec<ProcessSample>>>,
    handle: Option<JoinHandle<()>>,
    running: Arc<AtomicBool>,
}

impl ProcessMonitor {
    /// Create with default 500 ms sampling interval.
    pub fn new(pid: u32) -> Self {
        Self::with_interval(pid, 500)
    }

    /// Create with a custom sampling interval (milliseconds).
    pub fn with_interval(pid: u32, interval_ms: u64) -> Self {
        Self {
            pid,
            interval_ms,
            samples: Arc::new(Mutex::new(Vec::new())),
            handle: None,
            running: Arc::new(AtomicBool::new(false)),
        }
    }

    /// Start the background sampling thread.
    pub fn start(&mut self) {
        let pid = self.pid;
        let interval = Duration::from_millis(self.interval_ms);
        let samples = self.samples.clone();
        let running = self.running.clone();
        running.store(true, Ordering::SeqCst);

        let handle = thread::spawn(move || {
            let mut system = System::new_all();
            let start = Instant::now();

            while running.load(Ordering::SeqCst) {
                system.refresh_processes_specifics(
                    ProcessRefreshKind::new().with_cpu().with_memory(),
                );

                if let Some(process) = system.process(sysinfo::Pid::from_u32(pid)) {
                    let sample = ProcessSample {
                        timestamp_ms: start.elapsed().as_millis() as u64,
                        cpu_percent: process.cpu_usage(),
                        rss_bytes: process.memory(),
                    };
                    samples.lock().unwrap().push(sample);
                }

                thread::sleep(interval);
            }
        });

        self.handle = Some(handle);
    }

    /// Stop monitoring and return aggregated statistics.
    pub fn stop(&mut self) -> ProcessStats {
        self.running.store(false, Ordering::SeqCst);
        if let Some(handle) = self.handle.take() {
            let _ = handle.join();
        }

        let samples = self.samples.lock().unwrap().clone();
        Self::compute_stats(&samples)
    }

    fn compute_stats(samples: &[ProcessSample]) -> ProcessStats {
        if samples.is_empty() {
            return ProcessStats {
                max_cpu_percent: 0.0,
                max_rss_bytes: 0,
                avg_cpu_percent: 0.0,
                avg_rss_bytes: 0,
                samples: Vec::new(),
                duration_secs: 0.0,
                cpu_100pct_duration_ms: 0,
            };
        }

        let mut max_cpu = 0.0f32;
        let mut max_rss = 0u64;
        let mut cpu_sum = 0.0f32;
        let mut rss_sum = 0u64;
        let mut cpu_100_duration_ms = 0u64;

        for i in 0..samples.len() {
            let s = &samples[i];
            max_cpu = max_cpu.max(s.cpu_percent);
            max_rss = max_rss.max(s.rss_bytes);
            cpu_sum += s.cpu_percent;
            rss_sum += s.rss_bytes;

            if s.cpu_percent >= 100.0 {
                let next_ts = samples
                    .get(i + 1)
                    .map(|n| n.timestamp_ms)
                    .unwrap_or(s.timestamp_ms + 1000);
                let interval = next_ts.saturating_sub(s.timestamp_ms);
                cpu_100_duration_ms += interval;
            }
        }

        let count = samples.len() as f32;
        let duration_ms = samples.last().unwrap().timestamp_ms;

        ProcessStats {
            max_cpu_percent: max_cpu,
            max_rss_bytes: max_rss,
            avg_cpu_percent: cpu_sum / count,
            avg_rss_bytes: (rss_sum as f32 / count) as u64,
            samples: samples.to_vec(),
            duration_secs: duration_ms as f64 / 1000.0,
            cpu_100pct_duration_ms: cpu_100_duration_ms,
        }
    }
}

/// Wrap a closure with process monitoring and return `(closure_result, stats)`.
pub fn with_monitor<F, R>(pid: u32, interval_ms: u64, f: F) -> (R, ProcessStats)
where
    F: FnOnce() -> R,
{
    let mut monitor = ProcessMonitor::with_interval(pid, interval_ms);
    monitor.start();
    let result = f();
    let stats = monitor.stop();
    (result, stats)
}

/// Print stats in a CI-friendly format and enforce thresholds.
///
/// Thresholds:
/// - CPU 100 % duration <= 3 000 ms
/// - Max RSS <= 400 MB (for 800 K files)
pub fn print_ci_stats(label: &str, stats: &ProcessStats) {
    println!(
        "[METRIC] {} | max_cpu={:.1}% | max_rss={:.1}MB | avg_cpu={:.1}% | avg_rss={:.1}MB | duration={:.1}s | cpu_100pct_ms={}",
        label,
        stats.max_cpu_percent,
        stats.max_rss_bytes as f64 / (1024.0 * 1024.0),
        stats.avg_cpu_percent,
        stats.avg_rss_bytes as f64 / (1024.0 * 1024.0),
        stats.duration_secs,
        stats.cpu_100pct_duration_ms,
    );

    if stats.cpu_100pct_duration_ms > 3000 {
        println!(
            "[THRESHOLD-FAIL] {}: CPU 100% duration {}ms exceeds 3000ms limit",
            label, stats.cpu_100pct_duration_ms
        );
    }
    if stats.max_rss_bytes > 400 * 1024 * 1024 {
        println!(
            "[THRESHOLD-FAIL] {}: Max RSS {:.1}MB exceeds 400MB limit",
            label,
            stats.max_rss_bytes as f64 / (1024.0 * 1024.0)
        );
    }
}

/// Assert thresholds and panic if violated.
pub fn assert_thresholds(label: &str, stats: &ProcessStats) {
    assert!(
        stats.cpu_100pct_duration_ms <= 3000,
        "{}: CPU 100% duration {}ms exceeds 3000ms",
        label,
        stats.cpu_100pct_duration_ms
    );
    assert!(
        stats.max_rss_bytes <= 400 * 1024 * 1024,
        "{}: Max RSS {:.1}MB exceeds 400MB",
        label,
        stats.max_rss_bytes as f64 / (1024.0 * 1024.0)
    );
}
