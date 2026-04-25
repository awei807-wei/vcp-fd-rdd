//! System resource monitor for CI integration tests.
//!
//! Wraps `sysinfo` to sample CPU / RSS of a specific process and produce
//! threshold-friendly reports.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant};

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
    #[allow(dead_code)]
    pub avg_cpu_percent: f32,
    #[allow(dead_code)]
    pub avg_rss_bytes: u64,
    #[allow(dead_code)]
    pub samples: Vec<ProcessSample>,
    #[allow(dead_code)]
    pub duration_secs: f64,
    /// How long CPU stayed at or above 100 % (milliseconds).
    pub cpu_100pct_duration_ms: u64,
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
