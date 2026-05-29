//! Best-effort I/O governor primitives for background scanners.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Mutex;
use std::time::{Duration, Instant};

use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct IoGovernorConfig {
    #[serde(default)]
    pub enabled: bool,
    #[serde(default = "default_stat_rate_per_sec")]
    pub stat_rate_per_sec: u64,
    #[serde(default = "default_some_threshold")]
    pub psi_some_avg10_threshold: f32,
    #[serde(default = "default_full_threshold")]
    pub psi_full_avg10_threshold: f32,
    #[serde(default = "default_max_backoff_ms")]
    pub max_backoff_ms: u64,
}

impl Default for IoGovernorConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            stat_rate_per_sec: default_stat_rate_per_sec(),
            psi_some_avg10_threshold: default_some_threshold(),
            psi_full_avg10_threshold: default_full_threshold(),
            max_backoff_ms: default_max_backoff_ms(),
        }
    }
}

fn default_stat_rate_per_sec() -> u64 {
    50_000
}

fn default_some_threshold() -> f32 {
    15.0
}

fn default_full_threshold() -> f32 {
    5.0
}

fn default_max_backoff_ms() -> u64 {
    1_000
}

const PRESSURE_SAMPLE_INTERVAL_OPS: u64 = 1024;

#[derive(Clone, Copy, Debug, Default, PartialEq)]
pub struct IoPressure {
    pub some_avg10: f32,
    pub full_avg10: f32,
}

pub fn parse_io_pressure(text: &str) -> Option<IoPressure> {
    let mut pressure = IoPressure::default();
    for line in text.lines() {
        let mut parts = line.split_whitespace();
        let kind = parts.next()?;
        for part in parts {
            let Some(value) = part.strip_prefix("avg10=") else {
                continue;
            };
            let parsed = value.parse::<f32>().ok()?;
            match kind {
                "some" => pressure.some_avg10 = parsed,
                "full" => pressure.full_avg10 = parsed,
                _ => {}
            }
        }
    }
    Some(pressure)
}

#[derive(Clone, Copy, Debug)]
pub struct BackoffPolicy {
    pub some_threshold: f32,
    pub full_threshold: f32,
    pub base_delay: Duration,
    pub max_delay: Duration,
}

impl Default for BackoffPolicy {
    fn default() -> Self {
        Self {
            some_threshold: 15.0,
            full_threshold: 5.0,
            base_delay: Duration::from_millis(5),
            max_delay: Duration::from_secs(1),
        }
    }
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct BackoffState {
    pub current_delay: Duration,
}

impl BackoffState {
    pub fn observe(&mut self, pressure: IoPressure, policy: BackoffPolicy) -> Duration {
        let high = pressure.some_avg10 > policy.some_threshold
            || pressure.full_avg10 > policy.full_threshold;
        if high {
            self.current_delay = if self.current_delay.is_zero() {
                policy.base_delay
            } else {
                (self.current_delay * 2).min(policy.max_delay)
            };
        } else {
            self.current_delay /= 2;
        }
        self.current_delay
    }
}

#[derive(Debug)]
pub struct TokenBucket {
    rate_per_sec: u64,
    burst: u64,
    tokens: f64,
    last: Instant,
}

impl TokenBucket {
    pub fn new(rate_per_sec: u64, burst: u64) -> Self {
        let burst = burst.max(1);
        Self {
            rate_per_sec: rate_per_sec.max(1),
            burst,
            tokens: burst as f64,
            last: Instant::now(),
        }
    }

    pub fn try_take_at(&mut self, now: Instant, n: u64) -> bool {
        let elapsed = now.saturating_duration_since(self.last).as_secs_f64();
        self.last = now;
        self.tokens = (self.tokens + elapsed * self.rate_per_sec as f64).min(self.burst as f64);
        if self.tokens >= n as f64 {
            self.tokens -= n as f64;
            true
        } else {
            false
        }
    }
}

#[derive(Debug)]
pub struct IoGovernor {
    enabled: bool,
    bucket: Mutex<TokenBucket>,
    backoff: Mutex<BackoffState>,
    pressure_policy: BackoffPolicy,
    pressure_sample_interval_ops: u64,
    last_pressure: Mutex<Option<IoPressure>>,
    operations: AtomicU64,
    backoff_count: AtomicU64,
    backoff_millis: AtomicU64,
    token_bucket_limited_count: AtomicU64,
}

impl IoGovernor {
    pub fn from_config(config: &IoGovernorConfig) -> Self {
        Self::with_policy(
            config.enabled,
            config.stat_rate_per_sec,
            BackoffPolicy {
                some_threshold: config.psi_some_avg10_threshold,
                full_threshold: config.psi_full_avg10_threshold,
                base_delay: Duration::from_millis(5),
                max_delay: Duration::from_millis(config.max_backoff_ms.max(1)),
            },
            PRESSURE_SAMPLE_INTERVAL_OPS,
        )
    }

    pub fn new(enabled: bool, stat_rate_per_sec: u64) -> Self {
        Self::with_policy(
            enabled,
            stat_rate_per_sec,
            BackoffPolicy::default(),
            PRESSURE_SAMPLE_INTERVAL_OPS,
        )
    }

    fn with_policy(
        enabled: bool,
        stat_rate_per_sec: u64,
        pressure_policy: BackoffPolicy,
        pressure_sample_interval_ops: u64,
    ) -> Self {
        Self {
            enabled,
            bucket: Mutex::new(TokenBucket::new(
                stat_rate_per_sec,
                stat_rate_per_sec.max(1),
            )),
            backoff: Mutex::new(BackoffState::default()),
            pressure_policy,
            pressure_sample_interval_ops: pressure_sample_interval_ops.max(1),
            last_pressure: Mutex::new(None),
            operations: AtomicU64::new(0),
            backoff_count: AtomicU64::new(0),
            backoff_millis: AtomicU64::new(0),
            token_bucket_limited_count: AtomicU64::new(0),
        }
    }

    pub fn disabled() -> Self {
        Self::new(false, u64::MAX / 4)
    }

    pub fn before_io(&self) {
        self.before_io_with_pressure_reader(read_linux_io_pressure);
    }

    fn before_io_with_pressure_reader(&self, read_pressure: impl FnOnce() -> Option<IoPressure>) {
        if !self.enabled {
            return;
        }
        let operation = self.operations.fetch_add(1, Ordering::Relaxed) + 1;
        loop {
            if self.bucket.lock().unwrap().try_take_at(Instant::now(), 1) {
                break;
            }
            self.token_bucket_limited_count
                .fetch_add(1, Ordering::Relaxed);
            std::thread::sleep(Duration::from_millis(1));
        }
        if operation.is_multiple_of(self.pressure_sample_interval_ops) {
            self.sample_pressure_with(read_pressure);
        }
    }

    fn sample_pressure_with(&self, read_pressure: impl FnOnce() -> Option<IoPressure>) {
        if let Some(pressure) = read_pressure() {
            self.observe_pressure(pressure, self.pressure_policy);
        }
    }

    pub fn observe_pressure(&self, pressure: IoPressure, policy: BackoffPolicy) {
        if !self.enabled {
            return;
        }
        *self.last_pressure.lock().unwrap() = Some(pressure);
        let delay = self.backoff.lock().unwrap().observe(pressure, policy);
        if delay.is_zero() {
            return;
        }
        self.backoff_count.fetch_add(1, Ordering::Relaxed);
        self.backoff_millis
            .fetch_add(delay.as_millis() as u64, Ordering::Relaxed);
        std::thread::sleep(delay);
    }

    pub fn operations(&self) -> u64 {
        self.operations.load(Ordering::Relaxed)
    }

    pub fn backoff_count(&self) -> u64 {
        self.backoff_count.load(Ordering::Relaxed)
    }

    pub fn last_pressure(&self) -> Option<IoPressure> {
        *self.last_pressure.lock().unwrap()
    }

    pub fn token_bucket_limited_count(&self) -> u64 {
        self.token_bucket_limited_count.load(Ordering::Relaxed)
    }
}

#[cfg(target_os = "linux")]
pub fn set_current_thread_idle_io_priority_best_effort() -> std::io::Result<()> {
    const IOPRIO_WHO_PROCESS: libc::c_int = 1;
    const IOPRIO_CLASS_IDLE: libc::c_int = 3;
    let prio = IOPRIO_CLASS_IDLE << 13;
    let tid = unsafe { libc::syscall(libc::SYS_gettid) as libc::c_int };
    let ret = unsafe { libc::syscall(libc::SYS_ioprio_set, IOPRIO_WHO_PROCESS, tid, prio) };
    if ret == 0 {
        Ok(())
    } else {
        Err(std::io::Error::last_os_error())
    }
}

#[cfg(not(target_os = "linux"))]
pub fn set_current_thread_idle_io_priority_best_effort() -> std::io::Result<()> {
    Err(std::io::Error::new(
        std::io::ErrorKind::Unsupported,
        "ioprio_set is Linux-only",
    ))
}

pub fn read_linux_io_pressure() -> Option<IoPressure> {
    std::fs::read_to_string("/proc/pressure/io")
        .ok()
        .and_then(|s| parse_io_pressure(&s))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_proc_pressure_io_sample() {
        let p = parse_io_pressure(
            "some avg10=12.34 avg60=3.00 avg300=0.50 total=1\nfull avg10=2.50 avg60=1.00 avg300=0.10 total=2\n",
        )
        .unwrap();
        assert_eq!(p.some_avg10, 12.34);
        assert_eq!(p.full_avg10, 2.50);
    }

    #[test]
    fn backoff_increases_and_recovers() {
        let policy = BackoffPolicy::default();
        let mut state = BackoffState::default();
        assert_eq!(
            state.observe(
                IoPressure {
                    some_avg10: 20.0,
                    full_avg10: 0.0
                },
                policy
            ),
            Duration::from_millis(5)
        );
        assert_eq!(
            state.observe(
                IoPressure {
                    some_avg10: 20.0,
                    full_avg10: 0.0
                },
                policy
            ),
            Duration::from_millis(10)
        );
        assert_eq!(
            state.observe(IoPressure::default(), policy),
            Duration::from_millis(5)
        );
    }

    #[test]
    fn token_bucket_respects_rate_without_time_drift() {
        let now = Instant::now();
        let mut bucket = TokenBucket::new(10, 2);
        assert!(bucket.try_take_at(now, 1));
        assert!(bucket.try_take_at(now, 1));
        assert!(!bucket.try_take_at(now, 1));
        assert!(bucket.try_take_at(now + Duration::from_millis(100), 1));
    }

    #[test]
    fn governor_counts_token_bucket_waits() {
        let governor = IoGovernor::new(true, 1);
        governor.before_io();
        governor.before_io();

        assert_eq!(governor.operations(), 2);
        assert!(governor.token_bucket_limited_count() > 0);
    }

    #[test]
    fn disabled_governor_does_not_count_token_bucket_waits() {
        let governor = IoGovernor::disabled();
        governor.before_io();

        assert_eq!(governor.operations(), 0);
        assert_eq!(governor.token_bucket_limited_count(), 0);
    }

    #[test]
    fn governor_records_last_pressure() {
        let governor = IoGovernor::new(true, 1_000);
        let pressure = IoPressure {
            some_avg10: 18.5,
            full_avg10: 1.25,
        };
        governor.observe_pressure(
            pressure,
            BackoffPolicy {
                some_threshold: 100.0,
                full_threshold: 100.0,
                base_delay: Duration::from_millis(1),
                max_delay: Duration::from_millis(1),
            },
        );

        assert_eq!(governor.last_pressure(), Some(pressure));
        assert_eq!(governor.backoff_count(), 0);
    }

    #[test]
    fn governor_samples_pressure_on_configured_operation_interval() {
        let governor = IoGovernor::with_policy(
            true,
            1_000,
            BackoffPolicy {
                some_threshold: 10.0,
                full_threshold: 5.0,
                base_delay: Duration::from_millis(1),
                max_delay: Duration::from_millis(1),
            },
            2,
        );
        let pressure = IoPressure {
            some_avg10: 20.0,
            full_avg10: 0.0,
        };

        governor.before_io_with_pressure_reader(|| Some(pressure));
        assert_eq!(governor.last_pressure(), None);
        assert_eq!(governor.backoff_count(), 0);

        governor.before_io_with_pressure_reader(|| Some(pressure));
        assert_eq!(governor.last_pressure(), Some(pressure));
        assert_eq!(governor.backoff_count(), 1);
    }

    #[test]
    fn governor_config_controls_pressure_thresholds() {
        let config = IoGovernorConfig {
            enabled: true,
            stat_rate_per_sec: 1_000,
            psi_some_avg10_threshold: 50.0,
            psi_full_avg10_threshold: 50.0,
            max_backoff_ms: 1,
        };
        let governor = IoGovernor::from_config(&config);

        governor.sample_pressure_with(|| {
            Some(IoPressure {
                some_avg10: 20.0,
                full_avg10: 0.0,
            })
        });

        assert_eq!(
            governor.last_pressure(),
            Some(IoPressure {
                some_avg10: 20.0,
                full_avg10: 0.0,
            })
        );
        assert_eq!(governor.backoff_count(), 0);
    }

    #[test]
    fn ioprio_best_effort_never_panics() {
        let _ = set_current_thread_idle_io_priority_best_effort();
    }
}
