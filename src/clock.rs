//! Clock skew detection for mtime-cutoff based repair paths.

use std::time::{Duration, Instant, SystemTime};

#[derive(Clone, Debug)]
pub struct ClockSkewDetector {
    tolerance: Duration,
    last_wall: SystemTime,
    last_mono: Instant,
    skew_count: u64,
    last_negative_drift: Duration,
    cutoff_trusted: bool,
}

impl ClockSkewDetector {
    pub fn new(tolerance: Duration) -> Self {
        Self {
            tolerance,
            last_wall: SystemTime::now(),
            last_mono: Instant::now(),
            skew_count: 0,
            last_negative_drift: Duration::ZERO,
            cutoff_trusted: true,
        }
    }

    pub fn observe(&mut self, wall: SystemTime, mono: Instant) -> bool {
        let elapsed = mono.saturating_duration_since(self.last_mono);
        let expected = self
            .last_wall
            .checked_add(elapsed)
            .unwrap_or(self.last_wall);
        let skewed = match expected.duration_since(wall) {
            Ok(backward) => backward > self.tolerance,
            Err(_) => false,
        };
        if skewed {
            self.skew_count = self.skew_count.saturating_add(1);
            self.last_negative_drift = expected.duration_since(wall).unwrap_or_default();
            self.cutoff_trusted = false;
        }
        self.last_wall = wall;
        self.last_mono = mono;
        skewed
    }

    pub fn cutoff_trusted(&self) -> bool {
        self.cutoff_trusted
    }

    pub fn mark_reconciled(&mut self) {
        self.cutoff_trusted = true;
    }

    pub fn skew_count(&self) -> u64 {
        self.skew_count
    }

    pub fn last_negative_drift(&self) -> Duration {
        self.last_negative_drift
    }
}

pub fn cutoff_for_crawl(cutoff_ns: u64, trusted: bool) -> u64 {
    if trusted {
        cutoff_ns
    } else {
        0
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn detects_wall_clock_rollback_past_tolerance() {
        let base_wall = SystemTime::UNIX_EPOCH + Duration::from_secs(10);
        let base_mono = Instant::now();
        let mut detector = ClockSkewDetector {
            tolerance: Duration::from_secs(1),
            last_wall: base_wall,
            last_mono: base_mono,
            skew_count: 0,
            last_negative_drift: Duration::ZERO,
            cutoff_trusted: true,
        };

        assert!(detector.observe(base_wall, base_mono + Duration::from_secs(3)));
        assert!(!detector.cutoff_trusted());
        assert_eq!(detector.skew_count(), 1);
    }

    #[test]
    fn small_jitter_keeps_cutoff_trusted() {
        let base_wall = SystemTime::UNIX_EPOCH + Duration::from_secs(10);
        let base_mono = Instant::now();
        let mut detector = ClockSkewDetector {
            tolerance: Duration::from_secs(1),
            last_wall: base_wall,
            last_mono: base_mono,
            skew_count: 0,
            last_negative_drift: Duration::ZERO,
            cutoff_trusted: true,
        };

        let wall = base_wall + Duration::from_millis(250);
        assert!(!detector.observe(wall, base_mono + Duration::from_millis(500)));
        assert!(detector.cutoff_trusted());
    }

    #[test]
    fn untrusted_cutoff_forces_full_crawl() {
        assert_eq!(cutoff_for_crawl(123, true), 123);
        assert_eq!(cutoff_for_crawl(123, false), 0);
    }
}
