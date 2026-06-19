# M2 Scale-Up A/B Test Matrix

> **Date**: 2026-06-19
> **Branch**: `prototype/m2-cold-rotation`
> **Prerequisites**: benchmark script crash fixes (commit 9857d7e), waterline alarm (commit 48dac29)
> **Previous analysis**: [`m2-cold-window-ab-analysis-20260617.md`](../../wiki/m2-cold-window-ab-analysis-20260617.md)

---

## 1. Executive Summary

This plan defines a **4-point A/B test matrix** with increased scale, a dedicated inode-reuse stress test, waterline alarm validation, and parameter tuning. Each point is run 3 times for variance estimation, totaling 12 runs (~6–12 hours).

The goal is to answer four questions:
1. Does M2 improve cold-tier freshness at scale (1000+ dirs)?
2. Do high-budget parameters (budget 2048, TTL 90, dirs/tick 64) outperform current params?
3. Does the waterline alarm correctly trigger/recover under pressure?
4. Does inode reuse detection work under tmpfs pressure?

---

## 2. Test Matrix

### 2.1 Four-Point Matrix

| Point | Label | rotating | budget | TTL | dirs/tick | tick | waterline | Purpose |
|---|---|---|---|---|---|---|---|---|
| A | `baseline-scale` | OFF | — | — | — | — | OFF | Baseline at scale |
| B | `current-scale` | ON | 128 | 180 | 8 | 30 | OFF | Current params at scale |
| C | `highbudget-scale` | ON | 2048 | 90 | 64 | 30 | OFF | High-budget tuning |
| D | `full-scale` | ON | 2048 | 90 | 64 | 30 | ON | Full system with waterline |

### 2.2 Parameter Rationale

- **TTL 90s (not 60s)**: TTL 60s + tick 30s = only 2 ticks per dir. 90s gives 3 ticks, enough for deep subtree scans.
- **dirs/tick 64 (not 8)**: budget 2048 / dirs_per_tick 64 = 32 ticks = 960s full cycle. Fits within 1800s test window. With dirs/tick 8, full cycle = 7680s (128 min), unreachable.
- **L2 interval 300s**: unchanged. Waterline hard trigger = 80% × 300s = 240,000ms.
- **L3 interval 21600s → 86400s on hard degrade**: per waterline alarm design.

---

## 3. Fixture Setup

```bash
TEST_ROOT="$HOME/fd-rdd-m2-roots-scale"
mkdir -p "$TEST_ROOT/cold-a" "$TEST_ROOT/cold-b" "$TEST_ROOT/hot"

# Increased scale: 1000 dirs per cold root (was 300)
for i in $(seq -w 1 1000); do
  mkdir -p "$TEST_ROOT/cold-a/d$i" "$TEST_ROOT/cold-b/d$i"
  printf "cold-a-%s\n" "$i" > "$TEST_ROOT/cold-a/d$i/file_$i.txt"
  printf "cold-b-%s\n" "$i" > "$TEST_ROOT/cold-b/d$i/file_$i.txt"
done

# Hot root with some active files
for i in $(seq -w 1 50); do
  printf "hot-%s\n" "$i" > "$TEST_ROOT/hot/hot_$i.txt"
done
```

---

## 4. Exact CLI Commands

### Point A: Baseline (rotating OFF)

```bash
python3 scripts/m2-cold-window-vm-bench.py \
  --root "$TEST_ROOT/cold-a" --root "$TEST_ROOT/cold-b" --root "$TEST_ROOT/hot" \
  --run-dir "/tmp/fd-rdd-m2-runs/$(date -u +%Y%m%dT%H%M%SZ)_A_baseline_scale_r1" \
  --binary "./target/release/fd-rdd" --build never \
  --run-label "A-baseline-scale" \
  --duration-secs 1800 --sample-interval-secs 10 \
  --watch-mode tiered --runtime-profile default --tiered-profile balanced \
  --no-rotating-cold-window \
  --rotating-budget 2048 --rotating-tick-secs 30 --rotating-ttl-secs 90 \
  --rotating-max-cost-per-root 64 --rotating-max-dirs-per-tick 64 \
  --max-watch-dirs 8 --l0-max-cost-per-root 1 \
  --l1-scan-interval-secs 30 --l2-scan-interval-secs 300 --l3-scan-interval-secs 21600 \
  --l1-empty-scans-to-l2 5 --l2-empty-scans-to-l3 3 \
  --fast-scan --proc-sampler \
  --canary-root "$TEST_ROOT/cold-a" --canary-interval-secs 120 --canary-timeout-secs 90 \
  --passive-canary-root "$TEST_ROOT/cold-b" \
  --passive-canary-start-delay-secs 120 --passive-canary-interval-secs 180 \
  --passive-canary-settle-secs 120 --passive-canary-timeout-secs 0 \
  --event-storm \
  --event-storm-kind rw100,save100,git_clone,npm_install,subtree_rename,mount_storm,inode_reuse,inode_reuse_stress,time_skew \
  --event-storm-target-tier L2,L3 \
  --event-storm-start-delay-secs 120 --event-storm-interval-secs 10 \
  --event-storm-settle-secs 90 --event-storm-timeout-secs 0 \
  --event-storm-ops 500 --event-storm-duration-budget-secs 1 \
  --event-storm-file-count 200 --event-storm-depth 5 \
  --event-storm-inode-stress-iterations 200 --event-storm-inode-stress-tmpfs-inodes 200
```

### Point B: Current Params (rotating ON, budget 128, TTL 180, dirs/tick 8)

Same as Point A but change:
- `--no-rotating-cold-window` → `--rotating-cold-window`
- `--rotating-budget 128` `--rotating-ttl-secs 180` `--rotating-max-dirs-per-tick 8`
- Run label: `B-current-scale`

### Point C: High Budget (rotating ON, budget 2048, TTL 90, dirs/tick 64, waterline OFF)

Same as Point A but change:
- `--no-rotating-cold-window` → `--rotating-cold-window`
- `--rotating-budget 2048` `--rotating-ttl-secs 90` `--rotating-max-dirs-per-tick 64`
- Run label: `C-highbudget-scale`
- Waterline alarm is OFF by default in config (set `waterline_alarm_enabled = false` in config.toml)

### Point D: Full System (rotating ON, high budget, waterline ON)

Same as Point C but:
- Run label: `D-full-scale`
- Waterline alarm is ON (set `waterline_alarm_enabled = true` in config.toml, or rely on default)

> **Note**: The waterline alarm is enabled by default in the Rust config. To disable it for Points A/B/C, add `waterline_alarm_enabled = false` to the `[tiered_watch]` section of the generated config.toml. The benchmark script generates config under `config-home/fd-rdd/config.toml` in the run directory.

### Repetition

Each point is run 3 times (r1, r2, r3). Change `_r1` to `_r2` / `_r3` in run-dir and add `_rN` to run-label.

---

## 5. inode_reuse Stress Test Protocol

### 5.1 Setup

The `inode_reuse_stress` workload (newly added to the benchmark script) automatically:
1. Attempts to mount a tmpfs with limited inodes (`mount -t tmpfs -o nr_inodes=200,size=10m tmpfs <path>`)
2. Falls back to a regular directory if mount fails (logs warning)
3. Runs a tight create/delete/recreate loop (200 iterations by default)
4. Records dev/inode for each file, detects reuse
5. If reuse observed, verifies old path hidden + new path visible

### 5.2 Standalone Stress Test

For focused inode reuse testing outside the full A/B matrix:

```bash
python3 scripts/m2-cold-window-vm-bench.py \
  --root "$TEST_ROOT/cold-a" --root "$TEST_ROOT/hot" \
  --run-dir "/tmp/fd-rdd-m2-runs/$(date -u +%Y%m%dT%H%M%SZ)_inode_stress" \
  --binary "./target/release/fd-rdd" --build never \
  --run-label "inode-stress-focused" \
  --duration-secs 600 --sample-interval-secs 10 \
  --watch-mode tiered --runtime-profile default --tiered-profile balanced \
  --rotating-cold-window --rotating-budget 2048 --rotating-tick-secs 30 \
  --rotating-ttl-secs 90 --rotating-max-cost-per-root 64 --rotating-max-dirs-per-tick 64 \
  --max-watch-dirs 8 --l0-max-cost-per-root 1 \
  --l1-scan-interval-secs 30 --l2-scan-interval-secs 300 --l3-scan-interval-secs 21600 \
  --l1-empty-scans-to-l2 5 --l2-empty-scans-to-l3 3 \
  --fast-scan --proc-sampler \
  --event-storm \
  --event-storm-kind inode_reuse_stress \
  --event-storm-target-tier L2,L3 \
  --event-storm-start-delay-secs 60 --event-storm-interval-secs 10 \
  --event-storm-settle-secs 30 --event-storm-timeout-secs 0 \
  --event-storm-ops 10 --event-storm-duration-budget-secs 1 \
  --event-storm-inode-stress-iterations 500 --event-storm-inode-stress-tmpfs-inodes 100
```

### 5.3 Pass/Fail Criteria

| Criterion | Pass | Fail |
|---|---|---|
| `inode_reuse_observed > 0` | At least 1 inode reuse detected | 0 (inconclusive, need smaller tmpfs) |
| `inode_reuse_old_hidden_ok` = attempts | All old paths hidden | Any old path visible (ghost revival) |
| `inode_reuse_new_visible_ok` = attempts | All new paths visible | Any new path missing |
| `inode_reuse_stress_tmpfs_mounted` = true | tmpfs mount succeeded | Fallback to regular dir (weakens test) |

---

## 6. Waterline Alarm Test Protocol

### 6.1 Soft Degradation Trigger

**Goal**: Push `fast_scan_coverage_lag_p99_ms` above 4000ms (80% of 5000ms SLA).

**Method**: Create heavy hot-layer file churn during the run:
```bash
# In parallel with the benchmark, run a hot-layer churn script:
while true; do
  for i in $(seq 1 1000); do
    echo "$i $(date)" > "$TEST_ROOT/hot/churn_$i.txt"
  done
  # Atomic save pattern
  for i in $(seq 1 500); do
    tmp="$TEST_ROOT/hot/.tmp_$i"
    echo "save $i $(date)" > "$tmp"
    mv "$tmp" "$TEST_ROOT/hot/saved_$i.txt"
  done
  sleep 1
done
```

**Verify**:
- `waterline_soft_degraded` transitions from false → true
- `waterline_effective_rotating_budget` drops to 50% of configured (1024 for budget 2048)
- Log message appears: "waterline soft degradation triggered"

### 6.2 Hard Degradation Trigger

**Goal**: Push `fast_scan_coverage_lag_p99_ms` above 240,000ms (80% of 300s L2 interval).

**Note**: This is extreme — 4 minutes of scan lag. May require:
- Disabling fast-scan temporarily
- Creating a very large number of files (10,000+) in hot root
- Or reducing `--l2-scan-interval-secs` to 30s for testing purposes (80% = 24,000ms, more achievable)

**Test with reduced L2 interval**:
```bash
python3 scripts/m2-cold-window-vm-bench.py \
  ... (same as Point D but with) \
  --l2-scan-interval-secs 30 \
  --run-label "D-waterline-hard-trigger"
```

With L2 interval 30s, hard trigger = 80% × 30,000ms = 24,000ms. This is achievable with heavy churn.

**Verify**:
- `waterline_hard_degraded` transitions from false → true
- `waterline_effective_l3_scan_interval_secs` changes from 21600 to 86400
- Log message appears: "waterline hard degradation triggered"

### 6.3 Recovery Test

**Goal**: Verify alarm recovers when pressure is removed.

**Method**:
1. Start benchmark with heavy churn (trigger soft or hard degradation)
2. After degradation is confirmed, stop the churn script
3. Wait for lag to drop below recovery threshold (40%)
4. Verify:
   - Soft: recovers after 3 consecutive checks below 2000ms (or 40% of SLA)
   - Hard: recovers after 5 consecutive checks below recovery threshold
   - `waterline_effective_rotating_budget` returns to full budget
   - `waterline_effective_l3_scan_interval_secs` returns to configured value

### 6.4 Hysteresis Test

**Goal**: Verify no flapping when lag oscillates around threshold.

**Method**:
1. Create churn that pushes lag to ~3800–4200ms (oscillating around 4000ms threshold)
2. Run for 10+ minutes
3. Verify:
   - `waterline_soft_degraded` doesn't toggle more than once per 5 minutes
   - Recovery requires sustained low lag (3 consecutive checks), not a single dip

---

## 7. Scale Increase Specification

| Dimension | Previous | New | Rationale |
|---|---|---|---|
| Fixture dirs per cold root | 300 | 1000 | More cold dirs to rotate through |
| Event storm ops per burst | 100 | 500 | More events per burst |
| Event storm file count | varies | 200 (configurable) | More files per workload |
| Event storm depth (subtree_rename) | default | 5 (configurable) | Deeper directory trees |
| Event storm bursts | 16 | 32+ (more kinds × more tiers) | More data points |
| Test duration | 1800s | 1800s (keep, increase if needed) | Enough for full rotation cycle |
| Passive canary samples | 3-6 | 10+ (interval 120s × 1800s = 15) | More statistical power |
| inode stress iterations | 200 | 500 (focused test) | Higher reuse probability |

---

## 8. New Metrics to Collect

### 8.1 Waterline Alarm Metrics (from watch_state)

| Metric | Type | Description |
|---|---|---|
| `waterline_soft_degraded` | bool | Soft degradation active |
| `waterline_hard_degraded` | bool | Hard degradation active |
| `waterline_effective_l3_scan_interval_secs` | u64 | Actual L3 interval (21600 or 86400) |
| `waterline_effective_rotating_budget` | usize | Actual rotating budget (full or 50%) |

### 8.2 Cold Freshness Spike Metrics (from summary watch_state)

| Metric | Type | Description |
|---|---|---|
| `cold_freshness_age_spike_count` | int | Times age exceeded 2× running median |
| `cold_freshness_age_slope_max` | float | Max rate of change (per second) |

### 8.3 inode_reuse Stress Metrics (from event_storm.special)

| Metric | Type | Description |
|---|---|---|
| `inode_reuse_stress_tmpfs_mounted` | bool | Whether tmpfs mount succeeded |
| `inode_reuse_attempts` | int | Total create/delete/recreate cycles |
| `inode_reuse_observed` | int | Actual inode reuse count |
| `inode_reuse_new_visible_ok` | int | New paths visible in search |
| `inode_reuse_old_hidden_ok` | int | Old paths hidden in search |

---

## 9. Pass/Fail Criteria

### 9.1 Freshness ROI (Points A vs B/C/D)

| Criterion | Threshold | Priority |
|---|---|---|
| `passive_positive_first_query.success_rate` | C/D > A by ≥20pp | P0 |
| `passive_positive_first_query.success_rate` | C/D > B | P1 |
| `cold_freshness_age_p95_max` | C/D < A | P0 |
| `cold_freshness_age_spike_count` | C/D ≤ A | P1 |
| `cold_freshness_age_slope_max` | C/D ≤ A | P2 |

### 9.2 Cost Bounds (Points A vs B/C/D)

| Criterion | Threshold | Priority |
|---|---|---|
| `process.cpu_pct_p95` | C/D ≤ A + 5pp | P0 |
| `process.rss_bytes_max` | C/D ≤ A + 32MB or +10% | P0 |
| `fast_scan_coverage_lag_p99_ms_max` | C/D ≤ 5000ms | P0 |
| `process_swap_bytes_max` | C/D = 0 | P0 |

### 9.3 Parameter Tuning (Points B vs C)

| Criterion | Threshold | Priority |
|---|---|---|
| `cold_freshness_age_p95_max` | C < B | P0 |
| `rotating_cold_window_active_dirs_max` | C > B (more dirs active) | P1 |
| `rotating_cold_window_cycle_progress_pct_max` | C = 100% (full cycle completed) | P0 |
| `process.rss_bytes_max` | C ≤ B + 32MB | P0 |

### 9.4 Waterline Alarm (Point D)

| Criterion | Threshold | Priority |
|---|---|---|
| Soft degradation triggers when lag > 4000ms | Verified | P0 |
| Hard degradation triggers when lag > 80% × L2 interval | Verified | P0 |
| Recovery within 3-5 checks after pressure removed | Verified | P0 |
| No flapping (hysteresis) | Max 1 toggle per 5 min | P0 |
| `waterline_effective_l3_scan_interval_secs` = 86400 when hard degraded | Verified | P0 |
| `waterline_effective_rotating_budget` = 50% when soft degraded | Verified | P0 |

### 9.5 inode_reuse Stress

| Criterion | Threshold | Priority |
|---|---|---|
| `inode_reuse_observed > 0` | At least 1 reuse | P0 |
| `inode_reuse_old_hidden_ok` = `inode_reuse_observed` | All old paths hidden | P0 |
| `inode_reuse_new_visible_ok` = `inode_reuse_observed` | All new paths visible | P0 |

---

## 10. Risk Assessment

| Risk | Likelihood | Impact | Mitigation |
|---|---|---|---|
| tmpfs mount fails (no privileges) | Medium | inode_reuse test weakened | Fall back to regular dir, log warning; use `sudo mount` if available |
| Hard degradation not triggerable | High | Can't test waterline hard path | Use reduced L2 interval (30s) for focused test |
| RSS exceeds 32MB with budget 2048 | Medium | Cost threshold failure | Monitor RSS; if exceeded, reduce budget to 1024 |
| Test takes >12 hours | Medium | Practical constraint | Run points in parallel on different ports if possible |
| Baseline crashes again | Low | Can't compare A vs B/C/D | Crash bugs fixed (Fix A/C/D/E); run A first to verify |
| 1000 dirs × 3 roots = 3000 dirs indexing slow | Medium | Test startup delay | Use `--build never` with pre-built binary; allow 120s startup |

---

## 11. Timeline

| Phase | Duration | Description |
|---|---|---|
| Fixture setup | 5 min | Create 1000 dirs per cold root |
| Build binary | 2 min | `cargo build --release` |
| Point A (3 runs) | ~90 min | 3 × 1800s + overhead |
| Point B (3 runs) | ~90 min | 3 × 1800s + overhead |
| Point C (3 runs) | ~90 min | 3 × 1800s + overhead |
| Point D (3 runs) | ~90 min | 3 × 1800s + overhead |
| inode_reuse focused | ~30 min | 3 × 600s |
| Waterline focused | ~60 min | trigger + recovery + hysteresis |
| Analysis & report | ~30 min | Compare all metrics |
| **Total** | **~8 hours** | |

---

## 12. Sweep Mode (Alternative)

Instead of running commands manually, use the new `--sweep-config` feature:

```json
{
  "base_args": {
    "duration-secs": 1800,
    "sample-interval-secs": 10,
    "watch-mode": "tiered",
    "runtime-profile": "default",
    "tiered-profile": "balanced",
    "max-watch-dirs": 8,
    "l0-max-cost-per-root": 1,
    "l1-scan-interval-secs": 30,
    "l2-scan-interval-secs": 300,
    "l3-scan-interval-secs": 21600,
    "l1-empty-scans-to-l2": 5,
    "l2-empty-scans-to-l3": 3,
    "fast-scan": true,
    "proc-sampler": true,
    "event-storm": true,
    "event-storm-kind": "rw100,save100,git_clone,npm_install,subtree_rename,mount_storm,inode_reuse,inode_reuse_stress,time_skew",
    "event-storm-target-tier": "L2,L3",
    "event-storm-start-delay-secs": 120,
    "event-storm-interval-secs": 10,
    "event-storm-settle-secs": 90,
    "event-storm-timeout-secs": 0,
    "event-storm-ops": 500,
    "event-storm-duration-budget-secs": 1,
    "event-storm-file-count": 200,
    "event-storm-depth": 5,
    "event-storm-inode-stress-iterations": 200,
    "event-storm-inode-stress-tmpfs-inodes": 200,
    "canary-interval-secs": 120,
    "canary-timeout-secs": 90,
    "passive-canary-start-delay-secs": 120,
    "passive-canary-interval-secs": 180,
    "passive-canary-settle-secs": 120,
    "passive-canary-timeout-secs": 0
  },
  "variants": [
    {
      "label": "A-baseline-scale",
      "no-rotating-cold-window": true
    },
    {
      "label": "B-current-scale",
      "rotating-cold-window": true,
      "rotating-budget": 128,
      "rotating-ttl-secs": 180,
      "rotating-max-dirs-per-tick": 8
    },
    {
      "label": "C-highbudget-scale",
      "rotating-cold-window": true,
      "rotating-budget": 2048,
      "rotating-ttl-secs": 90,
      "rotating-max-dirs-per-tick": 64
    },
    {
      "label": "D-full-scale",
      "rotating-cold-window": true,
      "rotating-budget": 2048,
      "rotating-ttl-secs": 90,
      "rotating-max-dirs-per-tick": 64
    }
  ]
}
```

```bash
python3 scripts/m2-cold-window-vm-bench.py \
  --root "$TEST_ROOT/cold-a" --root "$TEST_ROOT/cold-b" --root "$TEST_ROOT/hot" \
  --binary "./target/release/fd-rdd" --build never \
  --canary-root "$TEST_ROOT/cold-a" \
  --passive-canary-root "$TEST_ROOT/cold-b" \
  --sweep-config sweep-config.json
```

The sweep mode outputs a comparison table at the end with key metrics across all variants.

---

## 13. Post-Test Analysis Checklist

After all runs complete, extract and compare:

- [ ] `passive_positive_first_query.success_rate` across A/B/C/D (mean ± std from 3 runs)
- [ ] `event_storm.first_query.success_rate` across A/B/C/D
- [ ] `event_storm.first_query.event_age_p95_secs` across A/B/C/D
- [ ] `cold_freshness_age_p95_max` across A/B/C/D
- [ ] `cold_freshness_age_spike_count` across A/B/C/D
- [ ] `process.cpu_pct_p95` across A/B/C/D
- [ ] `process.rss_bytes_max` across A/B/C/D
- [ ] `fast_scan_coverage_lag_p99_ms_max` across A/B/C/D
- [ ] `waterline_soft_degraded` / `waterline_hard_degraded` transitions (Point D only)
- [ ] `waterline_effective_l3_scan_interval_secs` timeline (Point D only)
- [ ] `waterline_effective_rotating_budget` timeline (Point D only)
- [ ] `inode_reuse_observed` and related metrics (all points with inode_reuse_stress)
- [ ] `event_storm.special.subtree_rename_*` across A/B/C/D
- [ ] `event_storm.special.mount_storm_old_hidden_ok` across A/B/C/D
- [ ] `event_storm.special.time_skew_backdated_visible_ok` across A/B/C/D
- [ ] `rotating_cold_window_active_dirs_max` (B/C/D)
- [ ] `rotating_cold_window_cycle_progress_pct_max` (B/C/D)
- [ ] `rotating_cold_window_scan_only_dirs_last` (B/C/D)
