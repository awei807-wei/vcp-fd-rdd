#!/usr/bin/env python3
"""Rebuild mod.rs with only core content, adding module declarations."""

MOD_RS = "src/event/tiered_watch/mod.rs"

with open(MOD_RS) as f:
    lines = f.readlines()

def extract(start, end):
    return lines[start-1:end]

# Build new mod.rs
new_lines = []

# Imports (1-16)
new_lines.append(''.join(extract(1, 16)))

# Module declarations
new_lines.append("""mod cold_window;
mod ephemeral;
mod fast_scan;
#[cfg(test)]
mod tests;
mod registry;
mod report;
mod types;

pub use types::*;

""")

# DirState struct (22-42)
new_lines.append(''.join(extract(22, 42)))
new_lines.append('\n')

# impl DirState (162-216) - make private methods pub(super)
dirstate_impl = ''.join(extract(162, 216))
import re
dirstate_impl = re.sub(r'^(\s+)fn ', r'\1pub(super) fn ', dirstate_impl, flags=re.MULTILINE)
new_lines.append(dirstate_impl)
new_lines.append('\n')

# TieredWatchRuntime struct (256-326)
new_lines.append(''.join(extract(256, 326)))
new_lines.append('\n')

# impl TieredWatchRuntime with core methods
new_lines.append('impl TieredWatchRuntime {\n')

# new (329-346)
new_lines.append(''.join(extract(329, 346)))
# new_with_ephemeral (347-365)
new_lines.append(''.join(extract(347, 365)))
# new_with_l0_max_cost_and_ephemeral (366-468)
new_lines.append(''.join(extract(366, 468)))
# set_proc_sampler_enabled (522-525)
new_lines.append(''.join(extract(522, 525)))
# record_proc_sampler_report (526-550)
new_lines.append(''.join(extract(526, 550)))
# record_event_paths (1392-1439)
new_lines.append(''.join(extract(1392, 1439)))
# covering_tier (1804-1811)
new_lines.append(''.join(extract(1804, 1811)))
# max_watch_dirs (1812-1815)
new_lines.append(''.join(extract(1812, 1815)))
# l0_max_cost_per_root (1816-1819)
new_lines.append(''.join(extract(1816, 1819)))
# note_watch_mount_policy_rejected (1820-1824)
new_lines.append(''.join(extract(1820, 1824)))
# note_watch_exclude_rejected (1825-1828)
new_lines.append(''.join(extract(1825, 1828)))

# record_last_budget_blocked (1829-1841) → pub(super)
rlbb = ''.join(extract(1829, 1841))
rlbb = re.sub(r'^(\s+)fn ', r'\1pub(super) fn ', rlbb, flags=re.MULTILINE)
new_lines.append(rlbb)

# set_dirty_queue_len (2798-2801)
new_lines.append(''.join(extract(2798, 2801)))
# set_query_stale_hit_count (2802-2805)
new_lines.append(''.join(extract(2802, 2805)))
# set_query_permission_denied_count (2806-2810)
new_lines.append(''.join(extract(2806, 2810)))

# state (2811-2814) → pub(super)
state_m = ''.join(extract(2811, 2814))
state_m = re.sub(r'^(\s+)fn ', r'\1pub(super) fn ', state_m, flags=re.MULTILINE)
new_lines.append(state_m)

new_lines.append('}\n\n')

# Helper functions that stay in mod.rs
# path_is_under_or_equal (2926-2929) → pub(super)
piuoe = ''.join(extract(2926, 2929))
piuoe = re.sub(r'^fn ', r'pub(super) fn ', piuoe, flags=re.MULTILINE)
new_lines.append(piuoe)

# path_has_component (2977-2981) → pub(super)
phc = ''.join(extract(2977, 2981))
phc = re.sub(r'^fn ', r'pub(super) fn ', phc, flags=re.MULTILINE)
new_lines.append(phc)

# unix_millis (3171-3177) → pub(super)
um = ''.join(extract(3171, 3177))
um = re.sub(r'^fn ', r'pub(super) fn ', um, flags=re.MULTILINE)
new_lines.append(um)

with open(MOD_RS, 'w') as f:
    f.write(''.join(new_lines))

print(f"Wrote new mod.rs with {sum(len(s.splitlines()) for s in new_lines)} lines")
