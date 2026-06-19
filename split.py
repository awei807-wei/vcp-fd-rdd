#!/usr/bin/env python3
"""Split tiered_watch/mod.rs into sub-modules."""
import re
import sys

MOD_RS = "src/event/tiered_watch/mod.rs"

with open(MOD_RS) as f:
    lines = f.readlines()  # 0-indexed, so line N is lines[N-1]

def extract(start, end):
    """Extract lines [start, end] (1-indexed inclusive)."""
    return lines[start-1:end]

def make_pub_super(text):
    """Add pub(super) to private items."""
    # struct/enum definitions (not already pub)
    text = re.sub(r'^(\s*)struct ', r'\1pub(super) struct ', text, flags=re.MULTILINE)
    text = re.sub(r'^(\s*)enum ', r'\1pub(super) enum ', text, flags=re.MULTILINE)
    # Private fn (not already pub) - both top-level and methods
    text = re.sub(r'^(\s+)fn ', r'\1pub(super) fn ', text, flags=re.MULTILINE)
    text = re.sub(r'^fn ', r'pub(super) fn ', text, flags=re.MULTILINE)
    # Struct fields: lines like "    field_name: Type," that aren't already pub
    # Match indented field definitions (word: type) that don't start with pub or //
    def field_repl(m):
        indent = m.group(1)
        return f"{indent}pub(super) " + m.group(0).lstrip()
    # Only apply to lines that look like struct fields (indent + identifier + colon)
    # but not to fn signatures, impl lines, use lines, etc.
    new_lines = []
    in_struct = False
    brace_depth = 0
    for line in text.split('\n'):
        stripped = line.lstrip()
        # Detect struct/enum definition to track we're in a struct body
        if re.match(r'(pub\s+)?(pub\(super\)\s+)?(struct|enum)\s+\w+', stripped):
            in_struct = True
            brace_depth = 0
            # Count braces on this line
            brace_depth += line.count('{') - line.count('}')
            new_lines.append(line)
            continue
        if in_struct:
            brace_depth += line.count('{') - line.count('}')
            if brace_depth <= 0 and '{' not in line and '}' not in line:
                # Might be a derive/macro line, skip
                new_lines.append(line)
                continue
            # Check if this line is a field definition
            # Field pattern: indentation + identifier: type,  (not a fn, not a comment, not pub)
            if (re.match(r'^\s+\w+:\s', line) and 
                not stripped.startswith('pub ') and 
                not stripped.startswith('pub(super) ') and
                not stripped.startswith('//') and
                not stripped.startswith('fn ') and
                not stripped.startswith('pub(super) fn ') and
                not stripped.startswith('impl ') and
                not stripped.startswith('use ')):
                indent = re.match(r'^(\s+)', line).group(1)
                rest = line[len(indent):]
                new_lines.append(f"{indent}pub(super) {rest}")
            else:
                new_lines.append(line)
            if brace_depth <= 0 and '}' in line:
                in_struct = False
        else:
            new_lines.append(line)
    return '\n'.join(new_lines)

# Common imports header for sub-modules that have impl TieredWatchRuntime
COMMON_IMPORTS = """use std::collections::{{HashMap, HashSet, VecDeque}};
use std::path::{{Path, PathBuf}};
use std::sync::atomic::{{AtomicBool, AtomicU32, AtomicU64, AtomicU8, AtomicUsize, Ordering}};
use std::sync::Arc;

use crate::config::{{L3ScanPolicy, NetworkFastScanMode, TieredWatchConfig}};
use crate::fs_policy::MountTable;
use crate::index::tiered::ScanOutcome;
use crate::stats::WatchStateReport;
use crate::util::unix_secs;

use super::{{DirState, TieredWatchRuntime}};
use super::types::*;
"""

# ============================================================
# fast_scan.rs
# ============================================================
fs_structs = ''.join(extract(44, 129))
fs_impl = ''.join(extract(469, 521)) + ''.join(extract(551, 822)) + ''.join(extract(1052, 1391))
fs_helpers = ''.join(extract(3001, 3052)) + ''.join(extract(3062, 3083))

fs_content = """//! Fast scan lease logic: sentinel state, tick config, lease management.

use std::collections::{{HashMap, VecDeque}};
use std::hash::{{Hash, Hasher}};
use std::path::{{Path, PathBuf}};
use std::sync::atomic::{{AtomicBool, AtomicU64, AtomicU8, AtomicUsize, Ordering}};
use std::sync::Arc;

use serde::{{Deserialize, Serialize}};

use crate::config::{{NetworkFastScanMode, TieredWatchConfig}};
use crate::fs_policy::{{is_remote_fstype, MountTable}};

use super::{{DirState, TieredWatchRuntime}};
use super::types::*;

"""
fs_content += make_pub_super(fs_structs)
fs_content += "\nimpl TieredWatchRuntime {\n"
fs_content += make_pub_super(fs_impl)
fs_content += "}\n\n"
fs_content += make_pub_super(fs_helpers)

with open("src/event/tiered_watch/fast_scan.rs", "w") as f:
    f.write(fs_content)
print("Wrote fast_scan.rs")

# ============================================================
# ephemeral.rs
# ============================================================
eph_structs = ''.join(extract(218, 254))
eph_impl = ''.join(extract(1440, 1803))
eph_helpers = ''.join(extract(3134, 3170))

eph_content = """//! Ephemeral watch logic: lease management, dirty scope observation, victim selection.

use std::collections::HashMap;
use std::path::{{Path, PathBuf}};
use std::sync::atomic::{{AtomicU64, Ordering}};
use std::sync::Arc;

use crate::config::TieredWatchConfig;

use super::{{DirState, TieredWatchRuntime}};
use super::types::*;

"""
eph_content += make_pub_super(eph_structs)
eph_content += "\nimpl TieredWatchRuntime {\n"
eph_content += make_pub_super(eph_impl)
eph_content += "}\n\n"
eph_content += make_pub_super(eph_helpers)

with open("src/event/tiered_watch/ephemeral.rs", "w") as f:
    f.write(eph_content)
print("Wrote ephemeral.rs")

# ============================================================
# cold_window.rs
# ============================================================
cw_impl = ''.join(extract(1842, 2388))
cw_helpers = ''.join(extract(2993, 3000))  # next_l3_scan_unix_secs

cw_content = """//! Rotating cold window logic: tier rotation, scan batching, promotion/demotion.

use std::collections::HashMap;
use std::path::{{Path, PathBuf}};
use std::sync::atomic::{{AtomicU64, Ordering}};
use std::sync::Arc;

use crate::config::L3ScanPolicy;
use crate::index::tiered::ScanOutcome;

use super::{{DirState, TieredWatchRuntime}};
use super::types::*;

"""
cw_content += "impl TieredWatchRuntime {\n"
cw_content += make_pub_super(cw_impl)
cw_content += "}\n\n"
cw_content += make_pub_super(cw_helpers)

with open("src/event/tiered_watch/cold_window.rs", "w") as f:
    f.write(cw_content)
print("Wrote cold_window.rs")

# ============================================================
# registry.rs
# ============================================================
reg_structs = ''.join(extract(131, 160))
reg_impl = ''.join(extract(823, 1051))
reg_helpers = ''.join(extract(3084, 3133))  # fast_scan_registry_path_for through fast_scan_config_fingerprint

reg_content = """//! Persistence and restore of the fast scan registry.

use std::path::{{Path, PathBuf}};

use serde::{{Deserialize, Serialize}};

use crate::config::TieredWatchConfig;
use crate::storage::snapshot::stable_snapshot_dir_for;

use super::fast_scan::{{DirSentinel, DirSentinelSignature, FastScanLease, FastScanState}};
use super::{{TieredWatchRuntime}};
use super::types::*;

"""
reg_content += make_pub_super(reg_structs)
reg_content += "\nimpl TieredWatchRuntime {\n"
reg_content += make_pub_super(reg_impl)
reg_content += "}\n\n"
reg_content += make_pub_super(reg_helpers)

with open("src/event/tiered_watch/registry.rs", "w") as f:
    f.write(reg_content)
print("Wrote registry.rs")

# ============================================================
# report.rs (with M1 split)
# ============================================================
# We'll extract report() and debug_dump(), plus the helper functions.
# The M1 split (collect_* helpers) will be done manually after extraction.
report_impl = ''.join(extract(2389, 2797))  # report()
debug_impl = ''.join(extract(2815, 2923))   # debug_dump()
report_helpers = (
    ''.join(extract(2926, 2929)) +   # path_is_under_or_equal
    ''.join(extract(2930, 2976)) +   # nearest_ancestor_root through nested_relation
    ''.join(extract(2977, 2981)) +   # path_has_component
    ''.join(extract(2982, 2992)) +   # display_freshness
    ''.join(extract(3009, 3016)) +   # network_fast_scan_mode_from_u8
    ''.join(extract(3025, 3043)) +   # fast_scan_mode_label
    ''.join(extract(3053, 3061))     # percentile_ms
)

report_content = """//! Report and debug dump assembly.

use std::collections::HashMap;
use std::path::{{Path, PathBuf}};
use std::sync::atomic::Ordering;

use crate::config::{{L3ScanPolicy, NetworkFastScanMode}};
use crate::stats::WatchStateReport;

use super::{{DirState, TieredWatchRuntime}};
use super::types::*;

"""
report_content += "impl TieredWatchRuntime {\n"
report_content += make_pub_super(report_impl)
report_content += make_pub_super(debug_impl)
report_content += "}\n\n"
report_content += make_pub_super(report_helpers)

with open("src/event/tiered_watch/report.rs", "w") as f:
    f.write(report_content)
print("Wrote report.rs")

# ============================================================
# tests.rs
# ============================================================
tests_content = ''.join(extract(3178, 4915))  # #[cfg(test)] mod tests { ... }
# Remove the outer #[cfg(test)] and mod tests { wrapper, keep inner content
# Actually, we want to keep the test functions but not the mod wrapper.
# The tests.rs file will be declared as #[cfg(test)] mod tests; in mod.rs
# So we need to strip the outer mod tests { and its closing }

# Find "mod tests {" and the matching closing "}"
test_lines = tests_content.split('\n')
# First line should be "#[cfg(test)]", second "mod tests {", last "}"
# Let's strip them
inner_tests = []
brace_depth = 0
started = False
for line in test_lines:
    if not started:
        if 'mod tests {' in line:
            started = True
            brace_depth = line.count('{') - line.count('}')
            continue
        continue
    brace_depth += line.count('{') - line.count('}')
    if brace_depth <= 0:
        # This is the closing brace
        break
    inner_tests.append(line)

tests_inner = '\n'.join(inner_tests)

# Build tests.rs with proper imports
tests_header = """//! Inline tests for the tiered watch subsystem.

use std::path::{{Path, PathBuf}};
use std::sync::atomic::Ordering;

use crate::config::{{L3ScanPolicy, NetworkFastScanMode, TieredWatchConfig}};
use crate::fs_policy::MountTable;
use crate::index::tiered::ScanOutcome;

use super::*;
use super::types::*;
"""

# Also need to import private types from sub-modules
tests_imports = """
use super::fast_scan::{{DirSentinel, DirSentinelSignature, FastScanLease, FastScanState}};
use super::ephemeral::{{EphemeralWatchLease, DirtyScopeObservation}};
"""

with open("src/event/tiered_watch/tests.rs", "w") as f:
    f.write(tests_header)
    f.write(tests_imports)
    f.write("\n")
    f.write(tests_inner)
print("Wrote tests.rs")

print("Done! Now need to rebuild mod.rs with remaining content.")
