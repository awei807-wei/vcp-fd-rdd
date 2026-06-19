#!/usr/bin/env python3
"""Fix all sub-module files: add missing derive attributes, imports, etc."""
import re

# ============================================================
# Fix mod.rs
# ============================================================
with open("src/event/tiered_watch/mod.rs") as f:
    mod_content = f.read()

# Replace the import section (lines 1-16) with cleaned-up imports
old_imports = """use std::collections::{HashMap, HashSet, VecDeque};
use std::hash::{Hash, Hasher};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU32, AtomicU64, AtomicU8, AtomicUsize, Ordering};
use std::sync::Arc;

use parking_lot::RwLock;
use serde::{Deserialize, Serialize};

use crate::config::{L3ScanPolicy, NetworkFastScanMode, TieredWatchConfig};
use crate::event::proc_sampler::ProcSamplerReport;
use crate::fs_policy::{is_remote_fstype, MountTable};
use crate::index::tiered::ScanOutcome;
use crate::stats::WatchStateReport;
use crate::storage::snapshot::stable_snapshot_dir_for;
use crate::util::unix_secs;"""

new_imports = """use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU32, AtomicU64, AtomicU8, AtomicUsize, Ordering};
use std::sync::Arc;

use parking_lot::RwLock;

use crate::config::NetworkFastScanMode;
use crate::event::proc_sampler::ProcSamplerReport;
use crate::util::unix_secs;

use ephemeral::{DirtyScopeObservation, EphemeralWatchLease};
use fast_scan::{network_fast_scan_mode_to_u8, FastScanState};"""

mod_content = mod_content.replace(old_imports, new_imports)

with open("src/event/tiered_watch/mod.rs", "w") as f:
    f.write(mod_content)
print("Fixed mod.rs imports")

# ============================================================
# Fix fast_scan.rs - add missing derive attributes and imports
# ============================================================
with open("src/event/tiered_watch/fast_scan.rs") as f:
    fs = f.read()

# Add derive attributes before struct definitions
fs = fs.replace(
    "pub(super) struct DirSentinelSignature {",
    "#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]\npub(super) struct DirSentinelSignature {"
)
fs = fs.replace(
    "pub(super) struct FastScanLease {",
    "#[derive(Clone, Debug)]\npub(super) struct FastScanLease {"
)
fs = fs.replace(
    "pub(super) struct DirSentinel {",
    "#[derive(Clone, Debug)]\npub(super) struct DirSentinel {"
)
fs = fs.replace(
    "pub(super) struct FastScanState {",
    "#[derive(Debug, Default)]\npub(super) struct FastScanState {"
)

# Add missing imports
fs = fs.replace(
    "use crate::fs_policy::{is_remote_fstype, MountTable};",
    "use crate::fs_policy::{is_remote_fstype, MountTable};\nuse crate::util::unix_secs;\n\nuse super::unix_millis;"
)

# Remove unused Hash, Hasher import
fs = fs.replace("use std::hash::{Hash, Hasher};\n", "")

with open("src/event/tiered_watch/fast_scan.rs", "w") as f:
    f.write(fs)
print("Fixed fast_scan.rs")

# ============================================================
# Fix ephemeral.rs - add missing derive attributes and imports
# ============================================================
with open("src/event/tiered_watch/ephemeral.rs") as f:
    eph = f.read()

# Add derive attributes
eph = eph.replace(
    "pub(super) struct EphemeralWatchLease {",
    "#[derive(Debug)]\npub(super) struct EphemeralWatchLease {"
)
eph = eph.replace(
    "pub(super) struct DirtyScopeObservation {",
    "#[derive(Debug)]\npub(super) struct DirtyScopeObservation {"
)

# Add missing imports
eph = eph.replace(
    "use crate::config::TieredWatchConfig;",
    "use crate::config::TieredWatchConfig;\nuse crate::util::unix_secs;\n\nuse super::path_has_component;\nuse super::path_is_under_or_equal;"
)

with open("src/event/tiered_watch/ephemeral.rs", "w") as f:
    f.write(eph)
print("Fixed ephemeral.rs")

# ============================================================
# Fix cold_window.rs - add missing imports
# ============================================================
with open("src/event/tiered_watch/cold_window.rs") as f:
    cw = f.read()

cw = cw.replace(
    "use crate::config::L3ScanPolicy;\nuse crate::index::tiered::ScanOutcome;",
    "use crate::config::L3ScanPolicy;\nuse crate::index::tiered::ScanOutcome;\nuse crate::util::unix_secs;\n\nuse super::path_is_under_or_equal;"
)

with open("src/event/tiered_watch/cold_window.rs", "w") as f:
    f.write(cw)
print("Fixed cold_window.rs")

# ============================================================
# Fix registry.rs - add missing derive attributes and imports
# ============================================================
with open("src/event/tiered_watch/registry.rs") as f:
    reg = f.read()

# Add derive attributes
reg = reg.replace(
    "pub(super) struct FastScanPersistedRegistry {",
    "#[derive(Clone, Debug, Serialize, Deserialize)]\npub(super) struct FastScanPersistedRegistry {"
)
reg = reg.replace(
    "pub(super) struct FastScanPersistedEntry {",
    "#[derive(Clone, Debug, Serialize, Deserialize)]\npub(super) struct FastScanPersistedEntry {"
)

# Add missing imports - Hash, Hasher for fast_scan_config_fingerprint
reg = reg.replace(
    "use serde::{Deserialize, Serialize};",
    "use std::hash::{Hash, Hasher};\n\nuse serde::{Deserialize, Serialize};"
)

# Add unix_secs import
reg = reg.replace(
    "use crate::storage::snapshot::stable_snapshot_dir_for;",
    "use crate::storage::snapshot::stable_snapshot_dir_for;\nuse crate::util::unix_secs;"
)

# Fix import path for fast_scan types - they're pub(super) so accessible via super::fast_scan::
# But also need FastScanLeaseKind, FastScanSentinelState, FastScanMountClass from types
# Check if the import line needs fixing
reg = reg.replace(
    "use super::fast_scan::{DirSentinel, DirSentinelSignature, FastScanLease, FastScanState};\nuse super::{TieredWatchRuntime};",
    "use super::fast_scan::{DirSentinel, DirSentinelSignature, FastScanLease, FastScanState};\nuse super::TieredWatchRuntime;"
)

with open("src/event/tiered_watch/registry.rs", "w") as f:
    f.write(reg)
print("Fixed registry.rs")

# ============================================================
# Fix report.rs - add missing imports
# ============================================================
with open("src/event/tiered_watch/report.rs") as f:
    rep = f.read()

# Add unix_secs import
rep = rep.replace(
    "use crate::config::{L3ScanPolicy, NetworkFastScanMode};",
    "use crate::config::{L3ScanPolicy, NetworkFastScanMode};\nuse crate::util::unix_secs;"
)

# Add super imports for path_is_under_or_equal
rep = rep.replace(
    "use super::{DirState, TieredWatchRuntime};\nuse super::types::*;",
    "use super::{path_is_under_or_equal, DirState, TieredWatchRuntime};\nuse super::types::*;"
)

# Add import for network_fast_scan_mode_from_u8, fast_scan_mode_label, normalize_fast_scan_dir, fast_scan_mount_info, dir_sentinel_signature
# These are in fast_scan module
rep = rep.replace(
    "use super::{path_is_under_or_equal, DirState, TieredWatchRuntime};",
    "use super::fast_scan::{fast_scan_mode_label, network_fast_scan_mode_from_u8, normalize_fast_scan_dir, fast_scan_mount_info, dir_sentinel_signature};\nuse super::{path_is_under_or_equal, DirState, TieredWatchRuntime};"
)

with open("src/event/tiered_watch/report.rs", "w") as f:
    f.write(rep)
print("Fixed report.rs")

print("All fixes applied!")
