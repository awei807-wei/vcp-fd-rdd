# M2 Scan Performance Investigation — L1/L2 Directory Scanning Bottleneck

**Date:** 2026-06-20  
**Branch:** `prototype/m2-cold-rotation`  
**Investigator:** scan-investigator  
**Scope:** Why L1/L2 periodic directory scanning is too slow at 1M-file scale, and how to optimize it.

---

## 1. Executive Summary

The core bottleneck is that **every periodic cold scan of a large directory (e.g., Projects/ with 300K files) performs a `stat()` (specifically `symlink_metadata`) syscall on every single child entry**. For a directory with 300K files, that's 300K stat() syscalls per scan cycle, which takes tens of seconds. The scan interval is 5–30 seconds, so the scan can never keep up.

The codebase already has three optimization mechanisms:
1. **Sliced scanning** (`read_dir_slice`) — breaks large directories into 512-entry slices with a 20ms time budget per slice.
2. **Directory manifest skip** (`DirectoryManifestStore`) — compares a hash summary of directory contents to skip unchanged directories.
3. **mtime-based fast-sync** (`visit_dirs_since`) — crawls only directories whose mtime changed since a cutoff.

However, **all three mechanisms still require at least one `stat()` per child entry** to compute metadata, which means the stat() cost is not actually avoided. The manifest skip mechanism is particularly problematic because it requires a full `read_dir + stat` pass just to compute the summary hash before it can decide whether to skip.

---

## 2. Full Code Path: Dirty Queue → Index Update

### 2.1. Entry Point: `spawn_dirty_queue_loop`

**File:** `src/main.rs:1250`

```rust
fn spawn_dirty_queue_loop(
    index: Arc<TieredIndex>,
    runtime: Option<Arc<TieredWatchRuntime>>,
    watch_command_tx: tokio::sync::mpsc::Sender<WatchCommand>,
    tiered: fd_rdd::config::TieredWatchConfig,
    exclude_dirs: Vec<String>,
    ignore_prefixes: Vec<PathBuf>,
)
```

The loop:
1. **Pops a batch** of up to 16 dirty queue entries (`index.dirty_queue_ready_batch(16)`, line 1260).
2. **Builds `manifest_skip_dirs`** — for `PeriodicColdScan` entries whose directories are covered by L2/L3 tier, adds them to a skip set (lines 1270–1289).
3. **Spawns a blocking task** (`tokio::task::spawn_blocking`, line 1293) that iterates the batch and calls `process_dirty_entry_with_project_markers_and_manifest_skip_dirs` for each entry (line 1297).
4. **Post-processing** (lines 1329+): records scan outcomes in the tiered watch runtime, applies scan policy, sends promotion/ephemeral watch commands.

### 2.2. Core Function: `process_dirty_entry_with_project_markers_and_manifest_skip_dirs`

**File:** `src/index/tiered/sync.rs:621`

This function dispatches based on `DirtyScope` and `DirtyReason`:

| Scope / Reason | Handler | Description |
|---|---|---|
| `DirtyScope::All` (any reason) | `fast_sync()` (line 635) | Full root-tree crawl with mtime cutoff |
| `Dirs` + `StartupRepairDeferred` | `fast_sync()` (line 644) | Same as All but scoped to specific dirs |
| `Dirs` + `FastScanChangedDir` | `fast_sync()` (line 652) | Fast-sync scoped to changed dirs |
| `Dirs` + `PeriodicColdScan` | `scan_dir_repair_slice_with_project_markers()` (line 690) | **Sliced repair scan** — the main periodic scan path |
| `Dirs` + other reasons | `scan_dirs_with_depth_and_project_markers_budgeted()` (line 714) | Immediate depth-1 scan |

### 2.3. The Periodic Cold Scan Path (Primary Bottleneck)

**Function:** `scan_dir_repair_slice_with_project_markers`  
**File:** `src/index/tiered/sync.rs:1222`

This is the function called for `PeriodicColdScan` entries. It has two phases:

#### Phase 1: Manifest Skip Check (lines 1230–1260)

```rust
if allow_manifest_skip && cursor.is_none() {
    if let Some((summary, complete)) = self.directory_manifest_summary_bounded(
        dir, project_markers, REPAIR_SLICE_MAX_ENTRIES,
    ) {
        if complete {
            let trusted = self.clock_cutoff_trusted();
            if self.directory_manifests.should_skip(dir, &summary, trusted) {
                // SKIP: return empty outcome
                return SlicedScanOutcome { ... manifest_skipped: true ... };
            }
        }
    }
}
```

**Problem:** `directory_manifest_summary_bounded` (line 1533) does a **full `read_dir` + `child.metadata()` (stat) on every child** up to `REPAIR_SLICE_MAX_ENTRIES` (512) entries. If the directory has more than 512 entries, it returns `complete=false`, and the manifest skip check is **bypassed entirely**.

This means:
- For directories with ≤512 entries: The manifest skip requires 512 stat() calls to compute the summary, then compares against the stored hash. If unchanged, it skips the upsert work but **still paid the stat() cost**.
- For directories with >512 entries (like Projects/ with 300K files): The manifest summary is never "complete", so **the manifest skip never fires**. Every scan does the full sliced scan.

#### Phase 2: Sliced Scan (lines 1262–1404)

```rust
let slice = read_dir_slice(dir, start_offset, REPAIR_SLICE_MAX_ENTRIES,
    Duration::from_millis(REPAIR_SLICE_MAX_MS))?;
```

`read_dir_slice` (line 178, Unix version):
- Uses `libc::opendir` + `libc::readdir` + `libc::telldir` + `libc::seekdir` (raw libc calls, not Rust `read_dir`).
- Reads up to 512 entries or 20ms elapsed, whichever comes first.
- **Does NOT stat() entries** — only collects names and paths.

Then for each entry in the slice (lines 1299–1355):
```rust
for child in &slice.entries {
    let path = super::normalize_path(child.path.as_path());
    self.io_governor.before_io();
    let meta = match std::fs::symlink_metadata(&path) {  // <-- STAT PER FILE
        Ok(meta) => meta,
        Err(err) => { ... continue; }
    };
    // ... file_key, mtime, path_freshness check ...
    // ... push upsert event + meta ...
}
```

**This is the bottleneck:** `std::fs::symlink_metadata(&path)` is called on **every single child entry** in the slice. Each `symlink_metadata` is a `lstat()` syscall.

For a 300K-file directory, the scan is sliced into ~586 slices of 512 entries each. Each slice requires 512 `lstat()` syscalls. At ~5μs per syscall (cold cache), that's ~2.5ms per slice, ~1.5s total for stat() alone. But with cold page cache and IO contention, each stat can take 50-200μs, pushing the total to 15-60 seconds.

### 2.4. The Fast-Sync Path

**Function:** `fast_sync`  
**File:** `src/index/tiered/sync.rs:783`

Used for `DirtyScope::All`, `StartupRepairDeferred`, and `FastScanChangedDir`.

1. **Collects changed directories** via `collect_dirs_changed_since` (line 799) — this crawls from roots using `visit_dirs_since`, which does `symlink_metadata` on each directory (not file) to check mtime. Directories whose mtime > cutoff are collected.

2. **Scans each collected directory** using `ignore::WalkBuilder` with `max_depth(1)` (line 867). For each entry, it calls `ent.metadata()` (line 920) — **another stat() per file**.

3. **Delete alignment** (lines 967–1005): For each file in the index that lives under a dirty directory, it calls `std::fs::symlink_metadata(&path)` to check if the file still exists. This is **another stat() per indexed file** in the dirty directories.

So fast_sync does: 1 stat per directory (for mtime check) + 1 stat per file (for metadata) + 1 stat per indexed file (for delete alignment) = **up to 3 stat() calls per file** in the worst case.

### 2.5. Index Comparison: `path_freshness`

**File:** `src/index/tiered/sync.rs:1612`

```rust
pub fn path_freshness(&self, path, file_key, mtime_ns: i64) -> PathFreshness {
    match self.l2.load_full().path_freshness(path, mtime_ns) {
        PathFreshness::Missing => self.base.load_full().path_freshness(path, file_key, mtime_ns),
        known => known,
    }
}
```

- **L2 (`PersistentIndex::path_freshness`)** at `src/index/l2_partition.rs:835`: Looks up the path in the path arena, checks if the stored mtime matches. Returns `Missing` if path not found, `Unchanged` if mtime matches, `Changed` otherwise.
- **Base (`BaseIndexData::path_freshness`)** at `src/index/base_index.rs:653`: Looks up by file_key, compares path + mtime.

This comparison itself is fast (hash lookup). The cost is in the `stat()` needed to get the current mtime, not in the comparison.

---

## 3. Key Data Structures

### 3.1. `ScanOutcome` (`src/index/tiered/mod.rs:56`)

```rust
pub struct ScanOutcome {
    pub scanned: usize,      // total files scanned
    pub changed: usize,      // files with changed/missing mtime
    pub elapsed_ms: u64,
    pub project_roots: Vec<PathBuf>,
}
```

### 3.2. `DirectoryManifest` / `DirectoryManifestSummary` (`src/index/tiered/directory_manifest.rs:11-37`)

```rust
pub(crate) struct DirectoryManifestSummary {
    pub child_count: u64,
    pub names_hash: u64,         // xxh3 hash of child names + kind tags
    pub child_mtime_hash: u64,   // xxh3 hash of child names + mtime + kind tags
    pub min_mtime_ns: i64,
    pub max_mtime_ns: i64,
}
```

The manifest stores a hash of all child names and their mtimes. `should_skip` returns true if the current summary matches the stored summary and the clock is trusted.

### 3.3. `DirtyQueueEntry` / `DirtyScope` (`src/event/sync.rs:8-146`)

- `DirtyScope::All { cutoff_ns }` — global dirty, triggers full fast-sync.
- `DirtyScope::Dirs { cutoff_ns, dirs }` — specific directories, triggers scoped scan.
- `DirtyReason::PeriodicColdScan` — the periodic L1/L2/L3 scan trigger.
- `DirtyRepairCursor { dir, offset }` — tracks seek position for sliced scanning.

### 3.4. Constants

```rust
const REPAIR_SLICE_MAX_ENTRIES: usize = 512;  // sync.rs:25
const REPAIR_SLICE_MAX_MS: u64 = 20;          // sync.rs:26
```

---

## 4. Bottleneck Analysis

### 4.1. Primary Bottleneck: stat() Per File

**Location:** `src/index/tiered/sync.rs:1302`

```rust
let meta = match std::fs::symlink_metadata(&path) {
```

This is inside the loop over `slice.entries` in `scan_dir_repair_slice_with_project_markers`. Every child file gets a `lstat()` syscall.

**Impact:** For Projects/ with 300K files:
- 300K × `lstat()` = 300K syscalls
- At 5-50μs per syscall (depending on cache state) = 1.5–15 seconds
- With cold cache or IO contention: 15–60 seconds

The same pattern exists in:
- `fast_sync` at line 920: `ent.metadata()` per file
- `scan_dirs_with_depth_and_project_markers_budgeted` at line 1149: `ent.metadata()` per file
- `directory_manifest_summary_bounded` at line 1571: `child.metadata()` per file
- `delete_alignment` at line 992: `std::fs::symlink_metadata(&path)` per indexed file

### 4.2. Secondary Bottleneck: Manifest Skip Doesn't Actually Save stat() Calls

**Location:** `src/index/tiered/sync.rs:1533` (`directory_manifest_summary_bounded`)

The manifest skip mechanism requires computing a `DirectoryManifestSummary`, which involves:
1. `std::fs::read_dir(dir)` — one `getdents` syscall (efficient, returns all entries at once)
2. `child.metadata()` for each child — **N stat() calls**

Only after computing the full summary does it compare against the stored manifest. If the summary matches, it skips the upsert work — but the N stat() calls have already been paid.

**For directories >512 entries:** The `directory_manifest_summary_bounded` function is bounded by `max_entries=512`. If the directory has more than 512 entries, it returns `complete=false`, and the manifest skip check at line 1236 (`if complete`) never fires. **The manifest skip is completely disabled for large directories**, which are exactly the ones that need it most.

### 4.3. Tertiary Bottleneck: Delete Alignment stat() Storm

**Location:** `src/index/tiered/sync.rs:990-996`

```rust
for (_doc_id, path) in to_delete {
    io_governor.before_io();
    match std::fs::symlink_metadata(&path) {
        Ok(_) => continue,           // file exists, skip
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {} // file deleted
        Err(_) => continue,
    };
    // ... create delete event ...
}
```

`delete_alignment_with_parent_index` (base_index.rs:706 / l2_partition.rs:986) finds all indexed files whose parent directory is in the dirty set. Then fast_sync does a `symlink_metadata` on **each of those files** to check if they still exist. For Projects/ with 300K indexed files, this is another 300K stat() calls.

### 4.4. No Directory mtime Pre-Check Before Scanning Children

The `visit_dirs_since` function (sync.rs:56) **does** check directory mtime before recursing — but it's only used in the `fast_sync` path (for `DirtyScope::All` and root-level dirs). The **periodic cold scan path** (`scan_dir_repair_slice_with_project_markers`) does **not** check the directory's mtime before scanning its children. It always does the full sliced scan.

---

## 5. Optimization Opportunities (Ranked by Impact/Effort)

### 5.1. 🔴 HIGH IMPACT, LOW EFFORT: Directory mtime Pre-Check Before Sliced Scan

**Current:** `scan_dir_repair_slice_with_project_markers` always does the full sliced scan, even if the directory hasn't changed.

**Proposal:** Before entering the sliced scan loop, check the directory's mtime. If it hasn't changed since the last scan, skip entirely.

```rust
// Before line 1262 (after manifest skip check):
if let Ok(dir_meta) = std::fs::symlink_metadata(dir) {
    if let Ok(dir_mtime) = dir_meta.modified() {
        let dir_mtime_ns = mtime_to_ns(Some(dir_mtime));
        if let Some(last_scan_mtime) = self.directory_manifests.last_mtime(dir) {
            if dir_mtime_ns == last_scan_mtime {
                return SlicedScanOutcome {
                    outcome: ScanOutcome::default(),
                    manifest_skipped: true,
                    completed: true,
                    next_cursor: None,
                    dropped_stale_batch: false,
                };
            }
        }
    }
}
```

**Why this works:** On Linux, a directory's mtime changes whenever a file is created, deleted, or renamed within it. If the directory mtime hasn't changed, no files have been added or removed. File content modifications don't change the parent directory's mtime, but those are handled by inotify watchers (L0) or the content-indexing pipeline.

**Caveat:** This misses files that were modified (not created/deleted) in non-L0 directories. But for the periodic cold scan, the purpose is primarily to detect new and deleted files, not content changes. Content changes are detected via `path_freshness` mtime comparison during the scan — but if we skip the scan, we miss those. This is an acceptable trade-off for the periodic scan, which is a best-effort mechanism.

**Impact:** Eliminates 100% of stat() calls for unchanged directories. For a steady-state system where most directories don't change between scan cycles, this reduces stat() calls from 300K to 1 per directory.

**Effort:** ~20 lines of code. Need to add `last_mtime_ns` to `DirectoryManifest` and expose a getter.

### 5.2. 🔴 HIGH IMPACT, MEDIUM EFFORT: readdir Without stat — Use d_type from getdents

**Current:** `read_dir_slice` (sync.rs:178) uses `libc::readdir` which returns `struct dirent` containing `d_type`. But the code **discards `d_type`** and only stores the path. Later, `symlink_metadata` is called per entry to get file type and metadata.

**Proposal:** Extract `d_type` from the `dirent` struct in `read_dir_slice` and store it in `DirChildEntry`. Use `d_type` to filter out non-file/non-dir entries without stat(). Only stat() files that pass the filter and whose mtime might have changed.

```rust
struct DirChildEntry {
    path: PathBuf,
    d_type: u8,  // DT_REG=8, DT_DIR=4, DT_LNK=10, DT_UNKNOWN=0
}
```

In the scan loop:
```rust
// Skip non-files/non-dirs without stat
if d_type != DT_REG && d_type != DT_DIR && d_type != DT_UNKNOWN {
    continue;
}
// Only stat if d_type is unknown or we need metadata
```

**Impact:** On most Linux filesystems (ext4, xfs), `d_type` is always populated. This eliminates stat() for symlinks, sockets, pipes, etc. But for regular files, you still need stat() to get mtime for the freshness check. So this doesn't eliminate the main bottleneck but reduces it by ~10-20% (assuming some non-file entries).

**Effort:** ~30 lines. Modify `DirChildEntry`, `read_dir_slice`, and the scan loop.

### 5.3. 🟡 MEDIUM IMPACT, MEDIUM EFFORT: Unbounded Manifest Summary (Remove 512-Entry Cap)

**Current:** `directory_manifest_summary_bounded` is capped at 512 entries (`REPAIR_SLICE_MAX_ENTRIES`). For directories >512 entries, `complete=false` and the manifest skip never fires.

**Proposal:** Create an unbounded version of `directory_manifest_summary` that reads all entries in a single `read_dir` pass. Compare against the stored manifest. If unchanged, skip the entire scan.

**Why this works:** The summary only stores a hash (child_count + names_hash + mtime_hash + min/max mtime), so memory is O(1) regardless of directory size. The cost is N stat() calls to compute the summary — but if the directory hasn't changed, this replaces N stat() + N upsert operations with just N stat() operations (the upsert/index-update work is skipped).

**Impact:** For unchanged large directories, saves the upsert/index-update work (which includes path normalization, file_key computation, path_freshness lookup, event creation, and index mutation). The stat() cost remains, but the index mutation cost is eliminated. Estimated 40-60% time reduction for unchanged directories.

**Limitation:** Still requires N stat() calls to compute the summary. Combine with optimization 5.1 (directory mtime pre-check) to eliminate stat() entirely for unchanged directories.

**Effort:** ~50 lines. Create `directory_manifest_summary_unbounded`, modify the manifest skip check to use it.

### 5.4. 🟡 MEDIUM IMPACT, HIGH EFFORT: Parallel stat() Using Rayon or tokio::task::spawn_blocking

**Current:** All stat() calls are sequential in a single blocking thread.

**Proposal:** Parallelize the stat() calls within a slice using a thread pool. For a 512-entry slice, spawn 4-8 parallel workers.

```rust
use rayon::prelude::*;
let metas: Vec<_> = slice.entries.par_iter()
    .filter_map(|child| {
        let path = normalize_path(child.path.as_path());
        let meta = std::fs::symlink_metadata(&path).ok()?;
        Some((path, meta))
    })
    .collect();
```

**Impact:** 4-8x speedup on the stat() phase for large directories. For 300K files, reduces from 15s to 2-4s.

**Effort:** ~40 lines. Add rayon dependency (or use existing tokio blocking pool). Need to handle IO governor integration and error handling.

**Risk:** Increased IO contention. The `IoGovernor` is designed to throttle IO — parallelizing stat() may bypass this. Need to integrate `io_governor.before_io()` calls in the parallel workers.

### 5.5. 🟢 LOW IMPACT, LOW EFFORT: Batch Delete Alignment Using readdir

**Current:** Delete alignment (sync.rs:990) does `symlink_metadata` per indexed file in dirty directories. This is O(indexed_files_in_dirty_dirs) stat() calls.

**Proposal:** Instead of stat()-ing each indexed file, do a single `readdir` of the dirty directory to get the set of current file names, then compare against the indexed file names. Files in the index but not in the readdir result are deleted.

**Impact:** For a directory with 300K indexed files, replaces 300K stat() with 1 readdir + N name comparisons. Massive improvement.

**Effort:** ~60 lines. Need to build a `HashSet<OsString>` from readdir results and compare against indexed paths. The code comments (sync.rs:857-858) mention this approach was intentionally avoided to prevent "large short-lived allocations" — but the current approach does 300K stat() calls which is far worse.

### 5.6. 🟢 LOW IMPACT, LOW EFFORT: Increase Slice Size for Large Directories

**Current:** `REPAIR_SLICE_MAX_ENTRIES = 512`, `REPAIR_SLICE_MAX_MS = 20`.

**Proposal:** For directories known to be large (based on previous scan counts), increase the slice size to 4096 or 8192. This reduces the number of `opendir`/`closedir`/`seekdir` cycles.

**Impact:** Marginal. The `opendir`/`closedir` overhead per slice is small compared to the stat() cost. But reduces queue churn (fewer re-enqueue cycles for large directories).

**Effort:** ~10 lines. Make the slice size adaptive based on directory size.

---

## 6. Feasibility of Incremental Directory Scanning (mtime-based)

### 6.1. Current State

The codebase **partially** implements mtime-based incremental scanning:

1. **`visit_dirs_since`** (sync.rs:56): Crawls directory trees and collects directories whose mtime > cutoff. Used only in `fast_sync`, not in the periodic cold scan path.

2. **`DirectoryManifestStore`** (directory_manifest.rs:126): Stores a hash summary per directory. `should_skip` returns true if the current summary matches. But computing the summary requires stat()-ing all children.

3. **`path_freshness`** (sync.rs:1612): Compares individual file mtime against the index. Used during scanning to skip unchanged files, but requires stat() to get the current mtime.

### 6.2. What's Missing

**Directory-level mtime pre-check before scanning children is NOT implemented in the periodic cold scan path.** The `scan_dir_repair_slice_with_project_markers` function always does the full sliced scan without checking if the directory's mtime has changed.

### 6.3. Feasibility Assessment

**Highly feasible.** The infrastructure is already in place:

1. The `DirectoryManifest` already stores `min_mtime_ns` and `max_mtime_ns` of children. Adding a `dir_mtime_ns` field is trivial.

2. The `visit_dirs_since` function already demonstrates the pattern: `symlink_metadata(dir).modified()` → compare against cutoff.

3. On Linux ext4/xfs/btrfs, directory mtime is updated atomically when entries are added or removed. This is a kernel guarantee.

4. The periodic cold scan is a best-effort mechanism — missing a content-only modification (which doesn't change directory mtime) is acceptable, as those are handled by:
   - L0 inotify watchers (for watched directories)
   - Lazy validation (`lazy_validation.rs`) — on-demand stat() when a query hits a potentially stale result
   - Content indexing pipeline

### 6.4. Recommended Implementation

```rust
// In scan_dir_repair_slice_with_project_markers, after the manifest skip check
// and before the sliced scan:

// NEW: Directory mtime pre-check
if cursor.is_none() {
    if let Ok(dir_meta) = std::fs::symlink_metadata(dir) {
        if let Ok(dir_modified) = dir_meta.modified() {
            let dir_mtime_ns = mtime_to_ns(Some(dir_modified));
            if self.directory_manifests.dir_unchanged(dir, dir_mtime_ns) {
                // Directory hasn't changed since last scan — skip entirely
                return SlicedScanOutcome {
                    outcome: ScanOutcome { elapsed_ms: 0, ..Default::default() },
                    manifest_skipped: true,
                    completed: true,
                    next_cursor: None,
                    dropped_stale_batch: false,
                };
            }
        }
    }
}
```

Add to `DirectoryManifest`:
```rust
pub(crate) struct DirectoryManifest {
    // ... existing fields ...
    pub dir_mtime_ns: i64,  // NEW: mtime of the directory itself
}
```

Add to `DirectoryManifestStore`:
```rust
pub fn dir_unchanged(&self, path: &Path, current_mtime_ns: i64) -> bool {
    self.manifests.lock()
        .get(path)
        .map(|m| m.dir_mtime_ns == current_mtime_ns)
        .unwrap_or(false)
}
```

---

## 7. Summary of Findings

| # | Bottleneck | Location (file:line) | stat() Count | Fix |
|---|---|---|---|---|
| 1 | stat() per file in sliced scan | `sync.rs:1302` | N per scan | Dir mtime pre-check (5.1) |
| 2 | stat() per file in fast_sync | `sync.rs:920` | N per sync | Dir mtime pre-check + batch readdir |
| 3 | Manifest summary requires full stat | `sync.rs:1571` | N per check | Combine with dir mtime pre-check |
| 4 | Manifest skip disabled for >512 entries | `sync.rs:1566` | 0 (but no skip) | Remove cap or use dir mtime |
| 5 | Delete alignment stat() per file | `sync.rs:992` | N per align | Use readdir name set comparison |
| 6 | No parallelism in stat() calls | `sync.rs:1299` | N (sequential) | Parallel stat with rayon |

### Root Cause

The fundamental design assumption is that periodic cold scans must `stat()` every file to determine freshness. But on Linux, **directory mtime is a free signal** that tells you whether any file was added or removed. The code uses this signal in `visit_dirs_since` (for fast_sync) but **not** in the periodic cold scan path (`scan_dir_repair_slice_with_project_markers`).

The directory manifest mechanism was designed to solve this, but it has two fatal flaws:
1. It requires a full stat() pass to compute the summary (defeating the purpose).
2. It's capped at 512 entries (useless for large directories).

### Recommended Priority

1. **Implement directory mtime pre-check** (5.1) — eliminates stat() for unchanged directories entirely. ~20 lines, immediate impact.
2. **Fix manifest skip for large directories** (5.3) — remove the 512-entry cap. ~50 lines.
3. **Batch delete alignment with readdir** (5.5) — eliminates the delete-alignment stat() storm. ~60 lines.
4. **Parallel stat()** (5.4) — for directories that have changed, speed up the stat() phase. ~40 lines.

Combined, optimizations 1+2+3 would reduce the periodic scan of a 300K-file unchanged directory from ~30 seconds to <1 millisecond (one stat() on the directory itself).
