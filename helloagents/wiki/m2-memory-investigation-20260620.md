# M2 Memory Investigation — 2026-06-20

## Executive Summary

Indexing 1 million files consumes ~760 MB RSS against a theoretical minimum of ~200 MB.
This investigation identifies **five major optimization opportunities** totaling an
estimated **430–520 MB** of potential RSS reduction, plus a systemic allocator
fragmentation factor that inflates all heap structures by ~30–40%.

The single highest-impact finding is that the lead's analysis contained a factual error:
**`PersistentIndex` does not have an `arena: PathArena` field at runtime.** The `PathArena`
only exists in snapshot structs (`IndexSnapshotV4`/`V5`). At runtime, all path data lives in
`paths: RwLock<Vec<Vec<u8>>>` — a `Vec` of 1 million individually heap-allocated `Vec<u8>`,
each storing the full absolute path. This is the primary memory waste.

---

## 1. Per-Component Memory Breakdown (1M files)

All figures are estimates for 1 million live files, based on struct definitions and the
`memory_stats()` estimator at `src/index/l2_partition.rs:1646`.

| Component | Field | Est. Heap | Notes |
|-----------|-------|-----------|-------|
| **entries** | `RwLock<Vec<FileEntry>>` | 32 MB | 32 B/file × 1M. `FileEntry` is `#[repr(C)]` 32B (dev:8 + ino:8 + gen:4 + path_idx:4 + mtime_ns:8). See `src/index/file_entry_v2.rs:20`. |
| **paths** | `RwLock<Vec<Vec<u8>>>` | **84 MB** | 1M × (24B Vec header + ~60B avg path). Each path is a separate heap allocation. See line 411. |
| **filekey_to_docid** | `RwLock<HashMap<FileKey, DocId>>` | **56 MB** | FileKey=20B, DocId=8B, entry=28B + 1B ctrl + ~50% load factor overhead. See line 416. |
| **path_hash_to_id** | `RwLock<HashMap<u64, OneOrManyDocId>>` | **48 MB** | key=8B, value=16B (enum One(u64) or Many(ptr)), entry=24B + 1B ctrl + load factor. See line 419. |
| **trigram_index** | `RwLock<HashMap<Trigram, RoaringTreemap>>` | **~100 MB** | ~16K distinct trigrams, each with RoaringTreemap postings. See line 422. |
| **parent_index** | `RwLock<Option<ParentIndex>>` | Variable | Built on demand; adds path table + directory bitmap. |
| **parent_path_table** | `RwLock<Option<RebuildPathTable>>` | Variable | `HashMap<Vec<u8>, u32>` + `Vec<Vec<u8>>` — duplicates all paths again. |
| **tombstones** | `RwLock<RoaringTreemap>` | <1 MB | Sparse bitmap. |
| **mimalloc fragmentation** | — | **~100 MB** | 1M+ small allocations cause ~30-40% RSS inflation over logical heap. |
| **Total estimated** | | **~420 MB** | Plus fragmentation → ~520–560 MB. With mmap snapshot pages → ~760 MB. |

### Why the gap to 760 MB?

1. **mimalalloc fragmentation**: 1M+ individual `Vec<u8>` allocations for paths, plus
   HashMap bucket arrays, create significant allocator overhead. Mimalloc rounds up small
   allocations and doesn't return memory to the OS eagerly.
2. **parent_path_table duplication**: When `rebuild_parent_index()` is called (line 956),
   it creates a `RebuildPathTable` that interns ALL paths again as `Vec<Vec<u8>>` + a
   `HashMap<Vec<u8>, u32>`. This is a full second copy of all path bytes (~84 MB + 56 MB).
3. **mmap snapshot pages**: If a v7 snapshot is loaded, mmap'd pages that are faulted
   during queries count toward VmRSS (see §4 below).

---

## 2. Redundancy Findings

### 2.1 `paths` vs PathArena — CRITICAL FINDING

**Lead's assumption was wrong.** The lead's analysis states:

> `arena: PathArena` — Continuous path bytes storage. Already optimized. ~60MB.

**There is no `arena` field in `PersistentIndex`.** The struct definition at
`src/index/l2_partition.rs:404-433` contains:

```rust
pub struct PersistentIndex {
    roots: Vec<PathBuf>,
    roots_bytes: Vec<Vec<u8>>,
    entries: RwLock<Vec<FileEntry>>,
    paths: RwLock<Vec<Vec<u8>>>,          // ← line 411: THE ONLY path storage
    filekey_to_docid: RwLock<HashMap<FileKey, DocId>>,
    path_hash_to_id: RwLock<HashMap<u64, OneOrManyDocId>>,
    trigram_index: RwLock<HashMap<Trigram, RoaringTreemap>>,
    tombstones: RwLock<RoaringTreemap>,
    dirty: AtomicBool,
    parent_index: RwLock<Option<ParentIndex>>,
    parent_path_table: RwLock<Option<RebuildPathTable>>,
}
```

`PathArena` is defined at line 92 but only used in:
- `IndexSnapshotV4` (line 219) — snapshot serialization format
- `IndexSnapshotV5` (line 239) — snapshot serialization format
- `build_legacy_metas()` (line 1954) — builds a **temporary** PathArena on each snapshot save, then discards it

**What `paths` stores**: Each DocId maps to a `Vec<u8>` containing the **absolute** path bytes
(e.g., `/home/user/project/src/main.rs`). This is written at:
- `alloc_docid()` line 802: `self.paths.write().push(abs_path_bytes.to_vec())`
- `from_snapshot_v5()` line 555: `paths.push(abs_bytes)` — reconstructs absolute paths from arena's root-relative bytes
- `update_entry_path()` line 1901: `*path = abs_path_bytes.to_vec()`

**What `paths` is read for** (15 read sites):
- `query()` line 862: trigram candidate filtering — reads path bytes for matcher
- `for_each_live_meta()` line 914: iteration for flush/compaction
- `rebuild_derived_indexes()` line 628: rebuilds filekey/path_hash/trigram from paths
- `rebuild_parent_index()` line 960: builds parent index from paths
- `lookup_docid_by_path()` line 2038: verifies path match after hash lookup
- `get_meta()` line 2249: IndexLayer trait — returns FileMeta with path
- `query_keys()` line 2195: IndexLayer trait — returns FileKeys for matched paths
- `live_paths_by_file_key()` line 1826: hardlink group computation
- `delete_alignment_with_parent_index()` line 1002: parent-index-based deletion
- `to_base_index_data()` line 2091: conversion to base index
- `build_legacy_metas()` line 1956: snapshot serialization
- `path_buf_for_docid()` line 1855: internal helper

**Optimization: Replace `Vec<Vec<u8>>` with a persistent `PathArena`**

Add `arena: RwLock<PathArena>` to `PersistentIndex`, storing root-relative path bytes
contiguously. Each `FileEntry` already has a `path_idx` field (u32), which can serve as
the arena offset. The `CompactMeta` struct (line 174) already has `path_off: u32` and
`path_len: u16` — this is the exact layout needed.

**Memory savings**:
- Current: 1M × (24B Vec header + 60B avg path + 8B alignment) ≈ 84 MB + allocator overhead
- Proposed: 60 MB contiguous bytes + 1M × 6B (off+len) = 66 MB, with **zero** per-path heap allocation
- **Net saving: ~18 MB logical + ~30 MB fragmentation = ~48 MB**

**Blast radius**: All 15 read sites use the pattern `paths.get(docid as usize)` which returns
`Option<&Vec<u8>>`. These would change to `arena.get_bytes(off, len)` returning `Option<&[u8]>`.
The `from_snapshot_v5()` path (line 522) would keep the arena directly instead of expanding
to Vec<Vec<u8>>. The `upsert_inner()` path (line 723) reads old path bytes for rename — would
need arena read. The `update_entry_path()` (line 1897) writes new path — would need arena append.

**Complexity**: Medium. The arena needs to support append (for upsert) and in-place update
(for rename). Since PathArena uses `Arc<Vec<u8>>`, append requires `Arc::make_mut`. For
rename, old path bytes become dead space in the arena (acceptable — compaction during
snapshot save reclaims this).

### 2.2 `filekey_to_docid` vs `path_hash_to_id`

These two HashMaps serve **fundamentally different purposes** and cannot be trivially merged:

**`filekey_to_docid: HashMap<FileKey, DocId>`** (line 416):
- Key: `FileKey { dev: u64, ino: u64, generation: u32 }` = 20 bytes
- Value: `DocId` (u64) = 8 bytes
- Purpose: FileKey → representative DocId for:
  - Event processing (rename/reconcile): `upsert_inner()` line 719
  - `get_meta()` by FileKey: line 2244
  - Hardlink detection: determines if a new path is a hardlink alias
  - `mark_deleted()` by FileKey: line 813
  - Snapshot serialization (FileKeyMap segment): line 1436

**`path_hash_to_id: HashMap<u64, OneOrManyDocId>`** (line 419):
- Key: `u64` (hash of absolute path bytes via `path_hash_bytes()`, line 2256)
- Value: `OneOrManyDocId` — either `One(DocId)` or `Many(Vec<DocId>)` for hash collisions
- Purpose: path → DocId for:
  - `lookup_docid_by_path()` line 2023: the primary upsert dedup check
  - `path_freshness()` line 836: checks if path already indexed
  - `mark_deleted_by_path()` line 822

**Why they can't merge**:
- `filekey_to_docid` maps inode identity → DocId (for hardlink/rename handling)
- `path_hash_to_id` maps path string → DocId (for dedup and freshness)
- A single file can have multiple paths (hardlinks) sharing one FileKey
- A renamed file has the same FileKey but different path

**However, `filekey_to_docid` can be optimized**:

The `entries: Vec<FileEntry>` already stores `dev`, `ino`, `generation` for every DocId.
The function `docids_for_filekey()` (line 1905) already does a linear scan. The
`filekey_to_docid` HashMap is just an acceleration index.

**Option A: Replace with sorted Vec + binary search** (like v6 snapshot's FileKeyMap):
- Store `Vec<(FileKey, DocId)>` sorted by `(dev, ino, generation)`
- Binary search for lookups: O(log N)
- Memory: 1M × 28B (20B key + 8B value) = 28 MB, no HashMap overhead
- **Saving: ~28 MB** (from 56 MB to 28 MB)

**Option B: Remove entirely, use entries Vec scan with caching**:
- `docids_for_filekey()` already works — just slow for large indexes
- Add an LRU cache for recent FileKey lookups
- **Saving: ~56 MB** but with performance regression for event-heavy workloads

**Recommendation**: Option A. The v6 snapshot format already serializes a sorted FileKeyMap
(line 1435). At runtime, use the same sorted Vec structure.

### 2.3 `parent_path_table` — Hidden Path Duplication

`RebuildPathTable` (line 441) stores:
```rust
struct RebuildPathTable {
    path_to_id: HashMap<Vec<u8>, u32>,  // ← duplicates ALL path bytes
    id_to_path: Vec<Vec<u8>>,            // ← duplicates ALL path bytes again
    dirs: HashSet<u32>,
}
```

When `rebuild_parent_index()` (line 956) is called, it interns every live path into this
table. This means **all 1M paths are stored a second time** as `Vec<u8>` keys in the HashMap
and a third time in the `id_to_path` Vec.

**Memory impact**: ~84 MB (HashMap keys) + ~84 MB (id_to_path Vec) = **~168 MB** duplicated.

**Optimization**: If `paths` is replaced with a PathArena, the parent path table can reference
arena bytes by offset instead of cloning. Alternatively, `id_to_path` can store `(u32, u16)`
arena offsets instead of `Vec<u8>`.

---

## 3. Trigram Index Analysis

### 3.1 Current Implementation

The trigram index (`trigram_index: RwLock<HashMap<Trigram, RoaringTreemap>>`, line 422)
indexes **basename trigrams only** — not full path trigrams. This is already an optimization
over full-path trigram indexing.

**How trigrams are generated** (`for_each_basename_trigram()`, line 83):
1. Extract `path.file_name()` (basename only)
2. Case-fold via Unicode lookup (`folded_lookup_bytes_lossy`)
3. Generate all 3-byte sliding windows

**Query path** (`trigram_candidates()`, line 2047):
1. Extract literal hint from matcher
2. Generate query trigrams from the hint
3. Intersect RoaringTreemap postings for all query trigrams
4. Return candidate DocId set

### 3.2 Memory Estimation for 1M files

- **Average basename length**: ~10 characters (typical source files)
- **Trigrams per basename**: max(0, len - 2) ≈ 8 trigrams/file
- **Total postings**: ~8M (with duplicates collapsed per trigram)
- **Distinct trigrams**: ~16K (realistic estimate — 256³ = 16.7M possible, but ASCII
  filenames use ~60 distinct characters, giving ~216K possible, of which ~16K appear in
  practice)

**Per-trigram RoaringTreemap cost**:
- Each `RoaringTreemap` stores DocIds as u64. For 1M files, DocIds fit in u32.
- Average postings per trigram: 8M / 16K ≈ 500 DocIds
- RoaringTreemap with 500 u64 values: ~2-4 KB serialized, but heap overhead is larger
  due to container structure (array of containers, each with cardinality, type, etc.)
- `serialized_size()` is used in `memory_stats()` (line 1668) but underestimates actual
  heap usage because RoaringTreemap's Rust implementation has per-container allocation overhead

**Estimated total**: ~100 MB (matches lead's estimate). This includes:
- HashMap overhead: 16K entries × (3B key + 1B pad + 48B RoaringTreemap struct + 1B ctrl) ≈ 1 MB
- RoaringTreemap postings: ~99 MB for 8M u64 DocIds with container overhead

### 3.3 Optimization: Use RoaringBitmap (u32) instead of RoaringTreemap (u64)

**Current**: `RoaringTreemap` = `Vec<RoaringBitmap>` for 64-bit DocIds. Since DocId is
assigned sequentially as `entries.len() as DocId` (line 797), and 1M files < 2³², all
DocIds fit in u32.

**Optimization**: Replace `RoaringTreemap` with `RoaringBitmap` (u32 keys):
- Halves posting storage (4B per DocId instead of 8B)
- Eliminates the high 32-bit container array overhead
- **Estimated saving: ~50 MB** (from ~100 MB to ~50 MB)

The v6 snapshot format already converts to `RoaringBitmap` during serialization (line 1383:
`posting.iter().map(|v| v as u32).collect()`), confirming u32 is sufficient.

### 3.4 Optimization: Move trigram index to mmap (cold storage)

The trigram index is read-only during queries (the `trigram_index.read()` guard at line 2055).
It is only written during upsert/delete/rebuild. For a large stable index, the trigram postings
could be serialized to a mmap'd file and queried zero-copy.

The v6 snapshot format already serializes trigram postings as a `postings_blob` segment
(line 1380). The v7 mmap format could serve trigram queries directly from mmap'd pages.

**Estimated saving: ~100 MB** moved from heap to mmap (pages only count toward RSS when
accessed, and can be evicted with `MADV_DONTNEED`).

**Trade-off**: Write path becomes more complex (need to rebuild mmap on significant changes).
Best suited for the cold/base index layer, not the hot L2 layer.

### 3.5 Trigram count reduction

Trigrams are already basename-only. Further reduction options:
- **Skip very common trigrams** (e.g., "ing", "ion", "txt") — saves postings for high-frequency
  trigrams but risks false negatives
- **Use bigram index for short basenames** — reduces distinct keys but increases postings per key
- **Not recommended**: The basename-only optimization is already near-optimal for this use case

---

## 4. Snapshot / tmpfs Analysis

### 4.1 /tmp IS tmpfs

```
$ df -T /tmp
Filesystem     Type   1K-blocks  Used Available Use% Mounted on
tmpfs          tmpfs  12203340  47108  12156232   1% /tmp

$ mount | grep tmp
tmpfs on /tmp type tmpfs (rw,nosuid,nodev,nr_inodes=1048576,inode64,huge=within_size,usrquota)
```

**If the snapshot file is on tmpfs, mmap'd pages count toward RSS.** This is because tmpfs
pages are RAM-backed — there is no disk backing, so mmap'd pages are always resident.

### 4.2 Default snapshot path is NOT on tmpfs

The benchmark script (`scripts/m2-cold-window-vm-bench.py:2490-2503`):

```python
run_dir = Path(args.run_dir) if args.run_dir else repo / "reports" / "m2-cold-window-vm" / f"{utc_stamp()}_{args.run_label}"
run_dir = run_dir.resolve()
...
snapshot_path = run_dir / "index.db"
```

By default, `run_dir` is under the repository directory
(`/home/shiyi/Downloads/vcp-FD/fd-rdd/reports/m2-cold-window-vm/...`), which is on the
real filesystem (ext4 or similar), NOT tmpfs.

**However**, if `--run-dir /tmp/...` is passed, the snapshot file lands on tmpfs, and
mmap'd snapshot pages inflate RSS.

### 4.3 How snapshot loading affects RSS

The L2 `PersistentIndex` is always a **heap** structure. When loading from a v7 snapshot
(`src/index/tiered/load.rs:641`):

```rust
let l2 = Arc::new(PersistentIndex::new_with_roots(roots.clone()));
```

The L2 is created **empty**. The v7 data goes into `v7_data` (the cold/base index layer),
which uses mmap (`src/storage/snapshot_v7.rs:1363`):

```rust
let mmap = unsafe { memmap2::MmapOptions::new().map_copy_read_only(&file)? };
```

This is `MAP_PRIVATE` (copy-on-write). Accessed pages count toward VmRSS as `Private_Clean`
(or `Private_Dirty` if modified, though this is read-only).

After loading, `MADV_DONTNEED` is called (line 1438) to release pages, but subsequent
queries will fault them back in.

### 4.4 During indexing (benchmark scenario)

When the benchmark indexes 1M files:
1. L2 PersistentIndex is populated via `upsert()` / `apply_file_metas()` — all heap
2. Snapshot is periodically saved via `export_snapshot_v5()` or `export_segments_v6()`
3. The snapshot file is written to disk (or tmpfs if run_dir is /tmp)
4. If the snapshot is later mmap'd (v7 cold layer), accessed pages add to RSS

**For the 760 MB RSS measurement**: If the benchmark runs with `--run-dir /tmp`, the
snapshot file (~200-300 MB on disk) would be on tmpfs. Any mmap'd pages from the v7 cold
layer would inflate RSS by the amount of snapshot data accessed.

**Recommendation**: 
1. Ensure benchmark `run_dir` is NOT on tmpfs (it isn't by default)
2. Check if the 760 MB measurement was taken with `--run-dir /tmp`
3. Use `/proc/pid/smaps_rollup` to distinguish `Private_Clean` (mmap) from `Anonymous`
   (heap) RSS

### 4.5 RSS measurement methodology

The benchmark reads VmRSS from `/proc/{pid}/status` (line 118):

```python
if key in {"VmRSS", "VmSwap", "VmSize"}:
    value = rest.strip().split()[0]
    out[key.lower() + "_bytes"] = int(value) * 1024
```

VmRSS includes:
- Anonymous heap pages (the PersistentIndex structures)
- Private mmap pages (v7 snapshot, if loaded)
- Shared mmap pages (proportional set size)

**To isolate heap RSS from mmap RSS**, use `/proc/{pid}/smaps_rollup` which provides
`Anonymous`, `Private_Clean`, `Private_Dirty` separately.

---

## 5. Specific Code-Level Recommendations

### Priority 1: Replace `paths: Vec<Vec<u8>>` with persistent PathArena
**Estimated saving: ~48 MB (18 MB logical + 30 MB fragmentation)**

- **What**: Add `arena: RwLock<PathArena>` to `PersistentIndex`. Store root-relative path
  bytes contiguously. Use `CompactMeta`'s `path_off`/`path_len` (or `FileEntry`'s
  `path_idx`) to index into the arena.
- **Where**: `src/index/l2_partition.rs:411` (field), lines 511, 555, 558, 802, 1897-1901
  (writes), lines 628, 723, 862, 914, 960, 1002, 1650, 1826, 1855, 1956, 2038, 2091, 2195,
  2249 (reads).
- **Complexity**: Medium. The arena needs append support. Rename causes dead space (reclaimed
  during snapshot save). All read sites change from `paths.get(docid)` → `arena.get_bytes(off, len)`.
- **Bonus**: Eliminates the need to reconstruct absolute paths in `from_snapshot_v5()` —
  keep the arena directly from the snapshot.

### Priority 2: Eliminate `parent_path_table` path duplication
**Estimated saving: ~168 MB**

- **What**: `RebuildPathTable` (line 441) stores all paths twice (HashMap keys + Vec).
  Replace with arena offset references.
- **Where**: `src/index/l2_partition.rs:441-493` (struct), line 956-983 (rebuild).
- **Complexity**: Medium. Depends on Priority 1 (PathArena). The `intern()` method would
  store arena offsets instead of `Vec<u8>` clones.
- **Impact**: This is the largest single saving because it eliminates a full second copy
  of all path data.

### Priority 3: Replace `RoaringTreemap` with `RoaringBitmap` in trigram index
**Estimated saving: ~50 MB**

- **What**: Change `trigram_index: HashMap<Trigram, RoaringTreemap>` to
  `HashMap<Trigram, RoaringBitmap>`. DocId fits in u32 for <4B files.
- **Where**: `src/index/l2_partition.rs:2` (import), line 422 (field), lines 660, 988,
  1998, 2047-2072 (usage).
- **Complexity**: Low. Mechanical replacement. The v6 snapshot already converts to u32.
- **Caveat**: DocId is currently `u64` (line 64). If DocId is changed to u32, downstream
  code in `OneOrManyDocId`, `filekey_to_docid`, tombstones, etc. needs updating. Alternatively,
  keep DocId as u64 but use RoaringBitmap with `as u32` casts (like v6 already does).

### Priority 4: Replace `filekey_to_docid` HashMap with sorted Vec
**Estimated saving: ~28 MB**

- **What**: Replace `HashMap<FileKey, DocId>` with `Vec<(FileKey, DocId)>` sorted by
  `(dev, ino, generation)`. Binary search for lookups.
- **Where**: `src/index/l2_partition.rs:416` (field), lines 631-646, 690, 711, 719, 775,
  804, 1085, 1237, 1436, 1583, 1920-1938, 2244 (usage).
- **Complexity**: Medium. Need to maintain sorted order on insert (insert + shift, or
  batch sort). The v6 snapshot already serializes a sorted FileKeyMap (line 1435).
- **Trade-off**: O(log N) lookup vs O(1). For 1M entries, ~20 comparisons per lookup.
  Event processing is not latency-critical, so this is acceptable.

### Priority 5: Move trigram index to mmap for cold/base layer
**Estimated saving: ~100 MB (moved from heap to mmap)**

- **What**: For the read-only cold/base index, serialize trigram postings to a mmap'd file.
  Query directly from mmap'd pages.
- **Where**: The v7 snapshot format already supports this (trigram_index segment in
  `src/storage/snapshot_v7.rs`). The L2 hot layer should keep heap trigrams for
  mutability; the L3 cold layer should use mmap.
- **Complexity**: High. Requires mmap-backed trigram query path in the cold layer.
- **Trade-off**: Pages only count toward RSS when accessed; can be evicted with
  `MADV_DONTNEED`. Best for stable, large indexes.

### Priority 6: Verify benchmark tmpfs configuration
**Estimated saving: 0-300 MB (depending on current config)**

- **What**: Ensure benchmark `run_dir` is NOT on tmpfs. Check if the 760 MB measurement
  was taken with `--run-dir /tmp`.
- **Where**: `scripts/m2-cold-window-vm-bench.py:2490`
- **Complexity**: Trivial. Just verify the measurement environment.
- **Diagnostic**: Use `/proc/{pid}/smaps_rollup` to separate anonymous heap from mmap pages.

---

## 6. Priority Ranking and Expected Impact

| Priority | Optimization | Est. Saving | Complexity | Risk |
|----------|-------------|-------------|------------|------|
| **1** | PathArena for paths | 48 MB | Medium | Low |
| **2** | Eliminate parent_path_table duplication | 168 MB | Medium | Medium (depends on #1) |
| **3** | RoaringBitmap (u32) for trigram | 50 MB | Low | Low |
| **4** | Sorted Vec for filekey_to_docid | 28 MB | Medium | Low |
| **5** | mmap trigram for cold layer | 100 MB | High | Medium |
| **6** | Verify tmpfs config | 0-300 MB | Trivial | None |
| **—** | mimalloc fragmentation (auto-reduces with #1,#2) | ~50 MB | — | — |

**Total estimated saving: 394-494 MB** (without tmpfs) or **up to 794 MB** (with tmpfs fix).

**Realistic target after P1-P4**: 760 MB - (48 + 168 + 50 + 28 + 50) = **~416 MB**
Still above the 200 MB target, but the remaining gap is mostly:
- `path_hash_to_id` HashMap: 48 MB (could be replaced with open-addressing or FxHashMap)
- `parent_index` structures: variable
- mimalloc residual fragmentation: ~30-50 MB
- Runtime overhead (thread stacks, code, etc.): ~30 MB

**To reach 200 MB**, additional work needed:
- Replace `path_hash_to_id` with a more compact structure (e.g., FxHashMap or open-addressing)
- Move the entire cold/base index to mmap (not just trigrams)
- Consider not building `parent_index` unless directory-scoped queries are active

---

## Appendix A: Key File Locations

| File | Key Lines | Content |
|------|-----------|---------|
| `src/index/l2_partition.rs` | 404-433 | `PersistentIndex` struct definition |
| `src/index/l2_partition.rs` | 92-157 | `PathArena` struct and methods |
| `src/index/l2_partition.rs` | 174-180 | `CompactMeta` struct (path_off/path_len) |
| `src/index/l2_partition.rs` | 344-396 | `OneOrManyDocId` enum |
| `src/index/l2_partition.rs` | 441-493 | `RebuildPathTable` (path duplication) |
| `src/index/l2_partition.rs` | 626-664 | `rebuild_derived_indexes()` |
| `src/index/l2_partition.rs` | 789-810 | `alloc_docid()` (paths write) |
| `src/index/l2_partition.rs` | 853-909 | `query()` (paths read) |
| `src/index/l2_partition.rs` | 956-983 | `rebuild_parent_index()` |
| `src/index/l2_partition.rs` | 1646-1741 | `memory_stats()` |
| `src/index/l2_partition.rs` | 1954-1981 | `build_legacy_metas()` (temporary PathArena) |
| `src/index/l2_partition.rs` | 2023-2045 | `lookup_docid_by_path()` |
| `src/index/l2_partition.rs` | 2047-2073 | `trigram_candidates()` |
| `src/index/l2_partition.rs` | 2256-2260 | `path_hash_bytes()` |
| `src/index/file_entry_v2.rs` | 20-26 | `FileEntry` (32B fixed-size) |
| `src/core/rdd.rs` | 21-26 | `FileKey` (20B: dev:8 + ino:8 + gen:4) |
| `src/index/case_policy.rs` | 46-54 | `for_each_folded_trigram()` |
| `src/storage/snapshot_v7.rs` | 1351-1459 | `load_v7_from_path()` (mmap loading) |
| `src/index/tiered/load.rs` | 641 | L2 created empty on v7 load |
| `scripts/m2-cold-window-vm-bench.py` | 110-123 | `read_proc_status()` (RSS measurement) |
| `scripts/m2-cold-window-vm-bench.py` | 2490-2503 | `run_dir` / `snapshot_path` setup |

## Appendix B: DocId Type Note

`DocId` is defined as `pub type DocId = u64;` (line 64). However:
- DocId is assigned as `entries.len() as DocId` (line 797) — sequential from 0
- For 1M files, all DocIds fit in u32
- The v6 snapshot already downcasts to u32 (line 1472: `(docid as u32).to_le_bytes()`)
- The v7 snapshot uses 32-bit entries

Changing `DocId` to `u32` would halve memory in: `filekey_to_docid` values, `path_hash_to_id`
values, `OneOrManyDocId`, tombstones, and trigram postings. This is a broader refactor but
worth considering as a follow-up.
