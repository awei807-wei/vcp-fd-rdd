# fd-rdd v0.6.2 — Complete Index File Size Composition Analysis

> **Analyst**: size-composition teammate
> **Date**: 2026-04-25
> **Scope**: Byte-level breakdown of v6/v7 snapshot index file structure

---

## 1. Exact Struct Sizes

### 1.1 In-Memory Rust Struct Sizes (x86_64, `#[repr(Rust)]` default layout)

#### FileKey (`src/core/rdd.rs`, line 15)

```rust
pub struct FileKey {
    pub dev: u64,
    pub ino: u64,
    pub generation: u32,
}
```

| Field | Offset | Size | Notes |
|-------|--------|------|-------|
| dev | 0 | 8 | u64, 8-byte aligned |
| ino | 8 | 8 | u64, 8-byte aligned |
| generation | 16 | 4 | u32, 4-byte aligned |
| (padding) | 20 | 4 | to align struct to 8 bytes |
| **TOTAL** | | **24B** | |

#### Span (`src/index/tiered/arena.rs`, line 11)

```rust
pub(super) struct Span {
    off: u32,
    len: u32,
}
```

| Field | Offset | Size |
|-------|--------|------|
| off | 0 | 4 |
| len | 4 | 4 |
| **TOTAL** | | **8B** |

#### DocId (`src/index/l2_partition.rs`, line 55)

```rust
pub type DocId = u64;
```

**8B** in memory. Serialized as u32 in segments (DocId ≤ file_count fits in u32).

#### CompactMeta (`src/index/l2_partition.rs`, line 187)

```rust
pub struct CompactMeta {
    pub file_key: FileKey,       // 24B
    pub root_id: u16,            // 2B
    pub path_off: u32,           // 4B
    pub path_len: u16,           // 2B
    pub size: u64,               // 8B
    pub mtime: Option<std::time::SystemTime>,  // 16B (niche-optimized)
}
```

| Field | Offset | Size | Notes |
|-------|--------|------|-------|
| file_key | 0 | 24 | FileKey struct, 8-byte aligned |
| root_id | 24 | 2 | u16 |
| path_off | 26 | 4 | u32, 4-byte aligned at offset 26 |
| path_len | 30 | 2 | u16 |
| size | 32 | 8 | u64, 8-byte aligned at offset 32 |
| mtime | 40 | 16 | Option\<SystemTime\>, niche-optimized (None = invalid nanosecond) |
| **TOTAL** | | **56B** | |

Note: `std::time::SystemTime` on Linux wraps a `Duration`-like structure (i64 seconds + u32 nanoseconds = 12B, padded to 16B). The `Option<SystemTime>` uses niche optimization — a sentinel value (e.g., negative nanoseconds) encodes `None`, so it remains 16B total.

#### FileKeyEntry (`src/core/rdd.rs`, line 97)

```rust
pub struct FileKeyEntry {
    pub key: FileKey,   // 24B
    pub doc_id: u64,    // 8B
}
```

| Field | Offset | Size |
|-------|--------|------|
| key | 0 | 24 |
| doc_id | 24 | 8 |
| **TOTAL** | | **32B** |

---

### 1.2 On-Disk Serialized Sizes (V6/V7 Segment Binary Format)

All multi-byte integers are little-endian.

#### MetaRecordV6 (serialized in `export_segments_v6`, line 1194-1208 of `l2_partition.rs`)

```
Byte Offset  Size  Field             Type    Notes
-----------  ----  -----             ----    -----
 0            8    dev               u64     Device ID
 8            8    ino               u64     Inode number
16            2    root_id           u16     Index into Roots array
18            4    path_off          u32     Byte offset into PathArena segment
22            2    path_len          u16     Byte length in PathArena
24            8    size              u64     File size in bytes
32            8    mtime_unix_ns     i64     Unix epoch nanoseconds (-1 = None)
─────────────────────────────────────────────────────
TOTAL: 40 bytes per record
```

Key optimization: mtime is stored as a single i64 (8 bytes) instead of the in-memory `Option<SystemTime>` (~16 bytes). The sentinel value `-1` encodes `None`.

On-disk size per file for metadata: **40B** vs in-memory **56B** — a 28.6% space reduction.

#### TrigramEntryV6 (serialized in `export_segments_v6`, line 1260-1266)

```
Byte Offset  Size  Field             Type    Notes
-----------  ----  -----             ----    -----
 0            3    trigram           [u8;3]  The 3-byte trigram key
 3            1    pad               u8      Always 0 (alignment)
 4            4    posting_off       u32     Byte offset into PostingsBlob segment
 8            4    posting_len       u32     Byte length of the serialized posting
─────────────────────────────────────────────────────
TOTAL: 12 bytes per entry
```

Entries are sorted by trigram bytes (memcmp order) to enable binary search at query time.

#### FileKeyMap Entry — Legacy Format (serialized in `export_segments_v6`, line 1314-1320)

```
Segment Header (8 bytes):
Byte Offset  Size  Field             Type    Notes
-----------  ----  -----             ----    -----
 0            4    magic             u32     = 0x464B4D00 ("FKM\0" little-endian)
 4            2    version           u16     = 1
 6            2    flags             u16     = 0 (legacy) or 1 (rkyv)

Legacy Record (24 bytes each):
Byte Offset  Size  Field             Type
-----------  ----  -----             ----
 0            8    dev               u64
 8            8    ino               u64
16            4    generation        u32
20            4    docid             u32 (DocId truncated from u64)
─────────────────────────────────────────────────────
TOTAL HEADER: 8 bytes
TOTAL PER RECORD: 24 bytes
```

Entries sorted by `(dev, ino, generation)` for binary search.

#### Roots Segment

```
Segment Header:
Byte Offset  Size  Field             Type
-----------  ----  -----             ----
 0            2    count             u16     Number of roots

Per Root Entry:
Byte Offset  Size  Field             Type
-----------  ----  -----             ----
 0            2    len               u16     Length of following path bytes
 2            n    path_bytes        [u8]    Root path in OS-encoded bytes
```

---

## 2. V6/V7 File Layout — Complete Segment Structure

The file format is defined in `src/storage/snapshot.rs` (`write_atomic_v6`, line 873).

### 2.1 Overall File Layout

```
┌─────────────────────────────────────────────────────────────┐
│ FILE HEADER (20 bytes)                                       │
│  Magic:     u32 = 0xFDDD_0002                                │
│  Version:   u32 = 6 (SimpleChecksum) or 7 (CRC32C)           │
│  State:     u32 = 0x00000001 (COMMITTED) or 0xFFFFFFFF       │
│                   (INCOMPLETE)                                │
│  ManifestLen: u32 = byte length of manifest that follows     │
│  ManifestChecksum: u32 = CRC32C (v7) or SimpleChecksum (v6)  │
├─────────────────────────────────────────────────────────────┤
│ MANIFEST (16 + 7 × 32 = 240 bytes)                           │
│  Magic:       u32 = 0x5646_444D ("VFDM" little-endian)       │
│  Version:     u32 = 1                                        │
│  SegCount:    u32 = 7                                        │
│  Reserved:    u32 = 0                                        │
│  For each of 7 segments (32 bytes each):                     │
│    Kind:      u32 = 1..7 (Roots..FileKeyMap)                 │
│    Version:   u32                                            │
│    Offset:    u64 (absolute byte offset in file)              │
│    Len:       u64 (segment data length)                      │
│    Checksum:  u32 (CRC32C or SimpleChecksum)                 │
│    Reserved:  u32                                            │
├─────────────────────────────────────────────────────────────┤
│ [padding to 8-byte boundary after manifest]                  │
├─────────────────────────────────────────────────────────────┤
│ SEGMENT 1: RootsSegment        (V6SegKind::Roots = 1)        │
│ SEGMENT 2: PathArena           (V6SegKind::PathArena = 2)    │
│ SEGMENT 3: Metas               (V6SegKind::Metas = 3)        │
│ SEGMENT 4: TrigramTable        (V6SegKind::TrigramTable = 4) │
│ SEGMENT 5: PostingsBlob        (V6SegKind::PostingsBlob = 5) │
│ SEGMENT 6: Tombstones          (V6SegKind::Tombstones = 6)   │
│ SEGMENT 7: FileKeyMap          (V6SegKind::FileKeyMap = 7)   │
│ [each segment followed by padding to 8-byte alignment]       │
└─────────────────────────────────────────────────────────────┘
```

### 2.2 Atomic Write Protocol

From `write_atomic_v6` (line 949-1017 of `snapshot.rs`):

1. **Create temp file** (`index.db.tmp`)
2. **Write INCOMPLETE header**: manifest_len=0, checksum=0
3. **Write manifest** with real segment descriptors
4. **Pad to 8-byte alignment** after manifest
5. **Write segments** sequentially, each aligned to 8 bytes
6. **Seek to byte 0**, overwrite header with **COMMITTED** state and real manifest_len/checksum
7. **fsync** the temp file
8. **Atomic rename** `index.db.tmp` → `index.db` (POSIX guarantees atomicity)
9. **fsync the directory** to persist the rename

**Crash safety**:
- Crash before step 6 → header says INCOMPLETE → load rejects
- Crash before step 8 → old file untouched → load uses old snapshot
- Crash before step 9 → rename lost but old file intact → load uses old snapshot

### 2.3 Load Protocol

From `load_v6_mmap_if_valid` (line 455-715 of `snapshot.rs`):

1. **Read header** (20B sync read — avoids mmap page faults at startup)
2. Verify magic == 0xFDDD_0002, version ∈ {6,7}, state == COMMITTED
3. **Read manifest** (streaming, based on manifest_len from header)
4. **Verify manifest checksum** (CRC32C for v7, SimpleChecksum for v6)
5. **Parse segment descriptors** from manifest
6. **For each segment, stream-read and verify checksum**: uses 64KB buffer to avoid touching mmap pages
7. **Verify Roots segment** matches expected roots
8. **Only after ALL checks pass**: mmap the file with `MAP_PRIVATE` (copy-on-read)

This means a single flipped byte in any segment → checksum mismatch → entire snapshot rejected.

---

## 3. Segment-by-Segment Encoding Details

### 3.1 RootsSegment

```
Encoding: u16 count + Σ[u16 len + path_bytes]

Example (2 roots: "/" and "/home/user"):
  2B: count = 2
  2B: len = 1,  1B: "/"
  2B: len = 9,  9B: "/home/user"
  TOTAL: 2 + 3 + 11 = 16 bytes
```

Root 0 is always forced to be `/` by `normalize_roots_with_fallback` (line 1908 of `l2_partition.rs`). Roots are sorted and deduplicated.

### 3.2 PathArena — Complete Encoding

The PathArena segment is a raw byte blob: all root-relative path bytes concatenated contiguously with:
- **NO separators** between paths
- **NO null terminators**
- **NO alignment padding** between paths
- **Root-relative encoding**: the root prefix is stripped

```
PathArena bytes layout:
┌──────────────┬──────────────┬──────────────┬─────┬──────────────┐
│ path₁_bytes  │ path₂_bytes  │ path₃_bytes  │ ... │ pathₙ_bytes  │
└──────────────┴──────────────┴──────────────┴─────┴──────────────┘
 ↑off=0         ↑off=L₁        ↑off=L₁+L₂             ↑off=ΣL₁..ₙ₋₁
   len=L₁         len=L₂         len=L₃                   len=Lₙ
```

Each path is referenced by `(path_off: u32, path_len: u16)` stored in MetaRecordV6.

**Path reconstruction at query time** (from `mmap_index.rs`):
```rust
let rel = &path_arena[path_off as usize..(path_off as usize + path_len as usize)];
let abs = roots[root_id] + rel;  // compose_abs_path_buf
```

**Root-relative example**: If root 1 is `/home/user/` and the file is `/home/user/Documents/report.txt`:
- `root_id = 1`
- PathArena stores: `Documents/report.txt` (16 bytes)
- At query time: `roots[1]` (= `/home/user/`) + `Documents/report.txt`

**Maximum path length**: `push_bytes` (line 137 of `l2_partition.rs`) limits paths to `u16::MAX` (65535) bytes. Exceeding paths are silently skipped with a warning log.

**Per-path overhead in PathArena**: **0 bytes**. All overhead lives in the MetaRecordV6 (`path_off: u32` + `path_len: u16` = 6 bytes).

**Path deduplication**: Not performed on disk. The in-memory `PathArenaSet` (`arena.rs`) uses hash-based dedup within a flush cycle, but the serialized arena may contain duplicates for files that happen to share identical root-relative paths.

### 3.3 Metas (MetadataSegment)

```
Encoding: Array of fixed-size MetaRecordV6 records
Record count: metas_bytes.len() / 40
Format: [MetaRecordV6; N]  (contiguous, no gaps)

Record layout (40 bytes, little-endian):
  bytes  0- 7: dev: u64
  bytes  8-15: ino: u64
  bytes 16-17: root_id: u16
  bytes 18-21: path_off: u32
  bytes 22-23: path_len: u16
  bytes 24-31: size: u64
  bytes 32-39: mtime_unix_ns: i64  (-1 = None)

DocId = array index (implicit — no separate DocId field needed).
```

**Key optimization**: DocId IS the array index, so no per-record DocId field is needed. This saves 4-8 bytes per record compared to storing DocId explicitly.

### 3.4 TrigramTable

```
Encoding: Sorted array of fixed-size TrigramEntryV6 records
Sort key: trigram bytes in memcmp order (big-endian interpretation)
Purpose: Binary search at query time to find posting list offset

Record layout (12 bytes, little-endian):
  bytes 0-2:  trigram: [u8; 3]    ← sorted by these 3 bytes
  byte 3:     pad: u8 = 0
  bytes 4-7:  posting_off: u32
  bytes 8-11: posting_len: u32

Query flow: binary search → read (off, len) → go to PostingsBlob[off..off+len] → deserialize RoaringBitmap
```

**Sentinel trigram**: A special entry `[0, 0, 0]` with an empty posting list is always appended if not already present (line 1245-1257 of `l2_partition.rs`). This sentinel:
- Does NOT match any real path (paths cannot contain NUL bytes)
- Signals to `MmapIndex` that this segment was built with **full-component trigrams** (not just basename-only)
- Without the sentinel, `MmapIndex` falls back to full-scan to avoid false negatives (line 558-564 of `mmap_index.rs`)

### 3.5 PostingsBlob

```
Encoding: Concatenated RoaringBitmap serialized blobs
Format: blob₁ || blob₂ || ... || blobₙ
Each blob = RoaringBitmap::serialize_into output (portable format)

Referenced from TrigramTable via (posting_off, posting_len).
```

### 3.6 Tombstones

```
Encoding: Single RoaringBitmap (portable serialized format)
Stores: Set of DocIds (as u32) that are marked deleted
Empty tombstone: 4B cookie + 4B count(0) = 8 bytes
```

Read lazily from mmap into `tomb_cache` (Mutex<Option<RoaringTreemap>>) on first access.

### 3.7 FileKeyMap

```
Encoding: Header + sorted array

Legacy format (flags = 0):
  Header:  magic(4) + version(2) + flags(2) = 8 bytes
  Records: [dev(8) + ino(8) + generation(4) + docid(4)] × N
  Record size: 24 bytes
  Sort key: (dev, ino, generation) ascending
  Search: Binary search at query time

Rkyv format (flags = 1, feature-gated):
  Header: 8 bytes
  Payload: rkyv::to_bytes of Vec<FileKeyEntry>
```

Used by `MmapIndex::get_meta_by_key()` — the `FileKey → DocId` reverse lookup. Without this segment, `MmapIndex` builds a fallback cache on first access by scanning all metas (O(N) one-time cost).

---

## 4. RoaringBitmap Serialization — Complete Details

### 4.1 Format Specification

Each posting list (set of DocIds for a given trigram) is serialized using `roaring::RoaringBitmap::serialize_into` in the "portable" format:

```
┌──────────────────────────────────────────────────────┐
│ cookie: u32 (4 bytes)                                 │
│   = 12347 for portable format with run containers     │
├──────────────────────────────────────────────────────┤
│ size: u32 (4 bytes) = number of containers            │
│   = 0 for empty bitmap (total 8 bytes)                │
├──────────────────────────────────────────────────────┤
│ For each container:                                   │
│   key: u16 (2 bytes)                                  │
│     = high 16 bits of the values in this container    │
│   cardinality_plus_1: u16 (2 bytes)                   │
│     Determines container type:                        │
│                                                        │
│     If 1 ≤ cardinality_plus_1 ≤ 4097:                 │
│       → Array container                               │
│       cardinality = cardinality_plus_1 - 1            │
│       data: [u16; cardinality] (sorted ascending)     │
│       Total container size: 4 + 2×cardinality         │
│                                                        │
│     If cardinality_plus_1 == 0:                       │
│       → Run container                                 │
│       run_count: u16 (2 bytes)                        │
│       runs: [start: u16, length: u16; run_count]      │
│       Total: 6 + 4×run_count                          │
│                                                        │
│     If cardinality_plus_1 > 4097:                     │
│       → Bitmap container                              │
│       cardinality = cardinality_plus_1 - 4097 - 1     │
│       bitmap: [u64; 1024] = 8192 bytes                │
│       Total: 4 + 8192 = 8196 bytes (fixed)            │
└──────────────────────────────────────────────────────┘
```

### 4.2 Container Size Analysis

| Container Type | Overhead | Per-Entry | Total for C entries | Use Case |
|---------------|----------|-----------|---------------------|----------|
| Array | 4B (key+card) | 2B/u16 | 4 + 2C | C ≤ 4096 (sparse) |
| Bitmap | 4B | 8192B flat | 8196B always | C > 4096 (dense) |
| Run | 6B + 2B | 4B/run | 6 + 4R | Sequential DocIds |

### 4.3 Per-Trigram Posting Size Estimates (DocId range 0..100K)

| Files per Trigram | DocId Count | Container | Approx Size |
|-------------------|-------------|-----------|-------------|
| 1–10 | 1–10 | Array | 6–24B |
| 10–50 | 10–50 | Array | 24–104B |
| 50–200 | 50–200 | Array | 104–404B |
| 200–500 | 200–500 | Array | 404–1,004B |
| 500–2,000 | 500–2,000 | Array | 1KB–4KB |
| 2,000–4,095 | 2,000–4,095 | Array | 4KB–8KB |
| 4,096–8,192 | 4,096–8,192 | Bitmap | **8,196B (fixed)** |
| 8,193–65,536 | 8,193–65,536 | Bitmap | **8,196B (fixed)** |

**Key insight**: The step function at 4096 → 8192B means a trigram with 4,095 files costs ~8KB (array) while one with 4,096 files also costs ~8KB (bitmap). The bitmap remains 8,196B regardless of whether it holds 4,096 or 100,000 DocIds. At 100,000 DocIds packed into one bitmap container, the per-DocId cost drops to ~0.08 bytes — but only if the DocIds fall within a single 16-bit key range.

### 4.4 Tombstone Bitmap Cost

| Tombstone Count | Container Type | Size |
|-----------------|---------------|------|
| 0 | Empty | 8B |
| 1–4,096 (sparse) | Array(s) | 8B + ~2B/tombstone |
| 4,097+ (dense) | Bitmap(s) | ~8KB+ |
| 100K (all) | ~1-2 Bitmaps | ~8-16KB |

---

## 5. Quantitative Breakdown for 100K Files

### 5.1 Assumptions

| Parameter | Value | Justification |
|-----------|-------|---------------|
| File count (N) | 100,000 | Target benchmark |
| Tombstones | 0 | Clean index, no deletions |
| Average root-relative path length | 60 bytes | Typical Linux paths like `usr/lib/python3/dist-packages/numpy/core/multiarray.so` |
| Distinct trigrams (D) | 15,000 | Zipfian over path component trigrams; Linux desktop vocabulary saturation |
| Avg trigrams per file | ~36 | Per-component enumeration; avg 5 normal components, ~12 chars each → 5×10 = 50 trigrams, minus intra-file overlap → ~36 distinct per file |
| Total trigram posting entries | 3,600,000 | N × 36 |
| Avg posting list size | 240 DocIds | 3,600,000 / 15,000 = 240 |
| Avg posting serialized size | 484B | Array container: 4 + 2×240 = 484B |
| Roots | 2 | "/" (forced) + "/home/user" |
| Root path bytes | 10 | 1 + 9 |

### 5.2 Per-Segment Calculation

#### HEADER
```
magic(4) + version(4) + state(4) + manifest_len(4) + manifest_checksum(4)
= 20 bytes
```

#### MANIFEST
```
Header: magic(4) + version(4) + seg_count(4) + reserved(4) = 16
Body: 7 segments × 32 bytes/descriptor = 224
Total: 16 + 224 = 240 bytes
```

#### ROOTS SEGMENT
```
count: u16(2) = 2
Root 0: len(2) + "/"(1) = 3
Root 1: len(2) + "/home/user"(9) = 11
Total: 2 + 3 + 11 = 16 bytes
```

#### PATH ARENA
```
N × avg_path_len = 100,000 × 60 = 6,000,000 bytes = 6.00 MB
```

#### METAS
```
N × 40 = 100,000 × 40 = 4,000,000 bytes = 4.00 MB
```

#### TRIGRAM TABLE
```
D × 12 = 15,000 × 12 = 180,000 bytes = 0.18 MB
(includes sentinel trigram [0,0,0] — 1 additional entry already counted)
```

#### POSTINGS BLOB
```
D × avg_posting_size = 15,000 × 484 = 7,260,000 bytes ≈ 7.20 MB
(using 480B rounded avg to be conservative)
```

#### TOMBSTONES
```
Empty RoaringBitmap: cookie(4) + count(0)(4) = 8 bytes
```

#### FILE KEY MAP (legacy format)
```
Header: 8 bytes
Records: N × 24 = 100,000 × 24 = 2,400,000 bytes
Total: 2,400,008 bytes = 2.40 MB
```

#### SEGMENT ALIGNMENT PADDING
```
Each of 7 segments padded to 8-byte boundary: ≤7 × 7 = at most 49 bytes
```

### 5.3 Complete Size Table

| # | Segment | Exact Size (bytes) | Size (MB) | % of Total |
|---|---------|-------------------|-----------|------------|
| — | File Header | 20 | 0.00002 | <0.01% |
| — | Manifest | 240 | 0.00023 | <0.01% |
| 1 | RootsSegment | 16 | 0.00002 | <0.01% |
| 2 | PathArena | 6,000,000 | 6.00 | **29.42%** |
| 3 | Metas | 4,000,000 | 4.00 | **19.61%** |
| 4 | TrigramTable | 180,000 | 0.18 | **0.88%** |
| 5 | PostingsBlob | 7,200,000 | 7.20 | **35.30%** |
| 6 | Tombstones | 8 | 0.00001 | <0.01% |
| 7 | FileKeyMap | 2,400,008 | 2.40 | **11.76%** |
| — | Alignment Padding | ≤49 | <0.00005 | <0.01% |
| — | **TOTAL** | **~19,780,341** | **~19.78** | **100%** |

### 5.4 Visual Breakdown

```
PostingsBlob   ████████████████████████████████████ 35.3%   7.20 MB  (RoaringBitmap trigram postings)
PathArena      ██████████████████████████████       29.4%   6.00 MB  (root-relative path bytes)
Metas          ████████████████████                 19.6%   4.00 MB  (40B fixed metadata per file)
FileKeyMap     ████████████                         11.8%   2.40 MB  (dev,ino→DocId sorted map)
TrigramTable   █                                     0.9%   0.18 MB  (12B directory entries)
Overhead       ░                                     <0.1%   <0.01 MB (headers, manifest, padding)
```

### 5.5 Per-File On-Disk Cost

| Component | Bytes per File | Cumulative | Type |
|-----------|----------------|------------|------|
| PathArena | 60.0 | 60.0 | Variable (path-dependent) |
| PostingsBlob | 72.0 | 132.0 | Variable (amortized across trigrams) |
| Metas | 40.0 | 172.0 | Fixed |
| FileKeyMap | 24.0 | 196.0 | Fixed |
| TrigramTable | 1.8 | 197.8 | Fixed (amortized) |
| Roots + Overhead | 0.003 | 197.8 | Fixed (amortized) |
| **TOTAL per file** | **~198 bytes** | | |

**Fixed overhead**: 64 bytes/file (Metas + FileKeyMap) — regardless of path length.
**Variable overhead**: ~132 bytes/file (PathArena + Postings) — scales with path length and complexity.

---

## 6. Growth Characteristics — Detailed Analysis

### 6.1 Linear Scaling O(n) Components

| Component | Formula | Per-File Growth | Source |
|-----------|---------|-----------------|--------|
| PathArena | N × avg_path_len | ~60B/file | `push_bytes` in `PathArena` |
| Metas | N × 40 | 40B/file | `export_segments_v6` fixed-size loop |
| FileKeyMap | 8 + N × 24 | 24B/file | `export_segments_v6` sorted array |

### 6.2 Sub-Linear Components

| Component | Formula | Growth Pattern |
|-----------|---------|----------------|
| TrigramTable | D(N) × 12 | D(N) saturates. After ~10K files, 90%+ of encountered trigrams are already in the vocabulary. Max practical D ≈ 50K for any Linux system. |
| Roots | O(roots) | Constant per scanning config (typically 1–5). |
| Header | O(1) | Fixed 20B. |
| Manifest | O(1) | Fixed 240B (7 segments). |

### 6.3 PostingsBlob — O(n) but Sensitive to Clustering

```
Total posting entries = N × avg_trigrams_per_file
Total posting bytes   = Σ(roaring_serialized_size for each D)

As N grows:
  - New files → new trigram entries in existing postings → +2B/DocId (array)
  - When a posting hits 4,096 DocIds → container upgrade to bitmap → +~4KB jump
  - Path clustering (many files in same dirs) → common trigrams become dense
  - Worst case: D is small (~100 common trigrams), N is large → ~100 × 8KB = 0.8MB flat
```

### 6.4 Effect of Average Path Length

Each additional byte of average path length adds:

| Impact | Amount | Mechanism |
|--------|--------|-----------|
| PathArena growth | +1 byte | Raw byte appended to arena |
| Trigram count growth | +~0.6 trigrams | Each new character in a component creates ~1 new window of length 3 |
| PostingsBlob growth | +~1.2 bytes | 0.6 trigrams × 2B/DocId (array container) |
| **Net per byte per file** | **+~2.2 bytes** | |

**Index size at different path lengths (100K files):**

| Avg Path Length | PathArena (MB) | PostingsBlob (MB) | Total Index (MB) | Per-File (B) |
|-----------------|----------------|-------------------|------------------|--------------|
| 20B (short names) | 2.0 | 4.8 | 13.9 | ~139B |
| 40B | 4.0 | 6.0 | 16.9 | ~169B |
| 60B (typical) | 6.0 | 7.2 | 19.8 | ~198B |
| 80B | 8.0 | 8.4 | 22.7 | ~227B |
| 120B (deep trees) | 12.0 | 11.0 | 29.5 | ~295B |

### 6.5 Effect of Tombstones (Before Compaction)

| Tombstone % | Tombstone Segment | Metas | PostingsBlob | Notes |
|-------------|-------------------|-------|-------------|-------|
| 0% (clean) | 8B | N×40B | full size | Baseline |
| 5% (5K) | ~10KB | N×40B | full size | Sparse tombstones, metas unchanged |
| 20% (20K) | ~40KB | N×40B | full size | Tombstoned DocIds remain in postings |
| 50% (50K) | ~10KB | N×40B | full size | Dense bitmap, but no space saved |

**Compaction** (`export_segments_v6_compacted`, line 1163 of `l2_partition.rs`):

Performs true tombstone GC by rebuilding the segment set from only live metas:

- Metas: saves 40B per tombstoned file
- PostingsBlob: regenerated without tombstoned DocIds
- TrigramTable: regenerated (removes entries with now-empty postings)
- FileKeyMap: tombstoned keys already excluded at mark time; no additional savings here
- After compaction: index size reflects only live file count

For 50% tombstone rate: compaction saves ~2MB of metas + ~3.6MB of postings = ~5.6MB (28% reduction).

### 6.6 Effect of rkyv Feature on FileKeyMap

| Format | Header | Per Entry | 100K Files |
|--------|--------|-----------|------------|
| Legacy (default) | 8B | 24B | 2.40 MB |
| Rkyv | 8B | ~28B (archived) | ~2.80 MB |

The legacy format is more space-efficient for this use case. The rkyv format adds overhead for schema flexibility (versioning, validation metadata).

---

## 7. Checksum and Integrity

### 7.1 Algorithm Comparison

| Version | Algorithm | Speed | Collision Resistance |
|---------|-----------|-------|---------------------|
| v6 | SimpleChecksum (byte sum with rolling multiply) | Fast | Low (simple sum-based) |
| v7 | CRC32C (Castagnoli, hardware-accelerated via SSE4.2) | Very fast | Good (standard CRC) |

### 7.2 What Is Checksummed

- **Manifest**: checksum stored in file header (manifest_checksum field)
- **Each segment**: checksum stored in manifest descriptor
- **File header itself**: NOT separately checksummed (magic + version + state are verified independently)

### 7.3 Checksum Computation

Writing (always v7): `Crc32c::new()` + `c.update(bytes)` + `c.finalize()` — uses the `crc32c` crate with hardware acceleration.

Reading (v6/v7 compatible): 
- If version == 7: `Crc32c`
- If version == 6: `simple_checksum` (from `crate::storage::checksum`)

### 7.4 Verification Strategy at Load

The load path (`load_v6_mmap_if_valid`) intentionally separates checksum verification from mmap:

1. **Header read**: 20B sync read (not mmap)
2. **Manifest read + verify**: streaming read + checksum
3. **Per-segment verify**: streaming reads with 64KB buffer
   - Exception: Roots segment is small (≤1MB cap), fully read into memory for both verify + later comparison
4. **Roots comparison**: loaded roots bytes must match expected roots exactly
5. **Mmap**: only after ALL checks pass, locked under `Arc`

This avoids faulting the entire index file into process RSS at startup (pages touched by streaming reads go to page cache, not process private memory).

---

## 8. In-Memory vs On-Disk Size Comparison

| Data Structure | In-Memory (per entry) | On-Disk (per entry) | Compression |
|----------------|----------------------|---------------------|-------------|
| FileKey | 24B (struct) | 20B (3 fields) | 16.7% |
| FileMeta/MetaRecord | 56B (CompactMeta) | 40B (MetaRecordV6) | 28.6% |
| DocId → posting | 8B (u64) | 2B (array container u16) | 75% (sparse) |
| Trigram entry | ~32B (HashMap entry) | 12B (sorted array) | 62.5% |
| Path data | PathArena Arc<Vec<u8>> same bytes | Same bytes | 0% |
| FileKey→DocId | HashMap entry ~48B | 24B (sorted array) | 50% |

Overall, the on-disk format achieves ~40-60% space reduction vs in-memory representation through:
- Fixed-size records instead of heap-allocated structs
- Collapsing DocId from u64 to u32
- Collapsing mtime from 16B (Option<SystemTime>) to 8B (i64)
- Sorted arrays instead of hash maps
- RoaringBitmap compression for posting lists
- Root-relative path encoding (strips root prefix)

---

## 9. Summary of Key Metrics

| Metric | Value |
|--------|-------|
| Bytes per file on disk | **~198 B** |
| Dominant component | PostingsBlob (35.3%, 7.20 MB) |
| Second dominant | PathArena (29.4%, 6.00 MB) |
| Fixed per-file cost (Metas + FileKeyMap) | 64 B |
| Variable per-file cost (Paths + Postings) | ~132 B |
| Empty tombstone overhead | 8 B |
| Trigram table overhead (amortized) | 1.8 B/file |
| File header + manifest | 260 B (constant, negligible at scale) |
| Path encoding overhead in arena | 0 B (overhead lives in MetaRecordV6: 6B) |
| MetaRecordV6 per record | 40 B |
| TrigramEntryV6 per entry | 12 B |
| FileKeyMap per entry (legacy) | 24 B |
| Roaring posting cost (sparse, array container) | ~2 B/DocId |
| Roaring posting cost (dense, bitmap container) | 8196 B flat per container |
| Container type threshold | 4096 DocIds (array → bitmap) |
| Max path length | 65535 bytes (u16) |
| Alignment waste per segment | ≤7 bytes |
| Total for 100K files | **~19.8 MB** |
| Per-byte-of-path cost increase | +2.2 bytes total per file |

---

## 10. Key Source References

| File | Lines | Content |
|------|-------|---------|
| `src/core/rdd.rs` | 15-20 | `FileKey` struct definition |
| `src/core/rdd.rs` | 97-100 | `FileKeyEntry` struct definition |
| `src/core/rdd.rs` | 104-115 | `FileMeta` struct definition |
| `src/index/l2_partition.rs` | 55 | `DocId = u64` type alias |
| `src/index/l2_partition.rs` | 105-170 | `PathArena` struct and methods |
| `src/index/l2_partition.rs` | 187-194 | `CompactMeta` struct definition |
| `src/index/l2_partition.rs` | 374 | `PersistentIndex` struct definition |
| `src/index/l2_partition.rs` | 1169-1332 | `export_segments_v6()` — segment encoding |
| `src/index/l2_partition.rs` | 1184-1208 | MetaRecordV6 serialization (40B) |
| `src/index/l2_partition.rs` | 1218-1266 | TrigramTable + PostingsBlob encoding |
| `src/index/l2_partition.rs` | 1268-1331 | FileKeyMap encoding |
| `src/index/mmap_index.rs` | 19-26 | `META_REC_SIZE`(40), `TRI_REC_SIZE`(12), `FILEKEY_MAP_REC_SIZE`(24) |
| `src/index/mmap_index.rs` | 119-151 | `meta_at()` — MetaRecordV6 decoding |
| `src/index/mmap_index.rs` | 387-425 | `posting_for_trigram()` — binary search + Roaring deserialize |
| `src/storage/snapshot.rs` | 16-26 | Magic, version, header size constants |
| `src/storage/snapshot.rs` | 204-228 | `V6SegDesc`, manifest layout |
| `src/storage/snapshot.rs` | 873-1017 | `write_atomic_v6()` — complete write protocol |
| `src/storage/snapshot.rs` | 455-715 | `load_v6_mmap_if_valid()` — complete load protocol |
| `src/index/tiered/arena.rs` | 11-14 | `Span` struct (in-memory path reference) |
| `src/index/tiered/arena.rs` | 49-58 | `PathArenaSet` struct (in-memory path dedup) |
| `src/stats/mod.rs` | 5-38 | `MemoryReport` struct |
| `src/stats/mod.rs` | 80-118 | `L2Stats` struct |
