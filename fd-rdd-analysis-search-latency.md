# fd-rdd v0.6.2 Search Query Latency — Complete Performance Assessment

## 1. QUERY EXECUTION PIPELINE — Stage-by-Stage Cost Analysis

The end-to-end query pipeline (TieredIndex::query_limit → results) executes these stages:

### Stage 1: DSL Parsing (`src/query/dsl.rs`)
- **Big-O**: O(L) tokenization + O(T) parsing where L=input length, T=token count
- **Expected cost**: 1–5 µs
- **Details**: Single-pass byte scan (`tokenize` function, line 793) iterates input bytes once, splitting on whitespace/|/! with quoted-string support. Parser builds an OR/AND AST via recursive descent (`Parser::parse_query`, line 538). Smart-case detection scans input chars once for uppercase (`compile_query`, line 260). Path-initials query detection scans for separators (line 225). Anchor selection (`select_anchors`, line 368) picks the best matcher per OR branch.
- **Key code paths**: `tokenize()` line 793, `Parser::parse_query()` line 538, `compile_query()` line 247

### Stage 2: Query Compilation (`src/query/dsl.rs`)
- **Big-O**: O(T + B·A) where B=branches, A=atoms per branch
- **Expected cost**: 1–5 µs
- **Details**: `compile_expr` (line 291) recursively converts the AST to `CompiledExpr`. For each Text atom, `create_matcher` (matcher.rs:294) decides between ExactMatcher (no wildcards), GlobMatcher (*/? present), WfnMatcher (wfn: prefix), RegexMatcher (regex: prefix), ExtMatcher, or PathInitialsMatcher. `best_anchor_in_branch` (line 392) scores anchors by `literal_hint().len()` + `prefix().len()/2`.
- **Key code paths**: `compile_expr()` line 291, `create_matcher()` matcher.rs:294

### Stage 3: QueryPlan Construction (`src/index/tiered/query_plan.rs`)
- **Big-O**: O(1)
- **Expected cost**: ~0.1 µs
- **Details**: Wraps the `CompiledQuery` struct, extracts anchor matchers. In legacy fallback path (when DSL compile fails), wraps a single Arc<dyn Matcher>.
- **Key code paths**: `QueryPlan::compiled()` line 18, `QueryPlan::legacy()` line 25

### Stage 4: L1 Cache Probe — Only on legacy fallback path (`src/index/l1_cache.rs`)
- **Big-O**: O(C) full scan where C = cache capacity (typically 1000)
- **Expected cost**: 5–50 µs for C=1000
- **Details**: `L1Cache::query` (line 144) iterates ALL entries in the HashMap. For each entry: optional prefix pre-filter (based on glob_mode and prefix()), then full matcher.matches() call. LRU touch on hits. This is a FULL SCAN of the cache — no hash-based lookup, no index. Only triggered when `compile_query` fails (legacy fallback in tiered/query.rs line 26-40).
- **Key code paths**: `L1Cache::query()` l1_cache.rs:144

### Stage 5a: Trigram Candidate Retrieval — L2 (`src/index/l2_partition.rs`)
- **Big-O**: O(H) hashmap lookups + O(P_min · H/64) bitmap intersection
- **Expected cost**: 0.5–2 µs (warm)
- **Details**: `trigram_candidates` (line 1772):
  1. Extract `literal_hint()` from matcher — returns `&[u8]` pattern bytes (or None if pattern contains path separator)
  2. Generate trigrams from hint: `query_trigrams(s)` (line 58) — sliding windows of 3 bytes on lowercased hint
  3. For each trigram, HashMap lookup in `trigram_index: HashMap<[u8;3], RoaringTreemap>` — O(1)
  4. Sort trigrams by posting size (ascending) — minimizes intersection work
  5. Intersect all RoaringTreemaps: `acc &= posting` iteratively; early return if accumulator becomes empty

- **RoaringBitmap intersection characteristics**:
  - Array containers: O(min(|A|,|B|)) two-pointer merge
  - Bitmap containers: bitwise AND of 1024 words
  - Run containers: O(num_runs) merge
  - For 100K files with ~40K distinct trigrams: average posting ~325 docs → intersection of 4 trigrams ≈ 325 bitmap words total → sub-microsecond

- **Key code paths**: `trigram_candidates()` l2_partition.rs:1772, `query_trigrams()` l2_partition.rs:58

### Stage 5b: Trigram Candidate Retrieval — Mmap (`src/index/mmap_index.rs`)
- **Big-O**: O(H · log T) binary search + O(P_min · H/64) intersection
- **Expected cost**: 1–10 µs (warm page cache), 50–500 µs (cold SSD), 5–50 ms (cold HDD)
- **Details**: `trigram_candidates` (line 470):
  1. Same hint extraction and trigram generation
  2. For each trigram, binary search on sorted `trigram_table` (mmap'd, 12B fixed records, sorted by trigram bytes) — `posting_for_trigram` (line 387) does O(log T) binary search
  3. Each posting is lazily decoded: `RoaringBitmap::deserialize_from` on slice from `postings_blob` — deserialized on demand, not pre-loaded
  4. Bitmaps sorted by length, intersected

- **Mmap page fault impact per trigram lookup**:
  - Binary search on sorted table: ~log₂(T) random accesses per lookup → for T=50K distinct trigrams, ~16 accesses
  - Each access may fault a 4KB page (12B record → ~341 records per page)
  - Posting deserialize: reading serialized bits from postings_blob → 1–several page faults
  - Cold SSD: ~10 µs × 20 faults = 200 µs; Cold HDD: ~5 ms × 20 = 100 ms

- **Key code paths**: `trigram_candidates()` mmap_index.rs:470, `posting_for_trigram()` mmap_index.rs:387

### Stage 5c: Short-Hint Candidate Fallback (Both L2 and Mmap)
- **Big-O**: O(T) scan all distinct trigrams
- **Expected cost**: 10–100 µs (L2), 50–500 µs (mmap warm), 1–10 ms (mmap cold)
- **Details**: Triggered when `trigram_candidates` returns None (hint < 3 chars). 
  - `short_hint_candidates` (L2: line 1800, Mmap: line 527):
    - For 1-char hint: `trigram_matches_short_hint` checks if `tri.contains(&hint[0])` — UNION of ALL trigrams containing that byte
    - For 2-char hint: checks `tri[0..2] == hint` OR `tri[1..3] == hint` — UNION of trigrams starting or ending with the 2-char sequence
    - Also scans `short_component_index` for 1-2 char path components
    - Result: OR of ALL matching postings → very broad candidate set (essentially all files for 1-char)
  - For the mmap path, `short_component_cache` is lazily built on first access (line 492): O(N) full scan of all metas to build HashMap<Box<[u8]>, RoaringTreemap>

- **Key code paths**: `short_hint_candidates()` l2_partition.rs:1800, `short_hint_candidates()` mmap_index.rs:527, `short_component_cache()` mmap_index.rs:492

### Stage 5d: Full Scan Fallback (Both Layers)
- **Big-O**: O(N) — ALL files in the layer
- **Expected cost**: 10 ms to 5+ seconds depending on N
- **Details**: When both trigram_candidates AND short_hint_candidates return None (literal_hint is None — path contains separator, regex without hint, MatchAllMatcher):
  - L2: `query_keys` None branch (l2_partition.rs:1858) — iterates ALL metas, reconstructs path from arena bytes, calls matcher.matches() per file
  - Mmap: `query_keys` None branch (mmap_index.rs:595) — iterates all meta records, reconstructs path, calls matcher
  - Each iteration: O(path_len) arena slice + compose_abs_path_bytes (root prefix prepend) + UTF-8 conversion + exact matcher call ≈ 1–10 µs per file
  - 1M files: 1M × 5 µs ≈ **5 seconds** → hits 5s timeout

### Stage 6: Candidate Iteration & Layer Merging (`src/index/tiered/query.rs`)
- **Big-O**: O(K · (R + M + D)) where K=candidate count, R=path reconstruct, M=matcher cost, D=dedup cost
- **Details**: `execute_query_plan` (line 161):
  1. Acquire L2 snapshot (ArcSwap — lock-free), clone disk_layers (RwLock read), lock overlay_state (Mutex)
  2. For each anchor in QueryPlan.anchors():
     - For each layer (L2 first → disk layers newest to oldest):
       - `layer.query_keys(anchor)` — returns Vec<FileKey> of candidates
       - For each FileKey:
         - Dedup: `seen.insert(key)` in HashSet<FileKey> — O(1) amortized
         - `layer.get_meta(key)` — retrieves FileMeta from layer
         - Deletion check: PathArenaSet (trie) + layer_deleted + deleted_sources — O(log path_len)
         - Block path: `blocked_paths.insert(path_bytes)` — prevents duplicates across layers
         - `plan.matches(&meta)` — calls CompiledQuery::matches or legacy matcher
         - If passed, push to results
         - If `results.len() >= limit` → EARLY RETURN (stops traversing layers)
  3. Supplement from overlay_upserted paths (during rebuild)
  4. Supplement from pending_events (not yet applied to L2)

- **Layer order**: L2 → disk_layers[newest] → ... → disk_layers[oldest]. This ensures "newest wins" semantics — once a FileKey is seen, it's skipped in older layers.
- **Key code paths**: `execute_query_plan()` tiered/query.rs:161, `query_layer()` tiered/query.rs:292

### Stage 7: Exact Matcher (`src/query/matcher.rs`)
- **Big-O**: O(P·Q) for contains, O(P+Q) for equality, O(P·Q) for glob worst-case
- **Expected cost**: 0.5–5 µs per candidate
- **Details**:
  - **ExactMatcher::matches** (line 59): If case-sensitive, `path.contains(&pattern)` — O(P·Q). If case-insensitive, `contains_ascii_insensitive` (line 246) — naive byte-level sliding window with per-byte lowercase comparison, O((P–Q)·Q). For P=100, Q=5: ~475 byte comparisons.
  - **GlobMatcher::matches** (line 112): FullPath mode — `glob_matches` on complete path. Segment mode — tries basename first, then each path component. Backtracking glob matching (line 193): O(P·Q) worst case with * backtracking.
  - **WfnMatcher::matches** (line 332): Straight equality on basename or fullpath — O(P+Q).
  - **RegexMatcher::matches** (line 381): Regex engine — depends on pattern complexity.
  - **PathInitialsMatcher::matches** (line 470): O(Q_segs × P_segs) — for each query segment, scans forward through path segments for prefix (non-last) or prefix/substring (last) match.
  - **MatchAllMatcher::matches** (line 529): Always true — O(1).

### Stage 8: Scoring (`src/query/scoring.rs`)
- **Big-O**: O(P·Q + P + S·J) per result, where S=segments, J=junk dirs
- **Expected cost**: 10–50 µs per result
- **Details**: `score_result` (line 330):
  1. `boundary_aware_match` (line 235): Case-insensitive substring scan over full path bytes (lowercased) — O((P–Q)·Q). For each match position, accumulates boundary bonuses from pre-computed position_bonuses array.
  2. Basename match check: `basename_lower.contains(q)` — O(P·Q)
  3. Exact stem match: `name_stem == q` — O(Q)
  4. Prefix match: `basename_lower.starts_with(q)` — O(Q)
  5. Basename multiplier: ×2.5 if match in basename
  6. Perfect boundary multiplier: ×2 if match follows `.` or `/`
  7. Length penalty: `-name_len × 0.1`
  8. Recent mtime bonus: +15 if modified within 7 days
  9. Hidden dir penalty: `path_has_hidden_dir` — O(S) segment scan; skipped if query_has_dot or basename match
  10. Node zone check: `path_in_node_zone` — O(S·2) scan against NODE_ZONE_NAMES; if matched, ×0.1 multiplier (unless query_has_node)
  11. Junk dir penalty: `path_in_junk_dir` — O(S·37) scan against JUNK_DIR_NAMES; -200 if matched
  12. Depth tiebreaker: `-depth × 0.5`

### Stage 9: Sorting (`src/query/fzf.rs`)
- **Big-O**: O(R log R) for sort
- **Expected cost**: 5–50 µs for R=100, up to ~500 µs for R=10000
- **Details**: `sort_results` (line 97): Uses `results.sort_by()` with multi-level comparison. For Score column: `score_result()` per item + path comparison as tiebreaker. For other columns: direct field comparison.

### Stage 10: Result Serialization
- **Big-O**: O(R · path_len)
- **Expected cost**: 10–100 µs
- **Details**: 
  - HTTP path (`server.rs`): JSON serialization with highlights computation — `compute_highlights` (scoring.rs:440) does substring or path-initials highlight computation per result
  - Socket path (`socket.rs`): Raw path bytes with newline separators, streamed via BufWriter with flush_every=1000

---

## 2. KEY LATENCY BOTTLENECKS (Ranked by Severity)

### 🔴 CRITICAL: Short-hint / Full-scan Path

**Trigger conditions**:
- Query string < 3 chars (literal_hint returns None after trigram generation)
- Pattern containing path separators `/` or `\` (ExactMatcher at matcher.rs:74 explicitly returns None)
- Regex patterns without extractable literal hint
- MatchAllMatcher (no anchors selected)

**Mechanism**: When `literal_hint()` returns None, the query falls through to `short_hint_candidates()` which does:
- For 1-char: Scans ALL distinct trigrams (~40-60K), checks if each trigram contains that byte → UNION → essentially ALL documents
- For 2-char: Scans ALL trigrams for prefix/suffix match → UNION → still very broad
- If that also fails: FULL SCAN of all metas

**Scale impact**:

| Query type | 10K files | 100K files | 1M files |
|-----------|-----------|------------|----------|
| 1-char ("a") | 10–50 ms | 100–500 ms | **>5s TIMEOUT** |
| 2-char ("ab") | 10–50 ms | 100–500 ms | **>5s TIMEOUT** |
| Path sep ("src/")  | ~10 ms | ~100 ms | **~1–2s** |

### 🟠 HIGH: Cold Mmap Page Fault Cascade

**Where**: `src/index/mmap_index.rs` — every data access goes through mmap

**Data accessed per query**:

| Data | Size | Faults per access | Frequency |
|------|------|------------------|-----------|
| Trigram table (binary search) | 12B records | 1 fault per ~16 steps per trigram lookup | H trigrams × log₂(T) |
| Postings blob | Varied (compressed) | 1–several per trigram | H trigrams |
| Meta records | 40B fixed | 1 per ~102 records | K candidates |
| Arena (path bytes) | Varied | 1 per ~20–80 paths | K candidates |
| Tombstones blob | Compressed | 1 (lazy, once) | Once per query |
| Short component cache | Built once | 0 (after build) | Once per segment lifetime |

**Latency multipliers**:

| Storage | Per-page latency | 100-candidate query |
|---------|-----------------|-------------------|
| Warm page cache | ~100 ns | 1× (baseline, ~50 µs) |
| Cold NVMe SSD | ~10 µs | 50–200× (~2–10 ms) |
| Cold SATA SSD | ~50 µs | 500–1000× (~50–100 ms) |
| Cold HDD (7200 RPM) | ~5–10 ms | 50,000–100,000× (~500–1000 ms) |

### 🟡 MEDIUM: Path Reconstruction per Candidate

**Every candidate** requires:
1. Arena slice lookup: `arena.get_bytes(off, len)` or `arena.get(start..end)` — O(1)
2. `compose_abs_path_bytes` or `compose_abs_path_buf`: prepends root prefix bytes to relative path bytes → allocates new Vec<u8> or PathBuf
3. UTF-8 conversion: `std::str::from_utf8` or `String::from_utf8_lossy`
4. Matcher call: operates on the reconstructed string

**Per-candidate cost**: ~1–5 µs (dominated by allocation + string conversion)
**Scales linearly**: K candidates × ~3 µs = dominant when K > 1000

### 🟡 MEDIUM: Fuzzy Match Full-Scan Fallback

**Trigger**: `FzfIntegration::query_index` (fzf.rs:202) when exact query returns 0 results

**Mechanism**: Calls `index.collect_all_live_metas()` (tiered/query.rs:57) which:
- Iterates ALL live metas from L2
- Iterates ALL live metas from each disk layer (newest→oldest)
- Iterates overlay_upserted paths
- Iterates pending_events
- Deduplicates by FileKey and path
Then runs `SkimMatcherV2::fuzzy_match` + `score_result` on EVERY result

**Cost**: For 1M files: N × (~10 µs SkimMatcher + ~30 µs scorer) ≈ **40 seconds** → guaranteed timeout

**Mitigation**: candidate_limit = clamp(limit×20, 512, 20000) — so for limit=100, max candidates = 2000 from exact phase. Full scan only triggers if exact phase finds 0.

### 🟢 LOW: RoaringBitmap Intersection

Despite being the core filtering mechanism, RoaringBitmap operations are surprisingly cheap:
- Intersection of 4 postings with sizes [50, 200, 500, 3000] docs:
  - Step 1: 50 ∩ 200 ≈ 50 steps
  - Step 2: result(~25) ∩ 500 ≈ 25 steps
  - Step 3: result(~10) ∩ 3000 ≈ 10 steps
  - Total: ~85 container-level operations → sub-microsecond
- Even for 1M files with average posting ~3000 docs:
  - 3000 ∩ 8000 ∩ 12000 ∩ 15000 = progressively smaller
  - Total: ~5000 container operations → ~2–5 µs
- **Key insight**: Bitmap intersection cost is dominated by the SMALLEST posting, not by dataset size

### 🟢 LOW: Scoring Overhead (Post-search)
- Only applied to returned results (max 100 by default)
- 100 × 50 µs = 5 ms — acceptable
- Would only be a concern with very high limits (10,000 × 50 µs = 500 ms)

---

## 3. OPTIMIZATIONS ALREADY IN PLACE

| # | Optimization | Location | Mechanism | Impact |
|---|-------------|----------|-----------|--------|
| 1 | **Trigram pre-filtering** | L2: l2_partition.rs:1772, Mmap: mmap_index.rs:470 | Extract literal_hint, generate trigrams, intersect RoaringBitmaps | Reduces candidates by 10³–10⁶× |
| 2 | **Sorted bitmap intersection** | L2: sorts by posting size (line 1782), Mmap: sorts by bitmap len (line 481) | Start with smallest bitmap → minimize intersection operations | ~2–10× fewer operations |
| 3 | **Early empty termination** | Both trigram_candidates (line 487 in mmap, line 1791 in L2) | If accumulator becomes empty during intersection, return None immediately | Prevents wasted work |
| 4 | **Short-hint candidate (1-2 chars)** | L2: line 1800, Mmap: line 527 | Separate short_component_index + trigram UNION scan | Better than full scan for short queries |
| 5 | **L1 Cache hit path** | l1_cache.rs:144 | Full scan of LRU cache (O(capacity)); auto-populated with query results | Sub-100 µs for repeated queries |
| 6 | **Lazy posting decode** | Mmap: posting_for_trigram (line 387) | Binary search on sorted trigram table, deserialize posting on demand | Avoids loading irrelevant postings |
| 7 | **Tombstone cache** | Mmap: tomb_cache (line 74) | RoaringBitmap deserialized once from mmap, stored in Mutex<Option<>> | Avoids repeated deserialization |
| 8 | **Short component cache** | Mmap: short_component_cache (line 492) | Lazy O(N) build on first access, stored as Arc<HashMap<>> | Amortizes across all queries on same segment |
| 9 | **Server timeout** | server.rs:18 — SEARCH_TIMEOUT = 5s | tokio::time::timeout wraps the blocking search task | Prevents runaway queries from starving server |
| 10 | **Request size limit** | socket.rs:87 — max_request_bytes = 8KB | Rejects oversized socket requests | Prevents memory DoS |

---

## 4. QUANTITATIVE LATENCY ASSESSMENT

### 4a. Simple 1-Word Query (e.g., "readme" — 6 chars, 4 trigrams: "rea","ead","adm","dme")

| Scale | Warm L2 (µs) | Cold SSD Mmap (ms) | Cold HDD Mmap (ms) | L1 Hit (µs) |
|-------|-------------|-------------------|-------------------|-------------|
| 10K files | 30–80 | 1–5 | 50–200 | 5–20 |
| 100K files | 50–200 | 5–20 | 200–800 | 10–50 |
| 1M files | 80–500 | 20–100 | 1000–5000 ⚠️ | 20–100 |

**Warm L2 breakdown (100K files)**:
- Parse + compile: ~3 µs
- Trigram hashmap lookup (4 trigrams): ~0.4 µs
- Bitmap intersection (4 postings, avg ~325 docs each): ~1 µs
- Candidate set size: ~10–50 docs (after 4-trigram intersection)
- Candidate iteration (path reconstruct + matcher): 10–50 × 3 µs = 30–150 µs
- Dedup + layer merge: ~1 µs
- **Total: ~35–155 µs**

### 4b. Complex Multi-Token Query (e.g., "VCP server plugin" — AND of 3 terms)

| Scale | Warm L2 (µs) | Notes |
|-------|-------------|-------|
| 10K files | 80–200 | 3 anchors, each with own trigram lookup |
| 100K files | 150–500 | More trigrams = tighter intersection |
| 1M files | 200–800 | Multiple trigrams IMPROVE precision through multiplicative selectivity |

**Warm L2 breakdown (100K files, 3 anchors)**:
- Parse + compile (AND of 3 terms): ~5 µs
- Per-anchor trigram lookup (3 anchors × 4–5 trigrams): ~5 µs
- Per-anchor intersection: ~3 µs
- Total candidates across 3 anchors (deduplicated): ~15–150
- Candidate iteration: ~45–450 µs
- **Total: ~60–460 µs**

**Key insight**: Additional query terms ADD trigrams to the intersection, which INCREASES precision (fewer false positives). Multi-word queries paradoxically can be FASTER than single common words because of multiplicative trigram selectivity.

### 4c. Fuzzy Query (e.g., "mdt" → "main_document.txt", limit=100)

| Scenario | Warm L2 | Notes |
|----------|---------|-------|
| Exact finds candidates | 2–10 ms | candidate_limit = clamp(100×20, 512, 20000) = 2000 |
| Exact returns 0 | **500–5000 ms+** ⚠️ | Falls back to collect_all_live_metas |
| 10K, exact hits | 1–5 ms | candidate_limit = 2000 > file_count → scan all |
| 100K, exact empty | 100–1000 ms | Full index scan |
| 1M, exact empty | **>5s TIMEOUT** ⚠️ | Guaranteed timeout |

**Fast path breakdown (limit=100, candidate_limit=2000)**:
- Exact phase (as above with higher limit): ~200–500 µs
- SkimMatcherV2 scoring (2000 candidates): ~2000 × 5 µs = 10 ms
- score_result per candidate: ~2000 × 30 µs = 60 ms
- Combined sort: ~2000 × log(2000) comparisons ≈ 22K → ~2 ms
- **Total: ~70–75 ms**

### 4d. Short Query (1–2 chars)

| Query | 10K files | 100K files | 1M files |
|-------|-----------|------------|----------|
| "a" (1 char) | 10–50 ms | 100–500 ms | **>5s TIMEOUT** |
| "ab" (2 chars) | 10–50 ms | 100–500 ms | **2–5s** |
| "abc" (3 chars, 1 trigram) | 1–5 ms | 10–50 ms | 100–500 ms |
| "abcd" (4 chars, 2 trigrams) | 0.1–0.5 ms | 0.5–2 ms | 5–50 ms |

The 3→4 character threshold is critical: it's the difference between 1-trigram (no intersection) and 2-trigram (intersection kicks in).

### 4e. Cold Start vs Warm Cache

| Layer | Warm (page cache) | Cold SSD | Cold HDD |
|-------|------------------|----------|----------|
| L2 (memory resident) | 1× baseline | N/A (always warm) | N/A |
| Disk Layer 1 (recent) | 1× baseline | 50–200× | 5000–10000× |
| Disk Layer 2 (older) | 1× baseline | 100–500× | 10000–50000× |

**Cold start impact by layer count**:

| Layers | Warm (µs) | Cold SSD (ms) | Cold HDD (ms) |
|--------|----------|---------------|---------------|
| 0 disk (L2 only) | 50–200 | 50–200 µs | 50–200 µs |
| 1 disk layer | 80–300 | 2–10 | 50–500 |
| 3 disk layers | 150–500 | 10–50 | 200–2000 |
| 5 disk layers | 200–800 | 20–100 | 500–5000 |

---

## 5. POTENTIAL LATENCY ISSUES / HOT PATHS

### 5a. O(N) Operations

| Operation | File:Line | Trigger | Cost @ 1M |
|-----------|-----------|---------|-----------|
| Full index scan (L2) | l2_partition.rs:1858 | literal_hint = None | **5–10s** |
| Full index scan (Mmap) | mmap_index.rs:595 | literal_hint = None | **5–10s** |
| Short_component_cache build | mmap_index.rs:492 | First query on segment | **50–200 ms** (once) |
| Filekey_map fallback build | mmap_index.rs:360 | Missing file_key_map segment | **100–500 ms** (once) |
| Fuzzy full fallback | fzf.rs:215 | Exact returns 0 | **40s+** |
| L1 cache scan | l1_cache.rs:151 | Every L1 probe | ~50 µs (C=1000) |

### 5b. O(N·M) Operations (per-candidate cost)

| Operation | Complexity | Per-unit cost | Dominant when |
|-----------|-----------|--------------|---------------|
| ExactMatcher (case-insensitive) | O((P–Q)·Q) byte scan | 1–5 µs | K > 100 |
| GlobMatcher (backtracking) | O(P·Q) worst | 2–10 µs | * or ? in pattern |
| boundary_aware_match (scoring) | O((P–Q)·Q) + O(P) | 10–50 µs | R > 100, high limit |
| PathInitialsMatcher | O(Q_segs × P_segs) | 2–10 µs | Path-initials auto-detected |
| SkimMatcherV2 (fuzzy) | O(P·Q) with pruning | 3–15 µs | Fuzzy mode |

### 5c. Trigram False-Positive Pathology

**Mechanism**: Single-trigram queries (3-char hint) produce 1 trigram → no intersection to reduce → ALL docs with that trigram become candidates.

**Example with "ing"**:
- 1 trigram: ["ing"]
- No intersection possible (need ≥2 trigrams for intersection)
- In an English filename corpus, ~50–80% of files contain "ing" in some path component
- For 1M files: ~500K–800K candidates
- 500K × 3 µs (path reconstruct + matcher) = **1.5 seconds**

**Other high-frequency trigrams**:

| Trigram | Estimated posting % | 1M file candidates | Estimated time |
|---------|-------------------|--------------------|----------------|
| "ing" | 50–80% | 500K–800K | 1.5–2.4s |
| "ion" | 40–60% | 400K–600K | 1.2–1.8s |
| "the" | 30–50% | 300K–500K | 0.9–1.5s |
| "ent" | 25–40% | 250K–400K | 0.75–1.2s |
| "tion" | 20–35% | 2+ trigrams → intersection | Fast |

### 5d. Disk Layer Traversal Overhead

**Layer management context** (from `src/index/tiered/mod.rs`):
- `COMPACTION_DELTA_THRESHOLD: usize = 8` — triggers compaction at 8 delta layers
- `COMPACTION_MAX_DELTAS_PER_RUN: usize = 4` — max deltas merged per compaction
- `COMPACTION_COOLDOWN: Duration = 300s` — cooldown between compactions
- Result: layers can accumulate to ~8 before compaction kicks in

**Per-layer query overhead**:
1. Acquire layer Arc (free)
2. `layer.idx.query_keys(anchor)` — full trigram lookup per layer
3. For each candidate: tombstone check + path check + matcher
4. Deleted_sources accumulation (Vec push)

**Worst case (8 disk layers, 1M files evenly distributed)**:
- Per layer: ~125K files → ~125K / 8 trigram lookups per query
- Layer count adds ~8× trigram lookup overhead
- But dedup prevents duplicate candidate processing across layers
- **Net impact**: ~2–5× slowdown vs single layer, not 8× (dedup saves work)

---

## 6. FUZZY QUERY DETAILED ANALYSIS

### 6a. Fuzzy Candidate Limit Calculation

```rust
fn fuzzy_candidate_limit(file_count: usize, limit: usize) -> usize {
    let scaled = limit.saturating_mul(FUZZY_CANDIDATE_MULTIPLIER); // × 20
    let bounded = scaled.clamp(FUZZY_MIN_CANDIDATES, FUZZY_MAX_CANDIDATES); // [512, 20000]
    bounded.max(limit.max(1)).min(file_count.max(1))
}
```

| Limit | Candidate Limit | Notes |
|-------|----------------|-------|
| 1 | 512 (MIN) | Clamped to minimum |
| 10 | 512 (MIN) | 10×20=200 < 512 |
| 26 | 520 | 26×20=520 |
| 100 | 2000 | 100×20=2000 |
| 500 | 10000 | 500×20=10000 |
| 1000+ | 20000 (MAX) | Clamped to maximum |

### 6b. Fuzzy Execution Paths

**Path A — Exact phase returns results** (common):
1. Exact query with candidate_limit → gets candidates
2. SkimMatcherV2 scoring per candidate
3. score_result per candidate
4. Combined sort
5. Take top `limit`

**Path B — Exact phase returns empty** (rare, pathological):
1. Exact query returns 0 results
2. `collect_all_live_metas()` — full index scan
3. SkimMatcherV2 + score_result on ALL files
4. Sort + take limit
5. **Catastrophic for >100K files**

### 6c. Fuzzy Scoring Cost Per Candidate

| Component | Time per candidate | Notes |
|-----------|-------------------|-------|
| SkimMatcherV2::fuzzy_match | 3–15 µs | O(P·Q) with smart pruning; returns Option<i64> |
| score_result | 10–50 µs | Full heuristic scoring pipeline |
| Combined | 13–65 µs | Sum of both scores |

---

## 7. SCORING WEIGHTS REFERENCE

### Scoring Formula (line 330 of scoring.rs):

```
FinalScore = BASE_SCORE(100)
           + match_quality (60 for basename contains)
           + EXACT_STEM_BONUS (60) if query == basename stem
           + PREFIX_BONUS (40) if basename.starts_with(query)
           + boundary_bonus from boundary_aware_match
           - basename.len() × LENGTH_PENALTY_FACTOR (0.1)
           + RECENT_MTIME_BONUS (15) if modified within 7 days
           - HIDDEN_DIR_PENALTY (30) if path has hidden dir
           ± NODE_ZONE_MULTIPLIER (×0.1) if in node_modules
           - JUNK_DIR_PENALTY (200) if in junk dir
           - depth × DEPTH_TIEBREAKER_FACTOR (0.5)

With multipliers:
  × BASENAME_MULTIPLIER (2.5) if match in basename
  × PERFECT_BOUNDARY_MULTIPLIER (2.0) if match follows . or /
```

### Boundary Bonuses:
- `STRING_START_BONUS`: 15.0 (first char of string)
- `BOUNDARY_BONUS_PER_CHAR`: 12.0 (char after /, \, ., -, _, space)
- `CAMEL_BOUNDARY_BONUS_PER_CHAR`: 8.0 (uppercase after lowercase)

### Junk Dir Names (37 entries):
node_modules, .node_modules, target, cache, .cache, __pycache__, .tox, dist, build, .build, vendor, .gradle, .mvn, .cargo, bower_components, .npm, .yarn, .pnpm-store, .next, .nuxt, coverage, .coverage, .pytest_cache, .mypy_cache, .ruff_cache, venv, .venv, env, .env, .eggs, .tox

---

## 8. ARCHITECTURAL OBSERVATIONS

### Strengths
1. **Trigram + RoaringBitmap design is excellent**: Sublinear scaling for queries ≥ 4 chars; intersection cost dominated by smallest posting, not dataset size
2. **L2 in-memory persistence**: Eliminates disk I/O for the active working set; ideal for interactive use
3. **Lazy mmap decode**: Postings, tombstones, and short-component caches are loaded on-demand, not at startup
4. **Layer isolation**: Each layer is independently queried with its own trigram indices; compaction can happen without blocking queries
5. **Layer newest-first ordering**: Early return when limit is met; natural "newest wins" dedup
6. **Sorted bitmap intersection**: Minimizes RoaringBitmap intersection cost by starting with smallest posting

### Weaknesses
1. **No 1–2 char optimization**: Short queries are O(N) — there's no frequency-based top-K index or prefix tree
2. **Path-separator disables hint**: `ExactMatcher::literal_hint()` returns None for patterns with `/` or `\` → triggers full scan. Could extract per-segment hints
3. **L1 cache is a full scan**: O(capacity) scan even for misses — could use a Bloom filter or hash-based lookup
4. **No result limit during candidate iteration**: Candidate enumeration happens before limiting — if a trigram produces 500K candidates, all 500K are processed even if limit=10
5. **Single-trigram vulnerability**: 3-char queries are on the edge — could add 2-char sliding window trigrams (bigrams) as a secondary filter
6. **Fuzzy full-scan fallback**: Guaranteed timeout for 1M files when exact returns empty — should have a hard cap or progressive fallback

### Recommendations
1. **Add a minimum query length of 3 chars** in the server layer to prevent O(N) short queries
2. **Warm the mmap page cache** at startup by touching first page of each segment range
3. **Monitor trigram posting size distribution** to detect runaway common trigrams
4. **Consider per-segment hint extraction** for path-separator patterns instead of disabling hints entirely
5. **Add a candidate cap** before path reconstruction to prevent 500K candidate explosions from single trigram queries

---

## 9. CODE REFERENCE INDEX

| Component | File | Key Lines |
|-----------|------|-----------|
| DSL tokenization | `src/query/dsl.rs` | 793–866 |
| DSL parsing (OR/AND/Not) | `src/query/dsl.rs` | 538–576 |
| Query compilation | `src/query/dsl.rs` | 247–289 |
| Anchor selection | `src/query/dsl.rs` | 368–441 |
| Matcher trait | `src/query/matcher.rs` | 15–38 |
| ExactMatcher (contains) | `src/query/matcher.rs` | 44–79 |
| GlobMatcher (*/?) | `src/query/matcher.rs` | 82–151 |
| Glob matching algorithm | `src/query/matcher.rs` | 193–240 |
| Case-insensitive contains | `src/query/matcher.rs` | 246–268 |
| literal_hint extraction | `src/query/matcher.rs` | 271–291 |
| Matcher factory | `src/query/matcher.rs` | 294–306 |
| WfnMatcher | `src/query/matcher.rs` | 316–362 |
| PathInitialsMatcher | `src/query/matcher.rs` | 446–524 |
| MatchAllMatcher | `src/query/matcher.rs` | 527–537 |
| IndexLayer trait | `src/index/mod.rs` | 16–22 |
| TieredIndex struct | `src/index/tiered/mod.rs` | 56–84 |
| TieredIndex::query_limit | `src/index/tiered/query.rs` | 19–55 |
| execute_query_plan | `src/index/tiered/query.rs` | 161–289 |
| query_layer (per-layer) | `src/index/tiered/query.rs` | 292–331 |
| QueryPlan | `src/index/tiered/query_plan.rs` | 12–42 |
| L2 query (full) | `src/index/l2_partition.rs` | 741–822 |
| L2 query_keys (IndexLayer) | `src/index/l2_partition.rs` | 1822–1880 |
| L2 trigram_candidates | `src/index/l2_partition.rs` | 1772–1798 |
| L2 short_hint_candidates | `src/index/l2_partition.rs` | 1800–1819 |
| L2 for_each_live_meta | `src/index/l2_partition.rs` | 825–848 |
| L2 upsert_inner | `src/index/l2_partition.rs` | 560–674 |
| L2 insert_trigrams | `src/index/l2_partition.rs` | 1704–1716 |
| query_trigrams (helper) | `src/index/l2_partition.rs` | 58–68 |
| for_each_component_trigram | `src/index/l2_partition.rs` | 74–88 |
| MmapIndex struct | `src/index/mmap_index.rs` | 72–80 |
| MmapIndex::query_keys | `src/index/mmap_index.rs` | 557–618 |
| MmapIndex::query (full) | `src/index/mmap_index.rs` | 643–722 |
| MmapIndex::trigram_candidates | `src/index/mmap_index.rs` | 470–491 |
| MmapIndex::short_hint_candidates | `src/index/mmap_index.rs` | 527–556 |
| MmapIndex::posting_for_trigram | `src/index/mmap_index.rs` | 387–425 |
| MmapIndex::meta_at | `src/index/mmap_index.rs` | 116–151 |
| MmapIndex::tombstones (lazy) | `src/index/mmap_index.rs` | 102–114 |
| MmapIndex::short_component_cache | `src/index/mmap_index.rs` | 492–526 |
| MmapIndex::for_each_live_meta | `src/index/mmap_index.rs` | 724–751 |
| MmapSnapshotV6 struct | `src/storage/snapshot.rs` | 142–151 |
| MmapSnapshotV6 accessors | `src/storage/snapshot.rs` | 170–201 |
| L1Cache struct | `src/index/l1_cache.rs` | 125–132 |
| L1Cache::query | `src/index/l1_cache.rs` | 144–194 |
| L1Cache::insert | `src/index/l1_cache.rs` | 196–216 |
| ScoreConfig | `src/query/scoring.rs` | 95–106 |
| score_result | `src/query/scoring.rs` | 330–431 |
| boundary_aware_match | `src/query/scoring.rs` | 235–303 |
| compute_position_bonuses | `src/query/scoring.rs` | 201–225 |
| execute_query (entry) | `src/query/fzf.rs` | 80–95 |
| FzfIntegration::query_index | `src/query/fzf.rs` | 202–224 |
| FzfIntegration::match_query | `src/query/fzf.rs` | 184–200 |
| fuzzy_candidate_limit | `src/query/fzf.rs` | 226–230 |
| sort_results | `src/query/fzf.rs` | 97–153 |
| QueryServer (HTTP) | `src/query/server.rs` | 104–146 |
| search_handler | `src/query/server.rs` | 159–218 |
| SEARCH_TIMEOUT | `src/query/server.rs` | 18 |
| SocketServer (Unix) | `src/query/socket.rs` | 93–176 |
| handle_connection_io | `src/query/socket.rs` | 188–277 |
| Compaction thresholds | `src/index/tiered/mod.rs` | 35–41 |
