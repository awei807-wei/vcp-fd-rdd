# Phase 6: ParentIndex Query Acceleration (v0.6.9)

## Why
ParentIndex was introduced in Phase 4 and made default in Phase 5, but it was only used during fast_sync. This phase integrates it into the query path so that `parent:`/`infolder:` filters can benefit from the pre-built parent directory index, reducing query latency for directory-scoped searches.

## Goals
- Use ParentIndex to pre-filter L2 candidates when a `parent:` filter is present
- Reduce the number of anchor scans and metadata lookups for parent-filtered queries
- Clean up accumulated dead_code warnings from previous phases
