# M1-3 分片 repair 与有界最终一致观测需求

## 背景

路线基准已确定为“热点 5 秒 + 全局有界最终一致 + 返回前验真”。M0、M1-1、M1-2 已分别完成 rename 语义修复、查询返回前 Top-K 验真、运行时 subtree tombstone。当前剩余主线地基是让冷目录 repair 不再一次性扫描大目录，并把“可能不全”的追平上界暴露到 `/health`。

## 问题

现有 `src/index/tiered/sync.rs` 的周期冷扫描入口位于 `process_dirty_entry_with_project_markers_and_manifest_skip_dirs`，约第 472 行开始；目录扫描由 `scan_dirs_periodic_cold_outcome_with_project_markers` 调用 `scan_dirs_with_depth_and_project_markers_budgeted`，约第 1017 行开始。当前冷扫描对单目录最多 10000 entries，预算主要是入口级，不会携带游标重入 DirtyQueue。大目录 repair 可能在单次 dirty queue 工作中造成长阻塞，且 `/health` 只有 `deferred_repair_queue_len`，不足以解释全局最终一致追平上界。

## 目标

- 新增游标式分片 repair：单 slice 默认约 512 entries / 20ms。
- 未完成 slice 携带进度重新进入 DirtyQueue，下一轮从游标后继续。
- `/health` 新增可查询字段：`cold_sweep_last_completed`、`cold_sweep_period_estimate`、`dirty_backlog`。
- 保持 runtime-only 行为，不修改 snapshot 持久化格式。
- 不改变查询正确性：返回前验真仍是正确性来源，repair 只负责补全追平。

## 验收

- 大目录周期 cold repair 单次处理不超过 slice 上界，未完成时 dirty backlog 保持可见。
- 分片重入后能扫描后续 entry，不从头反复扫描。
- `/health` 输出 cold sweep 完成时间、周期估计、dirty backlog。
- `cargo test -q sliced_repair`、`cargo test -q health`、`cargo test -q query`、`cargo test -q` 无新增失败。
