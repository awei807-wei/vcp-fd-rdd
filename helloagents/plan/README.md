# 方案包索引

> 更新于 2026-06-13。目录命名约定：`{时间戳}_{状态}_{方案名}`。
> 状态取值：`待执行`（修订完毕，确认后可直接实施）| `部分可执行`（部分已落地或部分冻结）| `不可执行`（门禁锁定，需决策后改写）。

## 当前方案包

| 方案包 | 状态 | 说明 |
|---|---|---|
| `202606011730_部分可执行_startup-cpu-fast-scan-governance` | 部分可执行 | Phase 1 止血已由 `5b52d62`、`d25c91d` 落地；Phase 2/4 已并入 M3；Phase 3 已从全量 sentinel registry 收缩为 M3 的 hotset-only sentinel |
| `202606061358_不可执行-调研参考_budgeted-adaptive-indexing-research` | 不可执行 | 技术路线调研与代码证据锚定。永不直接执行，确认后据它拆 P0 正式方案包 |

## 已完成

- `202606011812_lease-hotset-fast-scan-shrink` 已执行并归档到 `helloagents/history/2026-06/`：M3 lease hotset 正式化 + fast scan 收缩，5 秒 SLA 只覆盖 active lease hotset，普通冷目录保持有界最终一致；hotset-only registry、初始/真实队列拆分、观测字段和 CI/文档口径已同步。
- `202606061413_fix-rename-event-missing-new-file` 已执行并归档到 `helloagents/history/2026-06/`：孤立 `RenameMode::To` 当 Create，Create/Rename 不被后续 Modify 覆盖，`cargo test -q` 通过。
- `202606122238_topk-query-verify` 已执行并归档到 `helloagents/history/2026-06/`：新增 `[query]` 返回前验真预算，默认关闭 lazy validation，冷层/base 删除命中不返回，mtime/identity 变化返回 `Changed`。
- `202606122300_runtime-subtree-tombstone` 已执行并归档到 `helloagents/history/2026-06/`：删除父目录和 RenameFrom 创建运行时 subtree tombstone，cold/base 查询候选验真前按前缀过滤，TTL 与同名重建清理已覆盖。
- `202606122324_sliced-repair-health-observability` 已执行并归档到 `helloagents/history/2026-06/`：Periodic cold scan 按约 512 entries / 20ms 分片，未完成 slice 携带 cursor 重入 DirtyQueue；`/health`、diagnostics、metrics 暴露 cold sweep/backlog 字段，完整 `cargo test -q` 通过。
- `202606130036_proc-sampler-ephemeral-watch` 已执行并归档到 `helloagents/history/2026-06/`：Linux `/proc/<pid>/fdinfo` 写句柄采样只处理同用户进程，按预算把正在写入的目录送入现有 Ephemeral Watch；`/watch-state`、`/health`、diagnostics、metrics 暴露 proc sampler 字段，完整 `cargo test -q` 通过。

## 当前路线基准

已采纳"热点 5 秒 + 全局有界最终一致 + 返回前验真"语义：返回的结果必须是真的，但候选集合可能暂时不全；不全的部分需要通过 `/health`、DirtyQueue backlog、cold sweep 周期和 repair 游标给出可解释追平上界。

## 实施顺序（路线采纳后）

1. `202606011812_lease-hotset-fast-scan-shrink` 已完成并归档。
2. `202606011730_部分可执行_startup-cpu-fast-scan-governance` 仅保留 Phase 1 已落地记录和后续实测评估项，不再作为 M3 的独立实施入口。
