# 启动 CPU 峰值治理任务清单

> 状态：部分可执行（2026-06-13 复核）。`[√]` 为已由提交落地，`[-]` 为已并入 M3 或不再独立执行，`[ ]` 为仍需按实测评估。

## Phase 1：启动期背压（部分已落地）

- [√] 初始补扫队列积压达到单批预算时暂停 bootstrap（提交 `d25c91d`）
- [√] bootstrap 冷段候选收集流式早停、候选耗尽时冷却（提交 `5b52d62`）
- [√] 修复 bootstrap 把单批预算误当 sentinel 总量上限（提交 `d25c91d`）
- [ ] 新增启动期专用预算字段（`fast_scan_startup_grace_secs` 等）与 startup/steady-state budget 切换 — 是否仍需要，取决于 M3 后的 hotset/backfill 实测峰值

## Phase 2：拆分初始补扫与真实变化（已并入 M3）

- [-] 拆分新 sentinel 初始补扫与真实 changed-dir 补扫（`DirtyReason::FastScanBootstrapDir` 或 initial/changed 区分）— 并入 `202606011812_待执行_lease-hotset-fast-scan-shrink`
- [-] 为初始补扫接入低优先级 DirtyQueue 或独立小批量处理 — 并入 `202606011812_待执行_lease-hotset-fast-scan-shrink`

## Phase 3：sentinel registry 持久化（收缩为 hotset-only）

- [-] 新增 fast scan sentinel registry 持久化结构 — 不做全量 L1/L2 registry，收缩为 M3 的 hotset-only sentinel registry
- [-] clean shutdown + stable snapshot 场景恢复 sentinel registry — 只恢复 active lease hotset sentinel，并入 M3
- [-] config / mount / recovery 不可信时丢弃 registry 并走限速 bootstrap — 并入 M3

## Phase 4：观测与回归（已并入 M3）

- [-] 扩展 `/watch-state`、`/health` 或 metrics 的启动 backfill 观测字段（initial/real 队列区分后补齐）— 并入 M3 hotset/backfill 观测
- [-] 补单元测试：初始/真实队列拆分 — 并入 M3
- [-] 补集成测试：重启后初始补扫受限速约束，且 hotset 内新文件 SLA 不回退 — 并入 M3
- [-] 更新 README、CHANGELOG、`helloagents/wiki/tiered-watcher-runtime.md` — 并入 M3 文档同步
- [-] 运行 `cargo test -q fast_scan` 与完整 `cargo test -q` — 并入 M3 验收命令
