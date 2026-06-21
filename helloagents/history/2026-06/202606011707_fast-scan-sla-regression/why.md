# fast scan SLA 回归修复原因

## 背景

`tiered_watch` 的 L1/L2 fast scan lane 承诺本地可信文件系统上的已知非 L0 目录在目标窗口内完成目录项变化发现。真实运行中，`/home/shiyi/Documents/project/comfyui/.../output/anima/2025-06-01/batch_job_3-kengos-girl_00001_.png` 已存在但查询不到，手动 `/scan` 后立即可见。

## 根因

1. `l1_l2_fast_scan_bootstrap_budget_per_tick` 被实现为 sentinel 总量上限，运行态在约 2048 个 sentinel 后停止继续覆盖后续已知目录。
2. 新注册 sentinel 时只记录当前目录签名，不触发初始补扫；如果文件在 sentinel 覆盖前已经创建，会被当作干净基线吞掉。
3. `/watch-state.fast_scan_sla_ok` 只反映已注册 sentinel 的覆盖情况，无法暴露 bootstrap 卡死导致的未覆盖目录。

## 成功标准

- bootstrap 预算恢复为每 tick 批量预算，不因已注册 sentinel 达到单批预算而永久停止。
- 新注册 sentinel 至少触发一次 depth=1 初始补扫。
- 回归测试覆盖“超过单批预算后仍可继续 bootstrap”和“初始 bootstrap 能补偿已存在变更”。
