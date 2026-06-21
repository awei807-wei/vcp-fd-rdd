# M3 lease hotset 正式化 + fast scan 收缩需求

> 状态门禁：待执行（正式方案包）。本包由原 `202606011812_不可执行-设计未定稿_fast-scan-lease-hotset` 改写定稿，用于执行 M3 架构落地。

## 背景

当前路线基准已经确认：热点 5 秒 + 全局有界最终一致 + 返回前验真。M0/M1/M2 已完成 rename 可见性、查询返回前验真、运行时 subtree tombstone、分片 repair 观测、proc sampler 提前触发 Ephemeral Watch。

现有 L1/L2 fast scan lane 仍偏向“全部已知 L1/L2 目录 5 秒覆盖”。这能提升局部新文件体验，但在大规模目录、低性能设备和冷启动场景下会把系统推向第二套轮询 watcher：为了对没有 L0 watcher 覆盖的目录承诺 5 秒发现，必须在 5 秒窗口内主动 stat/readdir 大量目录，CPU/IO 成本不可消除。

## 问题

- fast scan 覆盖全部 L1/L2 已知目录，会导致启动 bootstrap、周期 sentinel 检查和初始补扫成本随目录规模膨胀。
- 当前 CI `p1_fast_scan_sla` 断言全量 L1/L2 目录 5 秒 SLA；收缩语义后，如果不先改断言，CI 会拦住正确改动。
- `202606011730_部分可执行_startup-cpu-fast-scan-governance` 的 Phase 3 原本偏向全量 sentinel registry 持久化，必须收缩为 hotset-only sentinel；Phase 2/4 可并入本里程碑。
- 产品口径必须避免继续表达“索引可能过期”。正确口径是：返回的结果是真的，但候选集合可能暂时不全；不全部分有可解释追平上界。

## 目标

- 正式化 lease hotset：把 5 秒 fast scan 能力集中到自动/显式热点租约集合。
- fast scan 覆盖从“全部 L1/L2”收缩为“lease hotset 内 5 秒”。
- 普通冷目录保持有界最终一致，追平上界由 cold sweep 周期、DirtyQueue backlog 和 repair 观测解释。
- sentinel 持久化只服务 hotset，恢复条件必须绑定 clean shutdown、stable snapshot、WAL、config fingerprint 与 mount identity。
- 启动治理包 Phase 2（初始补扫/真实变化队列拆分）与 Phase 4（观测与回归）并入本里程碑。
- 同步 CI、README、CHANGELOG 和 wiki，把 SLA 改为“hotset 内 5 秒 + 冷目录有界最终一致”。

## 成功标准

- 低性能 profile 下稳态 CPU 显著低于全量 L1/L2 fast scan 语义。
- Hotset 内 create/delete/rename 5 秒 SLA 不回退。
- 冷目录不承诺 5 秒，但追平时间不超过配置的巡检周期，并能通过 `/health` / metrics 解释。
- 查询返回前验真仍保证返回结果为真，不因 fast scan 收缩返回已删除或身份变化的旧结果。
