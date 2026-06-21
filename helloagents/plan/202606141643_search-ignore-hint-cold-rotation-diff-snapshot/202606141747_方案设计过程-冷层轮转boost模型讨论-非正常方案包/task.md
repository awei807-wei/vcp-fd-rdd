# 讨论产物清单（非待执行任务）

> 本文件不是正常 `task.md`。本目录不能被当作可执行方案包使用；不得通过 `~exec` 或类似流程直接执行。若要实施，需要另行创建正常方案包。

## 过程状态

[-] 已记录原始设想：L3 冷层临时获得 L1-like 更新待遇，再恢复层级。
[-] 已记录参考模型：MLFQ、Priority Aging、Weighted Round-robin、Anti-entropy、CLOCK、Temporary Promotion Lease。
[-] 已记录反面教材：Scan Pollution、Reconciliation Storm、Clock Skew Failure、Hard-threshold Thrashing。
[-] 已记录用户反驳：Lease / Aging 物理撕裂、Round-robin / Anti-entropy 谐振、MLFQ 优先级翻转、幽灵复活、墓碑海啸、可撤销租约级联、DirtyQueue 重载饥饿。
[-] 已记录追加反驳：Coalescing Queue 的精度坍缩与 I/O 放大、Subtree Tombstone 的 CPU 阻抗。
[-] 已记录最终收敛约束：Boost 是可撤销调度机会，不是正式 tier；Aging 只加候选分；Round-robin 不做重 I/O；DirtyQueue 应合并状态但不得放大单次扫描；tombstone 需要 subtree / range / segment 表达且 overlay 必须可编译、可跳过、可压缩；查询正确性依赖 bounded verification。
[-] 已记录 VM A/B 初测：home 场景几乎没有触发 M2，只能说明开启成本低，不能说明机制有效。
[-] 已记录 fixture 试验：专门冷层 root + `/tmp` run_dir 成功造出 cold freshness 滞后。
[-] 已记录 scan-only fallback 缺陷与修复：租约发放失败后应降级为 `scan_only`，不能直接取消冷层轮转。
[-] 已记录 direct canary 结果：M2 能降低 cold freshness age，资源成本暂时可控，direct create / rename / delete 最终正确性通过。
[-] 已记录 active canary 口径问题：baseline 也能通过 query-triggered repair 获得约 5 秒可见性，active 指标不能证明 M2 背景主动追平收益。
[-] 已记录 passive canary 为后续判定口径：后续应以 settle 后首次查询命中率区分后台轮转收益与查询触发修复。
[-] 已记录首轮 passive A/B：实验组 positive first-query 成功率 `6/6`，baseline 为 `4/6`；M2 出现背景轮转收益信号，但样本量仍小。
[-] 已记录成本对比：实验组 CPU p95 约 `2.004%`，baseline 约 `1.642%`；RSS、fd、dirty queue 基本持平。
[-] 已记录投入产出比验证方向：在 bench runner 内增加 `--event-storm`，先用短窗口 burst 粗测 M2 能主动捞回多少事件以及事件年龄，再决定是否投入独立 workload driver。
[-] 已记录新增极端场景：子目录重命名雪崩、挂载点断联海啸、inode 快速复用幽灵复活、时钟倒挂。
[-] 已记录 event-storm 代理边界：`mount_storm` 与 `time_skew` 在一体化脚本内为无特权代理测试；真实拔盘/NFS 断联和系统时间回拨必须在 VM/独立 workload driver 中以特权环境验证。

## fd-rdd 可复用模块清单

- `src/index/tiered/sync.rs`：DirtyQueue、DirtyReason、DirtyRepairCursor、dirty queue repair 入口。
- `src/index/tiered/mod.rs`：RuntimeSubtreeTombstone 与 runtime tombstone 清理 / 屏蔽路径。
- `src/io_governor.rs`：TokenBucket、I/O Governor、PSI backoff。
- `src/index/tiered/query.rs`：QueryVerifyBudget、cold result bounded verification、QueryHitStale repair。
- `src/event/tiered_watch.rs`：EphemeralWatchConfig、FastScanLeaseKind、fast scan lease / tick。
- `src/main.rs`：fast scan lease 授权、bootstrap、tick 调度接入点。
- `src/index/tiered/snapshot.rs`：snapshot / stable v7 写入边界。
- `src/index/tiered/events.rs`：事件、overlay、flush 请求与 snapshot dirty 边界。

## 后续转正常方案前必须回答的问题

- Boost 预算是否与正式 L1 / L2 watcher 预算完全隔离。
- Point Lookup 与 Scan 如何可靠区分，尤其是 CLI 批量查询、脚本遍历和用户显式单点搜索。
- DirtyQueue 满载时采用哪种合并、升格、watermark 与重扫策略。
- DirtyQueue 父级 scope 合并后如何保证每轮扫描仍被 stat 数、目录项数、耗时和 PSI 预算截断。
- 大 scope 如何反向拆成 child shard，避免父级 dirty 变成全量递归 scan。
- subtree / segment tombstone 如何持久化，如何被 cold query 快速判断，如何在 compaction 后释放。
- tombstone overlay 如何编译成 segment summary、mmap sidecar、sorted prefix table 或其他可跳过结构。
- tombstone 数量、重叠深度、覆盖比例和 CPU 成本达到什么阈值后强制 compaction。
- ReconciliationWorker 的 stat/s、目录数、字节数、耗时和 PSI 截断策略如何配置。
- 查询侧 stale / uncertain 状态是否需要暴露给用户或只用于内部 telemetry。
- snapshot delta / compaction 是否与现有 v7 写入路径兼容，是否会改善而不是扩大 RSS 峰值。
- M2 的收益验收应以 active canary 还是 passive canary 为主；当前试错结果倾向 passive。
- scan-only fallback 是否需要 child shard / deep repair，才能覆盖新建深层目录。
- 如何防止 query miss / fast scan 补偿掩盖 M2 背景轮转收益。
- cold freshness age 降低是否足够作为阶段指标，还是必须同时证明 passive first-query 成功率。
- `rotating_tick_secs`、TTL、scan-only slice 与 passive settle 时间之间应如何建立可解释的验收关系。
- passive positive 样本量需要扩大到多少，才能从“收益信号”升级为“可实施结论”。
- 正式验收阈值应如何定义：positive first-query 提升幅度、CPU p95 增幅、RSS 增幅、dirty queue 积压和 budget blocked 上限。
- 极端 workload 下的 passive 收益是否仍成立，尤其是 delete storm、rename storm、git checkout storm 与 watcher drop。
- 子目录 rename 只有父级事件时，DirSentinel / 递归对账能否在不递归阻塞前台的前提下追平深层路径。
- 挂载点离线时，删除熔断器依据什么 root/mount identity 判断 ENOENT 海啸，而不是写入整棵子树 Delete Tombstone。
- inode 快速复用需要哪些 generation / FileKey 证据才能证明旧索引没有复活；如果 VM 文件系统没有复用 inode，测试结论如何标注为未触发。
- 系统时钟回拨测试应与 mtime 代理测试分开记录，避免把 backdated mtime 成功误判成双轨时钟完全通过。

## 不得直接执行的事项

- 不得直接把 L3 目录提升为正式 L1。
- 不得让 Aging 直接抢占正式 watcher 额度。
- 不得在 Round-robin 线程内做重型 Anti-entropy。
- 不得把 DirtyQueue 当作无界 FIFO event log。
- 不得把 coalesced parent scope 解释成“立即全量递归对账”。
- 不得用逐文件 tombstone 承载百万级冷目录删除。
- 不得把 tombstone overlay 长期留在前台 cold query 的复杂动态树匹配路径上。
- 不得让前台查询无限等待后台 repair。

## 建议的正式方案拆分

后续若需要落地，建议拆成独立正常方案包，而不是把本过程包直接转执行：

1. `.gitignore` 忽略命中提示：优先落地，范围清晰，风险最低。
2. Query stale hit repair 增强：围绕 `QueryVerifyBudget` 和 `DirtyReason::QueryHitStale` 做小步验证。
3. DirtyQueue 合并式状态集：先解决过载语义、cost-aware coalescing、bounded scan slice，再讨论冷层轮转。
4. subtree / segment tombstone：先解决墓碑海啸、幽灵复活和 CPU 阻抗，定义 compiled tombstone overlay。
5. 冷层轮转调度器：最后接入 Boost / Aging / Round-robin，并纳入 I/O Governor。
