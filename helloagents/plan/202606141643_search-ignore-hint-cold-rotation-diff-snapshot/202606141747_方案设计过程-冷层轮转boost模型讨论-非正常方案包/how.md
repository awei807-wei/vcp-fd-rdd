# 讨论过程记录：L3 冷层轮转、Boost Lease 与对账模型

> 本文件是方案设计过程记录，不是实现说明书。所有结论都需要在正式方案包中重新验证代码路径、资源预算和验收标准。

## 1. 初始设想：把金字塔临时旋转

用户提出的原始模型可以概括为：

- L1 是塔尖，实时性最好。
- L2 / L3 不及时，尤其 L3 从某个阶段开始不再实时 watch。
- 当 L3 不及时，就让 L3 临时获得 L1 的 watch / fast scan 更新待遇。
- 然后换 L2、L1，等 L2 和 L3 更新完成后重置回来。

这个设想的价值在于：它试图把“长期冷层低新鲜度”变成“短期轮转补偿”，避免 L3 永远只能靠慢速冷扫修复。

## 2. 可参考但不能照搬的模型

讨论中提到过的通用模型包括：

- 多级反馈队列：热数据升层、冷数据降层，长期饥饿者可 aging。
- Priority Aging：被长期跳过的冷目录逐渐增加调度权重。
- Weighted Round-robin：冷目录按权重轮询，而不是平均分配扫描机会。
- Anti-entropy：后台对账用来修复事件丢失、崩溃或冷层过期。
- CLOCK / LRU 变体：用低成本近似热度替代精确访问历史。
- Temporary Promotion Lease：短期提升冷目录扫描待遇，TTL 到期撤销。

这些模型只提供词汇和启发，不应直接成为 fd-rdd 的实现，因为 fd-rdd 的硬约束是 watcher 额度、内存、I/O 压力和前台查询延迟，而不是抽象 CPU 时间片。

## 3. 用户指出的失效模式

### 3.1 一次性扫描导致缓存颠簸

如果用户运行 grep / find / 脚本遍历，一碰到冷目录就 Boost，会把大量只访问一次的目录提升为 L1-like，挤出真正热目录。

收敛约束：

- 必须区分 Point Lookup 与 Scan。
- 只有用户显式检索、stale hit、query repair 这类单点触发，才可能进入临时 Boost 候选。
- 批量扫描只允许低优先级冷扫，不得获得快速租约。

### 3.2 对账风暴

多个冷目录同时 Boost 或 Round-robin 触发重扫，会形成大量 stat / hash 比对，淹没磁盘 I/O。

收敛约束：

- 冷层轮转调度器不得直接执行重 I/O。
- 它只能投递“需要对账”的合并状态。
- 真正扫描必须经过 DirtyQueue / ReconciliationWorker，并受 TokenBucket、I/O Governor、PSI backoff、per-root cost 和前台延迟保护约束。

### 3.3 时钟负漂移导致租约错误

Lease TTL 如果依赖 wall time，NTP 回拨或双系统时间变化可能导致租约永久不过期或提前失效。

收敛约束：

- TTL 判断使用单调时间。
- 对外可观测顺序使用逻辑时钟 / generation / apply_seq 类标记。
- 撤销操作必须幂等。

### 3.4 硬阈值导致仰卧起坐

固定阈值会让目录在 Boost 与撤销之间频繁抖动。

收敛约束：

- 提升阈值和降级阈值必须有迟滞。
- 租约应有最小驻留时间，但最小驻留时间不能变成不可撤销的资源锁。
- 系统压力高于保护线时，调度机会可被撤销或延后。

## 4. 对组合模型的反驳与再收敛

### 4.1 Lease 与 Aging 的物理撕裂

用户质疑点：Lease 像强一致与独占承诺，Aging 像资源压力驱逐机制。二者叠加会产生“租约未过期但资源被驱逐”的悬空状态，或者反过来让大量 Lease 锁死 L1 空间。

收敛修正：

- fd-rdd 不应把 Boost Lease 定义为强一致承诺。
- Boost Lease 只表示“调度优先级临时提高”，不承诺常驻内存、不承诺独占 watcher、不承诺查询一定从热路径返回。
- Aging 只增加候选评分，不直接授予 Boost。
- 查询正确性不依赖 Boost，而依赖 bounded verification 与 stale hit repair。

### 4.2 Round-robin 与 Anti-entropy 的谐振过载

用户质疑点：Round-robin 游标扫到巨型冷目录时，如果原地触发 Anti-entropy，再叠加 Aging 提权，会产生对账风暴。

收敛修正：

- Round-robin 只能产生 Dirty Tag，不执行 Anti-entropy。
- DirtyQueue 应做 coalescing，把同一路径或同一子树的重复 tag 合并为一个状态。
- ReconciliationWorker 低优先级、限速、可暂停，不抢占前台查询。
- 每轮处理必须有 hard cap：stat 数、字节数、目录数、耗时和 PSI 阈值都要能截断。

### 4.3 MLFQ 的存储层优先级翻转

用户质疑点：CPU 调度里的 Aging 是防止任务饿死，但 fd-rdd 的 L1 watcher 和内存是硬资源。低价值冷目录被 Aging 提升后，可能挤掉真正热目录。

收敛修正：

- L1 正式 watcher 额度与 Boost 调度额度必须隔离。
- Aging 不能直接占用正式 L1 watcher。
- 临时 Boost 使用单独预算，预算耗尽时降级为普通 L3 冷扫。
- 热目录优先级由真实访问、事件活跃度和前台查询延迟保护，不被 aging 冷目录抢占。

### 4.4 Anti-entropy 的幽灵复活

用户质疑点：冷层没有实时 watcher，物理文件被删除后，如果旧 stable.v7 仍保留条目，Anti-entropy 可能误判并让幽灵引用反复出现。

收敛修正：

- 对账不能把 stable.v7 当作要恢复物理磁盘的源。
- 磁盘事实优先于旧索引事实。
- 如果磁盘不存在，正确动作是写删除屏蔽状态并触发后续 compaction，而不是复活旧条目。
- tombstone 必须有 segment / subtree / range 级表达，不能只靠逐文件记录。

### 4.5 Tombstone 海啸

用户进一步质疑点：stable.v7 只读且 mmap 直通，无法原地删除。删除百万文件冷目录时，如果生成百万 tombstone，会击穿零常驻堆内存目标。TTL 或 LRU 驱逐 tombstone 又会让幽灵索引复活。

收敛修正：

- tombstone 不能只做逐文件墓碑。
- 大规模删除必须优先表达为 subtree tombstone、range tombstone 或 segment tombstone。
- tombstone 的生命周期不应靠无限常驻内存结束，而应靠 cold segment compaction / snapshot 边界消化。
- 查询路径应先判断“该 cold segment / subtree 是否被 tombstone 覆盖”，再返回 cold candidate。
- 正式方案需要明确 tombstone 的内存上限、持久化位置和 compaction 触发条件。

### 4.6 可撤销租约的撤销级联

用户质疑点：系统压力高时撤销 Ephemeral Watcher，随后前台马上查询该目录，会因为队列积压和 cold query 降级导致延迟刺穿。

收敛修正：

- 撤销 watcher 不能让查询正确性失效。
- 前台查询必须有 bounded verification，不能无限等待后台 ReconciliationWorker。
- 对热点 stale hit 可以获得小预算同步验真，但必须有 deadline 和 stat 上限。
- 超过预算时返回带 stale / uncertain 注解的结果，或返回可解释的未命中，而不是卡死。
- Boost 的作用是改善后续新鲜度，不是当前查询的唯一救命路径。

### 4.7 DirtyQueue 重载饥饿

用户质疑点：长期高负载下低优先级 worker 饥饿。DirtyQueue 如果丢 tag 会语义断裂；如果阻塞写入会让调度器停摆。

收敛修正：

- DirtyQueue 不应设计成必须完整保留顺序的 FIFO event log。
- 它更适合设计成“合并式状态集”：每个 scope 保留最高优先级 reason、最新 generation、最早 first_seen、重试计数和 cost hint。
- 队列满时合并、升格或压缩，而不是简单丢弃或阻塞。
- Round-robin 游标推进与 DirtyQueue 接收成功不能强耦合；游标状态应记录“已发现但未消费”的 backlog watermark。
- 过载期结束后根据 watermark 重新 sweep，避免“扫了等于白扫”。

### 4.8 Coalescing Queue 的精度坍缩与 I/O 放大

用户进一步质疑点：如果 DirtyQueue 满载时把 `/a/b/c`、`/a/b/d` 合并升级成 `/a/b`，而 `/a/b` 下有 10 万个未变化文件，那么 worker 消费 `/a/b dirty` 时可能从 2 次轻量 `stat` 放大为一次完整 `readdir` + 10 万次递归 `stat`。这会形成“队列积压 → 精度坍缩 → 扫描暴涨 → PSI 触顶 → 队列更积压”的正反馈。

收敛修正：

- scope 升级只代表“精度降低”，不代表“物理工作量立即扩大”。
- `parent dirty` 不能被解释成“立即全量递归对账”，只能表示该 scope 下存在未消化变更。
- ReconciliationWorker 消费合并后的父级 scope 时必须按预算切片推进：目录项数、`stat` 数、字节数、耗时、PSI 都必须有硬上限。
- 如果发现父级 scope 成本过大，应反向拆成 child shard 或只记录 cursor，而不是继续递归扩散。
- coalescing 必须单调降低内存/队列压力，但不得单调放大单次 I/O 成本。

建议的 dirty entry 语义：

```rust
struct DirtyEntry {
    scope: DirtyScope,
    reason_set: DirtyReasonSet,
    priority: DirtyPriority,
    generation: u64,
    cursor: Option<DirtyRepairCursor>,
    estimated_cost: u64,
    max_next_slice_cost: u64,
    precision_loss_level: u8,
}
```

正确退化路径应是：

```text
队列积压
=> 精度降低
=> 单次物理扫描预算不增加
=> 收敛时间变长
=> 查询侧临时承担 bounded verification
```

禁止退化成：

```text
队列积压
=> 精度降低
=> 单次物理扫描范围暴涨
=> PSI 触顶
=> 更积压
```

### 4.9 Subtree Tombstone 的 CPU 阻抗

用户进一步质疑点：subtree tombstone 避免了逐文件墓碑，但多次“删除目录 → 重建同名目录 → 局部删除”会产生多层嵌套、generation 不同的 tombstone。Cold Query 从 mmap cold segment 读出 candidate 后，如果每个候选都要做复杂 prefix/generation 区间树匹配，会把零常驻堆内存节省下来的压力转移到前台 CPU。

收敛修正：

- tombstone overlay 不能是随意挂在查询热路径上的动态树；它必须被当成索引的一部分。
- cold segment 需要 segment-level tombstone summary，用于快速判断某个 segment 是否完全不受 tombstone 影响、是否被整段覆盖、是否需要精细过滤。
- tombstone overlay 在复杂后应编译为 mmap sidecar、sorted prefix table、radix/trie、range index 或 prefix negative cache，而不是长期使用运行时 Vec / IntervalTree。
- overlay 高度、prefix 重叠深度、query filter CPU、segment 覆盖比例都需要硬阈值；超过阈值时触发 compaction / new snapshot。
- 前台 cold query 只能承担 top-K bounded filter，不能为后台未完成的 compaction 无限付 CPU 债。

三层削峰策略：

1. **segment 级预过滤**：segment summary 先判断是否需要 tombstone 检查，很多 segment 直接跳过。
2. **tombstone overlay 编译化**：复杂 tombstone 下沉为 mmap sidecar 或紧凑前缀索引，降低热路径查找成本。
3. **overlay 高度上限**：当 tombstone 层数、重叠深度或 CPU 成本超限时，不继续堆叠 overlay，必须 compaction。

## 5. fd-rdd 中已有可复用模块

以下是当前代码里可复用或可借鉴的模块，不代表已完整满足上述设计：

### 5.1 DirtyQueue / DirtyRepairCursor

路径：`src/index/tiered/sync.rs`

已有能力：

- `DirtyReason`
- `DirtyQueueEntry`
- `DirtyRepairCursor`
- `dirty_queue_ready_batch`
- `retry_dirty_entry`
- `process_dirty_entry`

可复用方向：

- 作为冷层轮转候选的对账入口。
- 继续利用分片 cursor 避免单次 repair 扫完整目录。
- 后续需要增强 coalescing、watermark、cost hint 和队列过载语义。

### 5.2 RuntimeSubtreeTombstone

路径：`src/index/tiered/mod.rs`

已有能力：

- `RuntimeSubtreeTombstone`
- `path_blocked_by_runtime_subtree_tombstone`
- `note_runtime_subtree_tombstones_for_events`
- `cleanup_runtime_subtree_tombstones_locked`
- `clear_runtime_subtree_tombstones_for_path`

可复用方向：

- 已有 subtree 级 tombstone 的雏形。
- 当前更像 runtime-only、TTL、Vec 实现，不足以解决百万文件删除和 stable.v7 只读冷段问题。
- 后续正式方案应升级为可持久化、可 compaction 消化的 subtree / segment tombstone。

### 5.3 I/O Governor / TokenBucket / PSI Backoff

路径：`src/io_governor.rs`

已有能力：

- `IoGovernorConfig`
- `TokenBucket`
- `BackoffPolicy`
- `parse_io_pressure`
- `IoGovernor::before_io`

可复用方向：

- 作为冷层 Boost 和 ReconciliationWorker 的统一限流底座。
- PSI 超阈值时退避冷扫，而不是让后台 repair 抢占前台查询。

### 5.4 QueryVerifyBudget 与冷结果验真

路径：`src/index/tiered/query.rs`

已有能力：

- `QueryVerifyBudget`
- `annotate_query_results`
- `validate_cold_result`
- `apply_query_delete`
- `enqueue_dirty_parent`
- `DirtyReason::QueryHitStale`

可复用方向：

- 证明 fd-rdd 已经有“查询路径 bounded metadata stat”的基础。
- 后续 Boost 不应成为查询正确性的前置条件；正确性仍由 bounded verification 和 stale repair 兜底。

### 5.5 Ephemeral Watch / FastScanLeaseKind

路径：`src/event/tiered_watch.rs`、`src/main.rs`

已有能力：

- `EphemeralWatchConfig`
- `EphemeralWatchDecision`
- `FastScanLeaseKind`
- `grant_fast_scan_lease`
- `grant_fast_scan_leases`
- `bootstrap_fast_scan_dirs`
- `fast_scan_tick`

可复用方向：

- 可作为“临时照顾冷目录”的执行机制之一。
- 必须补充独立预算、准入控制和前台延迟保护，避免抢正式 L1/L2 热区资源。

### 5.6 Snapshot / Overlay / Delta 边界

路径：`src/index/tiered/snapshot.rs`、`src/index/tiered/events.rs`、`src/index/tiered/mod.rs`

已有能力：

- pending flush / auto flush / periodic flush 控制。
- snapshot 边界写入 v7 / stable v7。
- delta / overlay dirty 检测。

可复用方向：

- 可作为 tombstone 生命周期结束和冷段 compaction 的边界。
- 当前讨论不直接判断是否实现 diff snapshot；正式方案需要单独审查 materialize snapshot 的内存尖峰和 v7 写入路径。

## 6. 当前设计收敛

本轮讨论后的干净目标不是“把 L3 变成 L1”，而是：

1. L3 冷层可以获得临时调度关注，但不能获得正式 L1 身份。
2. Boost 是可撤销调度机会，不是强一致、常驻内存或独占 watcher 承诺。
3. Aging 只负责提高候选评分，不直接发放资源。
4. Round-robin 只发现候选，不执行重 I/O。
5. DirtyQueue 是合并式状态集，不是无界 FIFO 事件日志。
6. DirtyQueue 的 scope 升级是精度降级，不是扫描放大；父级 dirty 必须按预算切片处理。
7. 查询正确性靠 bounded verification，不靠后台 repair 及时完成。
8. 大规模删除靠 subtree / range / segment tombstone 与 compaction 消化，不靠逐文件 tombstone 常驻内存。
9. Tombstone overlay 必须可编译、可跳过、可压缩，不能让前台 cold query 长期承担复杂 generation-prefix 计算。
10. 后台 repair 必须受 TokenBucket、I/O Governor、PSI backoff 和前台延迟保护。

最终确认的模型是：

```text
Subtree / Segment Tombstone
+ Compiled Tombstone Overlay
+ Revocable Scheduling Lease
+ Cost-aware Coalescing DirtyQueue
+ Bounded Scan Slice
+ PSI / Token Backoff
+ Query-time Bounded Verification
+ Compaction Trigger by CPU/I/O Debt
```

其中最容易误导实现的命名需要避免：

- 不再使用 `boost_tier: L1-like` 作为核心字段，避免让实现把 Boost 当正式 tier。
- 建议改为 `BoostMode::{EphemeralWatch, FastScanLease, ScanSlice}`，表达“临时执行方式”而不是“层级身份”。
- `DirtyQueue` 的父级合并结果必须携带 cost hint、cursor 和 max slice budget。
- `TombstoneScope` 至少支持 `Path`、`Subtree`、`SegmentRange`，生命周期以 `until_compacted` / `until_superseded_by_new_snapshot` 为主，而不是简单 TTL。

## 7. 正式方案的证伪条件

后续若要转为正式方案，需要先设计可证伪指标：

- 批量扫描不能导致正式热目录 watcher 被挤出。
- 高 PSI 下前台查询 P95 / P99 不能因冷层 repair 明显刺穿。
- 删除百万文件冷目录不能产生百万级常驻 tombstone。
- DirtyQueue 过载后不能丢失“某子树需要重新对账”的语义。
- DirtyQueue scope 升级不能把 2 个叶子变更放大为无上限父级递归扫描。
- Coalesced parent scope 每轮扫描必须受 stat 数、目录项数、耗时和 PSI 预算截断。
- Tombstone filter 不能让 cold query 的 CPU 成本随 tombstone 层数无界增长。
- Tombstone overlay 超过数量、重叠深度、覆盖比例或 CPU 成本阈值时必须触发 compaction。
- Boost 租约撤销后，查询结果仍能通过 bounded verification 给出正确或可解释结果。
- snapshot / compaction 不能出现比当前全量物化更严重的 RSS 尖峰。

## 8. M2 VM 原型试错记录（2026-06-15 至 2026-06-16）

本节记录 M2 冷层轮转原型在虚拟机压测脚本中的试错过程。它不是最终验收结论，只用于把“设计设想”拉回到可观测指标和可证伪实验。

### 8.1 初始 home A/B：没有真正触发 M2

第一轮使用 `$HOME` 作为测试根目录，对比 `m2-rotating-home` 与 `m2-baseline-home`。

观测结果：

- 实验组 `rotating_cold_window_active_dirs_max = 0`。
- 实验组 `cold_freshness_age_p95_secs_max = 0`。
- baseline 与实验组的 CPU、RSS、canary 可见性几乎一致。
- active canary create / rename p95 约 `0.256s`，delete / old hidden 为毫秒级。

阶段判断：

- 这轮只能证明“开启 M2 在该 home 场景下没有明显额外成本”，不能证明 M2 有效。
- `$HOME` 场景没有稳定造出 L2 / L3 冷目录候选，冷层轮转没有实际参与。
- baseline 命令里曾同时出现 `--rotating-cold-window` 与 `--no-rotating-cold-window`，虽然最终以关闭为准，但命令表达有歧义，后续 A/B 必须避免这种写法。

### 8.2 forced 参数仍未触发：问题转向候选与租约路径

第二轮压低 watcher 与 L0 成本参数：

- `--max-watch-dirs 2048`
- `--l0-max-cost-per-root 128`

观测结果仍然是：

- `rotating_cold_window_active_dirs_max = 0`
- `cold_freshness_age_p95_secs_max = 0`

阶段判断：

- 这说明问题不是 workload 太轻，而是测试布局还没有造出可轮转的冷目录，或者 root / run_dir / canary 布局污染了根目录变化。
- 后续必须用专门 fixture，把 run_dir 放到被测 root 之外，并强制制造冷层滞后。

### 8.3 fixture + `/tmp` run_dir：造出冷层，但 M2 未接管

第三轮改为专门 fixture：

- 测试根：`$HOME/fd-rdd-m2-roots/cold-a`、`cold-b`、`hot`
- run dir：`/tmp/fd-rdd-m2-runs/...`
- `--max-watch-dirs 8`
- `--l0-max-cost-per-root 1`
- canary 放在 `cold-a/canary` 子目录。

观测结果：

- `cold_freshness_age_p95_secs_last = 742`
- `cold_freshness_age_p99_secs_max = 742`
- `rotating_cold_window_active_dirs_max = 0`
- active canary create / rename 全部 timeout。

阶段判断：

- fixture 已经成功造出冷层滞后。
- M2 原型没有接管，暴露出实现 bug：Ephemeral Watch / fast scan lease 发放失败时直接取消轮转租约，未降级为 scan-only repair。

### 8.4 修复 scan-only fallback 后：M2 接管，但深层 canary 仍失败

修复内容：

- 当 Ephemeral Watch 或 fast scan lease 发放失败时，不再取消轮转租约。
- 改为保留租约并降级为 `scan_only`，交给 `PeriodicColdScan` 执行低成本 repair。
- 对应提交：`75586c8 fix: keep cold rotation scan fallback`。

重跑 fixture 后的观测结果：

- `rotating_cold_window_active_dirs_max = 2`
- `cold_freshness_age_p95_secs_last = 172`
- active canary create / rename 仍 timeout。

阶段判断：

- 修复有效，M2 已经能接管冷目录并降低 cold freshness age。
- 但 canary 放在 `cold-a/canary` 新建子目录里，不适合测当前 scan-only 的一层 repair 能力。
- 如果正式机制要覆盖新建深层目录，需要继续设计 child shard / deep repair；否则 nested canary timeout 是预期风险，不应被误判为整个 M2 无效。

### 8.5 direct canary：M2 原型进入“可用原型”状态

第五轮将 canary root 直接改为 `cold-a`，避免新建子目录干扰。

实验组 `m2-rotating-cold-fixture-direct-canary` 观测结果：

- `rotating_cold_window_active_dirs_max = 1`
- `cold_freshness_age_p95_secs_last = 174`
- create visible：`13/13` 成功，p50 约 `5.13s`，p95 约 `5.38s`，p99 / max 约 `29.999s`
- rename new visible：`13/13` 成功，p50 / p95 约 `5.1s`
- delete hidden：`13/13` 成功，毫秒级
- rename old hidden：`13/13` 成功，毫秒级
- CPU p95 约 `1.36%`
- RSS max 约 `17.4MB`

阶段判断：

- M2 已触发，且资源成本在该 fixture 下较低。
- 冷目录 create / rename / delete 的最终正确性通过。
- p99 接近 `rotating_tick_secs = 30`，说明最坏可见性仍受 tick 粒度约束。

### 8.6 baseline direct canary：active canary 被 query-triggered repair 混淆

随后用相同 fixture 跑 baseline direct canary。

baseline `m2-baseline-cold-fixture-direct-canary` 观测结果：

- `rotating_cold_window_active_dirs_max = 0`
- `cold_freshness_age_p95_secs_last = 744`
- create visible：`13/13` 成功，p50 约 `5.065s`，p95 约 `5.098s`
- rename new visible：`13/13` 成功，p50 约 `5.058s`，p95 约 `5.077s`
- delete / old hidden 仍为毫秒级
- CPU / RSS 也保持较低

阶段判断：

- M2 确实降低了 cold freshness age：实验组约 `174s`，baseline 约 `744s`。
- 但 active canary 搜索可见性没有打赢 baseline。
- 原因是 active canary 在写入后立即轮询搜索，baseline 可以通过 query miss / fast scan 补偿路径在约 `5s` 内修复，因此这个指标测到的是“查询触发修复”，不是“M2 后台主动追平”。
- active canary 不能作为 M2 背景收益验收口径，只能作为最终可见性和正确性 smoke test。

### 8.7 脚本口径修正：区分 active 与 passive canary

为避免 active canary 混淆，脚本已拆出 passive canary：

- active canary：写入后立即轮询搜索，允许测到 query miss / fast scan 补偿。
- passive canary：先写入 / rename / delete，等待 settle 后才首次查询，用于观察后台是否已经主动追平。

脚本输出新增指标包括：

- `canary_active`
- `canary_passive`
- `passive_first_query`
- `passive_positive_first_query`
- `rotating_cold_window_cycle_progress_pct_max`
- `rotating_cold_window_scan_only_dirs_last`
- `cold_freshness_age_p95_secs_delta`
- `proc_sampler_triggered_watches_last`

阶段判断：

- 之后判断 M2 是否有用户可感收益，应优先看 passive first-query 成功率，而不是 active create / rename p50。
- 如果实验组 passive first-query 成功率明显高于 baseline，同时 CPU / RSS / fd / dirty queue 不明显恶化，才算证明“M2 背景轮转有实际价值”。

### 8.8 当前临时结论

临时结论按四个层面拆开：

1. **机制触发：初步成立。** 专门 fixture 下 M2 可以获得 active dirs，并降低 cold freshness age。
2. **成本边界：初步可控。** 当前样本里 CPU p95 约 `1%` 左右、RSS 约 `17MB` 级，没有看到资源爆炸。
3. **搜索可见性收益：尚未证明。** active canary 被 query-triggered repair 混淆，baseline 也能约 `5s` 可见。
4. **深层目录能力：仍有缺口。** scan-only fallback 对 direct canary 有效，但 nested canary 暴露出 child shard / deep repair 仍需设计。

因此，M2 目前不能宣称“收益已成立”，只能宣称：

> 冷层轮转原型已能触发并降低 cold freshness age；资源成本暂时可控；用户可见收益必须用 passive canary A/B 重新证明。

### 8.9 下一轮建议 A/B 口径

下一轮推荐使用 fixture + passive canary，并重点比较：

- `passive_positive_first_query.success_rate`
- `passive_first_query.operations.passive_create_first_query.ok`
- `passive_first_query.operations.passive_rename_new_first_query.ok`
- `rotating_cold_window_active_dirs_max`
- `rotating_cold_window_scan_only_dirs_last`
- `cold_freshness_age_p95_secs_delta`

实验组应开启 `--rotating-cold-window`；baseline 只使用 `--no-rotating-cold-window`，不要在同一命令里同时出现正反开关。

### 8.10 Passive A/B 首轮结果：出现背景轮转收益信号

用户按 8.9 口径补跑了首轮 passive A/B：

- A：`m2-rotating-passive`，run dir 为 `/tmp/fd-rdd-m2-runs/20260616T113730Z_rotating_passive`。
- B：`m2-baseline-passive`，run dir 为 `/tmp/fd-rdd-m2-runs/20260616T121135Z_baseline_passive`。
- 两组均为 1800 秒，sample interval 10 秒，fixture 为 `cold-a`、`cold-b`、`hot`，passive settle 约 120 秒。

核心结果：

| 指标 | A：rotating | B：baseline | 阶段判断 |
| --- | ---: | ---: | --- |
| `rotating_cold_window_active_dirs_max` | 1 | 0 | A 确认触发 M2 |
| `rotating_cold_window_cycle_progress_pct_max` | 100 | 0 | A 完成轮转周期 |
| `rotating_cold_window_scan_only_dirs_last` | 6 | 0 | A 走 scan-only fallback |
| `passive_first_query.success_rate` | 12/12 = 1.0 | 10/12 = 0.8333 | A 高 16.67 个百分点 |
| `passive_positive_first_query.success_rate` | 6/6 = 1.0 | 4/6 = 0.6667 | A 高 33.33 个百分点 |
| `passive_create_first_query` | 3/3 | 2/3 | A 避免 1 次 create 首查 miss |
| `passive_rename_new_first_query` | 3/3 | 2/3 | A 避免 1 次 rename 首查 miss |
| active create p95 | 5.202s | 5.191s | 基本无差异 |
| active rename p95 | 5.162s | 5.169s | 基本无差异 |
| CPU p95 | 2.004% | 1.642% | A 绝对增加约 0.36 个百分点 |
| RSS max | 18.80MB | 18.93MB | 基本持平 |
| fd max | 16 | 16 | 无额外 fd 压力 |
| `dirty_queue_len_max` | 0 | 0 | 未造成队列积压 |
| `budget_blocked_last` | 0 | 0 | 未触发轮转预算阻塞 |

阶段判断：

- 这轮 passive A/B 首次给出了 M2 背景轮转的正向证据：实验组在 settle 后首次查询的 positive 命中率为 `100%`，baseline 为 `66.67%`。
- active canary 仍然几乎没有差异，反向验证了 8.6 的判断：active 指标主要测 query-triggered repair，不适合证明后台轮转收益。
- 成本侧暂时可接受：CPU p95 绝对增加约 `0.36` 个百分点，RSS / fd / dirty queue 基本持平。
- `cold_freshness_age_p95_secs_max` 在本轮为 A `173s`、B `293s`，方向上仍支持 A；但两组 `last = 0` 且 `delta = 0`，所以本轮不能把 `cold_freshness_age_p95_secs_delta` 当作决定性指标。

更新后的临时结论：

> M2 从“能触发且成本可控”推进到“在 fixture + passive canary 下出现用户可见收益信号”。但 positive 样本量只有 6 次，仍不能作为生产级验收结论。

下一步正式化前需要补足：

- 至少重复多轮 passive A/B，避免 6 个 positive 样本带来的偶然性。
- 延长运行时间或缩短 passive interval，扩大 create / rename positive 样本。
- 加入 delete storm、rename storm、git checkout storm 后确认收益不被极端 workload 反转。
- 明确验收阈值，例如 positive first-query 成功率提升不少于 20 个百分点，同时 CPU p95 增幅小于 1 个百分点、RSS 增幅小于 10%、dirty queue 不积压。
