# 2026-05-29 待执行方案包合并开发计划

## 范围

本计划合并以下 7 个待执行方案包：

- `202605291305_runtime-boundary-closure`
- `202605291311_query-experience-features`
- `202605291312_watcher-balanced-evolution`
- `202605291313_disk-first-memory-light`
- `202605291314_arcswap-zero-copy-governance`
- `202605291315_storage-legacy-cleanup`
- `202605291316_hardlink-physical-grouping`

目标不是并行铺开所有功能，而是按依赖和风险把开发切成可验证批次。每个批次必须保持 `cargo fmt`、`cargo test -q` 与相关 smoke 脚本可通过，完成后更新对应方案包 `task.md` 并迁移到 `helloagents/history/2026-05/`。

## 总体优先级

1. 先收口运行时可靠性边界，避免后续功能建立在不完整的 watcher、quarantine、clock、I/O governor 语义上。
2. 再压实查询和内存热路径，缩短 ArcSwap guard 生命周期，补足 MemoryReport 与低内存 profile。
3. 先实现 hardlink physical grouping，再接入 `dupe:` 的 hardlink 阶段。
4. 目录索引、`empty:`、`dupe:`、内容索引分阶段进入查询体验，不一次性把全文索引默认打开。
5. watcher balanced 作为体验加速层推进，不能绕过 mount policy、exclude、freeze gate。
6. storage legacy cleanup 以审计和隔离为先，删除或迁移旧路径必须有兼容读取测试。

## 依赖关系

- `query-experience-features` 依赖 `hardlink-physical-grouping` 提供 `dupe:` hardlink 阶段的数据源。
- `watcher-balanced-evolution` 依赖 `runtime-boundary-closure` 的 mount policy reason matrix、clock skew 和 freeze gate 语义。
- `disk-first-memory-light` 与 `arcswap-zero-copy-governance` 共享 MemoryReport、I/O Governor、query hot path 指标，应连续开发。
- `storage-legacy-cleanup` 依赖前面批次的测试稳定性，避免在功能开发中同时移动持久化边界。
- 内容索引依赖 I/O Governor 和 mount policy，不应早于运行时边界收口。

## 批次 0：基线与执行护栏

### 目标

建立执行前基线，减少后续多方案串行开发时的回归定位成本。

### 状态

2026-05-29 已完成首轮基线、HTTP smoke、`/health.diagnostics` 字段确认和 storage legacy 只读审计清单。详细记录见 `helloagents/wiki/development-execution-2026-05-29.md`。

### 工作项

- 记录当前 `cargo test -q`、HTTP smoke、`scripts/smoke-search-syntax.sh` 结果。
- 记录当前 `/memory`、`/health`、`/watch-state`、`/debug/tiered-watch` 可用字段。
- 为 7 个方案包建立执行顺序记录，避免完成后遗漏迁移。
- 确认 `helloagents/history/index.md` 的迁移格式。
- 先做 storage legacy 只读审计清单：标出 v6 snapshot、旧 LSM API、`PersistentIndex` 旧结构、`refresh_base()`、L1 cache key 的当前调用点和风险等级。
- 明确前置开发期间不可触碰的旧路径：兼容读取、恢复路径、测试辅助入口只记录边界，不在批次 0 删除或迁移。

### 验收

- 基线命令输出可复现。
- 若已有失败项，先记录为已知问题，不把后续新失败混入旧失败。
- storage legacy 只读审计清单可供后续批次查阅，真正删除、迁移或隔离仍留到批次 7。

## 批次 1：Runtime Boundary Closure

### 目标

把 Stage H/I 的运行时边界从“契约和基础能力”推进到“真实闭环”。

### 状态

2026-05-29 已完成 1.1 mount policy / FUSE probe 小批次中的共享计数和 watcher 注册前拒绝：full build、rebuild、fast-sync、immediate scan、dynamic watch、ephemeral watch 使用同一 mount policy counters 并进入 `/health.diagnostics.watchers`。随后已完成 1.2 quarantine verify：启动时从 sidecar 安装 Freeze Gate，WAL 按原始 root/file 记录顺序回放，后台 worker 在 mount identity 匹配后先 append `ONLINE_ROOT`，再解除 freeze 并入队 affected prefixes 局部 scan。case/pathconf、clock/io diagnostics 仍待后续小提交。

### 工作项

- watcher dynamic scan、ephemeral watch 注册、子挂载点探测统一执行 mount policy。
- FUSE/SSHFS 探测进入后台线程和 timeout cache，扫描线程只消费结果或保守拒绝。
- 实现 quarantine sidecar 后台 verify worker：先 append `ONLINE_ROOT`，再解除 freeze，再入队局部 scan。
- 补完 pathconf fallback、只读/无权限/缺失 root `Unknown` 语义。
- clock skew detector 接入 dirty/WAL flush loop，漂移超过 1s 时挂起 mtime 剪枝并入队局部对账。
- I/O Governor 真实 loop 接入 token bucket、PSI、ioprio 状态计数。
- `/health.diagnostics` 收口 watchers/storage/clocks/io/security 细分字段。

### 执行拆分

批次 1 必须按小提交推进，顺序保持不变：

1. mount policy / FUSE probe：统一 watcher、ephemeral watch、子挂载点入口和 timeout cache。
2. quarantine verify：实现 sidecar 后台 worker、Freeze Gate 恢复顺序和 online 后局部 scan。
3. case / pathconf：补完 pathconf fallback、`Unknown` 语义和 Unicode fold byte offset 回归。
4. clock / I/O diagnostics：接入 clock skew、mtime cutoff、token bucket、PSI、ioprio 与 diagnostics 字段。

### 验收

- 离线 root 不产生破坏性 tombstone，查询默认不可见。
- 恢复 root 后先写 WAL online 事件，再解除 freeze，并能通过局部 scan 复活路径。
- FUSE timeout 不阻塞扫描线程。
- ioprio `EPERM` 或 unsupported 不导致扫描失败。
- `cargo test -q` 与 runtime boundary 单点测试通过。

## 批次 2A：ArcSwap / MemoryReport 指标

### 目标

在继续调 flush/compaction 策略前，先把查询 guard、generation、MemoryReport 和 full-scan metrics 做稳。调优必须建立在可靠仪表盘上。

### 工作项

- 审计 HTTP、UDS、internal query helper，调整为 guard 内只计算候选 DocId 和最小 handle。
- materialize、serialize、stdout、IPC streaming 前释放 ArcSwap guard。
- 增加 guard hold duration、active guard count、slow guard warning。
- 限制同一时间最多一个 rebuild/full build，冷却期合并请求。
- MemoryReport 增加 pending events、dirty set、dedupe queue、query cache、generation strong refs、rebuild staging。
- 增加 exact query、fuzzy fallback、no trigram hint、full-scan candidates/elapsed metrics。

### 验收

- 查询输出和 IPC streaming 期间不持有 ArcSwap guard。
- rebuild 期间查询不中断，旧代强引用可观测。
- `/memory` 能解释 pending/dirty/guard/generation 等关键来源。
- 短查询和 fuzzy fallback 的 full-scan 成本可在 metrics/diagnostics 中解释。

## 批次 2B：memory_light profile

### 目标

在 2A 的观测指标稳定后，再定义和验证低内存配置档，避免没有可靠指标就调整 flush、compaction 和 WAL 策略。

### 工作项

- 定义 `memory_light` profile，并接入 CLI/config。
- 低内存 profile 调整 L2 flush、batch flush 最大滞留时间、compaction cooldown、WAL seal/checkpoint。
- 与默认 profile 做 churn、RSS、P95、full-scan 指标对照。
- 确认强制 flush、clean shutdown final snapshot、offline/freeze gate 破坏性写入保护不受低内存策略影响。

### 验收

- `memory_light` 与默认 profile 行为都有测试覆盖。
- 相同 churn 负载下，低内存 profile 的 RSS 峰值和稳定值有可解释改善。
- P95 查询延迟保持可接受，冷查询退化能由 2A 指标解释。
- WAL replay、snapshot/restart 后 `memory_light` 配置仍生效。

## 批次 3：Hardlink Physical Grouping

### 目标

提供按物理文件身份聚合路径的派生视图，为 `dupe:` 第一阶段打基础。

### 工作项

- 设计 `HardlinkGroup` schema 与 physical dedupe stats。
- 基于当前可见 entry 临时聚合 `FileKey`，或实现 flat CSR 辅助表。
- 过滤单路径 group，支持 `min_links`、root/path prefix 范围。
- 覆盖 create/link、delete alias、rename alias、snapshot reload 语义。
- diagnostics 暴露 hardlink group 计数和最大 group。

### 验收

- 同一物理文件多路径可被分组，单路径文件不成组。
- 删除或 rename 某个 alias 不影响其他 alias。
- full build 不恢复 inode 去重，搜索主键仍是 path/docid。
- 该 helper 可直接供 `dupe:` hardlink 阶段调用。

## 批次 4：Query Experience Features 第一阶段

### 目标

先完成目录索引、`empty:` 与 `dupe:` hardlink 阶段，把用户可见功能做成最小闭环。

### 工作项

- 增加 entry kind，区分 file/directory。
- full build 与 watcher/scan 增量维护目录 entry。
- query materialize 与 HTTP `/search` 返回目录 type。
- 实现 `type:dir`、`type:file` 目录语义。
- 维护 parent index 或 directory metadata 的直属 child count。
- 实现 `empty:` DSL，明确 ignore、mount policy、frozen/offline root 边界。
- 接入 hardlink grouping，实现 `dupe:` hardlink group 查询和 reason/confidence 输出。

### 验收

- 普通文件名查询结果和排序不明显退化。
- `type:dir` 只返回目录。
- `type:dir empty:` 能定位真实空目录，且不把 ignore/mount/frozen 边界误判为空。
- `dupe:` 能返回 hardlink 重复组和判定理由。
- HTTP smoke 覆盖目录、空目录和 hardlink dupe 查询。

## 批次 5：Query Experience Features 第二阶段

### 目标

在 I/O 和内存边界已可观测后，引入内容重复候选和默认关闭的轻量内容索引。

### 工作项

- `dupe:` 增加 size + partial hash 候选和 full hash 确认。
- diagnostics 暴露 hash queue、跳过原因和最近耗时。
- 定义内容索引配置：enable、max_file_size、include_ext、exclude_ext。
- 实现后台低优先级 content worker，接入 I/O Governor 和 mount policy。
- 实现 `content:` 或 `text:` DSL。
- 未启用内容索引时返回明确 unsupported 行为。

### 验收

- 默认关闭内容索引时不拖慢文件名/路径查询。
- 开启后能在配置范围内搜索文本内容。
- 大文件、拒绝挂载、离线 root、exclude 目录均按策略跳过并可诊断。
- `dupe:` 不做自动删除，只展示重复候选和置信度。

## 批次 6：Watcher Balanced Evolution

### 目标

让 balanced/tiered watcher 在预算有限时更快发现和覆盖用户正在使用的项目目录。

### 工作项

- 定义 project marker 列表和配置。
- L1/L2/L3 scan 识别 project root candidate，marker 命中提升 event score。
- 新项目 root 预算足够时晋升 L0，预算不足时申请 ephemeral watch lease，再不足时进入 high-priority L1 scan。
- nested project 增加祖先/后代关系解释和预算隔离。
- Directory manifest crawler 维护 child count、names hash、mtime range、last scan generation。
- 区分 logical cost、kernel wd cost、skipped cost，并在 `/watch-state` 暴露。
- fanotify 只做权限模型、事件语义、资源占用、fallback 策略评估。

### 验收

- 新建项目 marker 后项目根能进入 candidate/promotion/lease 路径。
- 写入项目内新文件后短时间可搜索。
- nested project 状态可解释，不长期互相抢占。
- project marker 不绕过 exclude 或 mount policy。
- low_power/strict 模式行为不被 balanced 策略破坏。

## 批次 7：Storage Legacy Cleanup

### 目标

降低持久化旧路径的认知成本，避免 legacy v6/旧 LSM API 被误带回热路径。

### 工作项

- 审计 v6 snapshot、旧 LSM disk layer、`PersistentIndex` 旧结构、`refresh_base()`、L1 cache key。
- 将兼容读取移动到明确 compatibility 边界，热路径删除或隔离旧 API。
- 收敛 runtime 主存储到 `FileEntryIndex + PathTableV2` 或当前等价紧凑结构。
- `refresh_base()` 增加注释、diagnostics counter 和防误用测试。
- 评估 generation-aware L1 cache key，确保对外 API 不暴露 DocId。
- 写 rkyv manifest schema、依赖/CVE、离线构建评估，决定接入或继续延后。

### 验收

- 热路径不引用 legacy v6/旧 LSM 加载逻辑。
- 旧快照读取兼容仍有测试覆盖。
- 普通事件 apply 和查询常规路径不调用 `refresh_base()`。
- WAL replay、snapshot/rebuild、cold mmap 查询一致性不回退。

## 统一测试矩阵

每个批次完成时至少执行：

- `cargo fmt`
- `cargo test -q`
- 受影响模块单点测试
- 对应 HTTP smoke 或脚本 smoke
- 对应方案包的体验 fixture / 用户路径测试必须通过

跨批次完成后执行：

- `scripts/smoke-search-syntax.sh`
- runtime boundary smoke
- query experience fixture
- watcher project fixture
- memory_light churn profile
- old snapshot upgrade/reload fixture

## 风险控制

- 运行时边界批次中不得在 query hot path 执行 mount/case/clock 探测。
- FUSE 探测 timeout 后不能尝试杀死 D 状态线程，只能丢弃结果并记录诊断。
- 内容索引默认关闭，避免把全文索引风险扩散到默认体验。
- `dupe:` 只查询和展示，不执行删除、合并或自动清理。
- storage cleanup 不删除旧格式读取兼容，除非已有迁移和恢复测试覆盖。
- watcher project promotion 不能绕过 mount policy、exclude、freeze gate。

## 推荐执行顺序

1. 批次 0：基线与执行护栏。
2. 批次 1：执行 `202605291305_runtime-boundary-closure`。
3. 批次 2A：先执行 `202605291314_arcswap-zero-copy-governance` 的 guard、generation、MemoryReport 和 full-scan metrics。
4. 批次 2B：再执行 `202605291313_disk-first-memory-light` 的 profile、flush、compaction 与 churn 对照。
5. 批次 3：执行 `202605291316_hardlink-physical-grouping`。
6. 批次 4：执行 `202605291311_query-experience-features` 的目录、`empty:`、hardlink `dupe:` 部分。
7. 批次 5：继续 `query-experience-features` 的内容 hash 与内容索引部分。
8. 批次 6：执行 `202605291312_watcher-balanced-evolution`。
9. 批次 7：执行 `202605291315_storage-legacy-cleanup`。

## 收尾要求

- 每完成一个方案包，更新其 `task.md` 状态。
- 完成批次涉及的 README、CHANGELOG 和 wiki。
- 迁移已完成方案包到 `helloagents/history/2026-05/`。
- 更新 `helloagents/history/index.md`。
- 重新扫描 `helloagents/plan/`，确认只剩未执行方案包。
