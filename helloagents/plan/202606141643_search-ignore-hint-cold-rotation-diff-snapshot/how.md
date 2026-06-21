# 搜索忽略感知、冷层轮转更新与增量快照方案设计

## 总体策略

本包拆成三个可独立交付的阶段，优先级从“立刻改善用户解释”到“系统性降低资源峰值”：

1. **M1：Ignored Hint** — 搜索结果为空或显式要求诊断时，给出 `.gitignore` 忽略提示。
2. **M2：Rotating Cold Freshness Window** — 用轮转临时覆盖窗口加速 L2/L3，而不是把冷层整棵强行升 L0。
3. **M3：Delta Snapshot** — 将常规快照改为增量段写入，完整物化只保留给后台 compaction。

## M1：搜索忽略感知

### 用户体验

查询命中 ignored 文件时，不再静默返回空。建议提供两层能力：

- `/search` 保持兼容数组响应；当普通结果为空且 query 形态足够明确时，可附加一个 `SearchResult` 风格的提示项：
  - `path`: 被忽略的真实路径
  - `type`: `file` / `dir`
  - `freshness`: `ignored_by_rule`
  - `index_tier`: `IgnoredByRule`
  - `validated`: `true`
  - `reason`: `ignored_by_gitignore`
  - `confidence`: `1.0` 或按匹配强度填写
- 新增或扩展诊断查询参数：`explain=ignored` / `include_ignored_hint=true`，让客户端可以显式请求 ignored 提示；`fd-rdd-query` 可默认开启，并用醒目的文本标注“存在但被 ignore 规则排除”。

为避免破坏现有客户端，第一版建议默认只在“结果为空 + query 明确”时返回提示项；宽泛 query 只在显式参数开启时执行。

### 检测机制

不要在查询线程做全量遍历。采用“双路径”：

1. **直接路径探测**：query 含 `/`、绝对路径、相对路径或带明确文件名时，先检查该路径是否位于 configured roots 内，文件是否存在，再通过扩展后的 `IgnoreFilter` 判断是否被 ignore。
2. **Ignored Hint Sidecar**：后台低优先级构建轻量 sidecar，只记录 ignored 文件的 basename、路径、kind、mtime、规则来源摘要，不读取内容，不进入主索引。

Sidecar 构建策略：

- 独立于主索引，默认小预算运行，可暂停、可重建。
- 使用 configured roots、mount policy、exclude_dirs 和安全边界裁剪。
- 遍历时关闭 ignore crate 的自动跳过，再用 `IgnoreFilter` 判断单个路径是否 ignored；命中后写入 sidecar。
- 对大型目录采用分片 cursor，避免一次性扫描 home。
- sidecar 只服务提示，不参与普通排序、TopK 或内容查询。

`IgnoreFilter` 需要从布尔判断扩展为可解释结果：

- `is_ignored(path) -> bool` 保留兼容。
- 新增 `explain(path) -> Option<IgnoreMatchExplanation>`：
  - ignore 类型：`.gitignore` / `.ignore` / `.git/info/exclude` / global ignore
  - 规则来源文件
  - 行号和 pattern（如果 ignore crate 暴露；若不可得，第一版退化为来源文件 + ignored）
  - 是否为 negation 规则后的最终 ignore

### API 与观测

新增配置：

- `ignored_hint.enabled = true`
- `ignored_hint.max_matches_per_query = 8`
- `ignored_hint.query_timeout_ms = 30`
- `ignored_hint.sidecar_budget_entries_per_tick = 2000`
- `ignored_hint.sidecar_max_entries = 100000`

新增 metrics / health 字段：

- `ignored_hint_enabled`
- `ignored_hint_sidecar_entries`
- `ignored_hint_probe_count`
- `ignored_hint_probe_timeout_count`
- `ignored_hint_matches_returned`
- `ignored_hint_last_rule_source`

### 风险控制

- 不读取 ignored 文件内容。
- 不对外部路径返回提示。
- 宽泛 query 不触发全量扫描。
- HTTP 仍保持 localhost/same-user 语义；未来若开放远程访问，ignored hint 必须默认关闭或需要授权。
- `exclude_dirs` 命中项不作为 `.gitignore` 提示返回；它属于 fd-rdd 的索引策略排除，避免把大量 `node_modules` 当作用户误解提示。

## M2：冷层轮转更新窗口

### 设计取舍

本阶段的轮转机制设计引用并服从过程包：

- `helloagents/plan/202606141747_方案设计过程-冷层轮转boost模型讨论-非正常方案包/why.md`
- `helloagents/plan/202606141747_方案设计过程-冷层轮转boost模型讨论-非正常方案包/how.md`
- `helloagents/plan/202606141747_方案设计过程-冷层轮转boost模型讨论-非正常方案包/task.md`

该过程包不是可执行方案包，但其中关于 Boost、DirtyQueue、Tombstone 和查询验真的语义边界，作为 M2 的设计输入。

不建议字面执行“L3 临时变 L1、L1 临时变 L3”的全局 tier 交换。正式 tier 表示调度状态和预算责任，直接交换会破坏热点目录连续性。

建议落地为 **Rotating Cold Freshness Window**：

- 正式 L0 不被抢占。
- 轮转窗口从 L3/L2 中选一批最久未扫描、event_score 高、近期 query miss/stale 相关的目录。
- Boost 不是 `L1-like tier`，而是可撤销调度租约；执行模式只能是 `EphemeralWatch`、`FastScanLease` 或 `ScanSlice`。
- 对小成本目录可以尝试 Ephemeral Watch，但预算必须与正式 hotset watcher 隔离。
- 对中等候选发放 fast scan lease 与 sentinel bootstrap。
- 对大成本候选只进入 DirtyQueue 分片 scan，禁止整棵 recursive watch。
- 窗口 TTL 到期后撤销临时覆盖，选择下一批目录。
- 每个完整 cycle 结束后刷新冷层最坏新鲜度估算。
- 查询正确性不依赖 Boost 是否存在，仍依赖 query-time bounded verification。

### 候选选择

候选分数建议：

- `age_since_last_scan`：越久未扫越高。
- `event_score`：近期活跃更高。
- `query_miss` / `query_stale_hit`：用户刚搜索过的目录更高。
- `watch_cost`：成本越低越优先进入 ephemeral watch；高成本只 scan。
- `manifest_changed_probability`：directory manifest 最近变化或不可跳过时提高。

### 执行路径

复用现有机制，不新增第三套 watcher：

- 小目录：`WatchCommand::AddEphemeral`，注册成功后只按预算执行扫描切片，不把 lease 解释为强一致或常驻承诺。
- 中等目录：fast scan lease + sentinel bootstrap。
- 大目录：DirtyQueue `PeriodicColdScan` 分片 repair，禁止整棵 recursive watch。
- DirtyQueue 必须是 coalescing dirty state table，不是 FIFO event log。
- 父级 scope 合并只代表精度降低，不代表 worker 可以全量递归扫描；每轮必须受 stat 数、目录项数、字节数、耗时和 PSI 预算截断。
- 当合并后的 parent scope 成本过高时，应保留 cursor、反向拆成 child shard 或记录 backlog watermark，而不是扩大扫描。
- Tombstone 必须支持 subtree / segment 表达，并通过 compiled overlay / segment summary 减少 cold query CPU 阻抗；复杂度超限时触发 compaction。

新增配置：

- `tiered_watch.rotating_cold_window_enabled = true`
- `tiered_watch.rotating_cold_window_budget = 128`
- `tiered_watch.rotating_cold_window_tick_secs = 30`
- `tiered_watch.rotating_cold_window_ttl_secs = 180`
- `tiered_watch.rotating_cold_window_max_cost_per_root = 64`
- `tiered_watch.rotating_cold_window_max_dirs_per_tick = 8`

新增观测：

- `rotating_cold_window_enabled`
- `rotating_cold_window_active_dirs`
- `rotating_cold_window_cycle_id`
- `rotating_cold_window_cycle_progress_pct`
- `rotating_cold_window_promoted_to_ephemeral`
- `rotating_cold_window_scan_only_dirs`
- `rotating_cold_window_budget_blocked`
- `cold_freshness_age_p50/p95/p99`
- `dirty_queue_coalesced_scopes`
- `dirty_queue_precision_loss_level`
- `dirty_queue_slice_budget_exhausted`
- `tombstone_overlay_filter_cpu_ns`
- `tombstone_overlay_compaction_due`

### 验收口径

- 大 home 场景下，L3 最坏扫描年龄下降。
- active hotset 查询延迟和 5 秒 fast scan SLA 不回退。
- `Documents`、`Downloads` 这类大目录不被整棵注册 watcher。
- 轮转过程可在 `/debug/tiered-watch` 看见临时覆盖原因和到期时间。
- DirtyQueue 合并不会把少量叶子变更放大成无预算父级递归 scan。
- Tombstone overlay 不会让 cold query CPU 成本随 tombstone 层数无界增长。

### M2 VM 验证与 workload driver

M2 的可用性不能用“正常情况下能搜到”判断，必须在 VM 里用同一套 workload 对 baseline / experiment 做 A/B。baseline 关闭 `rotating_cold_window_enabled`，experiment 开启它，其余 root、seed、脚本、配置和时长保持一致。

验证链路拆成三个角色：

1. **runner + collector**：`scripts/m2-cold-window-vm-bench.py` 只负责隔离启动 fd-rdd、采集 `/health`、`/status`、`/metrics`、`/memory`、`/watch-state` 和 `/proc/<pid>`，并保留 daemon 自带 `reports/metrics/*.json`。
2. **workload driver**：新增 `scripts/m2-cold-window-workload.py`，只在 `--root` 指定的沙箱内制造文件系统操作，写 `workload-events.jsonl`，不直接判定 fd-rdd 是否通过。
3. **analyzer/report**：按 workload 事件时间线和 metrics 时间线对齐，计算 create / rename / delete 可见性、RSS、CPU、DirtyQueue、watcher、冷层 freshness 和 hotset SLA。

workload driver 的第一版接口：

```bash
python3 scripts/m2-cold-window-workload.py \
  --root "$HOME/fd-rdd-vm-workload" \
  --scenario daily,cold-canary,delete-storm,rename-storm,git-storm \
  --duration-secs 3600 \
  --rate normal \
  --seed 42 \
  --events-jsonl workload-events.jsonl
```

安全边界：

- driver 拒绝以 `/`、`$HOME`、项目仓库根目录作为 `--root` 直接运行，只允许操作沙箱子目录。
- 所有破坏性动作必须先解析到 `--root` 内部，再执行删除、rename 或 cleanup。
- 支持 `--dry-run` 输出计划，不写文件。
- 支持 deterministic seed，同一 seed 下 baseline / experiment 的操作序列一致。
- 支持 `daily`、`normal`、`stress`、`chaos` 四档 rate limit，避免 VM 一开始就被打死。

第一版场景：

- `daily`：小文件 create/edit/rename/delete、编辑器临时文件、下载器 `.part -> final`。
- `cold-canary`：在指定冷目录周期创建、rename、删除唯一文件名，用来测冷层可见性延迟。
- `delete-storm`：生成大子树后按批次删除或删除整棵 subtree，用来证明不产生海量逐文件 tombstone。
- `rename-storm`：批量 `file_i -> file_i_new -> file_i`，覆盖 rename 合并、`apply_seq` 和旧路径隐藏。
- `git-storm`：在临时 git repo 中执行 checkout / clean / reset 风格震荡，模拟真实工作区。
- `watcher-drop-proxy`：不直接控制内核 watcher；通过暂停 daemon 或 no-watch baseline 后做磁盘变更，再恢复观察 deferred repair。

`workload-events.jsonl` 至少记录：

- `ts`
- `phase`
- `scenario`
- `operation`
- `path`
- `count`
- `expected_query`
- `started_at`
- `finished_at`
- `ok`
- `error`

首轮通过标准：

- 正确性：create 可见、rename 新路径可见且旧路径隐藏、delete 隐藏，delete / rename storm 后旧结果不复活。
- 收益：冷层 canary p95 比 baseline 下降 ≥ 40%，p99 下降 ≥ 30%；如果 baseline 已很快，实验组 p95 / p99 增幅不超过 10%。
- 热层不退：`fast_scan_coverage_lag_p99_ms <= 5000`，或相对 baseline 增幅不超过 10%。
- 成本受控：CPU p95 增幅不超过 5–10 个百分点；RSS max 增幅不超过 32 MiB 或 10%；FD / watcher cost 不突破预算。
- 队列可解释：`dirty_queue_len` 操作后可回落，budget blocked 指标不能持续单调增长且无回落。

## M3：增量快照 / diff snapshot

### 目标形态

当前常规 snapshot 是完整物化；M3 改为：

- `stable/base.v7`：上一次完整基座，manifest-only cold segment。
- `stable/delta-<id>.v7`：每次 flush 写出的新增/修改条目段。
- `stable/delta-<id>.del`：路径级 tombstone，屏蔽旧 base/delta 中的删除或 rename-from。
- `stable/MANIFEST`：记录 base、delta 列表、顺序、checksum、WAL checkpoint、roots/config fingerprint。
- `events.wal`：继续记录 snapshot 边界后的事件。

查询合并顺序：

1. 内存 overlay / L2
2. delta segments newest → oldest
3. stable base

合并规则沿用 blocked path 语义：新层路径和 tombstone 屏蔽旧层同路径结果，保证 delete → recreate、rename 和修改覆盖正确。

### 常规 flush 流程

1. 短暂冻结事件 apply gate。
2. seal WAL，形成明确 checkpoint。
3. 交换当前 delta/overlay，释放写路径。
4. 后台把本次变更写成 delta v7 + tombstone。
5. 原子更新 manifest。
6. 清理已 checkpoint 的 sealed WAL。
7. 调用 `maybe_trim_rss()`，并记录本次写入字节、delta 条目数和 RSS 前后差异。

关键点：常规 flush 不再调用 `materialize_snapshot_base()` 全量收集 70 万级路径。

### Compaction

只有在下列条件满足时才完整物化：

- delta segment 数量超过阈值。
- delta 总大小超过 base 的一定比例。
- 查询合并层数导致延迟超过阈值。
- 用户手动触发 maintenance。
- clean shutdown 前可选执行轻量 compaction，但不能阻塞退出过久。

Compaction 必须可切片、可取消、可观测：

- `compaction_in_progress`
- `compaction_segments_in`
- `compaction_entries_processed`
- `compaction_elapsed_ms`
- `compaction_rss_peak_bytes`
- `delta_snapshot_segments`
- `delta_snapshot_bytes`

### 兼容与迁移

- 保留现有 `stable.v7` 加载路径。
- 首次启用 delta snapshot 时，如果没有 manifest，自动把现有 `stable.v7` 注册为 base。
- 如果 manifest 损坏但 `stable.v7` 可用，回退到旧路径并请求 full snapshot。
- recovery audit 扩展到 base/delta/tombstone/manifest/WAL checkpoint。
- 配置提供开关：
  - `snapshot_mode = "full"`：旧模式。
  - `snapshot_mode = "delta"`：新模式，默认先不启用。

### 风险

- 查询合并层数增加会影响 latency，需要 TopK 合并和去重优化。
- tombstone 跨段语义必须非常严格，否则会出现旧路径复活。
- manifest 原子性和 WAL checkpoint 是核心正确性边界。
- Compaction 仍会产生峰值，但应低频、可限速、可解释。

## 分期建议

M1 可以先独立执行，直接解决“存在但被 ignore 没提示”的用户体验问题。M2/M3 都属于运行时策略和存储架构改动，建议分别开实施包或在本包中按任务阶段执行，避免一次改动同时影响查询语义、watcher 策略和恢复路径。
