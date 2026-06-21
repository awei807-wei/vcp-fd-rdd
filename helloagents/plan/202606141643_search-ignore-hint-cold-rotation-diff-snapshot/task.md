# 搜索忽略感知、冷层轮转更新与增量快照任务清单

## M1：Ignored Hint 搜索提示

- [ ] 梳理 `/search` 与 `fd-rdd-query` 现有响应兼容边界，确认提示项是否默认仅在空结果时返回。
- [ ] 扩展 `src/event/ignore_filter.rs`：保留 `is_ignored()`，新增可解释的 ignore match 结果。
- [ ] 新增 ignored hint 配置结构与默认值：启用、query timeout、sidecar budget、最大返回数。
- [ ] 实现直接路径探测：仅检查 roots 内、路径形态明确、真实存在且被 ignore 的文件/目录。
- [ ] 设计并实现 `IgnoredHintSidecar`：低优先级、分片、只记录 basename/path/kind/mtime/规则摘要。
- [ ] 在 `/search` 接入 ignored hint：普通命中优先；空结果或显式 `explain=ignored` 时追加提示项。
- [ ] 在 `fd-rdd-query` 输出 ignored 提示标记，避免用户把提示项误读为已索引结果。
- [ ] 增加 metrics/health 字段：sidecar 条目数、probe 次数、超时、返回提示数。
- [ ] 回归测试：`.gitignore` 忽略的 `custom_phrase_double.txt` 搜索返回提示。
- [ ] 回归测试：ignored 文件不进入主索引，`ignore_enabled=false` 时行为恢复为普通结果。
- [ ] 回归测试：宽泛 query 不触发超预算遍历，timeout 后不影响普通搜索延迟。

## M2：Rotating Cold Freshness Window

- [ ] 将 M2 的轮转机制语义引用到 `helloagents/plan/202606141747_方案设计过程-冷层轮转boost模型讨论-非正常方案包`，以其中最终确认模型作为设计边界。
- [ ] 明确正式 tier 不做全局交换：L0/hotset 不因冷层轮转被强制降级。
- [ ] 新增轮转配置：enabled、budget、tick、TTL、max cost、每 tick 候选数。
- [ ] 在 `TieredWatchRuntime` 增加冷层轮转候选评分：scan age、event_score、query miss/stale、watch cost、manifest 变化。
- [ ] 将 Boost 语义实现为可撤销调度租约，使用 `EphemeralWatch`、`FastScanLease`、`ScanSlice` 这类执行模式，避免 `L1-like tier` 命名。
- [ ] 对小成本候选复用 `WatchCommand::AddEphemeral`，但 watcher 预算必须与正式 hotset 隔离，扫描仍按预算切片。
- [ ] 对中等候选发放 fast scan lease 与 sentinel bootstrap，不承诺查询正确性。
- [ ] 对大成本候选只入 DirtyQueue 分片 scan，禁止整棵 recursive watch。
- [ ] 将 DirtyQueue 设计为 coalescing dirty state table：合并 reason、提升 priority、保存 cursor、记录 generation 和 backlog watermark。
- [ ] 增加 cost-aware coalescing：父级 scope 合并只降低精度，不放大单次扫描；每轮受 stat 数、目录项数、耗时、字节数和 PSI 截断。
- [ ] 增加 parent scope 反向分片策略：大 scope 消费时可拆成 child shard，避免 I/O 放大正反馈。
- [ ] 引入 subtree / segment tombstone 与 compiled tombstone overlay 设计边界，避免逐文件墓碑和 cold query CPU 阻抗。
- [ ] 增加 tombstone overlay 复杂度阈值：数量、重叠深度、覆盖比例、filter CPU 成本超限后触发 compaction。
- [ ] 增加 `/watch-state`、`/health`、metrics 观测字段：cycle、progress、active dirs、budget blocked、cold freshness age、coalesced scopes、slice budget exhausted、tombstone filter CPU。
- [ ] 回归测试：L3 目录轮转后新文件更快可见。
- [ ] 回归测试：超大目录不会被整棵 watcher 覆盖。
- [ ] 回归测试：少量叶子 dirty 合并到父 scope 后不会触发无预算全量递归 scan。
- [ ] 回归测试：复杂 tombstone overlay 下 cold query CPU 有上限，超限触发 compaction due。
- [ ] 回归测试：active hotset 5 秒 SLA 不回退。

## M3：Delta Snapshot / Diff Flush

- [ ] 设计 manifest 格式：base v7、delta v7、delta tombstone、WAL checkpoint、roots/config fingerprint。
- [ ] 抽象 cold segment 查询合并：overlay/L2 → delta newest-to-oldest → base。
- [ ] 实现路径级 tombstone 跨段屏蔽，覆盖 delete、rename-from、delete→recreate。
- [ ] 新增 `snapshot_mode = "full" | "delta"`，第一阶段默认仍为 full。
- [ ] 实现 delta flush：seal WAL、交换 delta、写 delta v7、写 tombstone、原子更新 manifest。
- [ ] 扩展 recovery audit：校验 manifest、delta segment、tombstone、WAL checkpoint 和 orphan 临时文件。
- [ ] 实现 compaction 触发阈值：delta 数量、delta 总大小、查询层数、手动 maintenance。
- [ ] 实现后台 compaction：可切片、可取消、成功后替换 base 并 GC 旧 delta。
- [ ] 增加 metrics：delta segment 数、delta bytes、compaction 状态、RSS peak、写放大。
- [ ] 回归测试：delta flush 后重启可恢复新增、修改、删除、rename。
- [ ] 回归测试：manifest 损坏时回退 stable v7 并请求 full snapshot/rebuild。
- [ ] 压测：70 万级路径下常规 flush 不再出现全量物化级 RSS 尖峰。

## 文档与验证

- [ ] 更新 `README.md`：ignored hint、cold rotation、snapshot mode 配置与语义。
- [ ] 更新 `helloagents/wiki/tiered-watcher-runtime.md`：轮转冷层加速不是正式 tier 交换。
- [ ] 更新 `helloagents/wiki/storage-stage-c-lsm-compaction.md`：delta snapshot 与 compaction 新边界。
- [ ] 更新 `CHANGELOG.md`。
- [ ] 运行 `cargo test -q ignore`。
- [ ] 运行 `cargo test -q fast_scan`。
- [ ] 运行 `cargo test -q snapshot`。
- [ ] 运行完整 `cargo test -q`。

## M2 原型记录（2026-06-15）

- [√] 已在 `prototype/m2-cold-rotation` 分支实现 Rotating Cold Freshness Window 原型。
- [√] 已新增 `tiered_watch.rotating_cold_window_*` 配置，默认关闭。
- [√] 已实现 L2/L3 候选评分、窗口预算、TTL、cycle progress 与 `/watch-state` / `/debug/tiered-watch` 观测。
- [√] 已按 watch cost 将候选分流到 Ephemeral Watch、`RotatingColdWindow` fast scan lease 或 `PeriodicColdScan` scan-only。
- [√] 已验证原型不会修改正式 L0/L1/L2/L3 tier。
- [√] 已修复 VM fixture 暴露的问题：Ephemeral Watch / fast scan lease 发放失败时降级 scan-only 并入 `PeriodicColdScan`，不再取消轮转租约。
- [√] 已调整 VM benchmark：新增 passive canary，先写入、等待 settle 后首次查询，避免把 query miss / fast scan 触发补偿误算成 M2 背景追平收益。
- [√] 已增强 VM benchmark 输出：区分 active/passive canary，新增 passive first-query 成功率、positive first-query 成功率、轮转 action 计数、cycle progress、scan interval 配置。
- [√] 已增强 VM benchmark event storm：新增短窗口 `rw100`、`save100`、`git_clone`、`npm_install`、`subtree_rename`、`mount_storm`、`inode_reuse`、`time_skew` fixture，并输出 workload/tier/special 维度摘要。
- [ ] 待后续压测：在大 home 场景比较 cold freshness age p95/p99 与 hotset SLA 是否回退。

## M2 VM 验证与 workload driver

- [√] 已整理 M2 冷层轮转 VM 验证计划到 `BENCHMARK.md`。
- [√] 已明确现有 daemon metrics 可复用：`reports/metrics/*.json`、`/watch-state`、`runtime`、`memory`、`health`、`diagnostics`。
- [√] 已新增 `scripts/m2-cold-window-vm-bench.py` 作为 runner + collector，只负责启动隔离 daemon、采集 HTTP 端点、采集 `/proc/<pid>` 与汇总报告。
- [ ] 新增 `scripts/m2-cold-window-workload.py`，作为独立 workload driver，不混入 runner / collector。
- [ ] workload driver 增加安全护栏：拒绝 `/`、真实 `$HOME` 顶层和项目仓库根目录作为 `--root`，所有删除和 rename 必须解析到 sandbox 内部。
- [ ] workload driver 支持 `--dry-run`、`--seed`、`--duration-secs`、`--rate daily|normal|stress|chaos`、`--events-jsonl`。
- [ ] workload driver 输出 `workload-events.jsonl`，字段包含 `ts`、`phase`、`scenario`、`operation`、`path`、`count`、`expected_query`、`started_at`、`finished_at`、`ok`、`error`。
- [ ] 实现 `daily` 场景：小文件 create/edit/rename/delete、编辑器临时文件、下载器 `.part -> final`。
- [ ] 实现 `cold-canary` 场景：冷目录唯一 canary 的 create / rename / delete 可见性输入。
- [ ] 实现 `delete-storm` 场景：批量文件删除和 subtree 删除，验证 tombstone 不逐文件爆炸。
- [ ] 实现 `rename-storm` 场景：大批量 rename，验证 `apply_seq`、旧路径隐藏和 L2 不无限膨胀。
- [ ] 实现 `git-storm` 场景：临时 git repo checkout / clean / reset 风格震荡。
- [ ] 实现 `watcher-drop-proxy` 场景：通过 no-watch baseline、暂停 daemon 或重启窗口模拟丢事件后的 deferred repair。
- [ ] 实现 `subtree-rename` 场景：真实 `mv dir_a dir_b` 后验证深层子文件新路径可见、旧路径隐藏。
- [ ] 实现 `mount-storm` 场景：在 VM 中通过 U 盘、loop mount 或 NFS 断联验证删除熔断器，不把 ENOENT 海啸写成逐文件 tombstone。
- [ ] 实现 `inode-reuse` 场景：尽量诱发同 dev/inode 复用，验证 Generation Number / FileKey 防幽灵复活。
- [ ] 实现 `time-skew` 场景：真实回拨系统时间，验证双轨时钟和 mtime cutoff 不漏扫。
- [ ] 增加 analyzer/report：按 workload 事件和 metrics 时间线对齐，输出 baseline / experiment A/B 表。
- [ ] 首轮验收：正确性不退、冷层 p95/p99 改善、hotset SLA 不退、CPU/RSS/FD/DirtyQueue 在门槛内。
