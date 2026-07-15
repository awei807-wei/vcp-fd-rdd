# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### 原型

- 新增 M2 Rotating Cold Freshness Window 原型：默认关闭，通过 `tiered_watch.rotating_cold_window_*` 配置在独立窗口预算内轮转 L2/L3 冷目录；小成本目录走 Ephemeral Watch，中等成本目录走 `RotatingColdWindow` fast scan lease，大成本目录只入 `PeriodicColdScan` 分片补扫，正式 L0/L1/L2/L3 tier 不交换、不抢占 hotset。
- `/watch-state` 与 `/debug/tiered-watch` 新增冷层轮转观测字段，暴露 active dirs、cycle progress、动作计数、budget blocked、cold freshness age p50/p95/p99，以及单目录本轮已触达、当前动作、到期和分数，用于判断机制可实施性并给 burst 提供目标级归因证据。
- 新增 `scripts/m2-cold-window-vm-bench.py` 和 `helloagents/wiki/m2-cold-window-vm-benchmark.md`：在 VM 中隔离启动 fd-rdd、复用内建 metrics JSONL、周期采集端点和进程指标，并提供 M2 冷层轮转 A/B 场景、通过标准与报告模板。
- 新增 `scripts/m2-cold-window-ab.py` 一键 A/B 驱动：固定并安全重建 600 个冷目录 fixture，以 `--build always` 调用现有 VM runner，固化一小时 event storm/canary 参数；60 秒 settle 与 10 秒轮间隔保证完整覆盖 4 个 tier × 8 类 workload 的 32 个组合。运行中先校验 treatment、L2/L3 与 M2 活动，结束后要求 summary/manifest 均可比较、fd-rdd 退出码为 0，并校验 burst 和六项必需产物，任一失败均非零退出。
- `scripts/m2-cold-window-vm-bench.py` 新增 passive canary：先写入文件、等待 settle 后做首次查询，分离后台主动追平与 query miss / fast scan 触发补偿；报告同步输出 active/passive canary、passive first-query 成功率、轮转 action 计数和 scan interval 参数。
- `BENCHMARK.md` 与 M2 方案包补充 VM workload driver 计划：driver 独立于 runner / collector，只在 sandbox root 下生成 daily、cold-canary、delete-storm、rename-storm、git-storm、watcher-drop-proxy 压力，并输出 `workload-events.jsonl` 供指标时间线对齐。
- `scripts/m2-cold-window-vm-bench.py` 新增一体化 `--event-storm`：支持 `rw100`、`save100`、`git_clone`、`npm_install`、`subtree_rename`、`mount_storm`、`inode_reuse`、`inode_reuse_stress`、`time_skew`，并在 summary/report 中输出 first-query、after-query、workload/tier 维度与特殊正确性计数，用于快速比较 M2 对短窗口事件风暴的投入产出比。
- 新增 `scripts/m2-cold-window-falsification.py` 快速证伪套件：以 deterministic `2×AB + 2×BA` 运行八腿 L3 配对实验，支持同一 run-dir 断点续跑，并对 Git/binary/fixture/协议指纹、806 个唯一路径主断言、38 个 visibility probe、目标实时租约与写入后扫描/事件因果证据、35 秒恢复 SLA、跨 burst/workload 收益复现、95% 采样覆盖、snapshot、水位线、DirtyQueue 和成本执行 fail-closed 门禁。
- 新增 `scripts/m2-l3-causal-probe.py` 单 L3 根因果探针：以约 80 秒的一次 `subtree_rename` 分别门禁精确 L3/新鲜 lease、before→fence→after 同 action/cycle 的 scan/event 因果、10 组新旧路径、TTL 后 watcher 账本和 snapshot quiesce；支持进程归属端口验证、并发安全输出目录、dry-run、analyze-only，并输出独立 summary/report。分析器按 contract/audit/stages/snapshot/report 职责拆分，并对跨 burst 证据、缺失负证据、runner 状态矛盾和畸形字段 fail-close。最终 VM 短跑与离线重算一致地将失败定位为 scan/event 序列未推进与过期 ephemeral watch 未释放，同时证明 rename 20/20 和最终 snapshot 正常。
- M2 benchmark 新增 burst 上限、唯一路径有界 visibility probe、`/proc` I/O syscall/bytes 与 minor/major fault 采样；summary/report 同时提供完整运行和从 `burst_started` 开始的事件风暴诊断窗口，快速门禁以完整运行成本为准，不再漏掉预热、首个 burst 生成与 shutdown quiesce 成本。
- M2 A/B fixture manifest 新增完整内容 SHA256；快速套件跨腿核对初始状态、fixture 内容和除 treatment 外的协议指纹，避免仅凭文件数或相似参数声明可比。
- M2 快速套件新增可复核的 schema v3 `build-provenance.json`：使用 suite 私有 target dir 和 Cargo JSON compiler-artifact 绑定真实 binary，只允许在构建前后 Git HEAD 不变且工作区均 clean 时生成；八腿分别重算 clean worktree、Git/Cargo.lock/binary 身份，再执行腿私有只读副本，阻断并发 Cargo 替换窗口。磁盘 snapshot label 同时加入完整 run-dir 哈希，避免不同 block 的同名 attempt 交叉复用状态。
- M2 A/B wrapper 新增 fixture/run-dir 全生命周期独占锁和原子 `ab-wrapper-result.json`；运行中状态改写到 run-dir 同级 sidecar，并记录独立 benchmark 进程组，避免预创建目录触发 `run_dir_preexisting` 或超时后遗留孤儿 daemon。快速套件断点续跑只复用明确通过 wrapper 门禁且 variant/profile 匹配、daemon 退出码为 0、启动时间相邻的完整 block；半块、跨会话、失败、中断、信号终止、旧 schema 或缺失终态均 fail-closed。suite/wrapper 收到 SIGTERM 时会屏蔽清理阶段的重复信号并沿进程链清理、保留续跑证据。
- M2 快速 profile 关闭会阻塞工作负载节奏的 active canary，保留 passive canary 作为关机对账输入；suite 固定目录和保序确定性事件计划，持续执行等负载 visibility polling，并新增目标目录 M2 触达、visibility 传输、精确 `2×AB + 2×BA`、逐块成本异常和完整运行 CPU/read/write/syscall、RSS、fault、单位找回路径成本门禁。唯一主收益端点收敛为 A 相对 B 多找回至少 5% 的正向主断言路径；持久化静默阶段至多允许一次可归因且最终 `ready=true` 的安全 rebuild，其成本不从 A/B 中剔除。

### 性能

- 百万文件初始 rebuild 不再把完整 `PersistentIndex` 转成第二份 `BaseIndexData`：完成扫描的 generation 直接转交 current-v7 owned writer，先释放 FileKey、trigram、parent 等查询派生表，再以 32 MiB 有界外排/归并生成路径、entry、trigram 与 parent 段；源 entries/path arena 在最终段归并前释放。
- current-v7 周期快照新增 cold mmap + delta 直接流式合并：保留旧 DocId/path index，对覆盖、精确删除和子树删除追加 tombstone/new DocId，并按段增量合并 posting，避免重新热化 cold base 或同时持有六个完整编码段。
- 快照物化不再先构造百万级 `Vec<FileMeta>`：overlay 与 cold base 直接流入临时 `PersistentIndex`，再通过消费式 `into_base_index_data()` 转换，复用 trigram/tombstone 所有权并提前释放派生查找结构，缩短两代全量对象的共存窗口。
- 主 v7 写入成功后，stable 恢复副本改为复制、mmap/CRC 校验和原子轮转，不再对同一 `BaseIndexData` 做第二次完整编码与热反序列化；stable 禁用时会在确认主文件目录持久化后退休旧恢复副本。
- `PersistentIndex` 运行时路径从每条路径独立分配的 `Vec<Vec<u8>>` 改为连续 `PathStore` Arena 与 8 字节 `PathRef`，保留超长内存路径和重命名语义，同时消除百万级小对象分配与对应 allocator 碎片。
- L2 `DocId`、trigram posting 和 tombstone 收敛为 `u32` / `RoaringBitmap`；`FileKey` 代表索引改为只保存 `u32 DocId` 桶的开放寻址表，不再重复存储每个 20 字节 `FileKey`，并补齐同路径 inode generation 变化时的旧身份替换。
- ParentIndex 构建路径表改为连续 Arena + 哈希碰撞核验，构建后常驻表只保留目录路径反查；`/memory.l2` 新增 `parent_index_bytes` 与 `parent_path_lookup_bytes`，使百万文件 VM 基准可直接归因父路径索引内存。

### 修复

- 修复正式 M2 八腿报告暴露的多根因果与成本放大：falsification 腿延长为 1500 秒，六个 burst 使用 180 秒 cadence，并以整腿共享的 160 秒等待预算保证每次写入前租约至少剩余 125 秒；rotating watcher 命令在副作用前校验 active cycle，失败才降级到同 cycle scan-only，旧命令不能再污染新租约。Ephemeral bootstrap 与冷目录 rename 统一进入有界递归 DirtyQueue，递归属性和 cycle provenance 在合并、重试及后代任务中独立保留，避免成功路径重复深扫和 scan-only 因果丢失。
- 修复 subtree rename 与 snapshot 收敛边界：冷目录结构变更逐层扫描新子树并可信对齐旧前缀，超时未配对的 Rename From 会修复源父目录；仅对目录 Create/rename 目标执行消失路径 `NotFound -> Delete` 归一化，普通 Modify 继续 fail-closed，减少 treatment 独有 metadata syscall。waterline soft 告警改为连续三次超限才降级，恢复阈值允许精确边界，配置强制 recover pct 小于 trigger pct 且 SLA 不低于 fast-scan service target，避免 5001-5003ms 调度抖动长期压低轮转预算。
- 修复 M2 rotating cold-window watcher 生命周期竞态：活跃 scan-only lease 不再粗暴阻断普通 watcher（保留深层 subtree rename 可见性）；所有被 rotating lease 覆盖的新建/刷新 ephemeral lease 继承其剩余 TTL，`EphemeralWatch`/`ScanOnly` lease 过期至下一轮 rotation 前禁止普通路径重建。scan-only follow-up 限制为租约内两次有界扫描，并保留 rotating cycle provenance；冻结 L3 探针与离线复算均已通过五阶段门禁。
- M2 focused probe 写入可验证 fixture 完成声明，仅容忍 `git_worktree_dirty` / `artifact_provenance_unverified` 两类开发态 A/B 原因导致 runner=1；fixture、初始状态、未知门禁、daemon 异常或其他非零退出均归为基础设施错误。event storm 同时新增默认关闭的 cleanup 延迟审计，以实际 lease/rotation 时间窗记录 root/subtree 的 ephemeral/rotating watcher 残留；查询年龄按每次查询完成时刻计算。auto-port 在 daemon 启动前复查并通过本轮日志证明监听归属，避免误连外部服务。
- 修复 M2 快速证伪复用同一 `cold-a` 的 L3 重降级竞态：VM 证明即使把 burst 周期从 130 秒增至 150 秒，后台扫描、水位线和调度仍可能让第 4 轮严格预检看到 L2，固定墙钟余量不是可靠协议。falsification fixture 升级为六个 daemon 已登记的独立冷根，每根用纳入内容身份的 sentinel 让 watch cost 明确超过 L0 上限，每腿六个 burst 各使用一个新根；每 tick 目录上限覆盖全部 8 个冷配置根，固定调度只定义 A/B 一致的无重复根序列，实际 tier、精确活跃租约与写后因果继续由写前严格证据判定。cycle `seen` 清空仅是诊断状态，不再否定尚未过期的精确租约，且绝不接受 L2 冒充 L3。
- 修复 M2 快速证伪对 B 组的确定性假失败：event-storm 子目录现在从最近祖先继承 tier，debug 请求成功与精确 M2 entry 存在分开记录；A 仍要求逐 burst 精确 entry、有效租约和写后因果，B 允许 entry 不存在但禁止任何 M2 活动。falsification profile 新增写入前严格协议校验，前置证据不成立时立即结束该腿。
- M2 稳定性门禁用显式 `startup bootstrap` reason 拆分 bootstrap 与 post-ready background rebuild，消除后台日志晚于 ready 的调度竞态；允许的 post-ready rebuild 必须带 `snapshot recovery` reason 且启动日志落在 shutdown quiesce 字节窗口内，仅有全局 rebuild 布尔值不能放行。写后扫描/事件改用写前序列、写后租约栅栏和同租约周期校验；轮转 scan 通过带 `cycle_id` 的专用 dirty source 贯穿队列与分片扫描，普通周期/API/查询扫描不再发布 M2 序列，序列发布也在每目录锁内保持单调。固定调度改为选择 daemon 已登记的配置根，消除 `dNNN` 子目录与精确 M2 entry 契约冲突。租约判活同时受单调时钟与墙上时钟约束，既保留 mutation 期间即时完成的真实 M2 事件，又避免同秒交错、时钟回拨和租约外工作造成假阳性。suite 同时逐条打印 gate reasons，并只对最终八腿引用的 attempt 生成带必需成员校验和 SHA256 清单的证据包；根级构建回执实际 SHA256 必须与八腿审计记录一致，历史 attempt、build-target、执行副本、snapshot、symlink 与伪造路径均被排除，缺件、回执替换或打包失败会使旧包失效并返回基础设施失败。
- 补齐 `unbounded_summary_uses_walkbuilder_filter` 的 `.git/` fixture 前置条件，使测试与 `ignore` crate 仅在 Git 仓库启用 `.gitignore` 的真实语义一致，消除确定性误报。
- M2 benchmark 新增最终持久化静默屏障：可信 `/scan` 对账后同步 POST `/snapshot`，若目录 rename/mount storm 触发 `subtree move completeness is unproven`，daemon 在 shutdown 前执行完整 rebuild，runner 重试到 durable generation `ready=true` 后才发送 SIGTERM；`shutdown_snapshot_quiesce` 单独记录 rebuild、尝试次数、耗时和 CPU/RSS，失败或缺失继续拒绝 A/B。
- M2 benchmark 在时长结束后、SIGTERM 前新增可审计的关机静默对账：runner 同步 POST `/scan` 到 passive/active canary、event-storm parent/active burst 与 mixed hot roots，手动扫描复用完整目录读取、fingerprint、`event_seq` 与 freeze gate 的可信负事实逻辑并返回 `stable/deleted`；持续不稳定、记录缺失或失败都会拒绝 A/B，避免未完成 create→rename→delete 或 active burst 的 Live 路径击穿最终快照，同时保留 snapshot fail-closed。
- M2 冷层完整扫描新增可信负事实对账：仅在目录完整可读、目录 identity/mtime/ctime/nlink 未变化、`event_seq` 未前进且未命中离线 freeze gate 时，将 Base/L2 直接子项和 Delta 深层路径的缺失项折叠为直接子树 Delete；`read_dir` 失败不再把挂载断联误判为整目录删除。
- M2 event storm cleanup 改为只删除当前 burst root，记录目标、条目估算、耗时和错误；cleanup 失败纳入 execution fingerprint、A/B comparability 门禁、失败诊断与 `REPORT.md`，不再允许清理异常污染正式对照。
- M2 一键 A/B 驱动在底层 runner 非零退出、预检早退或收尾门禁失败时，不再只打印退出码；runner stdout/stderr 会同步落入 run 目录同级日志，停止流程异常不会遮蔽原始错误，门禁会有界汇总 manifest/summary 状态、缺失/空产物、最近关键事件及 runner/daemon 错误尾部，并输出完整现场路径。
- M2 event storm 的 tier 目标选择不再复用当前或历史 `fd-rdd-m2-event-storm-*` 目录及其后代；候选同时校验词法路径、真实路径和配置 root，避免 workload 递归嵌套或经 symlink 逃逸。
- 快照遇到已被精确或祖先 delete/rename 失效证据覆盖的 transient Live upsert 时，会将其收敛为删除并继续持久化最终事实；没有失效证据的 unresolved upsert 仍保持 fail-closed，不再让原子保存的旧 `.tmp` 或子树 rename 旧路径触发无意义 rebuild。
- fd-rdd 关机现在先关闭周期快照与 rebuild 准入，最终快照失败会保留 `Final snapshot failed` 日志并以错误退出；仅在最终快照、fast-scan registry 和 runtime state 全部持久化成功后写入 clean-shutdown，cooldown/retry 线程不能越过关机边界启动 rebuild。
- 修复 realistic fixture 的总量失真：区段改为最大余数法精确分配，`Misc` 从错误的 16.3% 收敛为 6.3%，`.git`/`node_modules` 脚手架计入 `Projects` 配额；completed manifest 现在同时校验计划总数、各区段实际数和请求总数完全一致。
- full rebuild 发布改为 fail-closed generation 边界：扫描期间的 overflow、目录 rename/子树失效会保留 delta 与 sealed WAL 并重试，普通文件精确 delete 仍可在边界内重放；snapshot 验证、stable 安装、cold remount 或 quarantine sidecar 任一步失败都不得清理恢复证据。
- M2 VM benchmark 新增参数、初始状态、执行三层指纹和 `ab_comparable` 门禁；正式 A/B 必须使用本轮 `cargo build --release --locked` 且来源 SHA 可证明的二进制、原子 completed verified fixture manifest、干净工作区与完整采样，legacy fixture 标记只展示、不再进入可比结果。
- benchmark manifest 改为原子生命周期状态机，并以独立 `/proc` sampler 精确覆盖 SIGTERM 后最终快照窗口；`--workload-seed` 固定 mixed workload，runner 报告 owned snapshot 状态及六项 L2 时间序列。
- M2 VM benchmark 将首次查询的传输成功与语义正确性分开统计，否定查询不再把 HTTP/解析失败误计为正确；inode reuse 输出 `not_run` / `inconclusive` / `exercised`，UDS 使用 `/tmp` 短哈希路径。
- M2 VM benchmark 从 daemon 启动时独立以默认 0.5 秒采集 `/proc` RSS，并输出 rebuild、initial build publish、cold steady、hot-base snapshot 四阶段峰值和六项 L2 时间序列；压缩序列保留每次阶段边界、尾点与指标极值。
- stable 写入、轮转、禁用清理或目录同步失败时，快照不再清理 sealed WAL 或记录成功，而是保留恢复日志并请求重试，避免旧 stable 抢先于新主 v7 导致恢复丢数。
- M2 冷层轮转在 Ephemeral Watch 或 fast scan lease 发放失败时会降级为 scan-only 并入 `PeriodicColdScan`，不再直接取消轮转租约，避免冷目录已滞后但 `rotating_cold_window_active_dirs` 始终为 0、canary create / rename 无法追平。

### 代码审查与质量加固（2026-06-19）

- 删除 `src/index/pathtable.rs`（Phase 4 未完成半成品，已被 `PathTableV2` 取代，零引用）。
- 修复 `query/dsl.rs` 查询编译路径 3 处 `unwrap()`/`expect()`，改为安全 fallback，避免异常输入导致进程崩溃。
- 修复 `index/l1_cache.rs` 4 处 TOCTOU 竞态：insert/remove/remove_by_path 改为同时持有 inner + path_index 写锁保证双向映射原子一致，建立统一锁序 inner → path_index → lru。
- 分析 `index/tiered/sync.rs` `finish_rebuild` 双锁路径，确认全项目锁序一致无死锁风险，补充文档注释。
- 提取 5 处重复代码为共享 helper：`entry_to_meta()`、`align_up()`、`read_u32()`（→ `util.rs`）；`atomic_write()`、`fsync_dir()`（→ `storage/mod.rs`）；消除 `snapshot.rs` 4 处内联 CRC32 改调已有 `checksum::crc32c_checksum()`。
- `TieredIndex` 上帝对象拆分（阶段 1）：82 个平铺字段提取为 5 个子 struct（`ContentIndexState`、`LazyValidationRuntime`、`RecoveryQuarantine`、`IoTuning`、`TombstoneTracker`），struct 定义从 82 行降至 41 字段。
- 拆分 `index/l2_partition/mod.rs` 上帝模块：2210 行 → 106 行 + 13 个子模块（每个 <400 行），外部 pub API 通过 re-export 完全保持不变。
- `main.rs` 瘦身：2154 行 → 9 行薄入口，全部守护进程编排逻辑下沉到新 `src/runtime.rs`。
- `storage/snapshot.rs` legacy 隔离：V2-V5 兼容加载代码移至新 `src/storage/snapshot_legacy.rs`（兼容代码保留，仅文件隔离）。
- storage 层引入 `StorageError`（thiserror），内部函数从 anyhow 改为精确错误类型。
- 删除 `src/core/dag.rs`（278 行，连编译都不参与的孤儿文件）。
- 删除 `src/core/rdd.rs` 中从未被调用的 `BuildLineage` 壳子。
- `src/core/lineage.rs` 改名为 `event_types.rs`（内容实为事件类型定义，与 RDD lineage 无关）。
- 去重 `l2_partition/export.rs` 段导出逻辑：抽取 `build_v6_segments()` 和 `write_v6_segments_to_writer()` 公共函数，`export_segments_v6` / `export_segments_v6_to_writer` 改为调用公共函数（完成方案包 `code-debt-cleanup-phase1` 最后一个未落实项）。
- 修复 daemon 集成测试的并发串扰：`FdRddProcess` 现在默认为每个子进程隔离 `XDG_CONFIG_HOME` 与 `XDG_RUNTIME_DIR`，避免 `--no-watch` 测试持久化的配置影响 crash recovery/watcher 测试。

### 代码审查修复（2026-06-19）

- 修复 `util.rs` 分层违规：`entry_to_meta` 从底层 `util.rs` 移至 `index/mod.rs`，消除底层工具模块对领域类型的向上依赖。
- 修复 `query/dsl.rs` 2 处 `unwrap_or(CompiledExpr::True)` / `unwrap_or(Expr::True)` 改为 `unwrap()`，消除不可达分支在重构后静默匹配所有文件的风险。
- 修复 `storage/mod.rs` `fsync_dir` 静默吞掉目录打开失败：为 `File::open` 失败添加 `tracing::debug!` 日志。
- 简化 `storage/snapshot.rs` `lsm_read_manifest` 的 `Ok(...?)` 模式为 `.map_err(Into::into)`。
- `storage/error.rs` 添加 `StorageError` 迁移状态说明，明确标注已完成和待完成的迁移范围。
- `storage/snapshot.rs` 为 `LoadedSnapshot` re-export 添加注释，说明与 `snapshot_legacy.rs` 的双向依赖是刻意设计。
- 新增 `export_segments_v6` 和 `export_segments_v6_to_writer` 单元测试，验证 7 段输出完整性与顺序。

### 代码审查修复第二轮（2026-06-19）

- 修复 `storage/snapshot_v7.rs` `snapshot_now_v7` 快照输出非确定性：HashMap 遍历顺序随机化导致相同数据产生不同快照字节，改为收集到 Vec 后按 FileKey 排序再重建索引。
- 修复 `index/l2_partition/tests.rs` 恒真断言：`!is_empty() || len() == 0` 永远为真，删除该无意义 assert。
- 修复 `query/dsl.rs` 2 处 `unwrap()` 改为 `expect()` 带诊断信息，避免不变量被破坏时守护进程无信息 panic。
- 修复 `runtime.rs` 多处 `u64::try_from(usize).unwrap_or(0)` 改为 `as u64` 直接转换，消除不可能溢出场景下的静默归零风险。
- 修复 `storage/mod.rs` `fsync_dir` 目录打开失败日志级别从 `debug!` 提升至 `warn!`，便于生产环境排查。
- 新建 `storage/snapshot_common.rs`：提取 `MAGIC`/`STATE_COMMITTED`/`STATE_INCOMPLETE`/`HEADER_SIZE` 共享常量，消除 `snapshot.rs` 与 `snapshot_legacy.rs` 之间的双向模块依赖。

### 代码审查修复第三轮（2026-06-19）

- 修复 `storage/snapshot_v7.rs` `snapshot_now_v7` 冗余双重排序：确定性修复已在重建 `entries_by_key` 时按 `FileKey` 排序，函数末尾的 `sort_by_key()` 为冗余二次排序，删除以消除每次快照写入的无谓 O(n log n) 开销。

### CI 与快照合并修复（2026-06-19）

- 修复 `storage/snapshot_v7.rs` `snapshot_now_v7` DocId 错位 bug：排序重建 `entries_by_key` 改变了 DocId（= 插入索引），但 `trigram_index`、`parent_index.dir_to_files`、`tombstones` 仍引用 base/delta 旧 DocId，导致 posting 与 tombstone 解析到错误路径。改为先确定最终条目顺序、分配新 DocId 并构建 old→new 映射，再对三个 DocId 索引逐一重映射后并集。
- 修复 `storage/snapshot_v7.rs` `snapshot_now_v7` 硬链接别名丢失 bug：`HashMap<FileKey, FileEntry>` 按 FileKey 单键去重会折叠同 inode 不同路径的硬链接别名，合并后静默丢弃多余路径。去重键改为 `(FileKey, path_index())`，delta 仅覆盖完全相同键的条目，别名全部保留。
- 新增两个单元测试：`snapshot_now_v7_remaps_docids_when_base_unsorted`（验证 base 未按 FileKey 排序时位图仍解析到正确路径）、`snapshot_now_v7_preserves_hardlink_aliases`（验证同 FileKey 不同 path_idx 的别名合并后均存活）。
- CI 新增快速门禁 job `gate`（`cargo check --locked --all-targets`），所有需要编译的 job 声明 `needs: [gate]`，编译错误时 fail-fast 只挂 1 个 job，不再浪费 15 份并行 runner 时间。

## [0.7.1] - 2026-06-13

### 质量加固

- Config 新增跨字段校验（`TieredWatchConfig::validate()`、`QueryConfig::validate()`），拦截 `tick_ms=0` 等退化值。
- 移除 `query.allow_sync_readdir` 死配置字段；该门禁改为内部硬编码 `false`，旧配置文件中的该字段会被静默忽略。
- Subtree tombstone TTL 改为可配置：`tiered_watch.runtime_subtree_tombstone_ttl_secs`，默认 300s。
- `LowPower` profile 自动调低 fast scan 参数：`stat_budget_per_tick` 降为 1000，`tick_ms` 升为 2000（仅在用户未显式覆盖时生效）。
- 新增 `query_permission_denied_count` metric，暴露因权限拒绝而无法验真的冷层候选累积计数。
- 收紧 26 处测试断言：`!is_empty()` → `len() == 1`、`>= N` → `== N` ��，确保功能退化时测试必须失败。
- 统一 17 处 `unique_tmp_dir` 复制品到 `tests/common/`；统一 `crc32_simple`、`WAL_MAGIC`、`create_event`、`get_json` 等 6 组重复 helper。
- 新增 `test_meta()` helper 减少 `p1_query.rs` 约 250 行 FileMeta 构造样板。
- `FdRddProcess` 新增 `Drop` guard，测试 panic 后自动清理子进程。
- `fd_rdd_client` 新增 `search_checked()` 返回 `Result`，区分"服务返回空结果"与"服务不可达"。
- `high_load_event_processing` 补充断言（file_count > 0 + 已知文件可查询），不再是零断言死测试。
- 新增查询错误响应测试（缺参数 → 4xx、空查询 → 200）和 watch 启用时 `index_health = "ok"` 测试。
- `p2_large_scale_hybrid` 端口改为动态分配，超时从 600s 收紧到 120s。

### 功能（从 Unreleased 合入）

- M3 fast scan 正式收缩为 lease hotset 语义：5 秒 SLA 只覆盖 active lease hotset，lease 来源包括查询命中、stale hit、project marker、L0 事件、proc sampler 和 `hot_dirs` 显式配置；路径形态 query miss 保持 DirtyQueue 冷目录补偿，不直接扩张 hotset；普通冷目录改由 PeriodicColdScan / dirty repair / query repair 提供有界最终一致。
- 新增 fast scan lease TTL、hotset 总预算和 sentinel registry 持久化配置；clean shutdown 仅持久化 hotset sentinel，恢复时校验 snapshot source、WAL checkpoint、配置 fingerprint 与 mount identity，不可信 registry 进入限速 backfill 且不报告 strict SLA ok。
- fast scan 初始补扫拆成低优先级 `FastScanBootstrapDir`，真实 sentinel 变化继续使用 `FastScanChangedDir`；`/watch-state`、`/health`、diagnostics 与 metrics JSONL 新增 hotset lease/sentinel、explicit/auto lease、lease evictions/renewals、initial backfill、real changed dirs、apply dropped stale batches、scan workers 和 IO budget limited 字段。
- `p1_fast_scan_sla` 集成测试改写为“hotset 内 5 秒 + 冷目录有界最终一致”，并同步 README、tests README 与 watcher wiki 的 SLA 口径，不再断言全部 L1/L2 目录 5 秒覆盖。
- 新增 M2 proc sampler：Linux tiered 模式下按预算采样 `/proc/<pid>/fdinfo` 与 `fd` symlink，只处理同用户进程的写句柄，将索引根内正在被写入的目录作为新鲜度线索送入现有 Ephemeral Watch lease 入口；采样结果不作为查询正确性来源，查询仍由返回前验真保证。`/watch-state`、`/health`、diagnostics 与 metrics JSONL 新增 `proc_sampler_*` 字段，暴露采样耗时、pid/fd 预算、命中目录、触发临时 watcher 数、预算耗尽和不可用状态。
- 新增 M1-3 分片 repair：Periodic cold scan 对大目录按约 512 entries / 20ms 分片处理，未完成 slice 携带运行时 cursor 重新入 DirtyQueue，避免百万 entry 目录在单次冷层巡检中长阻塞；StartupRepairDeferred 保留递归 fast-sync 补偿启动期 rename subtree；`/health`、diagnostics 与 metrics JSONL 新增 `cold_sweep_last_completed`、`cold_sweep_period_estimate`、`dirty_backlog`，把“返回结果是真的，但候选集合可能暂时不全”的全局有界最终一致上界变成可观测字段。
- 新增运行时 subtree tombstone：Delete 父目录和 rename-from 会创建带 TTL 的运行时前缀墓碑，cold/base 查询候选会在同步验真前被 prefix filter 屏蔽，避免删除大目录后宽泛查询对旧子路径逐个 `stat`；Create/Modify/RenameTo 会清理覆盖路径相关 tombstone，避免同名目录重建被误伤。第一版不持久化到 snapshot/WAL。
- 查询冷层/base 结果改为默认返回前同步验真：新增 `[query] max_verify_per_query`、`verify_timeout_ms`、`allow_sync_readdir` 配置；默认单查询最多验证 150 个候选、75ms 超时且禁止查询线程同步 readdir。删除路径不返回，mtime 或 identity 变化会以 `freshness = "changed"` / `validated=true` 返回当前 metadata 并入 DirtyQueue；`lazy_validation_enabled` 默认关闭，显式开启时仅作为低功耗后台补偿路径。
- 修复 rename 事件窗口期导致新下载文件最终名搜不到的问题：事件合并阶段只把孤立 `RenameMode::To` 视为 `Create`，孤立 `RenameMode::From` 保持普通修改语义；同批次 `Create`/`Rename` 不再被后续 `Modify` 覆盖，避免下载器 `.part` → 最终名、编辑器原子保存和配对 rename 在 merge 阶段丢失结构性变化语义。
- 修复 CI 回归误报：`fast_sync_reconciles_add_and_delete` 允许 Linux inode 复用场景下以单次 same-FileKey upsert 完成旧路径遮蔽；stress CI hardlink 断言同步为 PathEntry 多别名语义，不再要求同 inode 单路径折叠。
- L1/L2 tiered watcher 新增 fast scan lane；M3 后该 lane 只对 active lease hotset 提供本地可信文件系统 5 秒覆盖目标，按 `/proc/self/mountinfo` 将 ext4/xfs/btrfs/tmpfs/f2fs 归为 `local_strict`，网络/FUSE/未知文件系统默认 `best_effort`，不会报告 strict SLA 成功。
- 新增 fast scan 配置项：`tiered_watch.l1_l2_fast_scan_enabled`、target/tick/stat/readdir/bootstrap 预算、lease TTL、hotset 总预算、sentinel registry 上限，以及 `network_fast_scan_mode`、network stat/readdir 预算；配置解析、默认值、README 和 diagnostics 已同步。
- fast scan sentinel 发现 hotset 目录变化后以 `DirtyReason::FastScanChangedDir` 进入 DirtyQueue，并复用现有 `fast_sync` depth=1 scan/apply 路径更新索引，避免扫描器直接写索引。
- 修复 fast scan bootstrap 把单批预算误当 sentinel 总量上限的问题：`l1_l2_fast_scan_bootstrap_budget_per_tick` 现在只限制每 tick hotset sentinel 注册与初始 backfill 数量，已覆盖 sentinel 不会阻止后续 hotset lease 继续覆盖。
- 新注册 fast scan sentinel 会进入一次低优先级 `FastScanBootstrapDir` 初始补扫，补齐 sentinel 覆盖前已经发生的文件创建；真实变化继续使用 `FastScanChangedDir`，避免大规模冷段枚举和补扫队列失控。
- fast scan bootstrap 不再每个 tick 枚举全部 L1/L2/cold snapshot：调度层只处理尚未覆盖的 hotset lease，候选耗尽时冷却 bootstrap，避免 80 万级 manifest-only base 被每秒枚举造成 CPU 常驻和 RSS 高水位。
- `/watch-state`、`/health`、diagnostics 与 metrics JSONL 暴露 fast scan enabled/mode/SLA、hotset lease/sentinel、known/local/untrusted dir 数、pending queue、checked/changed/generated counters、coverage lag p50/p95/p99、budget degraded 和 degraded reason；health 区分 hotset 本地 strict 失败、预算降级、冷目录 backlog 与网络/FUSE best-effort。
- 根 README 中 `fd-rdd-sim` 长手册迁移到 `src/sim/README.md`，根 README 只保留入口说明；runtime/sim 字段映射明确 fast scan 为 runtime-only，当前 sim 不强行建模该成本。
- 新增 fast scan 回归测试，覆盖 mount 分类、预算不足降级、本地 sentinel 目录项变化、untrusted mount 不报告 strict SLA，以及深层 leased 目录通过 `FastScanChangedDir` 复用 dirty apply 后可被搜索；真实 daemon `p1_fast_scan_sla` 集成测试和 CI 专项 job 改为验证 hotset create/delete/rename 均在 SLA 窗口内更新搜索结果，冷目录在 cold sweep 周期内最终追平。
- 修复 manifest-only cold segment 在最终快照边界被 overlay 父目录删除误剪的问题：path-only delete tombstone 只屏蔽精确路径，snapshot materialization 会拒绝把大 Base 异常缩水成小快照；启动加载若发现 `stable.v7` 相比 `stable.prev.v7` 灾难性缩水，会自动回退 `stable.prev.v7`，避免整库索引被坏 stable 覆盖后继续扩大损失。
- 启动恢复新增 tiny stable root probe：当 `stable.v7`/`stable.prev.v7` 都已被旧版本覆盖成小快照、无法通过 prev 回退时，会用相同扫描过滤规则抽样根目录；若真实根目录明显大于已加载快照，则标记 `snapshot_too_small_for_roots` 并触发 rebuild 策略。
- rebuild 成功发布新 Base 后会清理当前恢复阻塞标志，避免 `/health` 和 metrics 在索引已恢复后继续报告 `recovery_requires_rebuild` 或 startup repair escalation issue。
- 启动 soft evidence 不再驱动前台 repair scan：`StartupRecoveryReport` 新增 `startup_scan_required`、`deferred_repair`、`deferred_dirty_dirs`、`deferred_unknown_scope`，`startup_repair_if_needed("dirty-only")` 只在 hard evidence、WAL gap 或空索引等需要前台扫描的场景运行；`unclean_shutdown` 与 WAL tail damage 会转入低优先级 `StartupRepairDeferred` 队列。
- WAL replay 改为 valid prefix 语义：遇到首个坏 frame、CRC mismatch、超大 len 或半写 payload 后停止读取 suffix，并返回 `WalReplayDamage`；能 best-effort 解析路径时脏化父目录，无法定位时回退最近 valid WAL 事件目录或标记 unknown scope。
- 新增 lazy validation 后台校验路径：配置项 `lazy_validation_enabled`、`lazy_validation_cache_entries`、`lazy_validation_ttl_secs`、`lazy_validation_stat_per_sec` 控制查询命中 cold segment 后只做非阻塞入队；后台 worker 限流执行 `stat`，发现 stale 后通过统一 apply 路径写 overlay 并推入 deferred repair。
- `DirtyReason::StartupRepairDeferred` 作为低优先级 dirty queue reason 接入后台补偿；deferred repair 可观测队列长度、WAL dirty dir 数、unknown scope 与 lazy validation pending/cache/rate-limit/stale 计数，并进入 `/health`、diagnostics 与 metrics JSONL。
- Recovery audit 对 v7 snapshot 改为浅审计，只检查 header/trailer/segment bounds；完整 segment/global CRC 仅在正式 `load_v7_from_path()` 执行，避免 audit + load 在启动期重复全量校验。
- 低优先级 deferred/periodic scan 在合并扫描结果前记录并检查 `event_seq`，若扫描期间已有更新事件进入统一 apply 路径，则丢弃该轮扫描结果，避免旧后台扫描覆盖新 watcher/lazy validation 事件。
- 启动恢复证据分层：`StartupRecoveryReport` 现在区分 `soft_repair_needed` 与 `hard_rebuild_needed`，`unclean_shutdown` / `wal_tail_truncated` 只作为补扫软证据；`bad_manifest`、`missing_segment`、`wal_gap`、`bad_current_wal`、坏 sidecar 等硬证据仍可触发 rebuild 策略。
- `startup_repair_budget_ms` 接入启动修复深扫，预算耗尽会停止本轮 repair scan 并记录 `startup_repair_budget_exhausted`；`force_rebuild_ratio` 保留为扫描后的次级升级条件，不再由单独异常退出直接放大成重建。
- `/health` 与 metrics JSONL 新增恢复证据观测字段：soft/hard reasons、reason counts、startup repair budget、budget exhausted 与 escalation reason，便于区分“需要补扫”和“需要重建”。
- `/memory` 默认返回轻量快照，复用最近完整采样并刷新 RSS、swap、smaps rollup、faults、pipeline、dirty queue、overlay、rebuild 与 query guard 等轻量字段；显式 `/memory?full=true` 才执行完整 Base/L2 统计并刷新缓存。`memory_report_loop` 与 metrics JSONL 复用同一缓存源，并且 RSS trim 只在完整采样检测到高水位时触发。
- 修复运行态复核发现的 WAL v4 审计误判：`WalStore` 当前写入 v4 WAL，recovery audit 现在复用写入端版本常量接受 v1..=current，不再把正常 `events.wal` 判成 `bad_current_wal` 并触发启动 rebuild。
- 修复 `/memory` light 缓存过期与 stale smaps 误导：snapshot/rebuild/compat refresh 等 Base/L2 结构切换边界会失效完整采样缓存，cache miss 时轻量报告会重新读取当前 Base/L2 结构统计；cache hit 时也会刷新当前 smaps rollup。Linux 上 `process_rss_bytes` 优先取同一次 smaps rollup 的 `rss_bytes`，smaps 不可读时才回退 statm，避免把当前 statm RSS 和旧 full sample 的 smaps 拆分混入同一响应。
- 修复启动 fast-sync 对账误写增量层：启动对账 cutoff 现在按实际 `snapshot_source` 选择 `stable.v7`、`stable.prev.v7` 或 legacy `index.v7` 的 mtime，避免使用错误快照时间放大 dirty window。
- fast-sync 对 dirty 目录中的文件先用 `path_freshness()` 对比 Base/L2 中的 file key 与 mtime，未变化路径不再生成 modify event、写入 L2/overlay/WAL，避免重启后把大量稳定文件重复灌入增量层。
- `DeltaBuffer::clear()` 在 flush/snapshot 后会释放过大的 HashMap capacity，降低一次大批增量对账后的 allocator 高水位常驻。
- strict tiered watcher 新增 `tiered_watch.l0_max_cost_per_root` 单根 L0 成本上限，默认 `8192`；超大 required hot dir 会进入 L1/scan 补偿并暴露为 strict coverage failure，不再在启动时整棵注册数万 inotify watch。
- manifest-only v7 冷段挂载不再遍历全部 live path 构建路径 Bloom-style filter，改为只扫描 entries/tombstones 统计 live count 与 mtime range；`/memory.base.cold_filter_bytes` 在该模式下为 `0`，降低重启时路径 case-fold/trigram 临时分配和 mimalloc 高水位。
- 优化 v7 cold mmap 查询路径：无 trigram hint、legacy fallback 或 `MatchAll` 触发全段扫描时，raw/decoded path table 解析会复用调用方 scratch buffer，只有真正命中的 `FileMeta` 才复制 path，降低启动后首次短查询/full-scan 查询把 80 万级冷段路径反复分配到堆上造成的 RSS 高水位。
- `/health` diagnostics 不再为 hardlink 统计或 case-policy conflict 统计枚举 manifest-only cold segment；hardlink 诊断改为只看热 L2/增量层，case-policy 启动刷新只做 root filesystem policy 探测，避免一次健康检查把 cold mmap 路径表物化到堆上。
- 回归测试补齐恢复证据与内存观测边界：覆盖 clean shutdown skip、unclean shutdown soft repair、bad manifest、missing segment、WAL gap、bad current WAL、当前 WAL 版本兼容、`/memory` 默认 light 与显式 full，以及轻量内存快照缓存复用/失效。
- 新增 v7 cold mmap `MatchAll` full-scan 回归测试，确认路径缓冲区复用后仍只返回 live entry，并保持 tombstone 过滤与 metadata 结果正确。
- 新增 `/health` cold diagnostics 回归测试，确认 manifest-only 冷段中即使存在 hardlink，也不会通过健康检查触发全段 metadata 枚举。

## [0.7.0] - 2026-05-30

- `dupe:content` 补齐查询期内容读取边界：复用内容索引的 frozen/offline、exclude 目录、mount policy 与 `content_index.max_file_size` 准入策略，并把 partial/full hash 慢 I/O 移出 query generation guard，避免内容重复扫描延长 base generation 强引用。
- `scripts/smoke-search-syntax.sh` 支持无 `--root` 默认自建临时 root，并在 HTTP daemon 不可用时自动启动本仓库 `fd-rdd` 做自包含 smoke；仍可用 `--no-auto-spawn` 保持只联调既有 daemon。
- 新增 Runtime Boundary State Contract：WAL 支持 `OFFLINE_ROOT` / `ONLINE_ROOT` root 状态记录，quarantine sidecar 使用物理 mount identity 锚定；启动时先恢复 sidecar 并安装 Freeze Gate，再按 WAL 原始记录顺序回放 root state 与文件事件，阻断离线 root 下 Delete/Modify/Rename 脏写。
- quarantine sidecar verify 接入后台 worker：设备 identity 匹配后先 append `ONLINE_ROOT`，写入成功后才解除 freeze，并将 affected prefixes 入队局部 scan；identity 不匹配或设备仍离线时保持冻结。
- `/health` 新增强类型 `diagnostics` 字段，按 system/storage/security/clocks/watchers/io 固定板块暴露 WAL、snapshot、quarantine verify、freeze、HTTP policy、UDS peer policy、scan reject、clock skew 与 mount policy 诊断，同时保留既有 summary 字段兼容旧客户端。
- mount policy 诊断收口到共享计数器：full build、rebuild、fast-sync、immediate scan、dynamic watch 和 ephemeral watch 入口统一记录拒绝原因，watcher 动态注册前会先执行 mount policy，拒绝时回滚 tiered/ephemeral reservation 并跳过 `watch()` 与后续深扫；FUSE/SSHFS 可疑 mount 通过后台 probe timeout cache 避免扫描线程直接执行可能挂起的 `readdir`。
- 配置支持结构化 `[[roots]]`，兼容旧 `roots = []`；`detected_policy`、`conflict_count`、mount state、freeze gate 等运行时探测状态不会写回 `config.toml`。
- case policy 探测先尝试平台 `pathconf(_PC_CASE_SENSITIVE)` 能力；`EINVAL`/unsupported 时回落到受控临时对象探测，只读、缺失或无权限 root 保持 `Unknown`，Unicode fold 回归继续覆盖 `ß -> ss` 的 byte-window trigram。
- case policy 自动探测结果进入 runtime state 与 `/health.diagnostics.storage.case_policy_roots` / `case_policy_conflict_count`，仍不写回 `config.toml`；恢复加载会先复用上次 runtime state，再在启动路径刷新当前 root 诊断。
- fast-sync 在使用 dirty mtime cutoff 前观察 wall/monotonic clock boundary；当 cutoff 被标记为不可信时转为全量 crawl，并在对账完成后恢复 trusted 状态，避免 wall-clock 回拨后漏掉离线新增文件。
- 后台 full build、rebuild 和 fast-sync 会记录 idle `ioprio_set` best-effort 结果，并通过 `/health.diagnostics.io` 暴露 `ioprio_class` 与 `ioprio_set_failed`，容器或权限受限环境失败时不影响扫描。
- I/O Governor 记录 token bucket 限流等待次数，为后续 scan loop 接入和 `/health.diagnostics.io.token_bucket_limited_count` 提供真实计数来源。
- `TieredIndex` 持有配置化共享 I/O Governor，并将 backoff/token bucket 计数汇总到 `/health.diagnostics.io`。
- I/O Governor 保存最近一次 PSI pressure 观测值，并通过 `/health.diagnostics.io.psi_some_avg10` / `psi_full_avg10` 暴露。
- full build、rebuild 和 `IndexBuilder` 增量补扫会在文件 metadata 读取前消费共享 I/O Governor token，相关操作计数进入 `/health.diagnostics.io`。
- fast-sync、dirty queue 即时扫描和启动修复扫描会在目录/metadata 检查前消费共享 I/O Governor token。
- I/O Governor 会按固定操作间隔低频采样 Linux PSI，并使用配置阈值触发 backoff，避免每次 I/O 都读取 `/proc/pressure/io`。
- 新增默认关闭的 `[mmap_warmup]` 配置：启动挂载 cold v7 mmap 后可执行 best-effort `MADV_WILLNEED`，并通过 `/health.diagnostics.storage` 暴露预热页数、耗时和取消原因。
- `/memory` 与 metrics JSONL 暴露当前 base/L2 generation 的 Arc strong refs，便于观察查询或后台任务是否延长旧代存活。
- `/memory` 与 metrics JSONL 暴露 dirty queue pending scopes、dirs 和估算字节，便于解释待局部对账/补扫的内存来源。
- `/metrics` 与 metrics JSONL 暴露 exact/fuzzy 查询计数、短查询 no-trigram hint 计数，以及 fuzzy fallback 全量候选扫描的 candidates/elapsed 统计。
- `/memory`、`/metrics` 与 metrics JSONL 暴露 query generation guard active/hold/slow 统计，便于观察查询是否延长 ArcSwap 旧代存活。
- 新增 `runtime_profile = "memory_light"` 与 `--runtime-profile memory_light`：降低 overlay/周期 flush 门槛，增加周期 flush 最大滞留时间，缩短 rebuild 合并冷却，并在 WAL 体积超过阈值时请求 snapshot 边界；默认 profile 保持原策略不变。
- `scripts/fs-churn.py --auto-spawn-fd` 更新为当前 daemon CLI，并支持 `--fd-runtime-profile memory_light`，可直接生成默认 profile 与低内存 profile 的 churn/RSS 对照。
- 新增 hardlink physical grouping 派生视图：`PersistentIndex::hardlink_groups()` 与 physical dedupe stats 会按当前可见 path/docid 临时聚合同一 `FileKey` 的多路径，不改变搜索主键；`/health.diagnostics.storage` 同步暴露 `hardlink_group_count` 与 `hardlink_max_group_size`。
- 补齐 case policy 自动探测基础、mount policy 拒绝原因矩阵、clock skew dirty window 和 root/system daemon 默认禁用未认证 HTTP query/scan 的安全策略测试。
- 测试补强：新增 daemon API/UDS E2E、真实 watcher create/rename/delete、abrupt kill 与坏 stable snapshot 启动修复组合测试，补齐此前偏模块级的关键真实链路缺口。
- 仓库清理：将本地运行生成的 `reports/`/`report/` 加入忽略，并停止追踪历史报告产物，避免测试与 daemon 运行污染提交。
- CI 压测补齐：`stress-large-scale` workflow 显式运行 `large_directory_scan_100k_files` 与 `high_load_event_processing` 两个 ignored 重型测试；80 万文件 hybrid 压测继续由 `stress-hybrid-large-scale` 执行。
- 提高 tiered watcher 默认预算：未显式配置 `max_watch_dirs` 时默认使用 `131072`，避免首次 `--watch-mode tiered` 仍因 balanced 低预算把 `Downloads` / `Documents` 留在非实时层。
- 新增 tiered watcher `profile = "strict" | "balanced" | "low_power"`：strict 模式要求 `strict_required_hot_dirs` 全部进入 L0；预算不足时 `/watch-state` 输出 required cost、shortfall 和 uncovered dirs，`/health` 按 fail-hard 配置返回 degraded 或 warning。
- 修正 L3 一致性语义：`/debug/tiered-watch` 将 L3 上次扫描干净展示为 `ScannedFresh`，metrics diagnostics 使用 `eventually_consistent_dirs` 标记未实时 watch 覆盖的 L3 目录。
- 新增 directory manifest crawler：扫描后为目录维护 child count、names hash、mtime range 和 last scan generation，L2/L3 periodic cold scan 会在 manifest 未变化且 clock cutoff 可信时跳过真实补扫；`/watch-state` 暴露 manifest dirs、skipped、changed 和 untrusted clock bypass 计数。
- `/watch-state` 补齐 watch cost 与拒绝诊断：明确区分 `logical_watch_cost`、`kernel_watch_cost`、`skipped_watch_cost`，预算拒绝记录最近一次真实 kernel wd cost、预算余量和原因，并将 mount policy 拒绝与 exclude 拒绝分别计数。
- 完成 fanotify 预研：明确当前不接入默认 watcher 热路径，仅作为未来 opt-in/capability-gated 实验后端；权限事件模式禁用，FID-only/path 恢复失败走 DirtyQueue reconcile fallback。
- 补齐批次 6 watcher 验收测试：覆盖 project marker candidate/promotion/ephemeral lease、`src/main.rs` dirty scan 后可搜索、nested project debug 解释、exclude/mount policy 拦截、low_power/strict budget 语义和 L0 idle 降级。
- 新增 metrics JSONL 统一诊断快照：保留顶层 `/watch-state` 字段兼容旧 jq 查询，并追加 `runtime`、`memory`、`health`、`diagnostics` 嵌套对象；`/health` 拆分底层 event watcher 降级与 tiered 非 L0 冷层目录口径，`/memory` 增加 `process_swap_bytes`。
- 优化 manifest-only v7 冷段查询：base 查询层改为直接从 cold mmap 返回 `FileMeta` 与 tier 标记，避免 `query_keys -> get_meta -> key_is_manifest_only` 对 50 万级 cold segment 重复全段扫描；冷段构建和冷查询后会对 v7 mmap 执行 `MADV_DONTNEED`，降低查询后 tmpfs snapshot 页长期计入 RSS 的概率。
- 修复 snapshot 后 base 重新热化：`snapshot_now()` 写出 v7 后会立即重新以 manifest-only cold segment 挂载，避免当前进程把 50 万级路径表留在 hot memory。
- 修复冷挂载与 tiered watch 成本口径：v7 mmap 完整 CRC 校验后执行 `MADV_DONTNEED`，避免 tmpfs snapshot 校验页长期计入进程 RSS；tiered L0/动态/临时 watcher 预算改按 notify 真实递归目录数估算，单根超过预算时返回 `cap + 1` 并拒绝进入 L0，不再用排除目录后的扫描成本低估 inotify watch 数。
- 调整 `/health` 严重级别：底层 event watcher 降级仍为 `index_health = "degraded"`，纯 tiered 非 L0 冷层、watch budget 满或 promotion blocked 只作为 `warning`，避免把预算策略误报成 watcher 故障。
- Breaking change: HTTP `/search` 响应正式删除 `size` 字段；`size:` 过滤器和 `sort=size` 已移除，传入时返回 400 而不是回退到文本匹配或 score 排序。
- 存储兼容：v7 单文件快照 header version 升级到 2，新写 entry 为 32B；读取端继续兼容旧 version 1 的 40B entry 并忽略历史 `size` 字段。
- 修复 L3 scan policy 实现缺口：`TieredWatchConfig`、daemon 调度、`fd-rdd-sim` policy/recommendation/config patch 现在都显式支持 `l3_scan_policy` 与 `l3_scan_interval_secs`，`validate_on_query` / `disabled` 不再被固定 `L2*2` 周期主动扫描。
- 修复 manifest-only cold segment 查询路径：冷段 query、metadata lookup 和 parent candidates 改为直接按需读取 v7 mmap 段，避免每次冷查询都 `to_base_index_data()` 全量 hydration。
- 修复 v7 mmap 冷段 trigram 预过滤的 false negative：旧 basename-only 段或 posting 缺失/空交集时会回退全段精确过滤，目录组件命中不再漏查。
- 修复 v7 snapshot 写入与 sim optimizer 遗漏：新写 v7 段会持久化完整路径 trigram posting 与 sentinel，使 manifest-only 冷段可直接使用 mmap posting；grid/evolve/optimize 现在会枚举和变异 `interval`、`validate_on_query`、`disabled` 三种 L3 策略模式。
- 完成 Storage Legacy Cleanup：运行时 `StorageBackend` 不再暴露 legacy v2-v6 snapshot / old LSM read API，这些读取能力保留在 `SnapshotStore` compatibility 边界与兼容测试中；常规 query 和 fast-sync 不再隐式调用 `refresh_base()`，空 base 兼容场景改为只读 L2 warm-memory fallback，并对真实文件系统命中执行 stale 校验和删除补偿。
- `refresh_base()` 标记为 compatibility/test boundary，新增 `refresh_base_count` 到 `/health.diagnostics.storage`、runtime metrics 和 stats；补齐 query 不物化 base、fast-sync 增删对齐、L1 rename/delete/recreate 失效、旧快照读取兼容、WAL replay、snapshot/rebuild 与 cold mmap 查询回归。
- rkyv manifest 继续延后：stable v7 手写 manifest schema 仍在演进，当前不引入额外依赖；DocId 仍保持内部身份，不进入对外 API。

## [0.6.16] - 2026-05-10

覆盖提交范围：`9a6d9d7` 到 `607e6c3`。

### 分层 watcher

- 完成 L0/L1/L2/L3 热度调度：引入 `event_score`、分层扫描队列、空扫降级、变化回升、冷 L0 替换和 watch 预算阻塞统计。
- 补齐 `/debug/tiered-watch` 与 `/watch-state` 观测闭环，暴露目录层级、dirty/freshness、next scan、预算阻塞、高优先级扫描、冷层校验和查询 stale 命中计数。
- 明确 L3 策略语义，支持 interval、validate-on-query、disabled 三类行为，不再把 L3 周期固定推导为 L2 的两倍。
- 实现 Ephemeral Watch 临时监听租约：重复 dirty scope 可申请独立预算的临时 watcher，并按 TTL、idle、无变化补扫、L0 覆盖和低价值驱逐自动释放。

### 冷层查询与补偿

- `/search` 结果增加 `freshness`、`index_tier`、`validated`，冷层/base 命中会执行 `stat` 校验。
- 删除或非文件命中会写 tombstone 并屏蔽旧结果；mtime 或文件身份变化会返回当前 metadata，并把父目录加入 DirtyQueue。
- 完成 DirtyQueue 闭环：统一承接 inotify 冷层事件、查询 stale hit、路径形态 query miss、周期冷层扫描、启动修复和 overflow recovery。
- DirtyQueue 支持 reason、priority、debounce、失败重试和父级 scope 扩大，局部补扫优先处理叶子目录。

### 仿真与策略回归

- 新增 `fd-rdd-sim` 策略仿真框架，支持 single、grid、evolve、adversarial、optimize，并输出 SLA、发现延迟、watch/scan 成本和策略动作计数。
- `emit-config` 支持从 benchmark report 生成可审阅的 `watch_mode = "tiered"` 与 `[tiered_watch]` TOML patch，并标注 sim-only 字段。
- 增加 runtime/sim parity 回归，覆盖热 L0 保留、BudgetBlocked 高优先级排序、祖先 L0 替换保护、watch budget 上限和固定 seed workload。
- 新增 `home-desktop` workload 与 `fd-rdd-sim regression`，用固定 seed golden workloads 在 CI 中检查发现延迟、预算阻塞、watch 成本、扫描量和最终层级分布。
- 优化 sim 运行日志，提前初始化 tracing，并输出优化轮次、试验进度、耗时和 ETA。

### 索引内存与查询性能

- 移除 `FileEntry.size`，降低 mtime 常驻精度，合并 `PathTableV2` 内部 Vec，并移除短组件索引，进一步压缩 base 常驻内存。
- trigram 索引改为 basename-only 候选，降低 posting 体量；路径字面查询不再误加 `PathInitialsMatcher`，避免 `fd-rdd/todo.md` 这类查询退化为额外全扫。
- v7 快照启动时挂载为 manifest-only 冷段，常驻 segment manifest、路径 Bloom-style filter、mtime range 和 dirty/freshness 状态。
- 冷段 metadata/postings 通过 mmap 按需加载，查询命中返回 `index_tier = "FrozenManifestOnly"`；`/memory` 拆出 hot entries、manifest-only entries、cold segment 和 cold mmap 字节。

### 文档与验证

- 更新 README、CHANGELOG 和 runtime/sim 字段映射文档，说明 tiered watcher、DirtyQueue、Ephemeral Watch、冷层查询校验和真实 index residency。
- 增加冷层查询、DirtyQueue、Ephemeral Watch、sim 配置回填、策略回归和 manifest-only 冷挂载测试。

## [0.6.14] - 2026-05-02

### Runtime footprint hardening

- Added default index-time directory exclusions for dependency/build/cache trees such as `.git`, `.cache`, `.cargo`, `.npm`, `.pnpm-store`, `.yarn`, `node_modules`, `target`, `dist`, `build`, and `vendor`.
- Wired exclusions through cold full build, incremental scans, fast sync collection, and watcher event filtering so excluded directories do not enter the index instead of being merely demoted at query scoring time.
- Added repeatable `--exclude-dir NAME` CLI overrides and `exclude_dirs` config support; existing config files without the field keep the default exclusion list.
- On startup, existing `config.toml` files that do not declare `exclude_dirs` are migrated by appending the default list, making the exclusions visible and user-editable.
- Added `GET /memory` to expose the same `MemoryReport` data as JSON for RSS/smaps/index/overlay/event-pipeline attribution.
- Included `BaseIndexData` in memory attribution and removed the redundant runtime `entries_by_path` clone; v7 snapshots still write the segment for format compatibility, but runtime uses a single `FileEntryIndex`.
- Triggered allocator collection after v7 snapshot loading and WAL replay to reduce startup decode/build high-water RSS.
- Removed the unused `FileEntryIndex` path permutation and tightened `PathTableV2` entry layout to reduce per-file base overhead.
- Changed newly written v7 path-table segments to store the compressed `PathTableV2` representation directly while keeping legacy full-path segment loading support.
- Added automatic high-water RSS trimming in the memory report loop, a one-shot idle trim after event bursts, and `GET/POST /trim` for manual allocator collection.
- Replaced runtime `ParentIndex` per-directory `RoaringBitmap` storage with sorted `Vec<u32>` direct-child doc IDs, keeping v7 parent segment compatibility while reducing many small heap allocations.
- Added `watch_enabled` config and `--no-watch` to run in static snapshot/manual-scan mode, allowing watcher memory attribution and low-RSS read-only operation.
- Added `watch_mode = "recursive" | "tiered" | "off"` plus `--watch-mode`; tiered mode admits only budgeted hot directory candidates into L0 and scans rejected candidates with a bounded warm-scan loop.
- Added `GET /watch-state` to expose watcher mode, L0/L1 counts, estimated watch budget use, scan backlog, and tiered scheduler notes.
- Raised the default periodic snapshot batch gate so a handful of filesystem events stay in WAL/DeltaBuffer instead of materializing the full base every snapshot interval.

### PersistentIndex storage migration

- Migrated `PersistentIndex` runtime storage to `FileEntry + Vec<Vec<u8>>` absolute paths.
- Kept `CompactMeta + PathArena` only as legacy v4/v5 loading and v5/v6 compatibility export formats.
- Kept `PathTableV2 + FileEntryIndex` as export-time read-only structures for `BaseIndexData` / v7 snapshots instead of maintaining them on the event hot path.
- Updated overlong-path behavior: runtime indexing is no longer constrained by the legacy `u16 path_len` arena format.

## [0.6.12] - 2026-05-02

### Wrap-up stabilization

- Restored the `tests` branch to a compiling and fully tested state after divergent wrap-up merges.
- Removed `TieredIndex` `disk_layers` hot-path state and deleted the unused `src/index/tiered/disk_layer.rs`.
- Kept `TieredIndex` startup on v7 snapshots or empty rebuild fallback; removed reintroduced LSM/v6 mmap loading from the query hot path.
- Removed synchronous deep directory scans from event apply on directory rename.
- Reconnected WAL replay after v7 snapshot loading and empty startup.
- Fixed `snapshot_v7` serialization to match the current `ParentIndex` structure without `compat_dir_to_files`.
- Added query-time BaseIndex refresh when direct L2 writes make the base snapshot stale.

## [0.6.11] - 2026-05-01

### Phase 8: DeltaBuffer 硬容量上限 + PathTable 内存优化 + Hybrid Crawler 清理

- **DeltaBuffer 硬容量上限**：默认 `max_capacity = 256 * 1024`，`insert`/`apply_events` 返回 `bool` 表示是否因容量限制丢弃
- **容量满自动 flush**：`TieredEvents::apply_tiered_events` 在检测到 `apply_events` 返回 `false` 时自动触发 flush
- **PathTable + FileEntry**：新增 `PathTable`（`path_to_id`/`id_to_path` 双射表），`FileEntry` 用 `PathId: u32` 替代 `PathBuf`，目标节省约 75% 内存（800MB → 200MB）
- **Hybrid Crawler 清理**：删除 `startup_reconcile`、`spawn_rebuild`、`reconcile_degraded_root`、overflow recovery loop、`DirtyTracker`；`spawn_fast_sync`/`spawn_repair` 签名简化，移除定时器驱动，改为纯事件驱动
- **编译/测试验证**：`cargo check` 0 errors 0 warnings；`cargo test --lib` 145 passed；`cargo test --test '*'` 2 passed；`cargo build --release` 成功

## [0.6.10] - 2026-05-01

### Phase 7: ParentIndex 增量维护正确性修复 + 死代码清理

- 在 `finish_rebuild` 完成 L2 替换后重建 ParentIndex，确保 rebuild 后的 parent 查询正确
- 在 `apply_events_inner`/`apply_events_inner_drain` 修改 L2 后重建 ParentIndex
- 在 `apply_upserted_metas_inner` 修改 L2 后重建 ParentIndex
- 进一步清理死代码警告（PathArenaSet 等）

## [0.6.9] - 2026-05-01

### Phase 6: ParentIndex query acceleration + dead code cleanup

- Integrate ParentIndex into query path for `parent:`/`infolder:` filters
- Add `extract_parent_filter()` to `CompiledQuery`
- Add `parent_candidates()` to `PersistentIndex`
- Clean up dead_code warnings (DiskLayer.id, fill_from_compaction, PathArenaSet methods, DeltaBuffer.capacity)

## [0.6.8] - 2026-05-01

### Phase 5: DeltaBuffer Default Enable

- **Default-enable DeltaBuffer**: Removed `USE_DELTA_BUFFER` environment variable gate. DeltaBuffer is now the sole overlay mechanism for event buffering.
- **Removed deprecated `overlay_state`**: Deleted `OverlayState` struct and `update_overlay_for_events` method from `src/index/tiered/events.rs`.
- **Removed `pending_events`**: Deleted the `pending_events: Mutex<Vec<EventRecord>>` field from `TieredIndex` and all related push/drain logic.
- **Simplified query paths**: `collect_all_live_metas` and `execute_query_plan` now unconditionally read from `delta_buffer`.
- **Simplified memory reporting**: `file_count()` and `memory_report()` no longer branch on environment variable.
- **Simplified snapshot flush**: `snapshot_now` unconditionally uses `delta_buffer` for dirty checks and drain.
- **Updated `finish_rebuild`**: Replaced `overlay_state` clear with `delta_buffer.clear()`.
- **Code reduction**: ~362 lines removed across 7 files in `src/index/tiered/`.

## [0.6.7] - 2026-05-01

### Changed

- **Phase 4: ParentIndex Default Enable + Deprecated Cleanup**
  - **Default enabled** `ParentIndex`: removed `USE_PARENT_INDEX` environment variable A/B switch. `fast_sync` Phase3 delete alignment now always uses `delete_alignment_with_parent_index()` (O(D)), removing the legacy `for_each_live_meta_in_dirs()` O(N) fallback.
  - **Removed** `for_each_live_meta_in_dirs()` method: no longer has any callers after the fallback path removal.
  - **Build ParentIndex at startup**: `rebuild_parent_index()` is now automatically called after each snapshot recovery path (LSM / v6 / v2-v5) in `load_or_empty_with_options`, eliminating cold-start latency on first `fast_sync`.
  - **Removed** deprecated `startup_reconcile()` and `spawn_rebuild()`:
    - Deleted both deprecated functions from `src/index/tiered/sync.rs`
    - Removed `startup_reconcile()` call from `src/main.rs`
    - Removed related test cases from `src/index/tiered/tests.rs`
  - Net code reduction from eliminating fallback paths and deprecated code.

## [0.6.6] - 2026-05-01

### Added

- **Phase 3: DeltaBuffer Unified Incremental Buffer**: Introduced `DeltaBuffer` module to unify `overlay_state` and `pending_events`.
  - Added `src/index/delta_buffer.rs`: `DeltaBuffer` backed by `HashMap<Vec<u8>, DeltaState>` mapping paths to their latest delta state (Live/Deleted).
  - Capacity limit 256K entries (default `262_144`), solving the original `pending_events` 4096 overflow issue.
  - State transitions: `Create/Modify` → `Live`, `Delete` → `Deleted`, `Rename` marks from→Deleted and to→Live simultaneously.
  - Refactored `TieredIndex`: added `delta_buffer` field, keeping old `overlay_state` / `pending_events` as fallback.
  - Refactored query path: `execute_query_plan` / `collect_all_live_metas` now read deleted/upserted/live state from `DeltaBuffer`.
  - Refactored event application path: `begin_apply_batch` / `update_overlay_for_events` changed to `update_delta_buffer_for_events`.
  - Environment variable `USE_DELTA_BUFFER=1` enables the new path; old path remains as fallback when unset.
  - 133 unit tests pass, integration tests pass.

## [0.6.5] - 2026-05-01

### Added

- **Phase 2: ParentIndex Module**: Introduced `ParentIndex` to eliminate the core performance bottleneck in `fast_sync` Phase3.
  - Added `src/index/parent_index.rs`: `ParentIndex` backed by `HashMap<PathBuf, RoaringTreemap>` mapping parent directories to their live `DocId`s.
  - `PersistentIndex` now carries `parent_index: RwLock<Option<ParentIndex>>`.
  - Added `rebuild_parent_index()` invoked after rebuild and snapshot to warm the index.
  - `fast_sync` Phase3 supports `USE_PARENT_INDEX` environment variable for A/B switching between:
    - `delete_alignment_with_parent_index()` (O(D) where D = dirty directory count)
    - Legacy `for_each_live_meta_in_dirs()` (O(N) full scan) as fallback.

## [0.6.4] - 2026-05-01

### Added

- **Benchmark Framework (Phase 0)**: Comprehensive benchmarking and profiling infrastructure.
  - `scripts/bench.sh`: Automated benchmark suite covering compilation time, startup latency, baseline RSS/CPU, query latency (warm/cold), and event-storm throughput.
  - `scripts/profile.sh`: Performance profiling harness for `perf` (flamegraph) and `dhat` (heap allocation).
  - `src/stats/mod.rs`: `StatsCollector` runtime metrics collection (RSS, CPU, query latency, event throughput).
  - `src/query/server.rs`: `/metrics` HTTP endpoint exposing Prometheus-compatible stats.
  - `BENCHMARK.md`: Baseline documentation for reproducible benchmarks.
  - `.github/workflows/bench.yml`: CI integration for automated regression detection.

### Removed

- **Dead Code Cleanup (Phase 1)**: Minimal cleanup targeting code that conflicts with the "low-footprint, long-running" positioning.
  - Deleted `src/index/tiered/compaction.rs` (~383 lines). Fast compaction path remains inline; legacy slow path removed.
  - Deleted polling functions from `src/index/recovery.rs`.
  - Deleted RSS trim loop and associated CLI arguments (`--rss-trim-interval`, `--malloc-trim`).
  - Deleted `dyn_walk_and_enqueue` and related dynamic walk helpers.
  - Marked rebuild/recovery functions in `src/sync.rs` as `#[deprecated]` (to be removed in v0.7.0).
  - Simplified CLI argument surface by removing unused tuning knobs.

## [0.6.3] - 2026-04-26

### Changed

- **BTreeMap → HashMap for internal index maps**: `filekey_to_docid` and `path_hash_to_id` changed from `BTreeMap` to `HashMap`, saving ~48 MB RSS at million-file scale. These maps only perform point lookups, inserts, and deletes — no ordering dependency.
- **EventPipeline default channel_size reduced**: Default `channel_size` in `EventPipeline::new()` reduced from 262144 to 131072 (~11 MB saved). Production path already overrides via CLI default (65536).
- **FAST_COMPACTION enabled by default**: `unwrap_or(false)` changed to `unwrap_or(true)` — the fast compaction path (`compact_layers_fast`) using bitmap OR merging is now the default, eliminating per-meta allocation spikes.
- **short_component_index key type optimized**: Changed from `HashMap<Box<[u8]>, RoaringTreemap>` to `HashMap<u16, RoaringTreemap>` using big-endian encoding for 1-2 byte path components, eliminating ~3 MB of heap metadata overhead (previously 21:1 overhead-to-data ratio).

### Added

- **L1 Cache path_index O(1) fast path**: Added `exact_path()` method to `Matcher` trait. `WfnMatcher` with `FullPath` scope now takes an O(1) lookup path via the existing `path_index` instead of O(N) full scan.

### Fixed

- **Dynamic directory monitoring**: Newly created directories (from `git clone`, `npm install`, `mkdir`) now automatically receive recursive inotify watches. The event processing loop detects `Create(Folder)` events and calls `watcher.watch(new_dir, Recursive)` dynamically.

## [0.6.2] - 2026-04-26

### Fixed

- **False-negative search results**: `trigram_candidates` in both `PersistentIndex` and `MmapIndex` returned `Some(empty bitmap)` instead of `None` when trigram intersection was empty, blocking fallback to `short_hint_candidates` and full scan. Changed to return `None`.
- **upsert race condition**: Added `upsert_lock` (write lock held during entire rename/new-file path) to prevent query-write races causing trigram index/metas inconsistency.
- **pending_events visibility gap**: Reordered `apply_events` before `remove_from_pending`, and integrated `pending_events` scan into `execute_query_plan`, ensuring debounce-window files are always visible.
- **file_count() snapshot inconsistency**: Added `apply_gate.read()` lock to prevent reading intermediate state between L2 swap and disk_layers update during snapshot.
- **file_count() undercount**: Changed to sum L2 + all disk_layers + overlay_upserted.
- **CI inotify limit exhaustion**: Raised `max_user_watches` to 1048576 and added `max_queued_events=524288` in CI workflow.
- **CI performance thresholds**: Relaxed CPU 100% duration threshold from 3000ms to 10000ms and RSS peak from 400MB to 600MB for 2-core CI runners.

### Changed

- **CompactMeta.mtime optimization**: `mtime` field changed from `Option<SystemTime>` (16B) to `i64` nanosecond timestamp (8B), saving ~8 MB at million-file scale.
- **filekey_to_docid / path_hash_to_id**: Changed from `HashMap` to `BTreeMap` in v0.6.2 (reverted to `HashMap` in v0.6.3 for further memory savings).

## [0.6.1] - 2026-04-25

### Added

- `Config::save()` method: `Config` now implements `serde::Serialize`, allowing the active configuration to be written back to `~/.config/fd-rdd/config.toml` in TOML format.
- First-run auto-configuration: on first startup, if `~/.config/fd-rdd/config.toml` does not exist, `--root` is required. After a successful start the default configuration is automatically saved. Subsequent launches need no arguments; simply run `fd-rdd`.
- Large-scale hybrid correctness test (`tests/p2_large_scale_hybrid.rs`): an 800K-file integration test that validates incremental indexing under realistic developer workflows (git clone, npm install, single-file CRUD). Marked with `#[ignore]` for explicit CI invocation.

### Fixed

- musl target build failure: switched `reqwest` dev-dependency to `rustls-tls`, removing the musl cross-compile dependency on system OpenSSL.
- CI `musl-build` job: added `musl-tools` installation step so the musl cross-compile environment is complete.
- `cargo fmt` formatting check: formatted all Rust sources so `cargo fmt --all -- --check` passes cleanly.
- Clippy `dead_code` / `unused` warnings: added module-level `#![allow(dead_code, unused)]` in test helper modules (`tests/common/`, `tests/fixtures/`) so `cargo clippy --all-targets -- -D warnings` passes.

## [0.6.0] - 2026-04-20

### Added

- `snapshot_loop` minimum interval guard (10s): prevents cascading snapshot triggers from high-frequency overlay flush requests.
- PendingMoveMap rename matching: resolves file disappearance caused by cross-batch renames.
- Dynamic delay back-pressure: monitors channel watermark and injects sleep when >80%, preventing OOM during event storms such as `npm install`.
- i_generation generational validation: uses `FS_IOC_GETVERSION` to fetch inode generation, completely eliminating ghost files caused by inode reuse.
- Directory rename deep sync: triggers deep fast-sync recursively when a directory rename is detected.
- Unicode NFC normalization: integrates `unicode-normalization`; all paths are forced to NFC to eliminate encoding traps.
- fd-rdd Stress CI: systematic stress tests covering overlay visibility, rename avalanches, concurrent intermediate states, mmap safety, trigram skew, etc.

### Fixed

- `snapshot_now` data visibility window: moved `export_segments_v6()` before L2 swap and serialized inside `apply_gate.write()` lock, eliminating the query data loss window between swap and disk_layers push.
- Compaction frequency causing CPU/RAM spikes: thresholds raised from `2 delta / 30s cooldown` to `8 delta / 4 max_deltas / 300s cooldown`, significantly reducing compaction frequency and temporary allocation in million-file scenarios.
- Watcher channel batch event overflow: default `event_channel_size` raised from 4096 to 262144, lowering the probability of silent event drops during bulk operations such as git clone / extraction.
- Fast-sync fallback latency: cooldown shortened from 5s to 1s, max-staleness from 30s to 5s, enabling faster incremental catch-up after overflow and reducing perceived "new file not found" latency.
- `execute_query_plan` overlay visibility: merged `overlay.upserted_paths` to ensure newly created files are visible in L2/L3 queries.
- Tombstones/trigrams atomicity: ensures `tombstones.insert` happens before `remove_trigrams`, eliminating the query miss window during deletion.
- GitHub Actions `fd-rdd` startup command line fragility: pressure workflow and smoke nodes changed to single-line startup, avoiding `\` being mistakenly passed to clap and causing tests to fail before daemon starts.
- Nightly ThreadSanitizer ABI mismatch: sanitizer job changed to use job-level `RUSTFLAGS=-Z sanitizer=thread`, ensuring the current crate, dependencies, and std use the same sanitizer ABI.
- `snapshot_now` synchronous stage blocking: moved synchronous stage to `spawn_blocking`, avoiding blocking the tokio runtime; also enforces `MIN_SNAPSHOT_INTERVAL` when `interval=0` to prevent high-frequency snapshot cascades.
- `apply_gate` write-lock starvation: uses `try_write` instead of `write`, avoiding persistent write-lock hold causing tokio worker read-priority starvation.
- `compute_highlights` Chinese UTF-8 out-of-bounds panic: cherry-picked main-branch fix where matching advanced start by `+1`, causing multi-byte UTF-8 Chinese characters to slice in the middle of a character on the next round; now advances by the actual matched substring byte length.
- Chinese exact query test missing `generation` field: added missing `generation` field to `FileKey` in `chinese_exact_query_via_trigram` test.
- inotify `max_user_watches` exhaustion causing deep subdirectory watch silent failure: `handle_notify_result()` in `watcher.rs` no longer silently drops notify errors; actively identifies ENOSPC (errno 28 / "no space") and marks all related directories dirty. Added `watch_roots_enhanced()` to estimate per-root watch demand before adding recursive watches; if system limit is tight, marks the root as degraded and records it in `DirtyTracker`, making failure observable.
- Hybrid Crawler degraded-root incremental reconciliation: in `stream.rs`, replaced simple polling fallback with Hybrid Crawler background task. Maintains 60s fast-sync for `failed_roots`; adds 30s reconciliation loop for `degraded_roots`, iterating DFS (max depth 20, skipping hidden dirs and ignore paths) over the directory tree and comparing mtime, marking changed subdirectories dirty via `DirtyTracker::record_overflow_paths()`, which triggers existing overflow recovery logic automatically.
- Fast-sync safety margin breakage causing new file loss in degraded mode: `reconcile_degraded_root` used `last_sync_ns - 10s` to detect changed directories, but `fast_sync` `DirtyScope::Dirs` branch re-filtered `root_dirs` with raw `cutoff_ns`, causing reconciled changed dirs to be incorrectly filtered. Unified to `cutoff_ns.saturating_sub(10_000_000_000)` so the safety margin is consistent end-to-end.
- Fast-sync semaphore race causing dirty state false consumption: when `spawn_fast_sync` was skipped due to semaphore contention, the old code incorrectly called `tracker.finish_sync()`, clearing dirty markers and `sync_in_progress`, causing changed directories to lose indexing opportunities. Changed to call the new `tracker.rollback_sync(scope)` to roll back dirty state and `sync_in_progress`, ensuring retry on next scheduling.
