# fd-rdd 基准数据

| 指标 | 基线 (v0.6.3) | v0.6.4 |
|------|---------------|--------|
| 编译时间 (release) | TBD | TBD |
| 启动时间 (有索引) | TBD | TBD |
| 空闲 RSS | ~700MB | TBD |
| 空闲 CPU | 100% 峰值 | TBD |
| 查询 QPS | TBD | TBD |
| 事件风暴恢复时间 | TBD | TBD |

## 2026-05-02 tests 分支压测

命令：

- `cargo test -q`
- `cargo test -q -- --ignored`

`p2_large_scale_hybrid` 结果：

| 阶段 | CPU 峰值 | CPU>=100% 时长 | RSS 峰值 |
|------|----------|----------------|----------|
| initial_indexing | 125% | 2034ms | 237680KB |
| git_clone | 0% | 0ms | 0KB |
| npm_install | 0% | 0ms | 0KB |

说明：冷启动全量扫描使用保守串行策略和批次节流，优先满足事件/查询可用性与 CPU 峰值约束。v7 快照启动改为直接挂载 `BaseIndexData`，不再把快照逐条回灌到 L2。

## M2 冷层轮转 VM 验证计划

M2 `Rotating Cold Freshness Window` 不能只用“正常情况下能搜到”证明可用。验证目标是：在事件丢失、后台扫描滞后、删除风暴、重命名风暴、冷段 mmap、Tombstone overlay、Lazy Validation、`apply_seq` 竞态同时存在时，系统仍然不爆 RSS、不拖死前台查询、不复活幽灵文件、不用后台旧事实覆盖 watcher 新事实。

现有指标可以复用：daemon 已经每 30 秒写 `reports/metrics/metrics_YYYY-MM-DD_HH.json`，顶层兼容 `/watch-state`，嵌套包含 `runtime`、`memory`、`health`、`diagnostics`。`scripts/m2-cold-window-vm-bench.py` 负责隔离启动 fd-rdd、周期采集 HTTP 端点和 `/proc/<pid>`，并把内建 metrics 一并保存在单次 run 目录。脚本输出同时区分 active canary 与 passive canary：active canary 创建后立刻轮询搜索，可能测到 query miss / fast scan 触发的补偿；passive canary 先写入、等待 settle 后只做首次查询，用来衡量后台主动追平。

### 一键 A/B

```bash
python3 scripts/m2-cold-window-ab.py a  # 开启 M2
python3 scripts/m2-cold-window-ab.py b  # 关闭 M2
```

驱动只接受 `a`/`b`，固定重建 `$HOME/fd-rdd-m2-roots` 专用 fixture，并把输出写入 `/tmp/fd-rdd-m2-runs`。两组除 treatment 开关外使用相同的一小时 event storm、canary、扫描预算和 seed；event storm 在 180 秒后开始，以 60 秒 settle 和 10 秒轮间隔完整执行 4 个 tier × 8 类 workload 的 32 个组合。fixture 从重建到 daemon 清理完成始终持有非阻塞独占锁；第二个并发 wrapper 会在改动目录前直接失败。运行中会提前拒绝开关错配、没有 L2/L3 或 A 无 M2 活动，结束后要求 summary 与 manifest 都明确 `ab_comparable=true`，再校验完整时长、burst 和 summary/manifest/process/endpoint 产物；fd-rdd 必须以 0 退出，失败均返回非零。wrapper 会把底层 runner 的 stdout/stderr 同步显示并持久化为 run 目录同级的 `<run-name>.runner.log`，并在 run 目录原子写入 `ab-wrapper-result.json`；只有明确的 `passed` 终态才可被快速 suite 续跑复用，失败、中断、运行中或缺失终态都必须新建 attempt。失败输出会直接汇总 run 目录、manifest/summary 关键状态、缺失/空产物、最近错误事件，以及 runner/daemon 各最多 20 行关键日志，避免只有“底层 benchmark 退出码 1”而无法定位。使用 `--dry-run` 可只打印固定命令，不修改环境。

### M2 快速证伪配对套件

在执行 12-run 规模矩阵前先运行：

```bash
python3 scripts/m2-cold-window-falsification.py
```

套件固定运行 4 个配对 block（`2×AB + 2×BA`，共 8 腿），每腿 1200 秒，只向
`cold-a` 的 L3 注入两轮 `save100`、`git_clone`、`subtree_rename`。相邻 burst 使用 120 秒 settle
和 30 秒轮间隔：150 秒周期覆盖变更后 `L1 -> L2 -> L3` 的 125 秒配置恢复路径，并保留 25 秒
调度余量；六轮和最后一次 settle 仍在 1200 秒内完成。每腿必须精确形成
6 个 physical burst、806 个唯一路径主断言和 38 个有界可见性探针。它跨腿校验
Git/binary/fixture/保序事件计划/协议指纹，并验证四块 order/position 精确满足 `2×AB + 2×BA`。
完整运行的配对 CPU service time、I/O、RSS、fault、查询轮询负载、最终状态断言、目标目录实时 M2 租约与写入后扫描/事件证据、
snapshot、水位线和 DirtyQueue 进入 fail-closed 门禁。唯一主收益端点是 A 相对 B 多找回至少 5% 的
正向主断言路径，且收益至少分布在 3 个 burst 和 2 类 workload；visibility 仅作正确性/SLA 诊断，A 的总体与各 workload p95 均须 ≤35 秒；至少 3/4 block 复现主收益。CPU、read bytes/read
syscall 的中位和每块比值均不超过 1.10，write bytes/write syscall 的中位和每块比值均不超过 1.25，
才允许进入规模矩阵；报告同时给出每找回一个正向路径的增量成本。通过不等于生产发布。完整阈值、
续跑方式和产物说明见 `helloagents/wiki/m2-cold-window-vm-benchmark.md`。suite 首次启动只构建一次并写出
schema v3 `build-provenance.json`；Cargo 在 suite 私有 target dir 构建并由 JSON compiler-artifact 消息证明
产物身份，构建前后必须是同一 Git HEAD 且工作区均为 clean。八腿虽然使用 `--build never`，但每腿都会
重新核对 clean worktree、Git、Cargo.lock 和 binary SHA256，再复制为腿私有只读执行文件。旧 schema、
回执缺失、被篡改或与当前 checkout 不符时直接拒绝运行。falsification profile 关闭会反复阻塞等待的
active canary，只保留 passive canary 作为关机可信对账输入；正确性收益由 806 个唯一主断言和 38 个有界
visibility probe 判断。每腿的磁盘 snapshot label 还包含完整 run-dir 哈希，避免不同 block 都叫
`attempt-01` 时误复用前一腿快照。hard waterline 一经观测即拒绝样本；结构移动在持久化静默阶段允许至多
一次可归因的安全 rebuild，但必须最终 `ready=true`、日志无 ERROR，且完整成本仍计入 A/B。
每腿 process 采样还必须覆盖至少 95% 的 1200 秒窗口、最大间隔不超过 3 秒且单调计数器不回退。同一 block 的两腿启动间隔不得超过 30 分钟；中断后只复用完整且相邻的整块，不能把跨会话腿拼成配对结果。suite、wrapper 和 benchmark 分别拥有独立进程组，超时清理按运行中 sidecar 记录的已验证 benchmark 进程组执行，避免孤儿 daemon 污染续跑。

快速 profile 额外启用 `--event-storm-strict-protocol`：固定调度只选择 daemon 已登记的配置根，不再选择没有独立 M2 entry 的 `dNNN` 子目录；每个 burst 在写入 fixture 前必须证明请求 tier 与实际 tier 一致，仍不会把 L2 放宽成 L3。非固定模式下，目录没有独立 tier 行时继承最近祖先目录的 tier，不会匹配 sibling。`/debug/tiered-watch` 请求成功与目标 M2 entry 是否存在分开记录：A 必须有精确 entry、有效租约和合法 action；B 允许精确 entry 不存在，但所有 M2 活动字段必须为零。写后因果不再依赖可回拨的秒级系统时间：runner 用写前序列和写后租约栅栏包住 mutation 窗口，只接受同一租约周期内、发生在该窗口中或窗口后的 M2 scan/event 单调序列推进；scan 序列只由携带轮转 `cycle_id` 的 dirty source 完成时发布，普通周期/API/查询扫描不能冒充。租约同时使用单调时钟与墙上时钟判活，任一过期即失效。任一前置条件不成立会立即结束该腿，不再浪费完整 1200 秒。

稳定性统计通过显式 `startup bootstrap` rebuild reason 识别 fresh-snapshot 启动构建，旧日志才回退到最早 ready 标记前后分账；bootstrap 最多一次。允许的 ready 后 rebuild 还必须带 `snapshot recovery` reason，且启动日志的字节偏移落在 shutdown snapshot quiesce 捕获窗口内；仅有全局 `rebuild_observed=true` 不能替运行期 rebuild 背书。suite 结束会逐条打印 `gate_reason`，并在 suite 目录旁生成 `<suite>-evidence.tar.gz`：只收集最终 summary 精确引用的八个 attempt，校验根级与逐腿必需成员，并附成员 SHA256 清单；根级 `build-provenance.json` 的实际 SHA256 还必须与八腿 `audit.receipt_sha256` 全部一致。不打包历史 attempt、`build-target`、腿内执行副本、snapshot、symlink 或伪造路径。缺件、摘要不一致、构建回执被替换或打包失败都会使旧包失效、写回基础设施失败并返回非零。

第二轮 A 在 `save100` cleanup 后发现 Delta Live 路径既不在文件系统也不在 L2，且缺少 delete/rename 失效证据，最终快照按 fail-closed 拒绝样本。该结果不应通过“任意 ENOENT 都当删除”绕过；修复边界是：event-storm cleanup 只操作当前 burst 并留下结构化记录，冷层完整扫描只有在目录可读、扫描完整且 apply sequence 未前进时，才能把 Base/L2/Delta Live 相对当前目录集合的缺失项转换为直接子路径 Delete。扫描错误、未完成或 stale batch 不得产生负事实。

第三轮 A 的 event-storm 写入和 cleanup 均成功，但关机时仍在 passive canary 的 `create -> rename -> delete` 生命周期中；`*_create.txt` 已离开文件系统，L2/WAL 的 Live 事实却尚未经过下一次可信目录对账，SIGTERM 后的最终快照因此继续 fail-closed。runner 现在在停止新 workload 后、发送 SIGTERM 前，对 passive/active canary root、event-storm roots、仍 active 的 burst root 和 mixed hot roots 同步调用 `/scan`；手动 `/scan` 会复用与冷层扫描相同的完整 `read_dir`、目录 fingerprint、`event_seq` 和 freeze-gate 校验来补齐负事实，并返回 `stable/deleted`。不稳定结果会在静默窗口内有界重试，不能把 HTTP 200 误算为收敛。该收尾动作发生在 passive first-query 指标采集之后，不改变 M2 的收益测量；结果会记录为 `passive_shutdown_reconcile`，缺失、失败或持续不稳定直接使样本不可比较。

第四轮 A 已证明上述负事实收敛有效：`passive_shutdown_reconcile` 删除 7 条且 `stable=true`，但最终快照转而命中 `direct_v7_unsupported: subtree move completeness is unproven`。这是 `subtree_rename` / mount storm 的安全降级要求，不是幽灵路径。runner 因此在 `/scan` 后、SIGTERM 前追加 POST `/snapshot` 持久化屏障：第一次直接快照若要求 rebuild，daemon 在运行态启动完整扫描，runner 有界重试直到 rebuild generation 真正写入并返回 `ready=true`。该阶段单独记录为 `shutdown_snapshot_quiesce`，包含 rebuild 是否发生、尝试次数、耗时以及窗口内 CPU/RSS；缺失、失败或超时继续拒绝 A/B，不能通过清除结构完整性标志绕过。定向验证先把旧目录写入冷基座再整体 rename，首次 `/snapshot` 确认返回同一 `direct_v7_unsupported`，full rebuild 发布后连续两次 `ready=true`；正式 runner smoke 最终 `fd_rdd_exit_code=0` 且无 `Final snapshot failed`。

### 总不变量

| 类别 | 不变量 |
|---|---|
| 前台查询 | 查询延迟不能被后台 scan / validation / compaction 明显拖死；同步验真必须受 `max_verify_per_query` 和 `verify_timeout_ms` 预算限制；lazy validation 模式下前台查询不得调用 stat。 |
| 正确性 | Watcher 新事实优先于 deferred scanner 旧事实；删除事实不得被 anti-entropy 复活；Dirty Scope 合并不能导致无限全量扫；Tombstone 多层叠加不能让 hot path 退化成解释型历史回放；启动恢复不能无脑全盘重建。 |
| 资源 | stable.v7 不整体反序列化进堆；RSS 增长有上界；mmap page fault 可观测；后台 stat/s 有上限；scan slice 受 entries/time/I/O token budget 限制；PSI 升高时后台任务应降速或撤销 BoostLease。 |

### 先做的 12 个架构骨头测试

| 优先级 | 测试 | 目标 |
|---|---|---|
| P0 | 初始 build + query | 基础索引与查询正确。 |
| P0 | create event -> L2 可查 | 新文件不依赖立即 rebuild stable.v7。 |
| P0 | delete event -> tombstone 过滤 L3 | 删除不返回旧 cold/base 结果。 |
| P0 | subtree tombstone 不逐文件展开 | 大目录删除不产生海量逐文件墓碑。 |
| P0 | scanner 旧事实不能覆盖 watcher 新事实 | `apply_seq` 竞态下新事实胜出。 |
| P0 | 删除后 anti-entropy 不复活幽灵文件 | 后台补扫不能反向恢复删除事实。 |
| P1 | 父级 dirty coalescing 不触发全量 scan | 父级合并只降低精度，不放大为无预算递归扫。 |
| P1 | scan slice 遵守 entries/time budget | 每轮处理数、耗时和 I/O token 受控。 |
| P1 | lazy 模式下前台 query 不 stat | lazy validation 的“只入队不等待”语义成立。 |
| P1 | validation worker 遵守 stat/s 限速 | 后台验证不抢前台资源。 |
| P1 | stable.v7 启动不整体反序列化 | VIRT 可增，RSS 不能随冷段大小线性暴涨。 |
| P1 | tombstone overlay 超阈值触发 compiled/compaction | 查询成本不随 tombstone 层数线性退化。 |

### VM 场景分层

| 场景 | 内容 | 主要观察 |
|---|---|---|
| S0 空闲基线 | 启动后不做操作，运行 10–15 分钟。 | RSS、CPU、DirtyQueue、cold freshness age 是否稳定。 |
| S1 日常桌面 | 小文件 create/edit/rename/delete，编辑器原子保存，下载器 `.part -> final`。 | canary 延迟、旧路径隐藏、hotset SLA。 |
| S2 冷目录追平 | 在 L2/L3 大目录持续制造 canary。 | 冷层 p95/p99 是否优于 baseline。 |
| S3 大目录压力 | 批量创建/移动/删除 1 万到 10 万文件，删除整棵子树。 | scan-only/分片 repair、RSS、DirtyQueue 回落。 |
| S4 热层保护 | 冷目录压力同时操作 L0/hotset 项目目录。 | fast scan p99、L0 watch cost、hotset lease 是否被挤占。 |
| S5 预算受限 | 降低 watch、L0 单根和 rotating budget。 | blocked 指标是否可解释，是否突破预算。 |
| S6 Rename 风暴 | 10 万级 rename，覆盖 old/new 查询。 | `apply_seq` 单调、旧名不复活、L2 不无限膨胀。 |
| S7 Git checkout 风暴 | 在临时仓库反复 checkout / clean / reset。 | 最终分支文件正确，删除文件不幽灵复活。 |
| S8 Watcher 丢事件 | 暂停/降级 watcher 后制造磁盘变化，再恢复对账。 | 最终收敛，前台不等待后台对账。 |
| S9 子目录重命名雪崩 | 执行 `mv dir_a dir_b`，只触发父目录级 rename 事件。 | 深层子文件新路径能追平，旧路径不复活；验证 DirSentinel / 递归对账。 |
| S10 挂载点断联海啸 | 扫描或查询过程中让挂载点突然离线。 | 删除熔断器拦截 ENOENT 海啸，不写满无用 Delete Tombstone。 |
| S11 幽灵文件复活 | 删除文件后立刻创建新文件，尽量诱发 inode 快速复用。 | Generation Number / FileKey 防止旧索引事实复活或覆盖新事实。 |
| S12 时钟倒挂 | 文件 mtime 或系统时钟向过去跳变。 | 双轨时钟防止 mtime cutoff 剪枝漏扫。 |

### VM workload driver 设计

`m2-cold-window-vm-bench.py` 保持为 **runner + collector**。下面的独立 workload driver 是后续规划，
当前仓库尚未实现 `scripts/m2-cold-window-workload.py`；现阶段可执行入口是快速证伪 suite，或 runner
内置的 `--event-storm`。规划中的接口会接收 sandbox root、scenario、duration、rate、seed 和 events JSONL 输出路径；它不是当前可执行命令。

设计原则：

- 只操作 `--root` 下的测试沙箱；不碰项目仓库和用户真实 `$HOME` 顶层。
- 拒绝危险 root：`/`、真实 `$HOME` 顶层、项目仓库根目录和空路径都不允许运行；cleanup 只清理 sandbox 内由 driver 创建的路径。
- 支持 `--dry-run`，先输出 fixture 和操作计划，用于确认 VM 压力规模。
- 所有阶段写 `workload-events.jsonl`，记录 phase、operation、path、count、started_at、finished_at、expected_query。
- deterministic seed：同一 seed 下 baseline / experiment 能复现相同操作顺序。
- rate limit：`daily`、`normal`、`stress`、`chaos` 四档，避免一上来把 VM 打死。
- phase 化：先生成 fixture，再按场景执行，再做 cleanup，便于和 metrics 时间线对齐。
- canary 与风暴分离：canary 用于可见性延迟，storm 用于资源和正确性压力。
- 查询验证不放在 driver 热路径；driver 只写预期，collector/analyzer 负责查 fd-rdd 和判定。

实现分层：

| 模块 | 职责 |
|---|---|
| `SandboxGuard` | 解析和校验 `--root`，提供 sandbox 内路径创建、rename、删除封装。 |
| `RateLimiter` | 按 `daily/normal/stress/chaos` 控制每秒操作数、批次大小和阶段休眠。 |
| `FixtureBuilder` | 生成小树、大树、rename 集合、git repo 和 canary 目录。 |
| `ScenarioRunner` | 执行 `daily`、`cold-canary`、`delete-storm`、`rename-storm`、`git-storm`、`watcher-drop-proxy`。 |
| `EventSink` | 追加写 `workload-events.jsonl`，记录操作开始、结束、预期查询和错误。 |
| `SummaryWriter` | 输出 workload 摘要，供 bench runner 的报告引用。 |

第一版 workload driver 场景：

| 场景 | 操作 |
|---|---|
| `daily` | 小文件 create/edit/rename/delete、编辑器临时文件、下载器 `.part` rename。 |
| `cold-canary` | 在指定冷目录周期创建/rename/delete 唯一文件名。 |
| `delete-storm` | 生成大子树后删除文件批次或整个 subtree。 |
| `rename-storm` | 批量 `file_i -> file_i_new -> file_i`，覆盖 rename 合并与 tombstone。 |
| `git-storm` | 在临时 git repo 中创建分支、checkout、clean、reset，模拟真实工作区震荡。 |
| `watcher-drop-proxy` | 不直接控制内核 watcher；通过短时间 `--no-watch` baseline 或暂停 daemon 后磁盘变更，再重启观察 deferred repair。 |
| `subtree-rename` | 执行 `mv dir_a dir_b`，验证深层子文件的新路径可见、旧路径隐藏。 |
| `mount-storm` | 在 VM 中用 loop/NFS/U 盘挂载点制造离线；无特权模式可用目录隐藏重命名做代理测试。 |
| `inode-reuse` | 删除旧文件后快速创建新文件，记录 dev/inode 是否复用，并验证旧路径隐藏、新路径可见。 |
| `time-skew` | 优先在 VM 中真实回拨系统时间；无特权模式可回拨 fixture mtime，验证 mtime cutoff 不漏扫。 |

轻量一体化 event storm 命令形态：

```bash
TEST_ROOT="$HOME/fd-rdd-m2-roots"
python3 scripts/m2-cold-window-vm-bench.py \
  --root "$TEST_ROOT/cold-a" \
  --root "$TEST_ROOT/cold-b" \
  --root "$TEST_ROOT/hot" \
  --binary "./target/release/fd-rdd" \
  --build always \
  --event-storm \
  --duration-secs 3600 \
  --event-storm-kind rw100,save100,git_clone,npm_install,subtree_rename,mount_storm,inode_reuse,inode_reuse_stress,time_skew \
  --event-storm-target-tier L0,L1,L2,L3 \
  --event-storm-ops 100 \
  --event-storm-duration-budget-secs 1 \
  --event-storm-max-bursts 36 \
  --event-storm-interval-secs 10 \
  --event-storm-settle-secs 60
```

该命令只用于单腿探索，不能替代带 schema v3 构建回执、平衡顺序和 suite 门禁的正式快速证伪入口。

event storm 按 `/debug/tiered-watch` 选择指定 L0-L3 的稳定 fixture 目录，但会排除路径任一组件以 `fd-rdd-m2-event-storm-` 开头的目录及其后代；词法路径与 symlink 解析后的真实路径都必须留在配置 root 内。没有安全的目标层候选时才回退到本轮显式 root，避免上一 burst 成为下一 burst 的父目录并形成递归 workload。

### A/B 判定标准

正式内存 A/B 的两腿必须分别从同一个已关机、只读的 VM/磁盘基线快照恢复；fixture 的 completed verified manifest 只证明生成时完整，不能证明当前目录树或 page cache 未被上一腿污染。报告目录应放在快照外并在每腿结束后立即导出。正式结果使用同一候选版 runner、`--build always`、相同参数指纹和初始状态指纹；顺序执行的 `--sweep-config` 只用于探索，不能单独作为初始构建峰值的正式 A/B。

| 类别 | 通过条件 |
|---|---|
| 正确性 | canary create 可见、rename 新路径可见且旧路径隐藏、delete 隐藏；delete/rename storm 后旧结果不复活。 |
| 冷层收益 | passive first-query create / rename 成功率高于 baseline，或冷层 canary p95 比 baseline 下降 ≥ 40%；p99 下降 ≥ 30%；如果 baseline 已很快，实验组 p95/p99 增幅不超过 10%。 |
| 热层不退 | `fast_scan_coverage_lag_p99_ms <= 5000`，或相对 baseline 增幅不超过 10%。 |
| 资源成本 | CPU p95 增幅不超过 5–10 个百分点；RSS max 增幅不超过 32 MiB 或 10%；swap 不应持续非 0。 |
| 队列预算 | `dirty_queue_len` 操作后可回落；`rotating_cold_window_budget_blocked` / `ephemeral_watch_budget_blocked` 不能持续单调增长且无对应回落。 |
| watcher | `watch_failures` 和 `overflow_drops` 不应持续增长，除非测试明确在验证 overflow recovery。 |
| event storm | `first_query.success_rate` 不低于 baseline，`special.subtree_rename_*`、`mount_storm_old_hidden_ok`、`inode_reuse_*`、`time_skew_backdated_visible_ok` 不能暴露正确性退化；真实挂载断联和系统回拨还需独立 VM driver 验证。 |
| 生命周期完整性 | `ab_comparable=true`；最终快照、fast-scan registry 与 clean-shutdown marker 全部成功。被 delete/rename 证据覆盖的 transient Live 可安全收敛，无法解释的缺失仍必须 fail-closed。 |

### 推荐指标补齐

后续应优先补齐这些 metrics，便于把 VM 黑盒现象和 Rust 白盒不变量接起来：

- `foreground_stat_count`
- `background_stat_count_per_sec`
- `scan_slice_entries_processed`
- `scan_slice_time_ms`
- `scan_cursor_advance_count`
- `tombstone_overlay_layers`
- `tombstone_compaction_count`
- `tombstone_segment_skip_count`
- `scanner_result_discarded_by_seq_count`
- `ghost_resurrection_prevented_count`
- `boost_lease_granted_count`
- `boost_lease_revoked_count`
- `psi_throttle_count`
