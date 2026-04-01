# fd-rdd（Linux 文件索引守护进程）

`fd-rdd` 是一个事件驱动的 Linux 文件索引服务：常驻守护进程对外提供 HTTP 查询接口，索引在后台持续增量更新，并能在事件风暴/溢出后自我修复。

本项目的重点是：

- 冷启动快：优先加载 mmap 段式快照（按需触页）
- 可恢复：任何快照/段损坏都能被识别并隔离（坏段跳过/拒绝加载），必要时走重建兜底
- 长期运行稳定：LSM（base+delta）控制段数增长；compaction 做物理回收；监控可量化触页与 RSS 组成

> 当前主线实现沿 v0.4.9 路线演进（Query DSL/Fuzzy 并存、DAG/Verifier 补强、UDS 认证与 fast-sync 热路径收敛）。

## v0.4.9 更新

- 版本：主线版本提升到 `v0.4.9`，便于区分包含 DAG/Verifier 补强、Fuzzy 查询接入以及后续 review 收敛修复的测试构建
- 查询：`FzfIntegration` 已接入 HTTP `/search`、UDS 文本协议与 `fd-rdd-query` 客户端；新增显式 `mode=fuzzy`
- 安全性：UDS 服务新增 peer credential 校验，默认仅接受 same-euid 或 `root` 发起的连接
- 稳定性：fast-sync 复用 `DirEntry::metadata()` 直接写入 `FileMeta`；`PathArena` 对超长 root-relative 路径改为跳过并告警，避免污染索引

## v0.4.8 更新

- 版本：主线版本提升到 `v0.4.8`，便于区分包含多索引源用法澄清与隐藏文件扫描开关的测试构建
- 索引源：README 明确 `--root` 可重复传入，以覆盖多个索引源
- 扫描：新增 `--include-hidden`，允许在冷启动全扫、后台重建与增量补扫时纳入 dotfiles / dotdirs

## v0.4.7 更新

- 版本：主线版本提升到 `v0.4.7`，便于区分包含最新查询语法联调验收的测试构建
- 查询：新增搜索语法冒烟脚本 `scripts/smoke-search-syntax.sh`，可自动创建样例文件并调用 HTTP `/search` 验证 Smart-Case、AND/OR/NOT、短语、glob、`ext/pic/dm/size`、`wfn/regex` 等关键语义

## v0.4.6 更新

- 版本：主线版本提升到 `v0.4.6`，便于区分包含最近内存治理修正的测试构建
- LSM：修复 compaction 仅替换“base + 被 compact 的 delta 前缀”时的 manifest 判定，避免 suffix delta 被误伤后长期不收敛
- Flush：新增 `--batch-flush-min-events` / `--batch-flush-min-bytes`，让低频小变更先留在 WAL/L2，攒够一批再落段
- 稳定性：保留 overlay 强制 flush 与退出前最终 snapshot 语义，不让批量门槛拖慢止血路径

## 核心能力

- 查询语义：newest → oldest 级联合并（支持 delete→recreate；同路径只返回最新）
- 事件管道：bounded channel + debounce；overflow/Rescan → dirty 标记 → cooldown/max-staleness 触发 fast-sync（必要时再 rebuild 兜底）
- 持久化：mmap 段式容器 + 目录化 LSM（`index.d/`）+ `events.wal` 增量回放
- 物理结界：段与 manifest 读前流式校验（v7=CRC32C），避免 bit rot 触发未定义行为
- 观测闭环：定期输出 MemoryReport（RSS + smaps_rollup + page faults）

## v0.4.5 更新

- 查询：新增可选 UDS 流式查询 `--uds-socket` 与 `fd-rdd-query` 客户端，避免大结果集在 Daemon/Client 端聚合导致内存峰值
- 可靠性：overflow 不再“立刻全盘 rebuild”，改为 dirty-region + cooldown/max-staleness 触发 fast-sync（弱一致兜底，允许短暂陈旧）
- 测试：补齐 P0/P1/P2 回归与联调测试（分配器可观测、socket streaming、fast-sync）
- 观测：MemoryReport 统计补充 disk tombstones 与 EventPipeline buffer cap，便于定位“常驻增量”的来源
- 工具：新增/增强 `scripts/fs-churn.py` 压力脚本，支持 churn + plateau 自动检查（含 `--spawn-fd`）

## v0.4.4 更新

- 安全性：移除路径解码中的不安全转换，避免损坏输入触发未定义行为
- 一致性：LSM 加载改为“任一段或 `.del` sidecar 异常即整体拒绝”，避免部分加载导致静默漏数
- 稳定性：`event_channel_size=0` 现在会明确报错，不再在运行时 panic
- 测试：新增 LSM 部分损坏/sidecar 损坏与 channel 参数防御测试

## 架构概览

- L1：热缓存（命中时直接返回）
- L2：内存 Delta（可变索引）
  - posting：`DocId(u32)` + `RoaringBitmap`
  - path：`PathArena`（连续 blob，`(off,len)` 引用）
- Disk：只读段（mmap）
  - LSM：base + 多个 delta segments（`seg-*.db` + `seg-*.del`）
  - 查询合并：newest → oldest，FileKey 去重 + path 维度屏蔽
- L3：后台全量构建器（基于 `ignore` walker）
  - AQE：`AdaptiveScheduler` 根据系统负载选择扫描并行度

## 持久化布局（index.db / index.d）

以 `--snapshot-path` 指定的 `index.db` 为基准：

- legacy 单文件：`index.db`
  - v2~v5：bincode 快照（兼容读取）
  - v6/v7：段式容器（mmap 读取；v7 写入默认采用 CRC32C 校验）
- LSM 目录：`index.d/`
  - `MANIFEST.bin`：段列表（原子替换写入）
  - `seg-*.db`：只读段（复用 v6/v7 容器）
  - `seg-*.del`：跨段删除墓碑（按路径 bytes）
  - `events.wal`：追加型事件日志（Append-only Log）
  - `events.wal.seal-*`：snapshot 边界切分后的 sealed WAL（checkpoint）

启动时优先加载 `index.d/`，随后按 `MANIFEST.bin` 的 `wal_seal_id` 回放 WAL，使查询包含“最后一次 snapshot 之后”的增量变更。

## 查询语法（`q=...`）

### Smart-Case（默认不敏感）

- 默认：大小写不敏感
- 若查询中包含大写字母，或显式使用 `case:`：切换为大小写敏感

### 逻辑运算符

- 空格（AND）：`VCP server`
- 竖线 `|`（OR）：`server.js|Plugin.js`
- 感叹号 `!`（NOT，全局排除）：`server.js !node_modules`

### 短语

- 双引号：`"New Folder"`（作为一个完整词组参与匹配）

### 基础匹配（路径字符串）

- 不带通配符：contains 子串匹配
- 带 `*`/`?`：glob 匹配
  - pattern 含 `/` 或 `\\`：对完整路径做 glob（FullPath）
  - 否则：按“文件名/任意路径段”匹配（Segment）

### 快捷过滤器

- `doc:` / `pic:` / `video:`：按扩展名集合过滤（支持 `pic:十一` 等价于 `pic:` AND `十一`）
- `ext:js;py`：按后缀过滤（`;`/`,` 分隔）
- `dm:today` / `dm:YYYY-MM-DD`：按修改日期过滤（以 Daemon 本地时间为准）
- `size:>10mb`：按大小过滤（单位支持 `b/kb/mb/gb/tb`，1024 进制）

### 高级

- `wfn:`：完整文件名匹配（默认 basename；若 pattern 含 `/` 或 `\\` 则对 fullpath 生效）
- `regex:`：正则匹配（默认 basename；pattern 含 `/` 或 Windows 分隔符 `\\` 时对 fullpath 生效；regex 内含 `|`/空格时请使用引号：`regex:"^VCP.*\\.(js|ts)$"`，Windows 示例：`regex:"^C:\\\\tmp\\\\VCP.*\\.js$"`）

## 快速开始

编译：

```bash
cargo build --release
```

默认启用 `mimalloc` 作为全局分配器（用于降低多线程 ptmalloc arena 导致的碎片与 RSS 回吐问题）。
如需回退到系统分配器：

```bash
cargo build --release --no-default-features
```

启动（示例）：

```bash
./target/release/fd-rdd \
  --root /path/to/scan \
  --root /another/path/to/scan \
  --include-hidden \
  --snapshot-path /tmp/fd-rdd/index.db \
  --ignore-path /tmp/fd-rdd \
  --http-port 6060 \
  --uds-socket /tmp/fd-rdd.sock
```

说明：

- `--root` 可重复传入，用于覆盖多个索引源。
- 默认会跳过 `.` 开头的文件/目录；如需将 dotfiles / dotdirs 纳入冷启动全扫、后台重建与增量补扫，请显式加 `--include-hidden`。

查询：

```bash
curl -G "http://127.0.0.1:6060/search" --data-urlencode "q=main.rs" --data-urlencode "limit=20"
curl -G "http://127.0.0.1:6060/search" --data-urlencode "q=*memoir*" --data-urlencode "limit=20"
curl -G "http://127.0.0.1:6060/search" --data-urlencode "q=server.js !node_modules" --data-urlencode "limit=20"
curl -G "http://127.0.0.1:6060/search" --data-urlencode "q=mdt" --data-urlencode "mode=fuzzy" --data-urlencode "limit=20"
```

UDS 流式查询（推荐用于大结果集；边收边输出，不聚合）：

```bash
./target/release/fd-rdd-query --socket /tmp/fd-rdd.sock --limit 2000 "main.rs"
./target/release/fd-rdd-query --socket /tmp/fd-rdd.sock --spawn --limit 2000 "*.rs"
./target/release/fd-rdd-query --socket /tmp/fd-rdd.sock --mode fuzzy --limit 200 "mdt"
```

说明：

- UDS 服务除 socket 文件 `0600` 外，还会校验 peer credential；默认仅接受“同一有效 UID”或 `root` 发起的连接，单独的同 GID 不构成放行条件。
- `mode=exact` 下的 1-2 字符短查询会先走“短组件候选索引”再做精确过滤，减少退化全扫的概率。
- `PathArena` 当前仍以 `path_len: u16` 编码路径；root-relative 路径超过 `65535` bytes 时会跳过该条索引并输出告警，而不是写入损坏占位记录。

## 内存与长期运行（如何判断“好用”）

百万级文件的内存/触页没有固定常数，取决于路径/名称分布、查询模式（负例多/热词多）、段数量与 OS 页缓存状态。

`fd-rdd` 提供两条“可量化”的判断路径：

- MemoryReport：区分堆高水位（Private_Dirty）与 file-backed 常驻（Private_Clean）
- page faults：在真实查询压力下量化“零拷贝是否真的少触页”

长期运行时可启用条件性 RSS trim（v0.4.4+）：

- `--trim-interval-secs`：检查周期（秒，0=禁用，默认 300）
- `--trim-pd-threshold-mb`：`Private_Dirty` 触发阈值（MB，0=禁用，默认 128）

如需减少“低频小变更也每轮都落成一个新 delta 段”的情况，可额外启用定时 flush 批量门槛：

- `--batch-flush-min-events`：周期性 flush 的最小事件数（0=禁用，默认 0）
- `--batch-flush-min-bytes`：周期性 flush 的最小事件字节数（0=禁用，默认 0）

说明：

- 这两个参数只影响 **周期性** flush（`--snapshot-interval-secs`）。
- overlay 达阈值触发的强制 flush、以及进程退出前的最终 snapshot **不受影响**。
- 它们的用途是“把小批次变更继续留在 WAL/L2，等攒够一批再落段”，用于减缓新段增长；不能替代 compaction 收敛。

该策略用于缓解“索引结构已很小，但匿名脏页高水位常驻”的场景。

## 压力回归（脚本化）

如果不想用“消耗时间”来验证长期常驻的内存/事件路径，可以用脚本在几分钟内制造大量文件事件：

```bash
# 1) 启动 fd-rdd（更适合做事件/常驻内存对照）
./target/release/fd-rdd \
  --root /tmp/fd-rdd-churn \
  --snapshot-path /tmp/fd-rdd/index.db \
  --no-build \
  --snapshot-interval-secs 0 \
  --auto-flush-overlay-paths 5000 \
  --auto-flush-overlay-bytes 0 \
  --report-interval-secs 5 \
  --trim-interval-secs 0

# 2) 生成文件系统事件（create/delete/rename/modify 混合）
python3 scripts/fs-churn.py --root /tmp/fd-rdd-churn --reset --populate 20000 --ops 200000
```

注：`--ops` 是“每轮操作数”；当 `--rounds N` 时，总操作数为 `ops*N`。

如果想把“长期不涨”也脚本化（多轮 churn + 每轮 settle 并从 /proc 读取 fd-rdd 的 `smaps_rollup`）：

```bash
PID=<fd-rdd-pid>
python3 scripts/fs-churn.py \
  --root /tmp/fd-rdd-churn --reset --populate 20000 \
  --ops 200000 --max-files 20000 \
  --rounds 10 --settle-secs 20 \
  --fd-pid "$PID" --fd-metric pd --max-growth-mb 8
```

注：当 `--fd-metric=pd` 且脚本能解析到 fd-rdd 的 MemoryReport 时，`--max-growth-mb` 实际比较的是  
`unaccounted=max(0, pdΔ-disk_tomb_estΔ)`（把 tombstones 等可量化的结构性增长从“泄漏”里剔除）；否则回退为 `pdΔ`。

如果希望“一次运行就得到 PASS/FAIL 结论”（脚本自动启动 fd-rdd + warmup + plateau 检查）：

```bash
python3 scripts/fs-churn.py \
  --verdict --reset --cleanup \
  --populate 20000 --ops 200000 --max-files 20000 \
  --warmup-rounds 1 --rounds 10 --settle-secs 20 \
  --fd-metric pd --max-growth-mb 8
```

脚本会在 PASS/FAIL 时输出“归因摘要”（包含 event overflow 与 MemoryReport 的 heap/index 拆分）。如需输出 fd-rdd 的详细日志，可追加 `--fd-echo`；想减少 overflow 干扰可调 `--fd-event-channel-size` / `--fd-debounce-ms` 或给 churn 加 `--sleep-ms`。

注：部分系统对 `/proc/<pid>/smaps_rollup` 有权限限制（常见于 yama/ptrace_scope 策略）。遇到无法读取时：

- 只想做 RSS 平台检查：把 `--fd-metric` 改为 `rss`（会 fallback 到 `/proc/<pid>/statm`）
- 想继续检查 `pd/pc/pss`：用 `--spawn-fd` 让脚本启动 fd-rdd（成为父进程；该参数需要放在命令最后）

```bash
python3 scripts/fs-churn.py \
  --root /tmp/fd-rdd-churn --reset --populate 20000 \
  --ops 200000 --max-files 20000 \
  --rounds 10 --settle-secs 20 \
  --fd-metric pd --max-growth-mb 8 \
  --spawn-fd ./target/release/fd-rdd \
    --root /tmp/fd-rdd-churn \
    --snapshot-path /tmp/fd-rdd/index.db \
    --no-build \
    --snapshot-interval-secs 0 \
    --auto-flush-overlay-paths 5000 \
    --auto-flush-overlay-bytes 0 \
    --report-interval-secs 5 \
    --trim-interval-secs 0
```

## 许可证

MIT License (c) 2026 fd-rdd Contributors
