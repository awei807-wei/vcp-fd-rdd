# fd-rdd（Linux 文件索引守护进程）

`fd-rdd` 是一个事件驱动的 Linux 文件索引服务：常驻守护进程对外提供 HTTP 查询接口，索引在后台持续增量更新，并能在事件风暴/溢出后自我修复。

本仓库的实现重点是“低常驻 + 可恢复 + 冷启动快”，因此索引持久化采用 **mmap 段式（v6）+ LSM 目录布局**，并引入 **events.wal** 作为增量回放账本。

## 核心架构（当前实现）

- L1：热缓存（命中时直接返回）
- L2：内存 Delta（可变索引）
  - posting：`DocId(u32)` + `RoaringBitmap`
  - path：`PathArena`（连续 blob，`(off,len)` 引用）
- Disk：只读段（mmap）
  - 目录化 LSM：`index.d/`（base + delta segments）
  - 查询合并语义：newest -> oldest（支持 delete->recreate）
- Build/Rebuild：全量扫描兜底（基于 `ignore` walker）
  - AQE：`AdaptiveScheduler` 根据系统负载选择扫描并行度，降低“百万文件扫描卡顿”

## 持久化布局（Stage B/C）

以 `index.db` 为基准：

- legacy 单文件：`index.db`（v2~v6 兼容读取；v6 为段式容器）
- LSM 目录：`index.d/`
  - `MANIFEST.bin`：段列表（原子替换写入）
  - `seg-*.db`：只读段（复用 v6 容器）
  - `seg-*.del`：跨段删除墓碑（按路径 bytes）
  - `events.wal`：追加型事件日志（Append-only Log）
  - `events.wal.seal-*`：snapshot 边界切分后的 sealed WAL（用于 checkpoint）

启动时优先加载 `index.d/`，随后按 `MANIFEST.bin` 的 `wal_seal_id` 回放 WAL，使查询包含“最后一次 snapshot 之后”的增量变更。

## 常见问题（压测与内存观测）

- `Memory Report` 的 L2 估算很小但 RSS 很大：通常是 **堆高水位（Anonymous/Private_Dirty）** 或 **影子内存（overlay/pending）**，以及历史 mmap 段的 `Private_Clean` 下界共同作用。
- 如果看到事件数异常增长/频繁 overflow：优先排查 watcher 是否在索引自身写入路径上形成反馈回路（snapshot/segments/log）。可以用 `--ignore-path` 排除路径前缀。

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
  --snapshot-path /tmp/fd-rdd/index.db \
  --ignore-path /tmp/fd-rdd \
  --http-port 6060
```

查询：

```bash
curl "http://127.0.0.1:6060/search?q=main.rs&limit=20"
```

## 许可证

MIT License (c) 2026 fd-rdd Contributors
