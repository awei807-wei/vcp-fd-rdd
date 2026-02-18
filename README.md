# fd-rdd（Linux 文件索引守护进程）

`fd-rdd` 是一个事件驱动的 Linux 文件索引服务：常驻守护进程对外提供 HTTP 查询接口，索引在后台持续增量更新，并能在事件风暴/溢出后自我修复。

本项目的重点是：

- 冷启动快：优先加载 mmap 段式快照（按需触页）
- 可恢复：任何快照/段损坏都能被识别并隔离（坏段跳过/拒绝加载），必要时走重建兜底
- 长期运行稳定：LSM（base+delta）控制段数增长；compaction 做物理回收；监控可量化触页与 RSS 组成

> 当前主线实现沿 v0.4.0 路线演进（语义锚定 + MergedView + LSM Hygiene）。

## 核心能力

- 查询语义：newest → oldest 级联合并（支持 delete→recreate；同路径只返回最新）
- 事件管道：bounded channel + debounce；overflow 触发后台 rebuild（带 cooldown 合并）
- 持久化：mmap 段式容器 + 目录化 LSM（`index.d/`）+ `events.wal` 增量回放
- 物理结界：段与 manifest 读前流式校验（v7=CRC32C），避免 bit rot 触发未定义行为
- 观测闭环：定期输出 MemoryReport（RSS + smaps_rollup + page faults）

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

## 查询匹配说明

- 不带通配符：contains 匹配（大小写敏感）
- 带 `*`/`?`：glob 匹配
  - pattern 含 `/` 或 `\\`：对完整路径做 glob（FullPath）
  - 否则：按“文件名/任意路径段”匹配（Segment）

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
curl "http://127.0.0.1:6060/search?q=*memoir*&limit=20"
```

## 内存与长期运行（如何判断“好用”）

百万级文件的内存/触页没有固定常数，取决于路径/名称分布、查询模式（负例多/热词多）、段数量与 OS 页缓存状态。

`fd-rdd` 提供两条“可量化”的判断路径：

- MemoryReport：区分堆高水位（Private_Dirty）与 file-backed 常驻（Private_Clean）
- page faults：在真实查询压力下量化“零拷贝是否真的少触页”

## 许可证

MIT License (c) 2026 fd-rdd Contributors
