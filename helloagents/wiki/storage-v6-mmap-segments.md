# fd-rdd 阶段 B（v6）段式持久化：mmap + Lazy Decode

本文记录 v6 段式快照的物理布局与加载/校验策略，目标是“冷启动秒开”（建立 mmap 后即可查询）。

## 1) 设计目标

- **零反序列化启动**：不再把整个快照 body 反序列化为内存结构；启动时仅解析 header+manifest，并 mmap 各段。
- **段式可校验/可迁移**：每段独立 checksum + version，便于后续平滑迁移。
- **posting Lazy Decode**：Trigram posting 落盘为 Roaring 序列化字节；查询命中后再按需解码。

> 注：当前 manifest 为手写二进制（无外部依赖）。后续如需严格 rkyv archived，可在不改变段布局的前提下替换 manifest 编码方式。

## 2) 文件容器（index.db）

Header 固定 20B（与旧版保持一致的大小，但语义不同）：

- magic: u32
- version: u32（v6=6）
- state: u32（INCOMPLETE/COMMITTED）
- manifest_len: u32
- manifest_checksum: u32（对 manifest bytes 的 simple_checksum）

Body：

- manifest bytes
- segments bytes（按 manifest 描述的 offset/len 定位；按 8B 对齐填充）

写入仍保持原子替换语义（tmp + fsync + rename + fsync(dir)）。

## 3) Manifest（v6）

manifest 记录段表（Segment Descriptor 列表）：

- kind（Roots/PathArena/Metas/TrigramTable/PostingsBlob/Tombstones）
- version（每段自带 schema 版本）
- offset/len（文件内绝对偏移）
- checksum（对该段 raw bytes 的 simple_checksum）

启动加载流程：

1. 校验 header 与 manifest checksum
2. 解析 manifest，并逐段校验 checksum
3. 校验 roots：运行时 roots 经规范化后必须与 Roots 段完全一致，否则拒绝加载（走 rebuild）

### 3.1 流式校验（Streaming Verification）

为降低冷启动时的进程 RSS（尤其是 `Private_Clean`），v6 加载将“校验”与“mmap 映射”解耦：

- 校验阶段：使用 `read + seek` 以 64KB 栈缓冲区对 manifest 与各 segment 做流式 checksum（不先 mmap，不遍历 mmap slice）
- 映射阶段：所有校验通过后，才执行 `mmap` 并返回只读视图

这样校验引起的读取主要落在 **内核 page cache**，不会显著体现在进程 RSS 上；真正的页 fault 将由后续查询按需触发。

## 4) 三个核心段（你关心的物理布局）

### 4.1 Path 段（PathArena）

- raw bytes：root 相对路径字节串联（不含 root 前缀）
- 由 Metadata 段的 `(root_id, off, len)` 引用

### 4.2 Metadata 段（Metas）

按 DocId 顺序顺排的定长记录（little-endian，40B/record）：

- dev: u64
- ino: u64
- root_id: u16
- path_off: u32
- path_len: u16
- size: u64
- mtime_unix_ns: i64（-1 表示 None）

### 4.3 Trigram 段（TrigramTable）

按 trigram 升序的定长记录（12B/record）：

- trigram: [u8;3]
- pad: u8
- posting_off: u32（在 PostingsBlob 段内）
- posting_len: u32

## 5) posting 与 tombstone（Lazy Decode）

- PostingsBlob：连续 blob，存 RoaringBitmap 的序列化字节
- Tombstones：单个 RoaringBitmap 序列化字节

查询时：

1. 对 query prefix 取 trigram 列表
2. 对每个 trigram 二分查 TrigramTable，定位 posting bytes
3. Roaring lazy decode → 交集 → 得到候选 DocId
4. tombstone 过滤 + path matcher 精确过滤

## 6) 与在线增量的关系（当前实现）

启动时若成功加载 v6：

- 立即启用 mmap base 参与查询（冷启动秒开）
- 后台 hydration：把 base 快照灌入可变 `PersistentIndex`，并回放期间事件
- hydration 完成后切换为纯内存 L2（便于 watcher 增量与快照写入）

后续演进方向（另开方案）：

- “mmap 基座 + 内存 delta”长期共存（不再做全量 hydration）
- 段合并/压缩（类似 LSM 的 compaction），降低常驻 RSS 与写放大
