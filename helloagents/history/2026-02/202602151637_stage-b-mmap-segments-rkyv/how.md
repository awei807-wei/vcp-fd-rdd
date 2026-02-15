# 方案设计：段式格式 + rkyv manifest + mmap 加载 + Roaring 按需解码

## 1) 文件级容器（v6）

- 单文件 `index.db`（继续使用 atomic replacement：tmp + fsync + rename + fsync(dir)）
- Header（固定大小）：
  - magic
  - version=6
  - state（INCOMPLETE/COMMITTED）
  - manifest_len
  - manifest_checksum（对 manifest bytes 的校验）
- body：
  - manifest bytes（rkyv archived）
  - segments raw bytes（按 manifest offset/len 定位）

## 2) Manifest（rkyv）

manifest 负责：

- roots 固化（并参与校验）
- 段表（Segment Descriptor 列表）：
  - kind（Trigram / Metadata / Path / Tombstone / PostingBlob 等）
  - version（每段自带 schema 版本）
  - offset/len（文件内位置）
  - checksum（对该段 raw bytes 的校验）

校验策略：

- 启动时先校验 manifest checksum
- 再按需/按配置校验各段 checksum（默认：启动时校验全部；后续可允许快速模式）
- roots 不一致：拒绝加载并返回 None（走 rebuild）

## 3) 物理段布局（核心 3 段 + 必要段）

### 3.1 Path 段（PathBlob）

- raw bytes：`path_arena_bytes`（root 相对路径 bytes 串联）
- 由 Metadata 段的 `(root_id, off, len)` 引用

### 3.2 Metadata 段（MetaRecords）

- `Vec<MetaRecord>`（rkyv archived）
- 结构只包含基本类型（便于跨版本迁移）：
  - file_key(dev, ino)
  - root_id: u16
  - path_off: u32
  - path_len: u16
  - size: u64
  - mtime_unix_ns: i64（-1 表示 None）

### 3.3 Trigram 段（TrigramTable）

目标：支持二分查找 trigram → posting bytes 范围。

- `Vec<TrigramEntry>`（rkyv archived，按 trigram 升序）
  - trigram: [u8;3]
  - posting_off: u32
  - posting_len: u32

### 3.4 PostingBlob 段（Postings）

- raw bytes：将每个 trigram 的 `RoaringBitmap` 用 roaring 的序列化格式写入一段连续 blob
- 查询时对命中的 trigram：
  - slice 出对应 bytes
  - `RoaringBitmap::deserialize_from(Cursor<&[u8]>)` 解码为内存 bitmap

### 3.5 Tombstone 段（可选但推荐）

- raw bytes：tombstones 的 Roaring 序列化
- 只在第一次查询时 lazy decode（并缓存），用于 `contains(docid)` 过滤

## 4) 与现有运行时的集成策略

阶段 B 的第一落地重点是“冷启动秒开 + 查询可用”：

- 启动加载：
  - 先尝试 v6 段式快照（mmap）
  - 不存在/校验失败/roots 不一致：回退到旧版本（v2~v5）或走 rebuild
- 在线写入（事件增量）：
  - 阶段 B 初版先保持现有内存 L2（mutable）作为在线索引
  - 段式快照作为持久化终局格式，用于下一次冷启动
  - 后续可扩展为“mmap 基座 + 内存 delta”的混合段合并策略（另开方案）

## 5) 迁移

- writer：新增写 v6（segments）路径；保留写 v5 的兼容开关（必要时回退）
- reader：兼容读取 v2~v6

