# PersistentIndex 内存优化——路径存储与索引结构冗余消除

## 背景

M2 真实规模基准测试（100 万文件）暴露出 RSS 达到 760 MB，是理论下限 ~200 MB 的 3.8 倍。项目此前已实施 mimalloc、PathArena（快照格式）、RoaringTreemap、CompactMeta 等优化，但调查发现这些优化在运行时数据结构层面并未生效。

## 根因分析

### 核心发现：PathArena 只存在于快照，运行时没有用

`PersistentIndex`（`src/index/l2_partition.rs:404-433`）的运行时路径存储是 `paths: RwLock<Vec<Vec<u8>>>`——100 万个独立堆分配的 `Vec<u8>`，每个存储完整绝对路径。

`PathArena`（连续字节存储 + 偏移量）只在 `IndexSnapshotV4`/`V5` 快照序列化时临时构建，写完丢弃。运行时完全不用。

这意味着此前"PathArena 已优化路径存储"的假设是错误的。

### 五项冗余/浪费

| # | 组件 | 当前结构 | 1M 文件估算 | 问题 |
|---|---|---|---|---|
| 1 | `paths` | `Vec<Vec<u8>>` | 84 MB | 100 万个独立堆分配，PathArena 本应替代但没接入 |
| 2 | `parent_path_table` | `HashMap<Vec<u8>, u32>` + `Vec<Vec<u8>>` | 168 MB | rebuild 时再次复制全部路径，是第三份副本 |
| 3 | `trigram_index` | `HashMap<Trigram, RoaringTreemap>` | 100 MB | DocId < 2³²，用 64 位 RoaringTreemap 浪费一倍 |
| 4 | `filekey_to_docid` | `HashMap<FileKey, DocId>` | 56 MB | HashMap 负载因子开销大，可改排序 Vec |
| 5 | mimalloc 碎片 | — | 100 MB | 大量小分配（每个 Vec<u8>、每个 HashMap entry）导致 |

### 为什么之前没发现

- 小规模测试（3000 文件）时 RSS 仅 ~35 MB，结构开销绝对值小，不引人注意
- 100 万文件把每文件 ~420 字节的结构开销放大到 420 MB，加上碎片和 mmap 达到 760 MB
- `memory_stats()` 估算函数（`src/index/l2_partition.rs:1646`）只统计逻辑大小，不含碎片和 HashMap 桶开销

## 影响范围

- **生产环境**：索引文件越多，浪费越大。10 万文件 ~76 MB 看似无所谓，但比例不变
- **M2 冷层轮转**：RSS 过高导致 tiered watcher 预算紧张，间接限制旋转窗口可同时激活的目录数
- **资源受限设备**：嵌入式或低内存 VPS 上 760 MB 不可接受
- 上述五项冗余在生产环境同样存在，不是测试场景特殊问题
