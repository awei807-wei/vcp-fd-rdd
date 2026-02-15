# 实现方案（A+）

## 1) rebuild 冷却与合并（TieredIndex 内聚）

- 在 `TieredIndex` 内部维护 `RebuildState`：
  - `last_started_at`：用于 cooldown
  - `requested/scheduled`：用于合并请求并延迟触发
  - `pending_events`：用于 rebuild 期间事件回放
- rebuild 请求语义：
  - 若已在 rebuild：仅标记 `requested`（合并）
  - 若在 cooldown：调度一次延迟触发线程（合并）
  - 冷却到期后执行一次 rebuild，避免频繁重扫

## 2) 内存统计拆项（capacity + Roaring）

- `L2Stats` 增加 capacity 与拆项字段：
  - `metas_capacity / filekey_to_docid_capacity / path_hash_to_id_capacity / trigram_index_capacity / arena_capacity`
  - `metas_bytes / filekey_to_docid_bytes / arena_bytes / path_to_id_bytes / trigram_bytes`
  - `roaring_serialized_bytes`（posting + tombstones 的 serialized_size 汇总）
- `MemoryReport` 输出增加上述拆项，避免“len 误判”。

## 3) root 相对路径压缩 + 快照 v5

- L2 内部 `CompactMeta` 增加 `root_id: u16`
- arena 存储相对路径 bytes，不重复存 root 前缀
- path hash/查询精确过滤使用 `root + rel` 组合得到的绝对路径 bytes
- 快照升级到 v5：
  - body 存 `roots_hash + arena(root-relative) + metas(root_id+off/len) + tombstones`
  - 加载时校验 `roots_hash`，避免 root 顺序变化导致 `root_id` 错位
  - 兼容读取 v4，并在加载后转换为 v5 内存布局

