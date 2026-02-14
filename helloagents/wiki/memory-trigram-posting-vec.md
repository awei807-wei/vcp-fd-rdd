# fd-rdd 内存优化记录：trigram posting 从 HashSet 改为 Vec

## 背景

在百万文件规模下，`trigram_index: HashMap<Trigram, HashSet<FileId>>` 的常驻内存占用偏高。
主要原因是 `HashSet` 的桶/指针/装载因子带来的结构性放大，而不是泄漏。

## 变更点（v0.2.x）

- posting list 表示由 `HashSet<FileId>` 改为 `Vec<FileId>`（append-only）。
- 为避免 Modify 等重复事件导致 posting 无限增长，`upsert_inner` 对“同 FileId 且同路径”的事件走快路径：只更新元数据，不再重复插入 trigram。

## 配套优化：不再存 `path_to_id: HashMap<PathBuf, FileId>`（v0.2.1 快照格式）

进一步降低常驻内存的下一刀是把路径反查从“存一份完整 PathBuf key”改为“存 hash(path) + 二次校验”：

- 内存：避免路径字符串在 `files` 与 `path_to_id` 中重复分配
- 快照：新快照格式不再落盘 path_to_id（启动时从 files 重建即可）

兼容性：
- snapshot loader 兼容读取旧 v2 快照（包含 path_to_id），写出新 v3 快照（不含 path_to_id）

## 查询候选集策略

为避免在大 posting 上做多次交集带来的分配与抖动，当前候选集采用“最短 posting”策略：

1) 从 query 中提取 trigram 列表
2) 在 trigram_index 中找到每个 trigram 的 posting
3) 选择 `len` 最小的 posting 作为候选集
4) 对候选集做 matcher 精确过滤（保持语义正确）

备注：
- 该策略在性能上通常比“全量扫描 files”好很多，但可能弱于“多 posting 交集”的极致筛选。
- 本阶段优先目标是降低常驻内存与避免抖动（可靠性优先），后续可升级到 DocId + RoaringBitmap/压缩 posting。

## 删除/rename 的处理

Vec posting 的删除为线性 `retain`，在 delete/rename 频率远低于查询频率的假设下可接受。
若 delete/rename 事件频繁（例如大量批量删除），应考虑：

- tombstone + 定期 compaction 重建 posting
- 或升级为压缩位图（RoaringBitmap）/doc_id 方案

## 后续演进（阶段 A）

阶段 A 已将 posting 升级为 `DocId(u32) + RoaringBitmap`，并配套引入 path blob arena（offset/len）。
详见：`helloagents/wiki/memory-docid-roaring-patharena.md`。
