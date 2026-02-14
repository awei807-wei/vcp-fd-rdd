# 阶段 A：任务清单（DocId + RoaringBitmap + Path Blob Arena）

> 标注：本方案包为“阶段 A（内存优化）”，不包含 mmap 段式与 ArcSwap（阶段 B）。

- [√] 代码结构：引入 `DocId`/`FileKey`/`CompactMeta`/`PathArena` 数据结构
- [√] L2 改造：主表改为 `Vec<CompactMeta>`，维护 `filekey_to_docid` 与 `path_hash_to_id`
- [√] posting 改造：`HashMap<Trigram, RoaringBitmap>`（替换 `Vec<FileKey>`）
- [√] 查询改造：trigram Roaring 交集 → 精确过滤（保持死锁规避的锁顺序）
- [√] 事件应用：Create/Modify/Delete/Rename 全链路适配 DocId
- [-] L1 适配：缓存键与失效逻辑改为 DocId（本阶段保留 L1 以 FileKey 为键，避免把 DocId 泄露到对外结构）
- [√] 快照升级：新增版本并实现 v2/v3 → v4 迁移加载
- [√] 指标更新：内存统计字段从 FileKey/PathBuf 模型迁移到 DocId/Arena 模型
- [√] 测试：新增 L2 行为测试（Roaring posting 基础用例）
- [√] 验证：`cargo test` 通过（14 tests）
