# 阶段 A：DocId + RoaringBitmap + Path Blob Arena（为什么做）

目标：在不引入 mmap 段式持久化与 ArcSwap 的前提下，先把 L2 常驻内存显著压缩，并为阶段 B 的“mmap 段式索引”打地基。

核心收益链路（严格依赖顺序）：

1. DocId(u32) 替代 FileId(dev,ino) 作为 posting 的元素类型
   - posting entry 从 16B → 4B，为后续压缩与 mmap 化创造条件。
2. RoaringBitmap 压缩 posting list
   - 在中等密度下压缩比高，且支持按需访问，降低常驻与查询抖动。
3. Path Blob Arena（offset/len）
   - 将每条路径从“独立堆分配 + String/PathBuf header”改为连续 arena，主表改为 Vec 紧凑布局。

约束：

- 本阶段仍使用 bincode 快照（不引入 rkyv/mmap 段式格式）。
- 必须兼容加载旧快照并自动迁移到新快照版本。

