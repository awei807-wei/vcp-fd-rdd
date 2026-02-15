# fd-rdd 阶段 A 内存布局：DocId + RoaringBitmap + Path Blob Arena

本记录描述阶段 A 的“串联内存优化”落地结果：先把 L2 常驻布局压紧凑，并为后续 mmap 段式索引格式打地基（ArcSwap 胶水在 Step 1 另行引入）。

## 1) DocId(u32) 作为 posting 元素

- L2 内部引入 `DocId = u32`。
- trigram 倒排索引的 posting 不再存 `FileKey(dev,ino)`，而是存 `DocId`。
- `FileKey` 仍保留用于：
  - 扫描/事件输入（从 fs metadata 得到 dev/ino）
  - 单路径策略（hardlink “first-seen wins”）

## 2) RoaringBitmap 压缩 posting list

- `trigram_index: HashMap<Trigram, RoaringBitmap>`。
- 查询时对多个 trigram 的 bitmap 做交集，得到候选 DocId 集合，再进行 matcher 精确过滤。
- 读路径加锁顺序保持：先读取 trigram 索引得到候选集，再读取元数据/arena，避免死锁。
- 内存统计口径：优先以 `RoaringBitmap::serialized_size()` 近似 posting 的压缩存储量，并叠加 HashMap/Vec 的 capacity 级开销（仍无法覆盖 allocator 碎片导致的 RSS 高水位）。

## 3) Path Blob Arena（offset/len）

- L2 不再在每条元数据里存 `PathBuf`；改为：
  - `PathArena { data: Vec<u8> }` 存所有路径字节
  - `CompactMeta { path_off: u32, path_len: u16, ... }` 仅存引用
- 路径反查采用 `hash(path_bytes) -> DocId`（允许少量冲突，最终二次校验 bytes）。

## 3.1) root 相对路径压缩（v5）

为进一步降低常驻 RSS，将 arena 中的路径字节改为“相对 root 存储”：

- `CompactMeta` 增加 `root_id: u16`
- arena 仅存 `relative_path_bytes`（不再重复存储 root 前缀）
- 查询/事件路径哈希/精确校验时，按 `roots[root_id] + relative_path` 组合出绝对路径 bytes

同时快照升级至 v5，并加入 `roots_hash` 校验，避免 root 顺序变化导致 `root_id` 解释错位。

## 4) 快照版本升级（bincode 仍在）

- 新增快照 v4：落盘 `arena(绝对路径) + metas + tombstones(DocId)`，派生结构（trigram/path_hash 映射）启动时重建。
- 新增快照 v5：落盘 `arena(root 相对路径) + metas(root_id+offset/len) + tombstones(DocId) + roots_hash`。
- loader 兼容：
  - v2/v3 仍可读取；加载后以内存结构重建为阶段 A 的 L2（并在后续写快照时自然迁移为 v5）。
  - v4 仍可读取；加载后转换为 v5 的内存布局（root 相对路径）。
