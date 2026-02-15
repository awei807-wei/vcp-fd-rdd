# 动态内存优化（怎么做）

## 1) mimalloc（可选 feature）

- 以 Cargo feature 方式接入（例如 `mimalloc`）：
  - 仅在启用 feature 时切换全局分配器，保持默认行为不变。
  - 记录平台差异：Linux/glibc 下对 RSS 回吐更敏感；macOS/其它平台收益可能不同。
- 验证：同样 workload 下对比
  - watcher 关闭：RSS 基线不应回归变差
  - watcher 开启 + 事件风暴：RSS 高水位应更可控（至少下降或更快回落）

## 2) 路径压缩：相对 root 存储（优先于 string interning）

目标：直接减少 `PathArena.data` 的 bytes，而不是引入“只增不减”的全局字符串表。

- 引入 `root_id`（u8/u16）：
  - TieredIndex 启动时将 `roots: Vec<PathBuf>` 固化为 `root_table`，每条文件记录归属某个 root。
- PathArena 存储内容改为“相对路径 bytes”（去掉 root 前缀与分隔符）：
  - `CompactMeta` 增加 `root_id`
  - 重建 `FileMeta.path` 时：`root_table[root_id] + '/' + relative_bytes`
- 事件路径解析：
  - watcher 给的是绝对路径；需要匹配落在哪个 root 下，并转为相对路径写入 arena。
- 快照兼容：
  - v4 → v5（或 v4 扩展字段）需要迁移策略；也可先保留 v4 格式，内部仅在内存里存相对路径（落盘仍绝对）作为过渡。

## 3) 字符串池化（可选/延后）

若仍需要：
- 仅考虑“路径段字典/前缀压缩”而不是全路径 interning；
- 或将 interning 限制在“构建期临时去重”，随索引替换整体释放（需要 ArcSwap/段式支持）。

