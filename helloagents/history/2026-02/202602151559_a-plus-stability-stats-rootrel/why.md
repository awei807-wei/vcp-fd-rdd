# 阶段 A+：动态侧优化（风暴合并 + 统计透明化 + root 相对路径）

## 背景

阶段 A 已完成 DocId/Roaring/PathArena 的静态内存表示法优化，但在动态场景下还存在三个“会把系统拖进泥潭”的问题：

1. **事件风暴下频繁 rebuild**：rebuild 会触发全量扫描与大量分配/释放，若没有冷却与合并，会导致 CPU/RSS 高水位自激。
2. **内存统计不够透明**：仅看条目数/len 容易误判，必须把 `capacity` 与 Roaring 压缩体量拆项展示，才能评估优化效果。
3. **路径前缀重复存储**：即使有 PathArena，绝对路径仍在百万规模下重复存储 root 前缀，常驻 RSS 仍有可观压缩空间。

## 目标

- overflow/rebuild 触发在风暴下具备 **冷却与合并** 能力（coalesce），避免重扫风暴。
- 内存报告拆项到 **metas / HashMap capacity / arena / Roaring serialized_size**，让 RSS 诊断可解释。
- 路径改为 **root 相对存储**：`root_id + relative_path`，并升级快照协议以保证可恢复与可校验。

