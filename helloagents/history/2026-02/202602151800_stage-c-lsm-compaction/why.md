# 阶段 C：LSM 演进与 Compaction（长期 mmap 基座 + 内存 Delta）

## 背景与动机

阶段 B 已经实现 v6 段式快照（mmap + lazy decode），冷启动的“加载成本”接近常数。但当前启动链路仍倾向于把 mmap 基座 hydration 为可变 L2，这会把 CPU/内存压力重新带回到启动阶段，并且无法自然支持“增量刷盘（Flush）/多段合并（Compaction）”。

阶段 C 的目标是把索引演进成真正的 LSM 形态：

- **长期 mmap 基座（Base）**：读链路直接查询 mmap 段，不做全量 hydration。
- **内存 Delta（Mutable）**：watcher 增量更新只写入一个小的可变索引。
- **Flush**：把内存 Delta 追加落盘为新的只读 Segment。
- **Compaction/Merge**：多段达到阈值后在后台合并为新的 Base，控制段数量，降低查询合并成本。

## 为什么先不补 rkyv

Manifest/代际（Gen）/Compaction 策略在阶段 C 仍会快速迭代。此时强行接入 rkyv 会把“结构调整”变成“类型对齐/Archived 兼容”的高摩擦工作，拖慢核心 LSM 心脏的落地节奏。

本阶段优先完成：

- 目录化布局（Manifest + 多 Segment 文件）
- Flush/Compaction 的运行闭环（正确性 + 可观测）

待策略与 schema 稳定后，再单独开任务将 Manifest 升级为 rkyv archived（或保留手写二进制，但补齐版本迁移策略）。

