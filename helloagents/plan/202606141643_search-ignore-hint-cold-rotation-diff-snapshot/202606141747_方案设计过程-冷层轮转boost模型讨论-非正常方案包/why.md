# 方案设计过程：冷层轮转与 Boost 模型讨论（非正常方案包）

> 本目录仅记录方案设计过程与讨论推演，不是待执行的正常方案包；不得直接按本目录执行开发任务。若后续要落地，需要另开正式方案包并重新定义需求、验收标准和实施任务。

## 创建原因

本过程包用于沉淀关于 fd-rdd 索引更新模型的连续讨论，尤其是“L3 冷层临时倒置为 L1-like 进行及时扫描，再恢复原层级”的设想及其风险边界。

讨论起点包括三类问题：

1. 搜索 `.gitignore` 忽略文件时，fd-rdd 是否能感知并提示“该文件被 ignore 规则屏蔽”。
2. L1/L2/L3 分层索引在非实时 watch 场景下，如何减少 L3 冷层索引不及时导致的漏搜或陈旧结果。
3. snapshot 当前看起来偏完整重建，是否可以引入 diff / delta 边界以降低资源尖峰。

本目录只聚焦第 2 类问题的设计讨论，同时记录它与 DirtyQueue、tombstone、I/O governor、查询验真和 fast scan lease 等已有模块的关系。

## 当前问题边界

fd-rdd 当前已经有分层索引、冷段 mmap、事件 WAL、overlay / delta、后台 repair、查询命中验真和 fast scan lease 等基础。新的问题不是“能不能扫 L3”，而是：

- 如何让 L3 在必要时更及时。
- 如何避免一次性扫描污染热区。
- 如何避免冷目录临时提升挤占真正热目录资源。
- 如何避免后台对账在高负载下产生 I/O 风暴。
- 如何避免冷段删除事实只能靠逐文件 tombstone 常驻内存。

## 本过程包的结论性质

本过程包不产出可直接执行的实现任务，只产出设计约束和后续正式方案的输入材料。当前最核心的收敛句是：

> L3 可以被临时照顾，但不能被临时加冕。Boost 不是正式 tier，不承诺强一致、常驻内存或独占 watcher；它只是通过准入控制获得的可撤销调度机会。

2026-06-15 至 2026-06-16 的 M2 VM 原型试错进一步说明：讨论记录不能只停留在“后台轮转会让冷层更及时”的直觉上。active canary 会被 query miss / fast scan 补偿路径混淆，导致 baseline 也能表现出较快可见性；后续必须把 cold freshness age、passive first-query 成功率、scan-only fallback 和 deep repair 缺口一起纳入正式方案的验收口径。

2026-06-16 的首轮 passive A/B 已经出现正向信号：实验组 passive positive first-query 成功率为 `6/6`，baseline 为 `4/6`，且 RSS、fd、dirty queue 基本持平。这说明 M2 有从“可触发原型”进入“可能可实施机制”的依据，但样本量仍小，不能跳过重复 A/B 和极端 workload 证伪。

后续正式方案应从本过程包中提炼出独立目标，例如：

- 被 `.gitignore` 屏蔽的搜索结果提示。
- 冷层 stale hit 后的 bounded query repair。
- L3 冷轮转候选调度器。
- subtree / range tombstone 与 cold segment compaction。
- snapshot delta 化或分段化。
