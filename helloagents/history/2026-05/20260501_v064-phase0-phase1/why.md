# v0.6.4：阶段 0 + 阶段 1（为什么做）

## 背景

基于 `fd-rdd-phased-implementation-plan.md` 的渐进式重构计划和 `causal-chain-report.md` 的 CPU/RAM 暴涨根因分析，fd-rdd 需要从当前臃肿的架构逐步重构为"低占用、长待机"的桌面文件搜索守护进程。

当前基线（v0.6.3）存在以下致命问题：
1. **for_each_live_meta_in_dirs** 每次 fast_sync 遍历全部 800 万条 metas，触发 CPU/RAM 雪崩
2. **Hybrid Crawler** 每 30/60 秒周期性全量轮询，导致空闲 CPU 100%
3. **compaction** 双索引内存峰值，snapshot 阻塞事件管线
4. **pending_events** 重建期间无界堆积，内存暴涨
5. **dyn_walk_and_enqueue** spawn_blocking 洪水
6. **RSS trim** 循环每 300 秒回收页，冲突"低占用"设计

## 目标（v0.6.4）

本次版本聚焦两个阶段性目标：

1. **阶段 0：建立安全网**
   - 不修改任何业务逻辑
   - 建立可量化的基准测试框架
   - 新增运行时指标收集（StatsCollector）和 /metrics HTTP 端点
   - 为后续重构提供"编译即正确"的验证手段

2. **阶段 1：最小化清理**
   - 删除所有冲突"低占用、长待机"定位的死代码
   - 删除 compaction、recovery 轮询、RSS trim、dyn_walk_and_enqueue
   - 简化 CLI 参数
   - 标记待删除函数为 deprecated
   - 验证功能不退化

## 非目标

- 不引入 ParentIndex、DeltaBuffer、PathTable 等新数据结构（阶段 2-4）
- 不修改查询管线（trigram → exact → fuzzy）
- 不修改事件管线（debounce/merge/apply）

## 预期收益

- 代码行数减少 ~578 行（src/ 目录：19,359 → 18,781）
- 删除所有周期性轮询代码路径
- 编译更快（删除了 382 行的 compaction.rs）
- 为阶段 2-7 打下可回滚的基线
