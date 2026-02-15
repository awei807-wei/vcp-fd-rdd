# 阶段：events.wal（Append-only Log）

## 背景

fd-rdd 依赖 notify watcher 做增量更新，但在事件风暴下会发生 channel overflow；为了保证一致性，目前的兜底路径是触发全量 rebuild。即使我们已经做了 cooldown + coalesce + ArcSwap 原子切换，rebuild 依旧是“重型恢复”。

同时，长期运行中还会出现“事件在内存里堆积/高水位 RSS 粘住”的现象，根因之一是：缺少一个可持久化的、可回放的事件账本；一旦 overflow 或进程异常退出，只能依赖 rebuild 扫全盘。

## 目标

- 引入高性能追加型日志 `events.wal`：把 merge 后的事件批次持久化为可顺序读的记录流
- 启动时可回放 WAL，减少 overflow/重启后的全量 rebuild 概率与恢复时间
- 与现有 v6 段式/LSM 目录布局兼容：WAL 与 snapshot/segment 的“持久化边界”明确

## 成功标准

- 正常运行时：事件批次在 apply 之前写入 WAL（best-effort，可配置 fsync）
- snapshot flush：在持久化成功后，WAL 能安全地推进 checkpoint（不丢未落盘事件）
- 启动恢复：加载 base/delta segments 后回放 WAL，查询结果包含最近增量
- 可观测：MemoryReport/日志能解释“WAL 回放/checkpoint/忽略 sealed WAL”的行为
