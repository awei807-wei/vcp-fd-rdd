# v0.6.4：阶段 0 + 阶段 1（任务清单）

> 标注：本方案包为"阶段 0（基准测试框架）+ 阶段 1（最小化清理）"

## 阶段 0：基准测试框架与监控

- [x] 新增 scripts/bench.sh — 基准测试脚本（编译/启动/内存/CPU/查询/事件风暴）
- [x] 新增 scripts/profile.sh — 性能分析脚本（perf/dhat）
- [x] 修改 src/stats/mod.rs — 新增 StatsCollector（原子计数器）和 StatsReport（serde 序列化）
- [x] 修改 src/query/server.rs — 新增 /metrics HTTP 端点，返回 JSON 指标
- [x] 新增 BENCHMARK.md — 基线数据记录表格
- [x] 新增 .github/workflows/bench.yml — CI 基准测试工作流
- [x] cargo check 通过
- [x] cargo test --lib 通过（133 tests）

## 阶段 1：最小化清理

- [x] 删除 src/index/tiered/compaction.rs（382 行）
- [x] 修改 src/index/tiered/mod.rs — 删除 `mod compaction;` 和相关字段
- [x] 修改 src/index/tiered/snapshot.rs — 删除 `maybe_spawn_compaction()` 调用
- [x] 修改 src/event/recovery.rs — 删除轮询函数（保留 DirtyTracker 数据结构）
- [x] 修改 src/index/tiered/memory.rs — 删除 `rss_trim_loop`
- [x] 修改 src/main.rs — 删除 trim CLI 参数和 RSS trim 启动
- [x] 修改 src/event/stream.rs — 删除 `dyn_walk_and_enqueue` 和 `walk_dir_send`
- [x] 修改 src/index/tiered/sync.rs — `startup_reconcile` 和 `spawn_rebuild` 标记 `#[deprecated]`
- [x] cargo check 通过（5 warnings，0 errors）
- [x] cargo test --lib 通过（133 tests）
- [x] cargo test --test p1_edge_cases/p1_event_processing/p1_query 通过（15 tests）
- [x] cargo build --release 通过

## 版本与文档

- [x] Cargo.toml 版本号更新为 0.6.4
- [x] CHANGELOG.md 添加 v0.6.4 条目
- [x] helloagents 历史记录创建（how.md / task.md / why.md）

## 量化结果

| 指标 | 基线 (v0.6.3) | v0.6.4 | 变化 |
|------|---------------|--------|------|
| src/ 代码行数 | 19,359 | 18,781 | -578 (-3.0%) |
| cargo test --lib | 133 pass | 133 pass | 无退化 |
| cargo check | 2 warnings | 5 warnings | 新增 3 warnings（deprecated） |
| release 构建 | 通过 | 通过 | 无退化 |

## 已知问题

- `startup_reconcile` 调用点（src/main.rs:199）触发 deprecation warning，将在阶段 2 后移除
- `DiskLayer.id` 字段未使用（dead_code warning），将在阶段 6 重构持久化格式时处理
