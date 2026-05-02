# v0.6.4：阶段 0 + 阶段 1（怎么做）

## 阶段 0：基准测试框架与监控

### 1. 新增 scripts/bench.sh
- 编译时间基准（release 构建）
- 启动时间基准（有索引时）
- 内存 RSS 基准
- 空闲 CPU 基准
- 查询性能基准
- 事件风暴基准（10,000 文件创建）

### 2. 新增 scripts/profile.sh
- perf CPU profile（30 秒采样）
- dhat memory profile（占位，待环境支持）

### 3. 修改 src/stats/mod.rs
- 新增 `StatsCollector` 结构体（原子计数器）
  - queries_total / queries_duration_us
  - events_applied / events_dropped
  - snapshot_count / snapshot_duration_ms
  - fast_sync_count / fast_sync_duration_ms
- 新增 `StatsReport`（serde::Serialize，用于 /metrics JSON 输出）

### 4. 修改 src/query/server.rs
- 新增 `/metrics` HTTP 端点，返回 `StatsReport` JSON
- 路由：`.route("/metrics", get(metrics_handler))`

### 5. 新增 BENCHMARK.md
- 基线数据记录表格（v0.6.3 vs v0.6.4）
- 指标：编译时间、启动时间、RSS、CPU、QPS、事件风暴恢复

### 6. 新增 .github/workflows/bench.yml
- CI 基准测试工作流
- 触发条件：workflow_dispatch / push to main

## 阶段 1：最小化清理

### 1. 删除 src/index/tiered/compaction.rs（382 行）
- 整个文件删除
- 从 mod.rs 删除 `mod compaction;` 声明
- 从 snapshot.rs 删除 `maybe_spawn_compaction()` 调用
- 从 mod.rs 删除 `compaction_in_progress`、`compaction_last_started_at` 字段

### 2. 修改 src/event/recovery.rs
- 删除 `reconcile_dirty_loop`、`poll_dirty_state` 等轮询函数
- 保留 `DirtyTracker` 数据结构本身（阶段 3 仍需要 dirty 跟踪）

### 3. 修改 src/index/tiered/memory.rs
- 删除 `rss_trim_loop` 函数

### 4. 修改 src/main.rs
- 删除 `trim_interval_secs`、`trim_pd_threshold_mb` CLI 参数
- 删除 RSS trim 启动调用
- 简化无用 CLI 参数

### 5. 修改 src/event/stream.rs
- 删除 `dyn_walk_and_enqueue` 和 `walk_dir_send` 函数定义
- 删除 `use crate::util::maybe_trim_rss;` 导入
- 删除 Create(Folder) 事件处理中的 `dyn_walk_and_enqueue()` 调用
- 删除 idle maintenance 中的 `maybe_trim_rss()` 调用

### 6. 修改 src/index/tiered/sync.rs
- `startup_reconcile` 标记 `#[deprecated]`
- `spawn_rebuild` 标记 `#[deprecated]`

## 编译检查策略

每步修改后必须 `cargo check` 通过：
```bash
cargo check
cargo check --all-targets
cargo test --lib          # 133 tests pass
cargo test --test p1_*    # 15 integration tests pass
cargo build --release     # release build success
```

## 回退策略

- 每个阶段完成时是一个独立的 git commit
- 如果出现问题，可 `git revert` 回退到上一稳定状态
- 保留旧实现到最终清理阶段才删除（使用 `#[deprecated]` 标记）
