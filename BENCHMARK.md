# fd-rdd 基准数据

| 指标 | 基线 (v0.6.3) | v0.6.4 |
|------|---------------|--------|
| 编译时间 (release) | TBD | TBD |
| 启动时间 (有索引) | TBD | TBD |
| 空闲 RSS | ~700MB | TBD |
| 空闲 CPU | 100% 峰值 | TBD |
| 查询 QPS | TBD | TBD |
| 事件风暴恢复时间 | TBD | TBD |

## 2026-05-02 tests 分支压测

命令：

- `cargo test -q`
- `cargo test -q -- --ignored`

`p2_large_scale_hybrid` 结果：

| 阶段 | CPU 峰值 | CPU>=100% 时长 | RSS 峰值 |
|------|----------|----------------|----------|
| initial_indexing | 125% | 2034ms | 237680KB |
| git_clone | 0% | 0ms | 0KB |
| npm_install | 0% | 0ms | 0KB |

说明：冷启动全量扫描使用保守串行策略和批次节流，优先满足事件/查询可用性与 CPU 峰值约束。v7 快照启动改为直接挂载 `BaseIndexData`，不再把快照逐条回灌到 L2。
