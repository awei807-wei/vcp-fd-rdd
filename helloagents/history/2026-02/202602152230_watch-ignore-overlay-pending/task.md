# 轻量迭代：抑制“自触发事件风暴” + 影子内存可观测 + pending/overlay 有界化

任务清单:
- [√] watcher 事件过滤：默认忽略 `snapshot_path` 与派生 `index.d/`（避免索引写入反哺 watcher）
- [√] CLI 增加 `--ignore-path`（可重复）允许用户手动排除日志文件等路径
- [√] overlay_state 优化：避免重复事件的重复分配；并在 MemoryReport 暴露 overlay 的条目数与字节数（影子内存）
- [√] rebuild_state 优化：pending_events 从 Vec 改为按 path 去重（HashMap），避免 rebuild 期间无限堆积
- [√] 验证：`cargo test` 与 `cargo test --features mimalloc`
- [√] 更新知识库与回忆录，并迁移方案包至 history
