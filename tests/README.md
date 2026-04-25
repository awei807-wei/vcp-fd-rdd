# fd-rdd 测试集

本目录包含 fd-rdd 核心模块的回归与集成测试。

## 测试模块索引

- `p0_allocator.rs` — 分配器可观测性（P0）
- `p0_storage_compat.rs` — 存储层兼容性（v2-v7 快照、WAL v1/v2）
- `p1_edge_cases.rs` — 边界场景
- `p1_event_processing.rs` — 事件管道处理
- `p1_ignore_rules.rs` — ignore 规则贯通
- `p1_lsm_compaction.rs` — LSM compaction 正确性
- `p1_multi_root.rs` — 多 root 隔离
- `p1_query.rs` — 查询与过滤
- `p1_snapshot_recovery.rs` — 快照恢复
- `p1_symlink_safety.rs` — 符号链接安全
- `p1_wal_recovery.rs` — WAL 回放与去重
- `p1_watch_degradation.rs` — watcher 降级轮询
- `p1_streaming_export.rs` — 流式导出字节一致性
- `p1_compaction_fast.rs` — fast/legacy compaction 等价性
- `p1_visibility_latency.rs` — 文件可见性延迟

## v0.6.0 测试相关变更

- compaction 阈值调整（8 delta / 300s 冷却）相关测试在 `p1_lsm_compaction.rs` 中覆盖。
- 事件管道 fast-sync 参数调整相关测试在 `p1_event_processing.rs` / `p1_watch_degradation.rs` 中覆盖。

## v0.6.1 测试相关变更

- CI 格式化与 Clippy 警告修复相关变更已使全部测试辅助模块通过 `cargo fmt --all -- --check` 与 `cargo clippy --all-targets -- -D warnings`。
- 新增大规模混合工作区测试 `tests/p2_large_scale_hybrid.rs`，覆盖 80 万文件冷扫、git clone、npm install、单文件 CRUD 与最终一致性验证，带性能阈值断言（CPU 100% 持续时间 ≤3000ms，峰值 RSS ≤400MB）。

## v0.6.0 更新（零拷贝序列化 P1 + Compaction 降维 P2）

### P1 — 零拷贝序列化

- `PathArena.data` 改为 `Arc<Vec<u8>>`，snapshot 时不再复制 path arena
- `export_segments_v6` 中 posting bitmap 直接序列化到 `postings_blob_bytes`，避免中间 `buf` 分配
- 新增 `export_segments_v6_to_writer` / `export_segments_v6_compacted_to_writer` 流式写入方法

### P2 — Compaction 降维

- 新增 `MmapIndex::for_each_trigram` 直接遍历 mmap 中的 trigram bitmap
- 新增 `compact_layers_fast`：通过位图 OR 合并 compaction，避免逐文件 re-tokenize
- `FAST_COMPACTION=1` 环境变量启用快速路径

### 新增测试

- `tests/p1_streaming_export.rs`：流式导出字节一致性
- `tests/p1_compaction_fast.rs`：fast/legacy compaction 等价性
- `tests/p1_visibility_latency.rs`：文件可见性延迟回归
- `tests/p2_large_scale_hybrid.rs`：80 万文件大规模混合工作区正确性（git clone、npm install、CRUD）
