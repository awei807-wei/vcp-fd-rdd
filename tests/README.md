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
- `p1_startup_repair.rs` — 启动 repair scan 决策与统计
- `p1_symlink_safety.rs` — 符号链接安全
- `p1_wal_recovery.rs` — WAL 回放与去重
- `p1_watch_degradation.rs` — watcher 降级轮询
- `p1_streaming_export.rs` — 流式导出字节一致性
- `p1_compaction_fast.rs` — fast/legacy compaction 等价性
- `p1_visibility_latency.rs` — 文件可见性延迟
- `p1_fast_scan_sla.rs` — lease hotset fast scan SLA 与冷目录有界最终一致
- `p1_api_e2e.rs` — HTTP API 与 `fd-rdd-query` 真实 daemon 端到端
- `p1_real_watcher.rs` — 真实 watcher create/rename/delete 端到端
- `p1_crash_recovery_matrix.rs` — abrupt kill、坏快照与启动修复组合恢复

## v0.6.0 测试相关变更

- compaction 阈值调整（8 delta / 300s 冷却）相关测试在 `p1_lsm_compaction.rs` 中覆盖。
- 事件管道 fast-sync 参数调整相关测试在 `p1_event_processing.rs` / `p1_watch_degradation.rs` 中覆盖。

## v0.6.1 测试相关变更

- CI 格式化与 Clippy 警告修复相关变更已使全部测试辅助模块通过 `cargo fmt --all -- --check` 与 `cargo clippy --all-targets -- -D warnings`。
- 新增大规模混合工作区测试 `tests/p2_large_scale_hybrid.rs`，覆盖 80 万文件冷扫、git clone、npm install、单文件 CRUD 与最终一致性验证，带性能阈值断言（CPU 100% 持续时间 ≤20000ms，峰值 RSS ≤600MB，与 CI workflow 阈值一致）。

## v0.6.15 测试相关变更

- 断电恢复基建新增 `p1_snapshot_recovery.rs` 的 stable snapshot 当前/前一版本回退、runtime-state 缺失/损坏保守恢复测试，`p1_wal_recovery.rs` 的 WAL 截断尾恢复信号测试，以及 `p1_startup_repair.rs` 的启动 repair scan 决策测试。
- CI 新增 `Poweroff recovery regression` 专项 job，显式运行快照回退、WAL 截断尾、startup repair 三组恢复测试，避免断电恢复只被全量测试隐式覆盖。

## v0.6.16 测试相关变更

- `stress-large-scale` workflow 显式运行 `large_directory_scan_100k_files` 与 `high_load_event_processing` 两个 ignored 重型测试。
- `stress-hybrid-large-scale` workflow 继续显式运行 `p2_large_scale_hybrid` 的 80 万文件混合工作区测试，并保持 `continue-on-error`，避免 GitHub runner 资源波动阻塞普通分支推进。
- 新增 `p1_api_e2e.rs`、`p1_real_watcher.rs`、`p1_crash_recovery_matrix.rs`，补齐 daemon HTTP/UDS 真实链路、真实 watcher 文件事件、坏快照/非干净退出后的启动修复组合。

## v0.7.0 测试相关变更

- M2 proc sampler 新增 `proc_sampler_*` 单元和序列化测试，覆盖 fdinfo flags 写权限解析、同 uid 写 fd 目录采样、非同 uid 拒绝后不读取 fdinfo、watch-state/health/metrics 中 proc sampler 观测字段与 budget/unavailable issue。
- M1-3 分片 repair 新增 `sliced_repair_*` 单元测试，覆盖 Periodic cold scan 大目录首次 slice 只处理约 512 entries、cursor 重新入队、后续 slice 继续补齐，以及 `/health` 新增 cold sweep/backlog 字段的 JSON 序列化；完整恢复审计继续覆盖 StartupRepairDeferred 的递归 rename subtree 补偿。
- 全量 `cargo test -q` 覆盖 runtime boundary、mmap v7 cold segment、WAL/root state、query DSL、content index、tiered watcher 和 sim parity 等回归。
- Runtime subtree tombstone 覆盖删除父目录后 cold/base 子路径验真前过滤、TTL 清理、同名目录重建解除过滤，以及 runtime-only 不持久化到 snapshot 的边界。
- 查询验真新增默认同步校验和预算回归：删除的 cold/base 命中不返回，mtime/identity 变化返回 `Changed` 且已验证，宽泛 stale 查询的同步 `stat` 不超过 `query.max_verify_per_query`，lazy validation 需显式开启才作为后台补偿路径。
- `src/event/stream.rs` 单元测试补齐孤立 `RenameMode::To`、孤立 `RenameMode::From`、Create/Rename 后续 Modify 合并优先级和 Delete 后 Modify 反例；`p1_visibility_latency.rs` 增加下载器 `.part` → 最终名 rename 可见性验证，防止新下载文件最终名因事件合并丢失而不可见。
- `p1_fast_scan_sla.rs` 启动真实 tiered daemon 并隔离 `XDG_CONFIG_HOME`，验证 active lease hotset 内 create/delete/rename 在 5 秒 SLA 窗口内更新搜索结果，同时验证冷目录不按 5 秒断言、而是在配置的 cold sweep 周期内最终追平；CI 专项 job 保留但口径改为 hotset SLA + cold eventual consistency。
- `scripts/smoke-search-syntax.sh` 支持自建临时 root、临时 HTTP 端口和自启动 daemon；在 `--no-watch` 下会递归分批调用 `/scan`，完整覆盖 HTTP search DSL smoke 矩阵。
- `dupe:content` 新增 exclude/oversized/mount policy 回归，确认内容重复扫描复用 frozen/offline、exclude 目录、mount policy 与 `content_index.max_file_size` 准入，并把 partial/full hash 慢 I/O 放在 query generation guard 外。
- `memory_light` 的 RSS/P95、idle RSS 和默认 profile 对照仍需在非沙箱 daemon 或真实数据集环境采集；本地测试不伪造性能数值。

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
