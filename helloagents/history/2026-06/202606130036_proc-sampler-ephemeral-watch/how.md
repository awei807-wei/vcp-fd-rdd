# M2 proc sampler 提前触发 Ephemeral Watch 设计

## 当前行号复核

- `src/event/proc_sampler.rs:1`：新增 proc sampler 模块。
- `src/main.rs:407`：tiered runtime 启动 fast scan 与 proc sampler loop。
- `src/main.rs:1437`：`spawn_proc_sampler_loop`。
- `src/main.rs:1566`：`maybe_send_ephemeral_watch_command` 返回是否实际触发 Add/Replace。
- `src/event/tiered_watch.rs:617`：`record_proc_sampler_report`。
- `src/query/server.rs:31`：`HealthTelemetry`。
- `src/stats/metrics_reporter.rs:105`：`MetricsHealthSnapshot`。

## 实现策略

1. 采样模块
   - `ProcSamplerConfig` 默认开启，interval 1000ms，单 tick 最多 128 pid、每 pid 64 fd、32 个目录。
   - Linux 下枚举 `/proc` 数字 pid，以 cursor 轮转避免每 tick 从低 pid 开始。
   - 先读取 pid 目录 metadata uid，只处理同 uid 进程。
   - 读取 `fdinfo/<fd>` 的 `flags`，只接受 `O_WRONLY` / `O_RDWR`。
   - 读取 `fd/<fd>` symlink，文件目标取 parent，目录目标取自身；socket、deleted、非绝对路径丢弃。
   - 目录必须位于 configured roots 内，且不命中 ignore prefix / exclude dirs。

2. runtime 接线
   - 仅在 `watch_mode = tiered`、watch enabled、`proc_sampler.enabled = true` 时启动 sampler loop。
   - loop 使用 `spawn_blocking` 执行 `/proc` 读取，避免阻塞 async runtime。
   - 采样目录调用既有 `maybe_send_ephemeral_watch_command`，使用 `repeat_threshold = 1`，让写句柄发现直接变为临时 watcher 候选。
   - 不写索引、不返回查询结果、不绕过返回前验真；它只提升后续事件新鲜度。

3. 观测字段
   - `TieredWatchRuntime` 保存 proc sampler 最近一次报告。
   - `/watch-state`、`/health`、diagnostics、metrics JSONL 暴露 enabled、duration、pids seen/scanned/denied、fdinfo/readlink/write fd、sampled dirs、triggered watches、budget exhausted、unavailable。
   - metrics diagnostics 对 budget exhausted 和 unavailable 生成 issue。

## 边界

- 非 Linux 平台返回 `unavailable=true`，不执行 `/proc` 采样。
- 不新增依赖，不新增 watcher 机制。
- 不处理跨用户进程，也不把采样结果视为正确性证据。
- Ephemeral Watch 注册、移除和回滚仍由 watcher task 串行执行。
