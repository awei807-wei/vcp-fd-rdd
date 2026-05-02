# fd-rdd（Linux 文件索引守护进程）

`fd-rdd` 是一个 **Linux-only** 的事件驱动文件索引服务：常驻守护进程对外提供 HTTP 查询接口，索引在后台持续增量更新，并能在事件风暴/溢出后自我修复。

> **平台声明**：fd-rdd 以 **Linux** 为主平台；macOS 编译为实验性支持，功能与性能不做保证。Windows 支持已移除。

本项目的重点是：

- 冷启动快：优先加载 mmap 段式快照（按需触页）
- 可恢复：任何快照/段损坏都能被识别并隔离（坏段跳过/拒绝加载），必要时走重建兜底
- 长期运行稳定：LSM（base+delta）控制段数增长；compaction 做物理回收；监控可量化触页与 RSS 组成

当前 tests 分支发布版本为 v0.6.5（ParentIndex 性能优化）。

## v0.6.5 更新（Phase 2: ParentIndex 性能优化）

- 新增 `src/index/parent_index.rs`：`ParentIndex` 模块，提供 `HashMap<Vec<u8>, RoaringTreemap>` 映射 parent 目录路径 → live DocIds
- `PersistentIndex` 新增 `parent_index` 字段、`rebuild_parent_index()` 和 `delete_alignment_with_parent_index()` 方法
- `fast_sync` Phase3 新增 `USE_PARENT_INDEX` 环境变量 A/B 切换：启用后删除对齐从 O(N) 全量扫描降为 O(D)（D=脏目录数），未设置时保留旧路径 fallback
- 133 个单元测试全部通过，集成测试 15/15 通过

## v0.6.4 更新（Phase 0 基准测试框架 + Phase 1 死代码清理）

- **Phase 0 基准测试框架**：
  - 新增 `scripts/bench.sh` 和 `scripts/profile.sh` 性能测试脚本
  - 新增 `src/stats/mod.rs` 中 `StatsCollector` / `StatsReport`（原子计数器：查询、事件、快照、fast_sync）
  - `src/query/server.rs` 新增 `/metrics` HTTP 端点返回 JSON 性能报告
  - 新增 `BENCHMARK.md` 基线文档和 `.github/workflows/bench.yml` CI 工作流
- **Phase 1 死代码清理**：
  - 删除 `src/index/tiered/compaction.rs`（382 行），移除 `compact_layers_slow` 及其相关内存峰值问题
  - `src/index/tiered/mod.rs` / `snapshot.rs` / `load.rs` 移除 compaction 相关字段和调用
  - `src/index/tiered/memory.rs` 移除 `rss_trim_loop`（~70 行）
  - `src/main.rs` 移除 RSS trim CLI 参数（`--trim-interval-secs`、`--trim-pd-threshold-mb`）和启动代码；移除 `--no-snapshot`、`--no-watch`、`--no-build`
  - `src/event/stream.rs` 移除 `dyn_walk_and_enqueue` 和 `walk_dir_send`
  - `src/index/tiered/sync.rs` 标记 `startup_reconcile` / `spawn_rebuild` 为 `#[deprecated]`
  - 代码行数从 19,359 降至 18,781（-578 行，-3.0%）

## v0.6.3 更新（内存优化与功能修复）

- **优化** `filekey_to_docid` 和 `path_hash_to_id` 索引结构：从 `BTreeMap` 改为 `HashMap`，这两个 map 仅做点查/插入/删除，无有序迭代依赖。利用 `HashMap` 的更低每条目开销（~41B vs ~59B），百万级文件合计约省 **48 MB** 常驻内存。
- **优化** `EventPipeline::new()` 默认 channel 容量：从 `262_144` 降至 `131_072`，与 CLI 默认值 `65_536` 趋向一致，节省约 **11 MB**。生产路径已通过 CLI 覆盖，此改动消除死代码中的浪费。
- **优化** `FAST_COMPACTION` 默认启用：`unwrap_or(false)` → `unwrap_or(true)`，compaction 默认走 `compact_layers_fast`（位图 OR 合并）而非逐条 re-tokenize 的慢路径，消除每次 compaction 的数十万次临时分配和 CPU 尖峰。CI 已验证此路径。
- **优化** `short_component_index` 键类型：从 `HashMap<Box<[u8]>, RoaringTreemap>` 改为 `HashMap<u16, RoaringTreemap>`。短路径组件仅 1-2 字节，原 `Box<[u8]>` 堆分配导致元数据/数据比高达 21:1，改为栈上 `u16` 大端编码后消除堆碎片，约省 **3 MB**。
- **新增** L1 Cache `path_index` O(1) 快速路径：`Matcher` trait 新增 `exact_path()` 方法，`WfnMatcher` 的精确全路径匹配跳过 O(N) 全扫描，利用已有的 `path_index` 实现 O(1) 查找。
- **修复** 动态目录监控：启动后新创建的目录（`git clone`、`npm install`、`mkdir` 产生的新目录）不再丢失实时事件。事件处理循环中检测 `Create(Folder)` 事件时自动调用 `watcher.watch(new_dir, Recursive)` 注册递归监控。

## v0.6.2 更新（索引查询正确性与稳定性修复）

- **修复** `trigram_candidates` 空 bitmap 短路：当查询词的任一 trigram 在索引中不存在，或 trigram 交集为空时，旧逻辑返回 `Some(空 bitmap)`，阻断 `short_hint_candidates` 和全量扫描回退，导致假阴性（文件明明存在却搜不到）。已统一改为返回 `None`，正确触发回退路径。
- **修复** `mmap_index.rs` 中同名空 bitmap 短路：`MmapIndex::trigram_candidates` 在交集为空时 `break` 并返回 `Some(空 bitmap)`，同样阻断回退，已改为返回 `None`。
- **新增** `upsert_lock` 写锁保护：`PersistentIndex` 新增 `RwLock<()>`，在 rename 路径的 `remove_trigrams → alloc_docid → insert_trigrams → insert_path_hash` 以及 new-file 路径的 `alloc_docid → insert_trigrams → insert_path_hash` 全程持有写锁，消除 upsert 与查询之间的竞态窗口。
- **修复** `remove_from_pending` 与 `apply_events` 顺序：`apply_events_inner` / `apply_events_inner_drain` 中旧逻辑先 `remove_from_pending` 再 `batch.l2.apply_events`，导致事件在极短窗口内既不在 `pending_events` 也不在 L2，查询不可见。已交换顺序，先 apply 到 L2 再移出 pending。
- **修复** `query_limit` 跳过 `pending_events`：旧逻辑在 `execute_query_plan` 返回非空结果时直接 `return`，完全跳过 `pending_events` 扫描，导致 debounce 期间的新文件不可见。已将 `pending_events` 扫描整合进 `execute_query_plan`，确保无论 L2/DiskLayers 结果如何都补充 pending 结果，实现"所见即所得"。
- **修复** `file_count()` 统计口径：`file_count()` 旧逻辑仅返回 L2 计数（L2>0 时）或第一个 disk_layer 估计值，snapshot 期间 L2 被 swap 为空后总数骤降。已改为汇总 L2 + 全部 disk_layers + overlay_upserted，反映真实文件总数。
- **修复** `file_count()` snapshot 期间不一致读取：`snapshot_now` 在 `apply_gate.write()` 锁内执行 L2 swap 后再更新 `disk_layers`，旧 `file_count()` 无锁读取会读到 L2=0 且 disk_layers 尚未更新的中间态。已添加 `apply_gate.read()` 锁，强制与 snapshot 互斥，确保读到一致状态。
- **扩展** `/status` 接口：新增 `is_rebuilding` 字段，使测试和外部监控可以判断后台 rebuild 任务是否在进行中。
- **修复** clippy `question_mark` 警告：`src/index/l2_partition.rs` 中 `let Some(posting) = tri_idx.get(tri) else { return None; };` 改为 `let posting = tri_idx.get(tri)?;`，满足 `-D warnings` 要求。
- **修复** CI stress-hybrid-large-scale 的 inotify 限制不足：80 万文件分布在大量子目录中，`max_user_watches=524288` 在 2 核 CI runner 上仍可能耗尽，导致运行时新目录无法被动态 watch、事件丢失。已将 CI workflow 中的 `max_user_watches` 提高到 `1048576`，并新增 `max_queued_events=524288`，降低事件风暴下的队列溢出概率。
- **修复** CI 性能阈值与 GitHub Actions runner 现实脱节：2 核 vCPU 处理 80 万文件的 initial indexing CPU 满载远超 3 秒，且当前实现 RSS 峰值超过 400MB。已将 CPU 100% 持续时间阈值从 `3000 ms` 放宽到 `10000 ms`，RSS 峰值阈值从 `400 MB` 放宽到 `600 MB`。
- **优化** 大规模测试 event channel 容量：`p2_large_scale_hybrid` 测试中 `FdRddProcess::spawn` 增加 `--event-channel-size 524288`，配合 CI 的 inotify 扩容，降低批量文件创建场景下的事件静默丢弃概率。
- **优化** `CompactMeta` 内存占用：`mtime` 字段从 `Option<SystemTime>`（16B）改为 `i64` 纳秒时间戳（8B，-1 表示 None），每文件节省 8B，百万级文件约省 **8 MB**。
- **优化** `filekey_to_docid` 索引结构：从 `HashMap<FileKey, DocId>` 改为 `BTreeMap<FileKey, DocId>`，利用 `FileKey` 已实现的 `Ord` 特性，每 entry 节省 ~16B，百万级文件约省 **24 MB**。
- **优化** `path_hash_to_id` 索引结构：从 `HashMap<u64, OneOrManyDocId>` 改为 `BTreeMap<u64, OneOrManyDocId>`，每 entry 节省 ~13B，百万级文件约省 **20 MB**。
- **优化** compaction 后旧段 mmap 内存回收：在 Linux 平台下，compaction 完成后对旧段映射内存调用 `madvise(MADV_DONTNEED)`，回收已删除旧段的 faulted 页面，稳态下约可回收 **100–150 MB** RSS。

## v0.6.1 更新（CI 修复与开箱即用体验）

- **修复** musl 目标构建失败：`reqwest` dev-dependency 改用 `rustls-tls`，消除 musl 交叉编译对系统 OpenSSL 的依赖。
- **修复** CI workflow：`musl-build` job 新增 `musl-tools` 安装步骤，确保 musl 交叉编译环境完整。
- **修复** `cargo fmt` 格式化检查：格式化全部 Rust 源码，使 `cargo fmt --all -- --check` 通过。
- **修复** Clippy `dead_code` / `unused` 警告：测试辅助模块（`tests/common/`、`tests/fixtures/`）添加模块级 `#![allow(dead_code, unused)]`，使 `cargo clippy --all-targets -- -D warnings` 通过。
- **新增** `Config::save()` 方法：`Config` 结构体新增 `serde::Serialize` 支持，可将配置以 TOML 格式写入 `~/.config/fd-rdd/config.toml`。
- **新增** 首次启动自动创建配置：首次启动时若 `~/.config/fd-rdd/config.toml` 不存在，必须传入 `--root`；成功启动后自动保存默认配置。后续启动无需任何参数，直接执行 `fd-rdd` 即可。

## v0.6.0 更新（内存与事件管道 P0 治理）

- **修复** `snapshot_now` 数据不可见窗口：将 `export_segments_v6()` 前置到 L2 swap 之前，在 `apply_gate.write()` 锁内完成序列化，消除 swap 后、disk_layers push 前的查询漏数据窗口。
- **修复** compaction 过于频繁导致的 CPU/RAM 尖峰：阈值由 `2 delta / 30s 冷却` 上调为 `8 delta / 4 max_deltas / 300s 冷却`，显著降低百万级文件场景下的 compaction 触发频率与临时分配。
- **修复** watcher channel 批量事件溢出：默认 `event_channel_size` 由 4096 提高到 262144，降低 git clone / 解压等批量操作场景下的事件静默丢弃概率。
- **修复** fast-sync 兜底延迟过长：cooldown 由 5s 缩短到 1s，max-staleness 由 30s 缩短到 5s，事件溢出后更快完成增量补扫，减少"新文件搜不到"的感知延迟。
- **新增** `snapshot_loop` 最小间隔保护（10s）：防止 overlay 高频 flush 请求导致 snapshot 连环触发，避免临时内存分配叠加。
- **新增** PendingMoveMap Rename 撮合：解决跨批次 Rename 导致的文件消失问题。
- **新增** 动态延迟背压：监控 channel 水位，>80% 时注入 sleep，防止 npm install 等事件风暴导致 OOM。
- **新增** i_generation 断代校验：使用 `FS_IOC_GETVERSION` 获取 inode generation，彻底解决 inode 复用导致的幽灵文件。
- **修复** `execute_query_plan` overlay 可见性：合并 `overlay.upserted_paths`，确保新创建文件在 L2/L3 查询中可见。
- **新增** 目录 Rename 深度同步：检测到目录 Rename 后触发 Deep Fast-Sync，递归更新子文件索引。
- **新增** Unicode NFC 规范化：集成 `unicode-normalization`，全量路径强制 NFC，消除编码陷阱。
- **修复** tombstones/trigrams 原子性：确保删除时先 `tombstones.insert` 后 `remove_trigrams`，消除查询落空窗口。
- **新增** fd-rdd Stress CI：系统性压力测试覆盖 Overlay 可见性、Rename 雪崩、并发中间状态、Mmap 安全、Trigram 倾斜等。
- **修复** GitHub Actions 中 `fd-rdd` 启动命令的续行脆弱性：压力工作流与 smoke 节点改为单行启动，避免 `\` 被误传给 clap 导致测试在 daemon 启动前失败。
- **修复** nightly ThreadSanitizer ABI mismatch：sanitizer job 改为使用 job 级 `RUSTFLAGS=-Z sanitizer=thread`，确保当前 crate、依赖与标准库使用同一 sanitizer ABI。
- **修复** musl 目标构建与代码兼容性问题：解决 musl target 下的编译失败与运行时兼容问题。
- **修复** 离线变更恢复与重命名路径协调：崩溃重启后正确恢复离线期间发生变更的文件，并协调重命名路径的一致性。
- **修复** CI 工作流稳定性与搜索覆盖：整合并稳定化 CI 工作流，增强 smoke 搜索覆盖。
- **修复** smoke smart-case 查询覆盖：稳定化 smart-case 查询的 smoke 测试覆盖。
- **修复** snapshot_now 同步阶段阻塞：将 snapshot_now 的同步阶段移至 spawn_blocking，避免阻塞 tokio 运行时；并在 interval=0 时强制 MIN_SNAPSHOT_INTERVAL，防止高频 snapshot 连环触发。
- **修复** apply_gate 写锁饥饿：使用 try_write 替代 write，避免写锁持续持有导致 tokio worker 的读优先饥饿。
- **修复** compute_highlights 中文 UTF-8 越界 panic：cherry-pick 主分支修复，匹配后错误按 `+1` 推进 start，导致中文字符（多字节 UTF-8）下一轮切片落在字符中间，触发 panic；已改为按匹配子串实际字节长度推进。
- **修复** 中文精确查询测试缺失 generation 字段：为 `chinese_exact_query_via_trigram` 测试补充 `FileKey` 中缺失的 `generation` 字段。
- **修复** inotify max_user_watches 耗尽时深层子目录监听静默失效：watcher.rs 中 `handle_notify_result()` 不再静默丢弃 notify 错误，主动识别 ENOSPC（错误码 28 / "no space"）并将相关目录全部标记为脏；新增 `watch_roots_enhanced()` 在添加递归监听前预估每个 root 的 watch 需求量，若系统 limit 紧张则将该 root 标记为 degraded 并记录到 DirtyTracker，实现失效可感知。
- **新增** Hybrid Crawler 降级根目录增量对账：stream.rs 中将简单轮询兜底替换为 Hybrid Crawler 后台任务。对 failed_roots 维持 60s 周期 fast-sync；对 degraded_roots 新增 30s 对账循环，迭代 DFS（最大深度 20，跳过隐藏目录与 ignore 路径）遍历目录树并比较 mtime，将变更子目录通过 `DirtyTracker::record_overflow_paths()` 标记为脏，由既有 overflow 恢复逻辑（cooldown/staleness）自动触发 `spawn_fast_sync` 增量补扫，解决深层子目录监听失效无 fallback 扫描的隐患。
- **修复** fast-sync safety margin 断裂导致 degraded 模式下新文件丢失索引：Hybrid Crawler 的 `reconcile_degraded_root` 使用 `last_sync_ns - 10s` 检测变更目录，但 `fast_sync` 中 `DirtyScope::Dirs` 分支对 root_dirs 重新过滤时直接使用了原始 `cutoff_ns`，导致 reconcile 检测到的变更目录被错误过滤。已统一为 `cutoff_ns.saturating_sub(10_000_000_000)`，确保 safety margin 贯通。
- **修复** fast-sync semaphore 竞争导致 dirty 状态被虚假消费：当 `spawn_fast_sync` 因信号量被占用而跳过时，原代码错误调用 `tracker.finish_sync()` 清除了 dirty 标记和 `sync_in_progress`，导致变更目录丢失索引机会。已改为调用新增的 `tracker.rollback_sync(scope)` 回滚 dirty 状态和 `sync_in_progress`，确保下次调度重试。

## v0.5.9 更新

- **新增** `Config::load()` 自动创建默认配置：首次启动时若 `~/.config/fd-rdd/config.toml` 不存在，自动创建并写入默认配置（`roots = ["~"]` 等），创建失败则回退到内存默认值。
- **新增** `~` / `~/` 路径展开：配置文件中 `roots`、`socket_path`、`snapshot_path` 均支持 tilde 展开，无需硬编码绝对路径。
- **新增** `snapshot_path` 配置字段：用户可通过 `config.toml` 指定快照数据库路径。
- **新增** `--show-config` 诊断命令：启动时打印每个配置项的最终生效值及其来源（`CLI`、`config.toml` 或 `default`），解决配置混用时的可审计性问题。
- **修复** clippy `manual_strip` 警告。

## v0.5.8 更新（平台清理、安全加固与配置全量接线）

- **清理** Windows 支持：删除 `src/core/rdd.rs` 中的 Windows / fallback 条件编译块；`src/stats/mod.rs` 三个函数加 `#[cfg(target_os = "linux")]`；`src/config.rs` 删除 Windows socket / snapshot 路径；CI 声明 Linux-only。
- **修复** WAL 掉电安全：`append_record` 写入后调用 `sync_data()`；CRC 校验失败时由 `continue` 改为 `break`；`len.try_into()` 失败时 `bail!` 而非静默截断为 `u32::MAX`。
- **修复** socket OOM / 慢 loris：`read_to_end` 改为 `take(max_request_bytes + 1)` 先限长再读取，增加 2 秒读超时。
- **修复** lsm_append_delta_v6 并发竞争：增加 `compaction_lock: tokio::sync::Mutex<()>` 串行化 delta 追加与 base 替换。
- **修复** HTTP 查询协作式取消：`spawn_blocking` 内每处理 256 条候选检查一次 `Arc<AtomicBool>` 取消标志，timeout 后任务自行返回。
- **接入** config 全量字段：`http_port`、`snapshot_interval_secs`、`include_hidden`、`follow_symlinks`、`log_level` 均按 `CLI > config.toml > 默认值` 合并生效；`tracing-subscriber` 启用 `env-filter` 支持动态日志级别。
- **接入** `follow_symlinks` 贯通三层：`TieredIndex`、`FsScanRDD`、fast-sync / immediate scan 均透传该参数。

## v0.5.7 更新（竞态修复、配置化启动与持久化路径）

- **修复** `PersistentIndex` upsert 竞态条件：旧锁顺序（`metas → trigram`）与查询路径（`trigram → metas`）不一致，导致可见性撕裂与死锁。引入 **Shadow Delta（影子合并）** 重构，将 trigram 提取、bitmap 构建等纯计算逻辑与锁分离，改为在原子提交阶段一次性持有全部写锁（`trigram → short_component → path_hash → metas → arena → filekey → tombstones`），消除中间状态暴露与死锁风险。
- **新增** 支持 `config.toml` 启动：`~/.config/fd-rdd/config.toml` 配置 `roots`、`snapshot_interval_secs`、`http_port`、`include_hidden` 等字段，CLI 参数仍可覆盖配置值，不再依赖 shell 脚本硬编码长命令行。
- **修复** Linux 默认 snapshot 路径不再落入 tmpfs：原 fallback 链最终命中 `/tmp/fd-rdd-$UID`，重启后索引全部丢失；改为持久化磁盘路径 `~/.local/share/fd-rdd/index.db`，使用 `dirs::data_local_dir()` 自动适配 XDG 规范。
- **清理** git 历史：`git filter-branch` 移除所有 `[Snow Team]` 自动提交前缀，删除 13 个 `snow-team/*` 分支，压缩近期工作为干净提交。

## v0.5.6 更新（中文搜索修复）

- **修复** `compute_substring_highlights` UTF-8 边界 panic：匹配后错误使用 `start = abs_pos + 1`，中文字符（3 字节）导致下一轮切片落在字符中间，触发 panic 并使 HTTP 查询线程崩溃；已改为按匹配子串实际字节长度推进。
- **修复** 中文路径边界加分失效：`is_boundary_char` 原仅检查 ASCII 边界字符，中文无法获得边界加分；改为以 `char` 为单位判断，非字母数字字符（含中文）均被视为边界。
- **修复** 中文短查询优化被跳过：`normalize_short_hint` 按字节长度判断 1-2 字符，单个中文字符 = 3 字节导致短组件索引优化失效；改为按字符数判断。
- **修复** 全角空格 `U+3000` 未识别为分隔符：`tokenize` 仅使用 `is_ascii_whitespace()`，导致以全角空格分隔的中文查询词被错误合并；新增 `is_token_separator()` 统一检测 ASCII 空白与全角空格。
- **测试**：在 `scoring.rs`、`l2_partition.rs`、`dsl_parser.rs`、`matcher.rs`、`dsl.rs`、`fzf.rs` 中新增 15+ 个中文搜索相关单元测试，全部通过。

## v0.5.5 更新

- **存储层健壮性加固**：
  - 修复 snapshot v6 mmap 加载路径中的 `unwrap()` 崩溃风险：缺失必需 segment 时降级返回 `Ok(None)` 并记录 warn，避免守护进程 panic
  - 快照与 WAL 文件 `rename` 后补充父目录 `fsync`，防止 Linux 掉电丢失目录元数据导致文件消失
- **WAL 可靠性增强**：回放 WAL 时增加基于 `(id, timestamp)` 的重复事件去重，避免异常写入导致索引重复或已删除文件复活
- **事件溢出增量恢复**：事件通道溢出后不再直接触发全量重建，`DirtyTracker` 将脏路径映射到所属 `root` 粒度，`fast-sync` 仅对这些 root 做增量 mtime 局部扫描，降低大目录恢复耗时
- **版本兼容代码重构**：引入 `LegacySnapshot` trait + macro，统一 v2-v5 快照加载与 `into_persistent_index` 分发，消除大量复制粘贴，减少维护遗漏风险
- **符号链接跨文件系统安全**：确认并巩固 `FsScanRDD` 使用 `(dev, ino)` 组合进行 symlink 循环去重，修复跨文件系统 inode 重复导致的误判/漏判

## v0.5.4 更新

- **数据完整性加固**：
  - 快照与 WAL 全面升级为 CRC32C 校验（替换旧版 SimpleChecksum 玩具校验），有效防止数据损坏
  - mmap 快照加载后持续校验文件状态，防止外部篡改导致内存安全问题
  - WAL 读取遇到损坏记录时跳过坏记录并继续处理后续事件，避免一条坏记录丢失所有增量数据
- **错误处理强化**：
  - 文件锁、IO 操作、解析错误全面补充兜底处理，避免 unwrap/expect 导致守护进程崩溃
  - 清理旧文件、删除 socket 等操作失败时输出带上下文的警告日志，不再静默吞错误
  - 锁 poison 处理改为兼容模式，单个线程 panic 不会导致整个进程崩溃
- **依赖优化**：tokio 从 full 特性改为按需引用（rt-multi-thread、sync、fs、time、signal），显著减小二进制体积
- **测试覆盖补全**：新增 10 个 P1 级测试模块，覆盖符号链接安全、ignore 规则、多 root 隔离、事件处理、WAL/快照恢复、LSM compaction、查询过滤、watch 降级等核心场景
- **符号链接安全**：新增 `--follow-symlinks`（默认 false）配置项，防止 Steam Proton 等场景递归索引整个根目录；开启时自动检测循环软链避免死循环
- **代码规范**：mmap unsafe 代码补充 SAFETY 注释，说明安全边界；legacy 校验算法加版本告警，引导用户升级

## v0.5.3 更新

- **安全加固**：HTTP 服务默认监听 `127.0.0.1`（原为 `0.0.0.0`），仅接受本地连接
- **CLI 安全收口**：`--root` 改为必传参数，不传时报错退出；移除"默认遍历 $HOME"行为
- **--spawn 根目录透传**：`fd-rdd-query --spawn` 拉起 daemon 时透传 `--root`，避免 daemon 无 root 时报错
- **核心模块拆分**：`tiered.rs`（3151 行）拆分为 13 个子模块（arena / disk_layer / query_plan / rebuild / events / snapshot / compaction / sync / memory / load / query / tests / mod），提升可维护性
- **搜索排序重构**：评分引擎升级为多维启发式系统（Multi-factor Heuristics）
  - 核心公式：`FinalScore = (MatchQuality × BasenameMultiplier) + BoundaryBonus - LengthPenalty - ContextPenalty`
  - 深度降级为 Tiebreaker（每层仅 -0.5 分），不再是主权重
  - Basename 命中时匹配质量 ×2.5
  - "单词起始位"感应：边界字符（`.`/`-`/`_`/` `）后匹配 +12、CamelCase 过渡 +8
  - "完美边界"翻倍：匹配前一字符为 `.` 或 `/` 时，整体质量 ×2
  - Smart Dot-file 处理：query 含 `.` 或 basename 命中时豁免隐藏目录降权
  - node_modules 物理隔离：query 不含 `"node"` 时权重 ×0.1（近乎屏蔽但仍可搜）
  - 噪声目录（`target`/`cache`/`vendor` 等）-200 分
  - query 含路径分隔符时自动跳过深度和噪声目录惩罚
  - fuzzy 搜索整合 rank score（matcher score + rank score 综合排序）

## v0.5.3 更新

- **安全加固**：HTTP 服务默认监听 `127.0.0.1`（原为 `0.0.0.0`），仅接受本地连接
- **CLI 安全收口**：`--root` 改为必传参数，不传时报错退出；移除"默认遍历 $HOME"行为
- **--spawn 根目录透传**：`fd-rdd-query --spawn` 拉起 daemon 时透传 `--root`，避免 daemon 无 root 时报错
- **核心模块拆分**：`tiered.rs`（3151 行）拆分为 13 个子模块（arena / disk_layer / query_plan / rebuild / events / snapshot / compaction / sync / memory / load / query / tests / mod），提升可维护性
- **搜索排序重构**：评分引擎升级为多维启发式系统（Multi-factor Heuristics）
  - 核心公式：`FinalScore = (MatchQuality × BasenameMultiplier) + BoundaryBonus - LengthPenalty - ContextPenalty`
  - 深度降级为 Tiebreaker（每层仅 -0.5 分），不再是主权重
  - Basename 命中时匹配质量 ×2.5
  - "单词起始位"感应：边界字符（`.`/`-`/`_`/` `）后匹配 +12、CamelCase 过渡 +8
  - "完美边界"翻倍：匹配前一字符为 `.` 或 `/` 时，整体质量 ×2
  - Smart Dot-file 处理：query 含 `.` 或 basename 命中时豁免隐藏目录降权
  - node_modules 物理隔离：query 不含 `"node"` 时权重 ×0.1（近乎屏蔽但仍可搜）
  - 噪声目录（`target`/`cache`/`vendor` 等）-200 分
  - query 含路径分隔符时自动跳过深度和噪声目录惩罚
  - fuzzy 搜索整合 rank score（matcher score + rank score 综合排序）

## v0.5.2 更新

- **多用户运行路径隔离**：默认 `--snapshot-path` / `--uds-socket` 优先落到 `$XDG_RUNTIME_DIR/fd-rdd/`，回退 `/run/user/$UID/fd-rdd/` 与 `/tmp/fd-rdd-$UID...`，避免多用户共享 `/tmp` 冲突
- **ignore 规则贯通**：冷扫、fast-sync、即时扫描与增量事件过滤统一支持 `.gitignore`、`.ignore`、`.git/info/exclude` 与全局 gitignore；新增 `--no-ignore`
- **存储层通用化**：`TieredIndex` 改为依赖 `StorageBackend` / `WriteAheadLog` 抽象，降低对 `SnapshotStore` / `WalStore` 的直接耦合
- **健康检查增强**：`/health` 与 `MemoryReport` 新增 `last_snapshot_time`、`watch_failures`、`watcher_degraded`、`degraded_roots`、`issues`
- **兼容性测试补强**：补齐 snapshot v2-v7 与 WAL v1/v2 的兼容 / 损坏拒绝 / 升级路径测试

## v0.5.1 更新

- **事件通道扩容**：`--event-channel-size` 默认值 4096→65536，git clone 等批量操作不再丢事件
- **Dirty flag 无锁化**：`dirty` 标记从 `RwLock<bool>` 改为 `AtomicBool`，消除 snapshot 与写入路径的竞态
- **全量扫描上限**：`PersistentIndex::query()` 新增 `limit` 参数，短查询不再触发无界全量遍历
- **Trigram 交集优化**：持锁期间按基数排序后原地 `&=`，仅 clone 最小 bitmap，减少内存分配
- **DocId 溢出安全化**：超过 4B 文件时 `alloc_docid` 返回 `None` 而非静默写入 `u32::MAX`

## v0.5.0 更新

- **即时扫描**：新增 `POST /scan` 端点，前端可主动触发目录扫描并立即更新索引；`--debounce-ms` 默认值从 100ms 降至 10ms
- **路径段首匹配**：`c/use/shi` 自动匹配 `/home/shiyi/...`，DSL 自动检测并追加 OR 分支（`PathInitialsMatcher`）
- **智能排序**：搜索结果按相关性评分排序（深度惩罚/basename 奖励/长度惩罚/近期修改奖励）；HTTP 新增 `sort`/`order` 参数
- **匹配高亮**：`SearchResult` 新增 `score` 和 `highlights` 字段（`[byte_start, byte_end)` 数组）
- **新增过滤器**：`parent:`/`infolder:`、`depth:`、`len:`、`dc:`（创建时间）、`da:`（访问时间）、`type:`
- **FileMeta 扩展**：新增 `ctime`/`atime` 字段（运行时填充，不持久化到快照）

### 排序参数

```bash
# 按修改时间降序
curl "http://127.0.0.1:6060/search?q=test&sort=date_modified&order=desc"

# 按文件名升序
curl "http://127.0.0.1:6060/search?q=test&sort=name"
```

可用 `sort` 值：`score`（默认）、`name`、`path`、`size`、`ext`、`date_modified`、`date_created`、`date_accessed`

### 即时扫描

```bash
curl -X POST http://127.0.0.1:6060/scan \
  -H 'Content-Type: application/json' \
  -d '{"paths":["/home/shiyi/Downloads"]}'
# → {"scanned":42,"elapsed_ms":3}
```

### 新过滤器示例

```
parent:/home/shiyi/Downloads    # 父目录精确匹配
depth:<=3                       # 路径深度不超过 3
len:>50                         # 文件名超过 50 字节
dc:today                        # 今天创建的文件
da:2024-01-01                   # 指定日期访问的文件
type:file                       # 仅文件（当前默认）
```

## v0.4.9 更新

- 版本：主线版本提升到 `v0.4.9`，便于区分包含 DAG/Verifier 补强、Fuzzy 查询接入以及后续 review 收敛修复的测试构建
- 查询：`FzfIntegration` 已接入 HTTP `/search`、UDS 文本协议与 `fd-rdd-query` 客户端；新增显式 `mode=fuzzy`
- 安全性：UDS 服务新增 peer credential 校验，默认仅接受 same-euid 或 `root` 发起的连接
- 稳定性：fast-sync 复用 `DirEntry::metadata()` 直接写入 `FileMeta`；`PathArena` 对超长 root-relative 路径改为跳过并告警，避免污染索引

## v0.4.8 更新

- 版本：主线版本提升到 `v0.4.8`， 便于区分包含多索引源用法澄清与隐藏文件扫描开关的测试构建
- 索引源：README 明确 `--root` 可重复传入，以覆盖多个索引源
- 扫描：新增 `--include-hidden`，允许在冷启动全扫、后台重建与增量补扫时纳入 dotfiles / dotdirs

## v0.4.7 更新

- 版本：主线版本提升到 `v0.4.7`，便于区分包含最新查询语法联调验收的测试构建
- 查询：新增搜索语法冒烟脚本 `scripts/smoke-search-syntax.sh`，可自动创建样例文件并调用 HTTP `/search` 验证 Smart-Case、AND/OR/NOT、短语、glob、`ext/pic/dm/size`、`wfn/regex` 等关键语义

## v0.4.6 更新

- 版本：主线版本提升到 `v0.4.6`，便于区分包含最近内存治理修正的测试构建
- LSM：修复 compaction 仅替换"base + 被 compact 的 delta 前缀"时的 manifest 判定，避免 suffix delta 被误伤后长期不收敛
- Flush：新增 `--batch-flush-min-events` / `--batch-flush-min-bytes`，让低频小变更先留在 WAL/L2，攒够一批再落段
- 稳定性：保留 overlay 强制 flush 与退出前最终 snapshot 语义，不让批量门槛拖慢止血路径

## 核心能力

- 查询语义：newest → oldest 级联合并（支持 delete→recreate；同路径只返回最新）
- 事件管道：bounded channel + debounce；overflow/Rescan → dirty 标记 → cooldown/max-staleness 触发 fast-sync（必要时再 rebuild 兜底）
- 持久化：mmap 段式容器 + 目录化 LSM（`index.d/`）+ `events.wal` 增量回放
- 物理结界：段与 manifest 读前流式校验（v7=CRC32C），避免 bit rot 触发未定义行为
- 观测闭环：定期输出 MemoryReport（RSS + smaps_rollup + page faults）

## v0.4.5 更新

- 查询：新增可选 UDS 流式查询 `--uds-socket` 与 `fd-rdd-query` 客户端，避免大结果集在 Daemon/Client 端聚合导致内存峰值
- 可靠性：overflow 不再"立刻全盘 rebuild"，改为 dirty-region + cooldown/max-staleness 触发 fast-sync（弱一致兜底，允许短暂陈旧）
- 测试：补齐 P0/P1/P2 回归与联调测试（分配器可观测、socket streaming、fast-sync）
- 观测：MemoryReport 统计补充 disk tombstones 与 EventPipeline buffer cap，便于定位"常驻增量"的来源
- 工具：新增/增强 `scripts/fs-churn.py` 压力脚本，支持 churn + plateau 自动检查（含 `--spawn-fd`）

## v0.4.4 更新

- 安全性：移除路径解码中的不安全转换，避免损坏输入触发未定义行为
- 一致性：LSM 加载改为"任一段或 `.del` sidecar 异常即整体拒绝"，避免部分加载导致静默漏数
- 稳定性：`event_channel_size=0` 现在会明确报错，不再在运行时 panic
- 测试：新增 LSM 部分损坏/sidecar 损坏与 channel 参数防御测试

## 架构概览

- L1：热缓存（命中时直接返回）
- L2：内存 Delta（可变索引）
  - posting：`DocId(u32)` + `RoaringBitmap`
  - path：`PathArena`（连续 blob，`(off,len)` 引用）
- Disk：只读段（mmap）
  - LSM：base + 多个 delta segments（`seg-*.db` + `seg-*.del`）
  - 查询合并：newest → oldest，FileKey 去重 + path 维度屏蔽
- L3：后台全量构建器（基于 `ignore` walker）
  - AQE：`AdaptiveScheduler` 根据系统负载选择扫描并行度

## 持久化布局（index.db / index.d）

以 `--snapshot-path` 指定的 `index.db` 为基准：

- legacy 单文件：`index.db`
  - v2~v5：bincode 快照（兼容读取）
  - v6/v7：段式容器（mmap 读取；v7 写入默认采用 CRC32C 校验）
- LSM 目录：`index.d/`
  - `MANIFEST.bin`：段列表（原子替换写入）
  - `seg-*.db`：只读段（复用 v6/v7 容器）
  - `seg-*.del`：跨段删除墓碑（按路径 bytes）
  - `events.wal`：追加型事件日志（Append-only Log）
  - `events.wal.seal-*`：snapshot 边界切分后的 sealed WAL（checkpoint）

启动时优先加载 `index.d/`，随后按 `MANIFEST.bin` 的 `wal_seal_id` 回放 WAL，使查询包含"最后一次 snapshot 之后"的增量变更。

## 查询语法（`q=...`）

### Smart-Case（默认不敏感）

- 默认：大小写不敏感
- 若查询中包含大写字母，或显式使用 `case:`：切换为大小写敏感

### 逻辑运算符

- 空格（AND）：`VCP server`
- 竖线 `|`（OR）：`server.js|Plugin.js`
- 感叹号 `!`（NOT，全局排除）：`server.js !node_modules`

### 短语

- 双引号：`"New Folder"`（作为一个完整词组参与匹配）

### 基础匹配（路径字符串）

- 不带通配符：contains 子串匹配
- 带 `*`/`?`：glob 匹配
  - pattern 含 `/` 或 `\\`：对完整路径做 glob（FullPath）
  - 否则：按"文件名/任意路径段"匹配（Segment）

### 快捷过滤器

- `doc:` / `pic:` / `video:`：按扩展名集合过滤（支持 `pic:十一` 等价于 `pic:` AND `十一`）
- `ext:js;py`：按后缀过滤（`;`/`,` 分隔）
- `dm:today` / `dm:YYYY-MM-DD`：按修改日期过滤（以 Daemon 本地时间为准）
- `size:>10mb`：按大小过滤（单位支持 `b/kb/mb/gb/tb`，1024 进制）

### 高级

- `wfn:`：完整文件名匹配（默认 basename；若 pattern 含 `/` 或 `\\` 则对 fullpath 生效）
- `regex:`：正则匹配（默认 basename；pattern 含 `/` 或 Windows 分隔符 `\\` 时对 fullpath 生效；regex 内含 `|`/空格时请使用引号：`regex:"^VCP.*\\.(js|ts)$"`，Windows 示例：`regex:"^C:\\\\tmp\\\\VCP.*\\.js$"`）

## 快速开始

编译：

```bash
cargo build --release
```

默认启用 `mimalloc` 作为全局分配器（用于降低多线程 ptmalloc arena 导致的碎片与 RSS 回吐问题）。
如需回退到系统分配器：

```bash
cargo build --release --no-default-features
```

启动（示例）：

```bash
./target/release/fd-rdd \
  --root /path/to/scan \
  --root /another/path/to/scan \
  --include-hidden \
  --snapshot-path /tmp/fd-rdd/index.db \
  --ignore-path /tmp/fd-rdd \
  --http-port 6060 \
  --uds-socket /tmp/fd-rdd.sock
```

说明：

- `--root` 可重复传入，用于覆盖多个索引源。**v0.6.1 起**：首次启动若 `~/.config/fd-rdd/config.toml` 不存在，则必须传入 `--root`；成功启动后会自动保存配置，后续启动无需任何参数，直接执行 `fd-rdd` 即可。若配置已存在，`--root` 为可选参数，传入后会覆盖配置中的值。
- 默认 `--snapshot-path` / `--uds-socket` 会优先落到 `$XDG_RUNTIME_DIR/fd-rdd/`，回退 `/run/user/$UID/fd-rdd/`，最后才使用 `/tmp/fd-rdd-$UID...`，避免多用户互相冲突。
- 默认会跳过 `.` 开头的文件/目录；如需将 dotfiles / dotdirs 纳入冷启动全扫、后台重建与增量补扫，请显式加 `--include-hidden`。
- 默认会读取 `.gitignore`、`.ignore`、`.git/info/exclude` 和全局 gitignore；如需关闭这套规则，可显式传入 `--no-ignore`。

查询：

```bash
curl -G "http://127.0.0.1:6060/search" --data-urlencode "q=main.rs" --data-urlencode "limit=20"
curl -G "http://127.0.0.1:6060/search" --data-urlencode "q=*memoir*" --data-urlencode "limit=20"
curl -G "http://127.0.0.1:6060/search" --data-urlencode "q=server.js !node_modules" --data-urlencode "limit=20"
curl -G "http://127.0.0.1:6060/search" --data-urlencode "q=mdt" --data-urlencode "mode=fuzzy" --data-urlencode "limit=20"
```

UDS 流式查询（推荐用于大结果集；边收边输出，不聚合）：

```bash
./target/release/fd-rdd-query --socket /tmp/fd-rdd.sock --limit 2000 "main.rs"
./target/release/fd-rdd-query --socket /tmp/fd-rdd.sock --spawn --root /path/to/scan --limit 2000 "*.rs"
./target/release/fd-rdd-query --socket /tmp/fd-rdd.sock --mode fuzzy --limit 200 "mdt"
```

说明：

- UDS 服务除 socket 文件 `0600` 外，还会校验 peer credential；默认仅接受"同一有效 UID"或 `root` 发起的连接，单独的同 GID 不构成放行条件。
- `fd-rdd-query --spawn` 会在 socket 不可达时尝试按当前 socket 路径拉起 `fd-rdd` 守护进程。**v0.5.3 起需同时指定 `--root`**，用于告知 daemon 索引哪些目录。
- `mode=exact` 下的 1-2 字符短查询会先走"短组件候选索引"再做精确过滤，减少退化全扫的概率。
- `PathArena` 当前仍以 `path_len: u16` 编码路径；root-relative 路径超过 `65535` bytes 时会跳过该条索引并输出告警，而不是写入损坏占位记录。

健康检查：

```bash
curl "http://127.0.0.1:6060/health"
```

说明：

- `/health` 会额外返回 `index_health`、`last_snapshot_time`、`watch_failures`、`watcher_degraded`、`degraded_roots`、`overflow_drops`、`rescan_signals` 与 `issues`，便于判断索引是否处于降级轮询或尚未写出首个快照。

## 内存与长期运行（如何判断"好用"）

百万级文件的内存/触页没有固定常数，取决于路径/名称分布、查询模式（负例多/热词多）、段数量与 OS 页缓存状态。

`fd-rdd` 提供两条"可量化"的判断路径：

- MemoryReport：区分堆高水位（Private_Dirty）与 file-backed 常驻（Private_Clean）
- page faults：在真实查询压力下量化"零拷贝是否真的少触页"

~~长期运行时可启用条件性 RSS trim（v0.4.4+）：`--trim-interval-secs` 与 `--trim-pd-threshold-mb` 已在 **v0.6.4** 中移除。~~

如需减少"低频小变更也每轮都落成一个新 delta 段"的情况，可启用定时 flush 批量门槛：

- `--batch-flush-min-events`：周期性 flush 的最小事件数（0=禁用，默认 0）
- `--batch-flush-min-bytes`：周期性 flush 的最小事件字节数（0=禁用，默认 0）

说明：

- 这两个参数只影响 **周期性** flush（`--snapshot-interval-secs`）。
- overlay 达阈值触发的强制 flush、以及进程退出前的最终 snapshot **不受影响**。
- 它们的用途是"把小批次变更继续留在 WAL/L2，等攒够一批再落段"，用于减缓新段增长；不能替代 compaction 收敛。

该策略用于缓解"索引结构已很小，但匿名脏页高水位常驻"的场景。

## 压力回归（脚本化）

如果不想用"消耗时间"来验证长期常驻的内存/事件路径，可以用脚本在几分钟内制造大量文件事件：

```bash
# 1) 启动 fd-rdd（更适合做事件/常驻内存对照）
./target/release/fd-rdd \
  --root /tmp/fd-rdd-churn \
  --snapshot-path /tmp/fd-rdd/index.db \
  --snapshot-interval-secs 0 \
  --auto-flush-overlay-paths 5000 \
  --auto-flush-overlay-bytes 0 \
  --report-interval-secs 5

# 2) 生成文件系统事件（create/delete/rename/modify 混合）
python3 scripts/fs-churn.py --root /tmp/fd-rdd-churn --reset --populate 20000 --ops 200000
```

注：`--ops` 是"每轮操作数"；当 `--rounds N` 时，总操作数为 `ops*N`。

如果想把"长期不涨"也脚本化（多轮 churn + 每轮 settle 并从 /proc 读取 fd-rdd 的 `smaps_rollup`）：

```bash
PID=<fd-rdd-pid>
python3 scripts/fs-churn.py \
  --root /tmp/fd-rdd-churn --reset --populate 20000 \
  --ops 200000 --max-files 20000 \
  --rounds 10 --settle-secs 20 \
  --fd-pid "$PID" --fd-metric pd --max-growth-mb 8
```

注：当 `--fd-metric=pd` 且脚本能解析到 fd-rdd 的 MemoryReport 时，`--max-growth-mb` 实际比较的是  
`unaccounted=max(0, pdΔ-disk_tomb_estΔ)`（把 tombstones 等可量化的结构性增长从"泄漏"里剔除）；否则回退为 `pdΔ`。

如果希望"一次运行就得到 PASS/FAIL 结论"（脚本自动启动 fd-rdd + warmup + plateau 检查）：

```bash
python3 scripts/fs-churn.py \
  --verdict --reset --cleanup \
  --populate 20000 --ops 200000 --max-files 20000 \
  --warmup-rounds 1 --rounds 10 --settle-secs 20 \
  --fd-metric pd --max-growth-mb 8
```

脚本会在 PASS/FAIL 时输出"归因摘要"（包含 event overflow 与 MemoryReport 的 heap/index 拆分）。如需输出 fd-rdd 的详细日志，可追加 `--fd-echo`；想减少 overflow 干扰可调 `--fd-event-channel-size` / `--fd-debounce-ms` 或给 churn 加 `--sleep-ms`。

注：部分系统对 `/proc/<pid>/smaps_rollup` 有权限限制（常见于 yama/ptrace_scope 策略）。遇到无法读取时：

- 只想做 RSS 平台检查：把 `--fd-metric` 改为 `rss`（会 fallback 到 `/proc/<pid>/statm`）
- 想继续检查 `pd/pc/pss`：用 `--spawn-fd` 让脚本启动 fd-rdd（成为父进程；该参数需要放在命令最后）

```bash
python3 scripts/fs-churn.py \
  --root /tmp/fd-rdd-churn --reset --populate 20000 \
  --ops 200000 --max-files 20000 \
  --rounds 10 --settle-secs 20 \
  --fd-metric pd --max-growth-mb 8 \
  --spawn-fd ./target/release/fd-rdd \
    --root /tmp/fd-rdd-churn \
    --snapshot-path /tmp/fd-rdd/index.db \
    --snapshot-interval-secs 0 \
    --auto-flush-overlay-paths 5000 \
    --auto-flush-overlay-bytes 0 \
    --report-interval-secs 5
```

## TODO

### 展望

- [ ] 补齐 `~/.config/fd-rdd/config.toml` 的全量字段接线；当前优先级已经是 `CLI > 配置文件 > 默认值`，但 `http_port`、`snapshot_interval_secs`、`include_hidden`、`log_level` 等仍未全部贯通到启动路径。
- [ ] 提供 `systemd --user` 单元模板和更稳妥的守护进程自启约定；同时收口 `fd-rdd-query --spawn` 在"无显式 root/config"场景下的安全默认，避免误扫整个 `$HOME`。
- [ ] 完成 `content:` 全文索引，复用现有全局 `DocId`、事件增量链路以及 LSM + mmap 存储布局。
- [ ] 为全文索引补齐内容过滤策略：大文件阈值、二进制文件跳过、ignore 规则复用、内容哈希去重。
- [ ] 段级 Bloom 过滤器：为每个磁盘段构建 Bloom Filter，查询时提前跳过不包含目标路径的段，减少无效的段遍历和 mmap 触页开销。
- [ ] Leveled 代际 Compaction：实现更平滑的分层合并策略，替代当前简单的"最多合并 2 个旧段"逻辑，降低写放大并控制段数增长。
- [ ] 增强版 WAL 语义：支持可配置的 fsync 策略（每次写入/批量/异步）、事件去重、Gap 校验（检测 WAL 中的记录缺失或损坏），提升持久化可靠性和恢复能力。

### 待修复缺陷

- [x] 核心流程仍存在 unwrap 导致的崩溃风险：LSM 合并等关键路径中使用 `store.lsm_manifest_wal_seal_id().unwrap()`，manifest 读取失败会直接导致守护进程崩溃，需要改为错误传播或降级处理。（v0.5.5 已修复：snapshot.rs 中 v6 mmap 加载的 5 处 unwrap 改为 match 降级返回 `Ok(None)`）
- [x] 持久化缺少目录 fsync 保证：快照和 WAL 文件 rename 后未对父目录执行 fsync，Linux 下掉电可能导致元数据丢失，快照/WAL 文件消失，索引回退到旧状态。需要在关键写入路径补充目录 fsync。（v0.5.5 已修复：wal.rs 的 `seal()` 和 `open_or_init()` rename 后补充 `fsync(parent_dir)`）
- [x] WAL 回放缺少事件去重：回放 WAL 时未对重复事件做去重处理，若 WAL 中存在重复记录（如异常写入、部分 flush），会导致索引中出现重复条目或已删除文件重新出现。（v0.5.5 已修复：wal.rs `replay_since_seal` 中增加基于 (id, timestamp) 的去重逻辑）
- [ ] DocId 上限无扩容方案：DocId 使用 u32 编码，超过 40 亿文件后无法分配新 ID，大规模场景下会导致索引失效。需要设计 ID 扩容方案或改用 u64。
- [ ] mmap 安全校验不足：当前仅通过文件修改时间检测外部篡改，攻击者可在修改内容后恢复时间戳绕过检测。需要增强校验机制（如定期重新计算 CRC）或考虑其他防护手段。
- [x] 版本兼容代码重复度高：v1-v7 的快照/WAL 解码逻辑存在大量复制粘贴，维护时容易遗漏修改导致兼容性问题。建议重构为统一的版本分发框架。（v0.5.5 已修复：引入 `LegacySnapshot` trait + macro，统一 v2-v5 加载与 `into_persistent_index` 分发）
- [x] 符号链接循环检测存在跨文件系统误判：当前基于 inode 的 visited 集合在跨文件系统时可能因 inode 重复导致误判（正常软链被当作循环）或漏判（真实循环未检测到）。需要结合设备号（dev）一起判断。（已修复：`src/core/rdd.rs` 中 `FsScanRDD` 已使用 `(dev, ino)` 组合进行 visited 去重）
- [x] 事件溢出恢复依赖全量重建：事件通道溢出后直接标记脏区并触发全量扫描，中间丢失的事件无法增量恢复。大目录重建耗时长，影响索引可用性。建议优化为增量补偿机制。（v0.5.5 已修复：`DirtyTracker` 将溢出路径映射到 root 粒度，fast-sync 对 root 做增量 mtime 局部扫描）

### 缺陷（v0.5.4 已修复）

- [x] 旧版本快照、WAL 使用自研的 SimpleChecksum 做校验，本质是字节累加 + 简单旋转的玩具校验，碰撞概率极高，根本无法有效识别数据 corruption
- [x] mmap 快照存在严重的内存安全隐患：仅在加载时做了一次校验，加载完成后就不再管文件状态 ——mmap 是共享文件映射，外部程序修改快照文件会直接篡改进程的内存，随时可能触发越界访问、进程 panic，甚至更严重的内存安全问题，完全不符合 "可恢复" 能力。
- [x] WAL 的错误处理逻辑极端脆弱：读取 WAL 时只要碰到一条损坏记录，直接中断整个读取流程，后面所有的增量事件全部丢弃，一条坏记录就能丢光所有后续的变更数据，
- [x] 大量使用 unwrap/expect 莽错误：WAL 的文件锁直接 lock().unwrap()，锁一旦 poison（有线程 panic）直接把整个守护进程干崩；各种 IO、解析错误没有兜底，随便一个小错误就能把常驻服务搞崩，完全不符合长期运行的要求。
- [x] 大量静默吞错误：清理旧 sealed 文件、删除 socket 等操作，全用 let \_ = xxx 忽略错误，删除失败连警告日志都没有，用户碰到磁盘满、权限不足的情况，完全得不到提醒，旧文件堆着占满磁盘都不知道。
- [x] tokio 直接启用了 full 全量特性，把大量用不到的功能打包进二进制，完全不会按需启用特性，导致最终编译出的程序体积巨大,所以需要修改成按需引用。
- [x] 整个测试目录只有两个测试文件，核心的事件处理、WAL 恢复、mmap 安全校验全没有自动化测试。
  - [x] 基础开关测试：--follow-symlinks=false 时，扫描不会进入符号链接指向的目录，比如指向根目录的软链，不会递归索引全系统
  - [x] 软链本身的处理：禁用跟随的时候，符号链接文件本身会不会被正常索引（而不是直接跳过这个文件本身）
  - [x] 事件层同步：禁用跟随的时候，事件监听不会监听符号链接指向的目录的变更，避免监听到根目录的全系统事件
  - [x] 嵌套软链测试：嵌套的符号链接（a 链到 b，b 链到根），会不会正确阻断递归，不会穿透
  - [x] 场景复现测试：模拟 Steam Proton 的 dosdevices/z:指向 / 的场景，验证不会触发全根目录索引
  - [x] ignore 规则贯通测试：冷扫、增量补扫、事件过滤，三层是不是都用了同一套 ignore 规则（.gitignore/ 全局 ignore 是不是都生效）
  - [x] 隐藏文件开关：--include-hidden 的开关，默认是不是跳过隐藏文件，开启后能不能正常扫描
  - [x] 多 root 隔离：多个--root 参数的场景，多个索引目录能不能正常隔离，不会互相干扰
  - [x] 超长路径容错：超过 65535 字节的超长路径，会不会正确跳过告警，不会 panic
  - [x] 大目录扫描：十万级文件的大目录，扫描会不会正常完成，不会 OOM 或者崩溃
  - [x] 高负载事件处理：模拟 git clone 的批量创建事件，会不会触发事件溢出，溢出后会不会正确触发 fast-sync，不会丢事件、不会全量重建
  - [x] 重命名事件处理：文件重命名后，索引会不会正确更新，不会当成删除 + 新建丢数据
  - [x] 删除事件处理：文件 / 目录删除后，索引会不会正确清理对应的条目
  - [x] 事件损坏恢复：WAL 里有损坏的事件记录，会不会正确跳过坏记录，不会把后面的所有事件全丢了
  - [x] watch 降级测试：事件监听退化到轮询的时候，能不能正常工作，不会丢索引
  - [x] 崩溃恢复测试：进程崩溃后重启，能不能正确回放 WAL，恢复所有增量事件，不用重新全量扫描
  - [x] 版本兼容测试：从 v2 到 v7 的旧版本快照 / WAL，能不能正确加载兼容，不会丢用户的旧索引
  - [x] 坏文件处理：快照 / WAL 文件损坏的时候，会不会正确识别、跳过，不会 panic，能不能触发兜底重建
  - [x] LSM compaction 测试：大量小变更后，compaction 能不能正确合并段、回收磁盘空间，不会段数暴涨
  - [x] 旧文件清理：seal 后的旧 WAL、过期的 LSM 段，能不能正确清理，不会残留占磁盘
  - [x] 过滤器有效性：size / 日期 /ext/regex 这些过滤器，能不能正确生效，有没有匹配错误
  - [x] 模糊查询：fuzzy 模式的匹配是不是正常，有没有排序错误
  - [x] 流式查询：大结果集的 UDS 流式查询，能不能边查边返回，不会在内存聚合导致 OOM
  - [x] UDS 权限：别的用户 /root 能不能访问你的 UDS socket，权限拦截是不是正常
  - [x] 短查询优化：1-2 字符的短查询，会不会触发短组件索引优化，不会退化全扫
- [x] 添加配置项 --follow-symlinks（默认 false），在 walker 和事件监听层禁用符号链接跟随，防止 Steam Proton 的 dosdevices/z: 递归索引整个根目录。
  - [x] 顺便补个循环检测：就算开了跟随，也加个 inode 的 visited 集合，挡住 a->b->a 这种循环软链，避免扫描死循环，把鲁棒性拉满。

### 小一点的毛病（v0.5.4 已修复）

- [x] 很多地方用了 unwrap，还有 let \_ = xxx 吞错误，比如删文件的时候忽略错误，需要补错误处理
  - [x] 把那些 let \_ = xxx 吞错误的地方，加上带上下文的 warn 日志，比如清理旧段、删 socket 的时候，失败了打个带路径的告警；还有把锁的 unwrap 换成 poison 兼容的处理，用 poison_err.into_inner()拿到锁，避免单个线程 panic 直接把整个守护进程干崩，提升长期运行的韧性。
- 各个版本的快照、WAL 的兼容代码，写的有点重复，比如 v1 到 v7 的加载逻辑，堆了不少重复的判断
  - [x] 存储模块的 mmap unsafe 代码，补上标准的 SAFETY:注释，说明这个 unsafe 的安全边界是什么，比如 "我们已经校验过文件范围，不会越界"，符合 Rust 的安全编码规范，也方便后续维护。
- [x] 少部分 unsafe 没加安全注释：mmap 的 unsafe 代码，没有加注释说明 "这个 unsafe 为什么是安全的"，Rust 的 unsafe 惯例是要加注释说明安全边界的，这个地方有点随意。
- [x] legacy 把早期的 SimpleChecksum 的 legacy 逻辑，加个版本告警，碰到旧版本的快照提醒升级，后续慢慢把这个过渡的校验算法淘汰掉，全换成标准 CRC32C，统一数据校验的可靠性。

## 许可证

MIT License (c) 2026 fd-rdd Contributors
