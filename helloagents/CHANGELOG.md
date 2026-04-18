# CHANGELOG（HelloAGENTS）

本文件记录面向”方案包/知识库”的变更轨迹（以可读性为主，不替代 Git log）。

## Unreleased

- **配置治理（P1-3）—— CLI 与 config.toml 全量接线**：`src/main.rs` 将 `http_port`、`snapshot_interval_secs`、`include_hidden`、`follow_symlinks`、`log_level` 改为 `Option<T>`，启动时按 `CLI > config.toml > 默认值` 合并；`tracing-subscriber` 启用 `env-filter` 特性以支持动态日志级别；修复 `args.http_port` 在就绪日志中未使用合并值的问题
- **follow_symlinks 贯通（P1-2）**：`TieredIndex::empty_with_options` / `load_with_options` / `load_or_empty_with_options` 新增 `follow_symlinks` 参数；`IndexBuilder` 新增 `follow_symlinks` 字段并透传至 `FsScanRDD::with_follow_links`；`sync.rs` 的 `fast_sync` 与 `scan_dirs_immediate` 将 `ignore::WalkBuilder::follow_links(false)` 硬编码改为 `self.follow_symlinks`，实现配置值在冷扫、增量补扫、即时扫描三层全贯通
- **平台清理 —— 移除 Windows 支持**：`src/config.rs` 删除 Windows socket 路径 (`\\.\pipe\fd-rdd-{username}`) 与 snapshot 路径 (`dirs::data_local_dir()` on Windows) 的条件编译块及文档；`src/core/rdd.rs` 删除 Windows / fallback 分支，保留 Unix 逻辑；`src/stats/mod.rs` 三个函数加 `#[cfg(target_os = "linux")]`；CI/release 工作流声明 Linux-only 并标记 macOS 为实验性
- **缺陷修复（B3）—— WAL append 后 fsync**：`wal.rs` 的 `append_record` 在写入后调用 `sync_data()`，防止掉电丢失未落盘事件
- **缺陷修复（B4）—— WAL CRC mismatch 中断回放**：`wal.rs` 的 `replay_since_seal` 中遇到 CRC 校验失败记录时由 `continue` 改为 `break`，避免后续有效事件被错误跳过
- **缺陷修复（B6）—— socket OOM / 慢 loris 防护**：`query/socket.rs` 将 `read_to_end` 改为 `take(max_request_bytes + 1)` 先限长再读取，增加 2 秒读超时，消除恶意客户端通过超大请求或慢速发送耗尽内存的风险
- **缺陷修复（B8）—— HTTP 查询协作式取消**：`query/server.rs` 的 `spawn_blocking` 查询内每处理 256 条候选检查一次 `Arc<AtomicBool>` 取消标志；timeout 后设置标志位，任务自行提前返回，避免线程池饿死
- **缺陷修复（B11）—— lsm_append_delta_v6 并发竞争**：`storage/snapshot.rs` 新增 `compaction_lock: tokio::sync::Mutex<()>`，`lsm_append_delta_v6()` 与 `lsm_replace_base_v6()` 开头均获取该锁，串行化 delta 追加与 base 替换，防止同 id segment 文件覆盖
- **缺陷修复（B17）—— /scan 端点路径数量限制**：`query/server.rs` 的 `POST /scan` 由 `paths.iter().take(10)` 静默截断改为显式校验，超过 10 个路径时返回 `400 Bad Request`
- **缺陷修复（B18）—— LSM manifest delta_ids 溢出**：`storage/lsm.rs` 中 `unwrap_or(u32::MAX)` 改为显式 `bail!`，拒绝非法 delta_ids 数量而非静默截断
- **缺陷修复（B20）—— ignore_filter 目录状态推断**：`event/ignore_filter.rs` 的 `is_ignored` 新增 `is_dir: bool` 参数，`event/stream.rs` 通过 `notify::EventKind` 推断目录/文件状态并传入，消除每事件 `path.is_dir()` syscall
- **缺陷修复（B22）—— WAL len try_into 安全化**：`wal.rs` 中将 `len.try_into().unwrap_or(u32::MAX)` 改为 `bail!`，拒绝非法长度而非静默写入 `u32::MAX`
- **体验修复（2026-04-17）—— 中文路径边界加分**：`src/query/scoring.rs` 中 `is_boundary_char` 原仅检查 ASCII 边界字符，中文字节无法获得边界加分；改为以 `char` 为单位判断，增加 `!c.is_alphanumeric()`，使非字母数字字符（含中文）均被视为边界；`compute_position_bonuses` 改用 `char_indices()` 迭代，确保多字节字符首字节正确得分
- **体验修复（2026-04-17）—— 中文短查询优化失效**：`src/index/l2_partition.rs` 中 `normalize_short_hint` 按字节长度判断 1-2 字符，单个中文字符 = 3 字节导致短组件索引优化被跳过；改为按 `chars().count()` 判断，`trigram_matches_short_hint` 改为通用 `windows()` 匹配
- **体验修复（2026-04-17）—— 全角空格未识别为分隔符**：`src/query/dsl_parser.rs` 中 `tokenize` 仅使用 `is_ascii_whitespace()`，中文全角空格 `U+3000`（`E3 80 80`）被忽略导致查询词合并；新增 `is_token_separator()` 统一检测 ASCII 空白与全角空格
- **代码质量治理（2026-04-17）—— 模块拆分与 DRY**：`snapshot.rs`（1867 行）拆分为 `snapshot_v6.rs` / `snapshot_legacy.rs` / `lsm.rs`，`dsl.rs`（1262 行）拆分为 `filter.rs` / `dsl_parser.rs`，主文件保留公共 API 并通过 `pub use` 聚合；`EventPipeline` 三个构造函数统一委托给 `new_with_config_and_ignores`，消除 14 行 `Arc::new(AtomicXxx::new(0))` 的复制粘贴
- **代码质量治理（2026-04-17）—— 死代码清理**：删除无调用方的 `DAGScheduler`（`src/core/dag.rs` 277 行）；移除 `Filter` 枚举未使用的 variant 及 `#[allow(dead_code)]`；删除不可达的 `qi > query_segments.len()` 分支；清理仅有一行重导出的 `core/partition.rs`
- **代码质量治理（2026-04-17）—— 跨平台与可观测性**：`local_date_range` 在非 Unix 平台 fallback 到 `std::fs::metadata().modified()`，Windows 用户可使用 `dm:` 日期过滤；`MemoryReport::read_process_rss()` 改为 `libc::sysconf(_SC_PAGESIZE)` 动态获取页大小，失败时回退 4096；`Content(String)` filter 编译阶段通过 `eprintln!` 告警未实现，避免静默返回 false
- **代码质量治理（2026-04-17）—— 测试工具统一**：新建 `src/test_util.rs` 提供共享 `unique_tmp_dir`，替换 `rdd.rs` / `verify.rs` / `ignore_filter.rs` / `wal.rs` / `snapshot.rs` 中 5 处重复定义；`JUNK_DIR_NAMES` 去重 `.tox`；`encode_roots_segment` 补充 `/` 插入设计注释

- **竞态修复（2026-04-18）—— PersistentIndex upsert 锁顺序**：`alloc_docid` 与 `insert_trigrams` 原分属独立锁序列，存在可见性窗口导致 query 线程看到不一致状态（metas 已更新但 trigram_index 未更新）而漏查新文件；引入 `upsert_lock` 写锁将完整 upsert 流程串行化，以并发写入性能为代价换取查询一致性
- **并发重构（2026-04-18）—— Shadow Delta + 全量锁卫士**：将 upsert 路径拆分为 Phase 1 无锁纯计算（提取 trigram/short component/path hash）与 Phase 2 全量锁卫士（按查询顺序同时持有全部写锁并统一释放），彻底消除死锁与越界 Panic 陷阱；锁持有时间从毫秒级压缩到微秒级，并发写入性能显著回升
- **路径治理（2026-04-18）—— Linux 默认 snapshot 路径持久化**：`default_snapshot_path()` 的 Linux fallback 从 `/tmp/fd-rdd-$UID` (tmpfs) 改为 `dirs::data_local_dir()` (`~/.local/share/fd-rdd/`)，避免 LSM mmap segment 被计入 RSS 导致内存虚高；同步更新 systemd 服务文件与 README 中 config.toml 使用说明
- **历史治理（2026-04-18）—— 清理 [Snow Team] 自动提交**：使用 `git filter-branch` 重写所有分支历史，移除 `[Snow Team]` 前缀；删除遗留 `snow-team/*` 分支及 `refs/original` 备份，统一提交信息风格

- **缺陷修复（2026-04-16）—— 存储层健壮性**：修复 snapshot v6 mmap 加载路径中 5 处 `unwrap()` 导致的守护进程崩溃风险（缺失 segment 时降级返回 `Ok(None)` 并记录 warn）；在 WAL `seal()` 与 `open_or_init()` 的 `rename` 后补充目录 `fsync`，防止掉电丢失元数据
- **缺陷修复（2026-04-16）—— WAL 与事件恢复**：WAL 回放增加基于 `(id, timestamp)` 的重复事件去重，避免异常写入导致索引重复或已删文件复活；事件通道溢出后不再直接触发全量重建，而是将脏路径映射到所属 `root` 粒度，`fast-sync` 仅对这些 root 做增量 mtime 局部扫描
- **缺陷修复（2026-04-16）—— 版本兼容与符号链接**：引入 `LegacySnapshot` trait + macro，统一 v2-v5 快照加载与 `into_persistent_index` 分发，消除大量复制粘贴；确认 `src/core/rdd.rs` 中 `FsScanRDD` 已使用 `(dev, ino)` 组合进行 symlink 循环去重，修复跨文件系统 inode 重复导致的误判/漏判

- **编年史更新（2026-04-12）**：更新仓库根目录 `fd-rdd-编年史.md` 截止至 2026-04-12，并补齐 2026-02-17 至 2026-04-12 的关键里程碑（v0.4.0 语义锚定、Query DSL、内存治理与验收脚本、fuzzy/排序重构、安全收口与模块拆分）
- **安全加固（2026-04-12）**：HTTP 服务默认监听 `127.0.0.1` 而非 `0.0.0.0`，防止外部网络直接访问索引服务
- **CLI 安全收口（2026-04-12）**：`--root` 改为必传参数，不传时明确报错退出；移除"默认遍历 $HOME"行为，避免误扫整个用户目录
- **--spawn 传根目录（2026-04-12）**：`fd-rdd-query --spawn` 拉起 daemon 时会将 `--root` 透传给 `fd-rdd`，不再导致 daemon 无 root 报错或误扫 $HOME
- **核心模块拆分（2026-04-12）**：将 3151 行的 `tiered.rs` 单文件拆分为 13 个子模块（arena / disk_layer / query_plan / rebuild / events / snapshot / compaction / sync / memory / load / query / tests / mod），职责分离，便于后续迭代
- **搜索排序重构（2026-04-12）**：评分引擎升级为多维启发式评分系统（Multi-factor Heuristics）——核心公式 `FinalScore = (MatchQuality × BasenameMultiplier) + BoundaryBonus - LengthPenalty - ContextPenalty`；深度从主权重降级为 Tiebreaker（每层仅 -0.5）；basename 命中时匹配质量 ×2.5；新增"单词起始位"感应（边界字符 `.`/`-`/`_` 后 +12、CamelCase 过渡 +8）；"完美边界"翻倍（匹配前一字符为 `.` 或 `/` 时质量 ×2）；Smart Dot-file 处理（query 含 `.` 或 basename 命中时豁免隐藏目录降权）；node_modules 物理隔离（query 不含 "node" 时权重 ×0.1）；ScoreConfig 预解析 query_has_dot / query_has_node / query_basename；27 个评分测试全部通过
- **路径感知免惩罚（2026-04-12）**：当 query 包含 `/` 或 `\` 时自动识别为"用户指定了路径"，跳过深度和噪声目录惩罚
- 版本号更新：crate 版本升级至 v0.5.3

- **性能与安全修复（2026-04-05）**：v0.5.1 版本更新——event_channel_size 默认值从 4096 提升至 65536，应对 git clone 等批量操作防止事件丢失；dirty flag 从 RwLock<bool> 改为 AtomicBool（Ordering::Release/Acquire），消除 snapshot 与写入路径的竞态；PersistentIndex::query() 新增 limit 参数，短查询不再触发无界全量遍历；Trigram 交集优化为持锁期间按基数排序后原地 &=，仅 clone 最小 bitmap；DocId 溢出安全化，超过 4B 文件时 alloc_docid 返回 None 而非静默写入 u32::MAX
- **查询增强（2026-04-05）**：新增路径段首字母匹配（PathInitialsMatcher）；DSL 自动检测含 `/`/`\` 且无通配符的查询，追加 OR 分支；新增过滤器 `parent:`/`infolder:`、`depth:`、`len:`、`dc:`/`da:`、`type:`；FileMeta 扩展 `ctime`/`atime` 字段（运行时填充，不持久化）
- **排序与高亮（2026-04-05）**：`execute_query` 集成相关性评分排序（深度惩罚/basename 奖励/长度惩罚/近期修改奖励）；HTTP `/search` 新增 `sort`/`order` 参数（name/path/size/ext/date_modified/date_created/date_accessed）；`SearchResult` 新增 `score` 和 `highlights` 字段
- **即时扫描 API（2026-04-05）**：新增 `POST /scan` 端点，接受 `{“paths”:[...]}` 同步扫描指定目录（最多 10 目录 × 10000 条目）并立即更新索引；`TieredIndex::scan_dirs_immediate()` 提供底层实现
- **debounce 降低（2026-04-05）**：`--debounce-ms` 默认值从 100ms 降至 10ms，减少文件创建后的索引延迟
- 版本号更新：crate 版本升级至 v0.4.9，便于区分包含 DAG/Verifier 补强、Fuzzy 查询接入与后续 review 收敛修复的测试构建
- 查询/可靠性：UDS 查询服务增加 peer credential 认证（默认 same-euid 或 root）；fast-sync 复用 `DirEntry::metadata()` 直接写入 `FileMeta`，减少热路径重复 `metadata` 调用
- 路径索引：`PathArena` 对超长 root-relative 路径改为显式跳过并告警，避免写入空占位元数据；`compose_abs_path_*` 的薄包装继续收口到共享工具函数
- 查询：`FzfIntegration` 已接入 HTTP `/search`、UDS 文本协议与 `fd-rdd-query` 客户端；新增显式 `mode=fuzzy`，默认仍保持原有 DSL / 精确查询语义
- 可靠性：`DAGScheduler` 从空壳升级为可用的 DAG 规划器（拓扑排序、并行层规划、缺依赖/环检测）；`ElasticVerifier` 从占位实现升级为“事件序号缺口检测 + fast-sync 修复”，并接入 overflow/recovery 链路
- 版本号更新：crate 版本升级至 v0.4.8，便于区分包含多索引源说明与隐藏文件扫描开关的测试构建
- 索引源：README 明确 `--root` 可重复传入以覆盖多个索引源；新增 `--include-hidden`，允许在冷启动全扫、后台重建与增量补扫时纳入 `.` 开头的文件/目录
- 版本号更新：crate 版本升级至 v0.4.7，便于区分包含 Query DSL 通配符兼容联调验收的测试构建
- 工具：新增 Query DSL 冒烟脚本 `scripts/smoke-search-syntax.sh`，用于创建样例文件并通过 HTTP `/search` 验证关键语义
- 查询：新增 Query DSL（AND/OR/NOT/短语、`doc/pic/video`、`ext/dm/size`、`wfn/regex` + Smart-Case），HTTP/UDS 查询统一支持；并在元数据过滤下避免旧段同路径旧元数据“误命中”回流
- 版本号更新：crate 版本升级至 v0.4.6，便于区分包含 compaction 前缀替换与批量 flush 门槛的测试构建
- 批量 flush：新增 `--batch-flush-min-events` / `--batch-flush-min-bytes`，为周期性 flush 增加最小批量门槛；低频小变更可继续保留在 WAL/L2，待攒够一批再落成 delta 段
- 规划补充：将“事件日志时间窗 + 阈值批量 flush”加入 `wiki/todo-disk-first-memory-light.md`，作为减缓新段增长的辅助手段；明确其不能替代 compaction 收敛
- LSM 修正：`lsm_replace_base_v6()` 现在按“被 compact 的前缀层”校验 manifest，并保留未参与本轮 compaction 的 suffix delta；修复 `delta > COMPACTION_MAX_DELTAS_PER_RUN` 时 compaction 长期 `manifest changed` 不收敛的问题
- 测试：新增 `compaction_prefix_replaces_base_and_keeps_suffix_deltas` 回归，锚定“base + 部分 delta compaction 后 suffix 仍保留”的语义
- 查询：新增可选 UDS 流式查询服务（`--uds-socket`）与 `fd-rdd-query` 客户端；查询端引入 `query_limit`（避免大结果集在 Daemon/Client 端聚合造成内存峰值）
- 可靠性：overflow/Rescan 不再立刻 rebuild；改为 dirty-region + cooldown/max-staleness 触发 fast-sync（目录树遍历不假设 mtime 冒泡）；EventPipeline 统计新增 `rescan_signals`，便于区分“channel overflow”与“内核 Rescan”
- 测试：补齐 P0/P1/P2 的单元/联调回归（allocator 可观测、socket streaming/limit、Rescan→dirty_all、fast-sync 对齐离线变更）
- 工具修正：`scripts/fs-churn.py` 的 auto-spawn 会检测构建产物是否过期并给出警告，避免误用旧 `target/release/fd-rdd`；并明确 `--ops` 为“每轮操作数”
- 工具：新增 `scripts/fs-churn.py` 文件事件压力脚本，用于 watcher/EventPipeline/Flush/Compaction 的快速回归（替代“长时间空跑”）；支持 `--rounds/--settle-secs/--fd-pid/--spawn-fd` 做“长期不涨”自动检查（rss 指标在权限受限时会 fallback 到 statm）
- 工具增强：`scripts/fs-churn.py` 新增 `--verdict` 一键 PASS/FAIL 输出，并支持 `--warmup-rounds` 在 baseline 前做预热（剔除“第一段台阶上升”），默认可自动启动仓库内 `fd-rdd` 做检查；并在 PASS/FAIL 时输出基于 MemoryReport 的归因摘要（overflow/idx_est/non-idx 等）
- 文档新增：`wiki/memoir-2026-03-04.md`，记录“常驻内存抬升”排查、观测增强与 plateau 压测验收的阶段结论与下一步
- 内存治理（P0）：MemoryReport 增加 Disk segments tombstones（delete/rename-from sidecar）的路径数/字节数/估算堆占用，并补充 EventPipeline 缓冲区 capacity（raw/merged/records）便于定位常驻增量来源
- 内存治理（P2 第二批）：compaction 增加冷却与 flush 优先跳过；执行前做层快照快速校验；`manifest changed` 降级为并发预期日志；compaction 尝试结束后追加一次 trim
- 内存治理（P2 第一批）：`DiskLayer.deleted_paths` 改为 `Arc<Vec<Vec<u8>>>`，查询层快照复制不再深拷贝删除 sidecar 路径；`EventPipeline` 合并缓冲改为复用，降低长稳运行分配抖动
- 内存治理（P0/P1）：MemoryReport 新增 `index_estimated_bytes`、`non_index_private_dirty_bytes`、`heap_high_water_suspected`，并在报告循环输出 RSS 趋势信号
- 内存治理（P1）：新增条件性 RSS trim 参数 `--trim-interval-secs`、`--trim-pd-threshold-mb`，按 Private_Dirty 阈值触发后台 trim
- 文档更新：README 增补条件性 RSS trim 参数说明；`wiki/todo-disk-first-memory-light.md` 状态更新为进行中
- TODO 新增：`wiki/todo-disk-first-memory-light.md`（低占用守护进程 + 磁盘主索引模式，含内存抬升观测与验收标准）
- 文档新增：`wiki/product-structure-book.md`，沉淀产品结构书（技术栈、架构图、时序、替代方案评估、风险矩阵、迭代逻辑）
- 版本号更新：crate 版本升级至 v0.4.5（与当前实现阶段对齐）
- 分配器默认切换：默认启用 `mimalloc`（可用 `--no-default-features` 回退到系统分配器）
- 初始化：补齐 `project.md`、`CHANGELOG.md`、`history/index.md` 等知识库骨架
- Step 1：引入 ArcSwap 后台重建原子切换（rebuild 期间查询不中断），并提供 `mimalloc` 可选全局分配器开关
- 阶段 A+：rebuild 冷却/合并策略；内存报告拆项；路径改为 root 相对存储并升级快照至 v5（含 roots_hash 校验）
- 阶段 B：新增 v6 段式快照（mmap + Trigram/Metadata/Path 段 + posting lazy decode），启动优先加载 v6；快照写入改为 v6（仍兼容读取 v2~v5）
- 阶段 C：目录化 LSM（`index.d/` + `MANIFEST.bin` + `seg-*.db/.del`），查询合并按 newest→oldest；Flush 将内存 Delta 追加为新段；段数阈值触发后台 Compaction 合并为新 base
- 动态 RSS 回吐：在 rebuild/full_build 完成时触发手动 trim（mimalloc: `mi_collect(true)`；glibc: `malloc_trim(0)`）
- 弹性计算（AQE）：接回 `AdaptiveScheduler`，rebuild/full_build 按系统负载选择并行度；`FsScanRDD` 支持可控并行扫描（ignore parallel walker）
- watcher 反馈回路止血：事件管道默认忽略 snapshot 路径（index.db/index.d），并支持 `--ignore-path` 手动排除日志等路径；MemoryReport 增加 overlay/pending 的“影子内存”统计；rebuild pending_events 按路径去重避免堆积
- 持久化补齐：引入 `events.wal` 追加型事件日志（seal + manifest checkpoint + 启动回放），降低 overflow/重启后的全量 rebuild 概率
- v0.4.0 阶段 1（语义锚定）：引入 `FileIdentifier(Path/Fid)` + `path_hint`，rename 升级为“双身份”语义；事件合并从“路径合并”升级为“身份合并”；WAL 协议升级至 v2（v1 文件非破坏性封存为 `events.wal.seal-*.v1`，回放端同时支持 v1/v2）；定义 `IndexLayer`（query_keys/get_meta）契约为后续 MergedView/mmap layer 铺路
- v0.4.0 阶段 2（MergedView）：v6 快照增加 FileKeyMap 段（FileKey->DocId）；PersistentIndex/MmapIndex 实装 `IndexLayer`；TieredIndex 查询改为 FileKey 先到先得去重（rename/覆盖语义）并保留 path 屏蔽集合（delete/同路径只取最新）
- 兼容性：MmapIndex 在 FileKeyMap 段缺失或为空（如旧段/模拟段）时，按需构建 fallback cache（扫描 metas + 排序）以保证 get_meta 可用
- Stage 3（Zero-copy Evolution）：FileKeyMap 段加入 magic/header（b"FKM\\0"+version+flags），支持 legacy 裸表/带头 legacy 与 rkyv bytes 双格式；rkyv 路径用 OnceLock 缓存校验状态，避免每次 get_meta 触发线性校验
- 影子内存升级：Overlay 改为 arena + hash-span（碰撞 byte-compare）减少常驻开销；MemoryReport 补全 overlay/rebuild 的容量与估算堆占用；引入 overlay 达阈值自动唤醒 snapshot_loop 的强制 Flush（`--auto-flush-overlay-*`）
- LSM 物理清理：Compaction/Replace-base 完成后，清理 manifest 未引用的旧 `seg-*.db/.del` 文件（GC stale segments），避免重启后仍残留占用资源
- 瘦身期加速：Compaction 触发阈值更激进（delta ≥ 2）；启动加载后执行 Startup Scavenger（best-effort `gc_stale_segments()` + 触发一次 compaction 检查）；GC 解析支持 `seg-*.db/.del` 及其 `.tmp` 变体
- Stage 4（终极闭环）测试锚定：新增 TieredIndex 查询链路的级联去重回归测试（同路径替换写/rename-from tombstone/同 FileKey 多段幽灵），钉死 newest→oldest 覆盖语义
- v0.4.0 战役五（trigram 预过滤）：trigram 索引覆盖所有路径组件并写入哨兵 key；Matcher 增加 literal_hint（零分配、分隔符安全提取）；mmap layer 通过哨兵能力探测对旧段禁用预过滤，避免目录段命中假阴性
- Stage 6（LSM Hygiene）：v6/v7 校验从弱校验升级为 CRC32C 并兼容旧段；compaction/replace-base 改为 live-only 重写回收 tombstones；MemoryReport 增加 smaps_rollup + page faults 观测闭环
