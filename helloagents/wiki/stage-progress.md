# 阶段进度总览（已完成 / 未完成）

本页用于把 fd-rdd 的演进拆成阶段，便于对外讨论与复盘。勾选项以 `helloagents/history/*/task.md` 与当前代码为准。

## 阶段 A（内存布局压缩：DocId + RoaringBitmap + PathArena）

已完成：

- DocId(u32) 替代 FileKey 作为 posting 元素
- posting list 改为 `RoaringBitmap`
- Path blob arena（offset/len）+ 主表紧凑化（Vec）
- 事件链路全量适配 DocId
- 快照升级至 v4（兼容 v2/v3 迁移读取）

未完成 / 延后：

- L1 缓存键/失效逻辑完全切到 DocId（当前仍以 FileKey/Path 语义为主，避免 DocId 泄漏到对外结构）

## 阶段 A+（动态侧止血：稳定性 + 观测 + 路径进一步压缩）

已完成：

- rebuild 冷却与合并（事件风暴场景避免频繁重扫）
- ArcSwap 后台构建 + 原子切换（rebuild 期间查询不中断）
- 弹性计算（AQE）：AdaptiveScheduler 接入 rebuild/full_build，按系统负载选择并行度执行全量扫描
- 内存统计校准：Arena/HashMap capacity/Roaring serialized_size 拆项
- root 相对路径压缩：`root_id + relative_path`，快照升级至 v5（含 roots_hash 校验）

## 阶段 B（持久化终局 v6：mmap 段式 + posting lazy decode）

已完成：

- v6 容器：manifest + 多 segment descriptor（per-segment checksum）
- writer：原子写入 v6
- reader：mmap 加载 + 校验（roots 不一致拒绝加载）
- 查询：TrigramTable 二分 + PostingBlob Roaring lazy decode（按需解码）
- 启动：优先加载 v6，失败回退 v2~v5

未完成 / 选做：

- manifest 严格 rkyv archived（当前为手写二进制；等 schema 稳定再接入）

## 阶段 C（LSM：长期 mmap 基座 + 内存 Delta + Flush/Compaction）

已完成：

- 目录化布局：`index.d/` + `MANIFEST.bin` + `seg-*.db` + `seg-*.del`
- events.wal：追加型事件日志（seal + manifest checkpoint + 启动回放），降低 overflow/重启后的全量 rebuild 概率
- 查询合并：newest→oldest 覆盖语义（跨段 delete 支持 delete→recreate）
- Flush：内存 Delta 追加为新段（并在运行时加入查询层）
- Compaction：delta 段数量阈值触发后台合并为新 base（best-effort 清理旧段）
- Compaction 前缀替换：允许“仅 compact base + 最老一段 delta 前缀”，并在 replace-base 时保留未参与本轮 compaction 的 suffix delta，避免长稳运行下段数失控
- 观测：MemoryReport 增加 disk segments 数量

说明：

- 本阶段刻意不引入 rkyv（Manifest/Gen/Compaction 策略仍在快速迭代）

## 阶段 D（Stage 4 终极闭环：级联查询全贯通）

已完成：

- 查询链路：TieredIndex 查询按 newest→oldest 级联合并 disk segments，并同时执行 FileKey 去重与 path 维度屏蔽（覆盖语义锚定）
- 回归测试：新增“同路径不同 FileKey/rename-from tombstone/同 FileKey 多段幽灵”的级联语义测试，防止未来重构回退
- 性能防御：mmap layer 引入 literal_hint + trigram 候选预过滤（组件级 trigram + 哨兵能力探测），避免 Exact/Glob 退化全扫并保证不漏结果
- 物理结界：v6/v7 段式加载校验升级为 CRC32C；compaction/replace-base 使用 live-only 重写做真·Tombstone GC；MemoryReport 增加 smaps_rollup/page faults 指标

未完成 / 预研：

- 段数较多时进一步评估 Bloom Filter / posting 级统计优化（减少触页与 HashSet 去重开销）

## 阶段 E（查询体验对齐 Everything：2026-04-05）

已完成：

- **即时索引**：`--debounce-ms` 默认 100→10ms；新增 `POST /scan` 端点 + `TieredIndex::scan_dirs_immediate()`，支持前端主动触发目录扫描
- **路径段首匹配**：`PathInitialsMatcher`（`c/use/shi` → `/home/shiyi/...`），DSL 自动检测并追加 OR 分支
- **智能排序**：`execute_query` 集成评分排序（深度/basename/长度/近期修改）；HTTP 新增 `sort`/`order` 参数（name/path/size/ext/dm/dc/da）
- **多维启发式评分**：评分引擎升级为 `FinalScore = (MatchQuality × BasenameMultiplier) + BoundaryBonus - LengthPenalty - ContextPenalty`；深度降级为 Tiebreaker（每层 -0.5）；basename 命中 ×2.5；边界感知匹配（`.`/`-`/`_` 后 +12、CamelCase +8）；完美边界翻倍（匹配前一字符为 `.` 或 `/` 时 ×2）；Smart Dot-file（query 含 `.` 或 basename 命中 → 豁免隐藏目录降权）；node_modules 物理隔离（query 无 "node" → ×0.1）；ScoreConfig 预解析 query_has_dot / query_has_node / query_basename
- **匹配高亮**：`SearchResult` 新增 `score` + `highlights`（`[byte_start, byte_end)` 数组）
- **新增过滤器**：`parent:`/`infolder:`、`depth:`、`len:`、`dc:`、`da:`、`type:`
- **FileMeta 扩展**：新增 `ctime`/`atime`（运行时填充，不持久化到快照）

未完成 / 延后：

- 文件夹索引（当前跳过目录）
- `dupe:` 重复文件检测
- `empty:` 空文件夹
- fanotify 内核级变更感知（USN Journal 对标）

## 阶段 F（性能与安全加固：2026-04-05）

已完成：

- **事件通道扩容**：`event_channel_size` 默认 4096→65536，git clone 等批量操作不再丢事件
- **Dirty flag 无锁化**：`RwLock<bool>` → `AtomicBool`（Release/Acquire），消除 snapshot 与写入路径的竞态
- **全量扫描上限**：`query(limit)` 在 None 分支加 `.take(limit)`，防止短查询触发无界遍历
- **Trigram 交集优化**：持锁期间按基数排序后原地 `&=`，仅 clone 最小 bitmap
- **DocId 溢出安全化**：`alloc_docid` 超过 4B 文件时返回 `None` 而非静默写入 `u32::MAX`

## 明确延后 / 放弃

- fanotify：暂不做（后续结合 watcher 架构与段式/LSM 一起评估）

## 阶段 G（2026-05-02 收尾稳定化 / v0.6.14）

已完成：

- 恢复 `tests` 分支编译与全量测试通过，修复多路 merge 后 `ParentIndex` / `snapshot_v7` / `FileEntry` 迁移不一致的问题。
- `TieredIndex` 查询/加载热路径收敛到 `BaseIndexData + DeltaBuffer + v7 snapshot`，移除被重新带回的 `disk_layers` 字段、v6 mmap/LSM 加载分支和未引用 `disk_layer.rs`。
- 事件应用路径删除目录 rename 后同步 `scan_dirs_immediate_deep()`，避免递归 walk 阻塞事件管线。
- v7 快照加载和空索引启动后回放 WAL，避免 snapshot 后未落盘事件在重启后丢失。
- 查询入口在 L2/BaseIndex 计数不一致时自动刷新 base，兼容测试和内部直接写 L2 的路径。
- `PersistentIndex` 运行时主存储迁移为 `FileEntry + Vec<Vec<u8>>` 绝对路径表，`FileEntry.path_idx == DocId`；`PathTableV2 + FileEntryIndex` 仅在 `BaseIndexData`/v7 导出时构建，`CompactMeta + PathArena` 仅用于旧快照读取和 v5/v6 兼容导出。
- 事件热路径不再每批 `rebuild_parent_index()` + `to_base_index_data()`。普通事件只更新 L2 与 `DeltaBuffer`；查询以 `BaseIndexData + DeltaBuffer` 合并可见性；完整 BaseIndex materialize 收敛到 snapshot/rebuild 边界，以及空 base 测试兼容路径。
- `BaseIndexData` 的 `PathTableV2` 现在同时包含文件路径和父目录路径，ParentIndex 删除对齐可以基于 base 执行，不再在 fast_sync Phase3 临时全量重建 ParentIndex。
- v7 快照加载直接挂载到 `BaseIndexData`，L2 保持为空增量层，避免启动时逐条回灌快照和二次 `to_base_index_data()`；`file_count()` 改为以 base 为准，避免主程序误触发 `spawn_full_build()`。
- snapshot 边界改为物化当前可见全集（base + DeltaBuffer），再写 v7；修复 v7 反序列化后 `FileEntryIndex` 未重建排序置换导致“计数正确但查询 miss”的问题。
- `cargo test -q -- --ignored` 大规模压测通过；`p2_large_scale_hybrid` initial_indexing 指标：CPU 峰值 125%，CPU>=100% 时长 2034ms，RSS 峰值 237680KB。
- 冷启动 full_build 默认改为保守串行策略，并在串行扫描中批次短 sleep，避免 800K 文件初始索引长时间多核满载。
- 新建目录 watch 注册后补一次浅层 `scan_dirs_immediate()`，补齐目录创建与 watch 生效之间丢失文件事件的竞态，不恢复目录 rename 递归深扫。
- 默认全局目录排除从“搜索降权”前移到索引入口：`.git`、`.cache`、`.cargo`、`.npm`、`.pnpm-store`、`.yarn`、`node_modules`、`target`、`dist`、`build`、`vendor` 等目录在 cold full build、incremental scan、fast-sync 与 watcher 事件路径中都不会进入索引；旧配置未写 `exclude_dirs` 时仍使用默认排除列表。
- `config.toml` 缺少 `exclude_dirs` 时启动会追加写入默认列表，让默认屏蔽项变为用户可见、可编辑配置；新增 `GET /memory` JSON 端点用于真机 RSS/smaps/索引拆项归因。
- 真机 `/memory` 显示旧索引 445K 文件时 base 估算约 100MB、非索引匿名脏页约 131MB；后续修正 MemoryReport 纳入 base 拆项，并移除运行时冗余 `entries_by_path` 副本，v7 加载后主动触发 allocator collect。
- 继续压缩 base：移除 `FileEntryIndex` 未使用的 path permutation，压紧 `PathTableV2::EncodedEntry` 字段布局；新写 v7 path-table 段改为直接保存压缩结构，旧完整路径段仍可读取。
- 补齐 RSS 回吐触发点：memory report loop 在 heap high-water suspected 时执行 trim；事件批处理结束并进入 idle 后对本轮事件高水位做一次 trim；HTTP 新增 `/trim` 手动触发端点。
- ParentIndex 运行时表示从每目录 `RoaringBitmap` 改为排序 `Vec<u32>` 直属文件列表；v7 父索引段仍按旧 Roaring 编码读写兼容，但加载后不再常驻大量小 Roaring 对象。
- 新增 `watch_enabled` / `--no-watch` 静态模式，用于验证 watcher 在大 `$HOME` 场景下的常驻 RSS，并支持只读快照查询 + 手动 `/scan`。
- 新增 `watch_mode = "recursive" | "tiered" | "off"` 和 `--watch-mode`；默认保持 recursive，tiered 作为预算受控热点监听预览模式，先按 XDG 热点目录和 `max_watch_dirs` 准入 L0。
- HTTP 新增 `/watch-state`，输出 watcher 模式、L0/L1/L2/L3 数量、估算 watch 目录预算、扫描 backlog 和 tiered 调度说明。
- tiered 模式下未准入 L0 的热点候选进入有界 warm-scan loop，按 `l1_scan_interval_secs` 和 `scan_items_per_sec` 分批浅扫，避免直接回到全递归 watcher RSS。
- 周期 snapshot 默认增加小批量门槛：少量事件先留在 WAL/DeltaBuffer，不再每个 snapshot interval 都全量 materialize 44 万级 base，避免 watcher 测试中出现非索引 RSS 高水位。
- 真机验证：`--watch-mode tiered` 在约 44.5 万文件索引下，启动 RSS 约 96-97MB，运行一段时间后约 97-98MB，`non_index_private_dirty` 保持约 14MB，明显低于 recursive watcher 的 260MB+ 常驻区间。

仍保留 / 后续处理：

- `spawn_full_build` 仍作为无 v7 快照时的 fallback。
- legacy v6 snapshot 底层 API 仍存在，但不再由 `TieredIndex` 加载热路径使用。
- `refresh_base()` 仍是全量 materialize；仅应作为 snapshot/rebuild/测试兼容入口使用，不能重新放回普通事件批次或查询常规路径。
- tiered watcher 当前仍是第一阶段控制面和 L1 warm scan 骨架，尚未实现动态事件评分、L0 替换、L1/L2/L3 迟滞降级和 fanotify 后端。
