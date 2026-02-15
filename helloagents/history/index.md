# history 索引

## 2026-02

- `202602150103_stage-a-docid-roaring-patharena`：阶段 A（DocId + RoaringBitmap + PathArena）
- `202602151511_arcswap-mimalloc`：Step 1 动态止血（ArcSwap 后台重建原子切换 + mimalloc 可选分配器）
- `202602151559_a-plus-stability-stats-rootrel`：阶段 A+（rebuild 冷却与合并 + 内存统计拆项 + root 相对路径压缩/快照 v5）
- `202602151637_stage-b-mmap-segments-rkyv`：阶段 B（v6 段式快照：mmap + Trigram/Metadata/Path 段 + posting lazy decode）
- `202602151800_stage-c-lsm-compaction`：阶段 C（目录化 LSM：长期 mmap 基座 + 内存 Delta + Flush/Compaction）
- `202602151958_manual-rss-trim`：轻量迭代（rebuild/full_build 完成后手动 RSS trim：mimalloc collect / glibc malloc_trim）
- `202602152019_streaming-verify-v6`：轻量迭代（v6 加载改为 read/seek 流式校验后再 mmap，降低冷启动 Private_Clean RSS；LSM 坏 delta 段跳过）
- `202602152200_elastic-adaptive-scheduler`：轻量迭代（弹性计算接回：AdaptiveScheduler 控制 rebuild/full_build 扫描并行度，FsScanRDD 支持并行 walker）
- `202602152230_watch-ignore-overlay-pending`：轻量迭代（默认忽略 snapshot 路径避免 watcher 反馈回路；MemoryReport 增加 overlay/pending 影子内存；pending_events 按路径去重）
- `202602152315_events-wal`：阶段 C 补齐（events.wal 追加日志：seal + manifest checkpoint + 启动回放；降低 overflow/重启后的全量 rebuild）
- `202602152340_docs-readme-help-git`：轻量迭代（同步 README/--help/回忆录，并提交 git 作为阶段性锚点）
- `202602152420_chronicle-architecture`：轻量迭代（输出编年史；生成立项架构书 SSOT；对外副本需手动拷贝到项目上层目录）
- `202602150226_dynamic-memory-vs-rss`：动态内存与 RSS 优化清单（阶段 A/A+/B 任务汇总；fanotify 延后）
