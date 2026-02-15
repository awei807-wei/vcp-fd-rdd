# CHANGELOG（HelloAGENTS）

本文件记录面向“方案包/知识库”的变更轨迹（以可读性为主，不替代 Git log）。

## Unreleased

- 初始化：补齐 `project.md`、`CHANGELOG.md`、`history/index.md` 等知识库骨架
- Step 1：引入 ArcSwap 后台重建原子切换（rebuild 期间查询不中断），并提供 `mimalloc` 可选全局分配器开关
- 阶段 A+：rebuild 冷却/合并策略；内存报告拆项；路径改为 root 相对存储并升级快照至 v5（含 roots_hash 校验）
- 阶段 B：新增 v6 段式快照（mmap + Trigram/Metadata/Path 段 + posting lazy decode），启动优先加载 v6；快照写入改为 v6（仍兼容读取 v2~v5）
- 阶段 C：目录化 LSM（`index.d/` + `MANIFEST.bin` + `seg-*.db/.del`），查询合并按 newest→oldest；Flush 将内存 Delta 追加为新段；段数阈值触发后台 Compaction 合并为新 base
- 动态 RSS 回吐：在 rebuild/full_build 完成时触发手动 trim（mimalloc: `mi_collect(true)`；glibc: `malloc_trim(0)`）
- 弹性计算（AQE）：接回 `AdaptiveScheduler`，rebuild/full_build 按系统负载选择并行度；`FsScanRDD` 支持可控并行扫描（ignore parallel walker）
- watcher 反馈回路止血：事件管道默认忽略 snapshot 路径（index.db/index.d），并支持 `--ignore-path` 手动排除日志等路径；MemoryReport 增加 overlay/pending 的“影子内存”统计；rebuild pending_events 按路径去重避免堆积
- 持久化补齐：引入 `events.wal` 追加型事件日志（seal + manifest checkpoint + 启动回放），降低 overflow/重启后的全量 rebuild 概率
