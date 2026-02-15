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
- 观测：MemoryReport 增加 disk segments 数量

说明：

- 本阶段刻意不引入 rkyv（Manifest/Gen/Compaction 策略仍在快速迭代）

## 明确延后 / 放弃

- fanotify：暂不做（后续结合 watcher 架构与段式/LSM 一起评估）
