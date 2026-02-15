# fd-rdd 项目说明（SSOT）

## 项目定位

`fd-rdd` 是一个事件驱动的文件索引服务：

- L1：热缓存（短期命中）
- L2：内存常驻的持久索引（DocId + Roaring posting + PathArena）
- L3：离线/后台构建器（全量扫描灌入 L2）

对外提供 HTTP 查询接口，并支持周期性快照落盘（当前为 bincode 的原子替换快照）。

补充：阶段 B 引入 v6 段式快照（mmap + posting lazy decode），用于冷启动“秒开”。阶段 C 进一步演进为目录化 LSM：长期 mmap 基座 + 内存 Delta，通过 Flush/Compaction 追加段与合并段，启动不再做全量 hydration（详见 `wiki/storage-stage-c-lsm-compaction.md`）。

## 关键目标

- 查询链路低延迟、可用性优先
- 动态更新场景下内存可控、可观测（RSS/结构体拆项）
- 支持后台重建/滚动升级能力（通过 ArcSwap 等原子切换手段）

## 目录结构（概览）

- `src/index/`：三级索引实现（L1/L2/L3 + 组合层）
- `src/event/`：notify watcher + 事件管道（debounce/合并/批量应用）
- `src/storage/`：快照存储（header + checksum + 原子替换）
- `src/query/`：HTTP/Socket 查询服务
- `helloagents/wiki/`：项目知识库（架构/性能/可靠性笔记）
- `helloagents/plan/`：方案包（设计阶段产物）
- `helloagents/history/`：已执行方案包归档
