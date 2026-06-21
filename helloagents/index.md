# HelloAGENTS 知识库索引

> 更新时间：2026-06-21  
> 项目：fd-rdd  
> 用途：作为 `helloagents/` 知识库的根入口，连接项目概览、专题文档、方案包与历史记录。

## 快速入口

- 项目说明：[README.md](../README.md)
- 更新日志：[CHANGELOG.md](../CHANGELOG.md)
- 编年史：[fd-rdd-编年史.md](../fd-rdd-编年史.md)
- 架构总览：[wiki/architecture-charter.md](wiki/architecture-charter.md)
- 阶段进度：[wiki/stage-progress.md](wiki/stage-progress.md)
- 待执行方案包合并开发计划：[wiki/development-plan-2026-05-29.md](wiki/development-plan-2026-05-29.md)
- 历史记录索引：[history/index.md](history/index.md)

## Wiki 专题

### 架构与存储

- [wiki/architecture-charter.md](wiki/architecture-charter.md)：项目定位、架构原则与演进路线。
- [wiki/storage-v6-mmap-segments.md](wiki/storage-v6-mmap-segments.md)：v6 mmap 段式快照设计。
- [wiki/storage-stage-c-lsm-compaction.md](wiki/storage-stage-c-lsm-compaction.md)：LSM 目录、flush 与 compaction。
- [wiki/poweroff-recovery-stable-snapshot.md](wiki/poweroff-recovery-stable-snapshot.md)：stable snapshot 与断电恢复语义。

### 代码审查报告

- [wiki/code-style-audit-report.md](wiki/code-style-audit-report.md)：代码风格审计报告（屎山指标、God 文件/函数、复制粘贴等）。
- [wiki/architecture-review-report.md](wiki/architecture-review-report.md)：架构评审报告（循环依赖、模块组织、改进建议等）。

### 内存与索引结构

- [wiki/memory-docid-roaring-patharena.md](wiki/memory-docid-roaring-patharena.md)：DocId、RoaringBitmap 与 PathArena 压缩方案。
- [wiki/memory-trigram-posting-vec.md](wiki/memory-trigram-posting-vec.md)：trigram posting 结构与内存权衡。
- [wiki/dynamic-rebuild-arcswap-mimalloc.md](wiki/dynamic-rebuild-arcswap-mimalloc.md)：动态重建、ArcSwap 与 allocator 策略。
- [wiki/shadow-memory-overlay-rebuild.md](wiki/shadow-memory-overlay-rebuild.md)：overlay/rebuild 期间的影子内存治理。
- [wiki/m2-memory-investigation-20260620.md](wiki/m2-memory-investigation-20260620.md)：1M 文件规模下 760MB RSS 内存构成根因分析。
- [wiki/m2-scan-performance-investigation-20260620.md](wiki/m2-scan-performance-investigation-20260620.md)：周期扫描 stat() 风暴瓶颈分析（300K stat/周期）。

### 查询与运行时

- [wiki/query-dsl.md](wiki/query-dsl.md)：查询 DSL 与过滤器语义。
- [wiki/query-matching-segment-glob.md](wiki/query-matching-segment-glob.md)：segment/glob 匹配策略。
- [wiki/daemon-client-uds-fast-sync.md](wiki/daemon-client-uds-fast-sync.md)：daemon、UDS 客户端与 fast-sync 链路。
- [wiki/tiered-watcher-runtime.md](wiki/tiered-watcher-runtime.md)：tiered watcher runtime 与预算调度。
- [wiki/reliability-deadlock-overflow.md](wiki/reliability-deadlock-overflow.md)：死锁、overflow 与可靠性补偿。
- [wiki/development-plan-2026-05-29.md](wiki/development-plan-2026-05-29.md)：7 个待执行方案包的合并开发计划、依赖顺序与验收矩阵。

### 仿真、报告与产品结构

- [wiki/fd-rdd-sim.md](wiki/fd-rdd-sim.md)：`fd-rdd-sim` 仿真框架。
- [wiki/runtime-sim-report-mapping.md](wiki/runtime-sim-report-mapping.md)：runtime 与 sim report 字段映射。
- [wiki/product-structure-book.md](wiki/product-structure-book.md)：产品结构说明。
- [wiki/todo-disk-first-memory-light.md](wiki/todo-disk-first-memory-light.md)：disk-first / memory-light 后续事项。
- [wiki/fanotify-prestudy.md](wiki/fanotify-prestudy.md)：fanotify 预研笔记。

### M2 冷层轮转验证

- [wiki/m2-cold-window-vm-benchmark.md](wiki/m2-cold-window-vm-benchmark.md)：M2 冷层轮转 VM 基准测试脚本设计与参数说明。
- [wiki/m2-cold-window-ab-analysis-20260617.md](wiki/m2-cold-window-ab-analysis-20260617.md)：小规模（3000 文件）A/B 分析，M2 冷层轮转有效性确认。
- [wiki/m2-bench-baseline-error-investigation-20260617.md](wiki/m2-bench-baseline-error-investigation-20260617.md)：基准测试崩溃根因调查（ENOTEMPTY、fixture 路径冲突）。
- [wiki/m2-memory-investigation-20260620.md](wiki/m2-memory-investigation-20260620.md)：1M 文件规模 760MB RSS 内存构成根因分析。
- [wiki/m2-scan-performance-investigation-20260620.md](wiki/m2-scan-performance-investigation-20260620.md)：周期扫描 stat() 风暴瓶颈分析。

### Memoir

- [wiki/memoir-2026-02-14.md](wiki/memoir-2026-02-14.md)
- [wiki/memoir-2026-02-15.md](wiki/memoir-2026-02-15.md)
- [wiki/memoir-2026-02-16.md](wiki/memoir-2026-02-16.md)
- [wiki/memoir-2026-03-04.md](wiki/memoir-2026-03-04.md)
- [wiki/memoir-2026-03-05-consolidated-memory-governance.md](wiki/memoir-2026-03-05-consolidated-memory-governance.md)

## 方案包与历史

- 待执行方案包目录：`plan/`
- 已执行方案包目录：`history/YYYY-MM/<方案包>/`
- 历史记录总索引：[history/index.md](history/index.md)

方案包生命周期约定：

1. 新方案放入 `plan/YYYYMMDDHHMM_<feature>/`。
2. 执行完成后更新 `task.md` 状态。
3. 迁移至 `history/YYYY-MM/`。
4. 同步更新 [history/index.md](history/index.md) 与相关 Wiki。

## 当前状态

- **分支**：`prototype/m2-cold-rotation`，HEAD `fd37829`（本地与远程同步）。
- **M2 冷层轮转验证**：已在 3000 文件小规模和 1M 文件 realistic 规模完成 A/B 对照。小规模 M2 有效性 100%，realistic 规模 passive_positive 0%→66.7%、cold_freshness_max 291s→92s。残余瓶颈：hot layer canary 0/30（inotify 预算不足）、760MB RSS（内存优化待实施）。
- **扫描优化**：已完成（commit `6d7eeb8`），dir mtime 预检 + readdir 批量删除对齐，消除周期扫描 stat() 风暴。
- **内存优化**：方案包 `plan/202606200420_persistent-index-memory-optimization` 待实施（4 Phase，预估节省 ~344MB）。
- `plan/` 当前待执行方案包：`202606141643_search-ignore-hint-cold-rotation-diff-snapshot`、`202606200420_persistent-index-memory-optimization`、`202606191200_m2-scale-up-ab-matrix`。
- 已完成的方案包已迁移至 `history/2026-06/`，详见 [history/index.md](history/index.md)。
- 根索引由本文件承接，后续新增 Wiki 或历史主题时应同步维护本页。
