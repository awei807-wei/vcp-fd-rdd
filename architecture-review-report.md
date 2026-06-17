# fd-rdd 架构评审报告

> 评审者：architecture-reviewer（团队 team-1781692487254）
> 评审日期：2026-06-17
> 项目版本：v7.1.0
> 代码规模：src/ 80 文件 ~51,800 行 Rust + tests/ 31 文件 ~6,400 行 = ~58,200 行

---

## 1. 总体架构描述

`fd-rdd` 是一个事件驱动的 Linux 文件索引守护进程，用户态实现类似 Everything 的毫秒级文件名搜索。项目使用 Rust（edition 2021），基于 tokio 异步运行时 + Axum HTTP + notify (inotify) 文件系统监听。

### 1.1 模块组织

项目按**功能领域 + 分层混合**方式组织，`src/lib.rs` 导出 14 个顶层模块：

```
src/
├── main.rs          (2,091 行 — 入口 + 编排逻辑)
├── config.rs        (1,603 行 — 配置定义/加载/校验)
├── core/            (RDD 抽象层：lineage, rdd, partition, adaptive, dag)
├── event/           (事件管道：stream, watcher, tiered_watch, proc_sampler, sync, ignore_filter, verify)
├── index/           (索引核心：base_index, l1_cache, l2_partition, l3_cold, mmap_index, tiered/*, parent_index, path_table*)
├── query/           (查询：dsl, fzf, matcher, scoring, server, socket)
├── storage/         (持久化：snapshot, snapshot_v7, wal, mmap, quarantine, recovery_audit, checksum, serde, traits)
├── stats/           (内存/性能指标)
├── sim/             (tiered watcher 离线仿真优化器)
├── security.rs      (路径安全检查)
├── fs_policy.rs     (挂载表/文件系统策略)
├── io_governor.rs   (I/O 限流)
├── clock.rs         (时钟抽象)
├── util.rs          (工具函数)
└── bin/             (fd-rdd-query, fd-rdd-sim 两个辅助二进制)
```

### 1.2 架构模式

采用**分层索引 + 事件驱动增量更新**架构：
- **L1 Cache**（热缓存）→ **L2 Partition**（持久索引）→ **L3 Cold**（mmap 只读段）
- 事件管道（EventPipeline）接收 inotify 事件 → debounce → DirtyQueue → 局部对账/fast-sync/rebuild
- Tiered Watcher（L0 热监听 / L1-L3 分级扫描 / ephemeral watch / fast-scan lease / proc sampler / rotating cold window）
- 存储层：v7 mmap snapshot + WAL（write-ahead log）+ stable snapshot + quarantine 隔离

---

## 2. 架构优点

| # | 优点 | 说明 |
|---|------|------|
| 1 | **模块划分清晰** | 14 个顶层模块按功能领域划分，每个模块职责相对明确（index 做索引、event 做事件、storage 做持久化、query 做查询） |
| 2 | **CI/CD 完善** | `.github/workflows/` 有 9 个 workflow：fmt check、clippy (-D warnings)、test、release LTO、no-default-features、poweroff-recovery、sim-regression、ThreadSanitizer (nightly)、musl 构建、smoke test、compaction correctness 等 |
| 3 | **测试覆盖全面** | tests/ 有 27 个集成测试文件（p0/p1/p2 分级），src/ 内有 ~300 个内联单元测试，覆盖 index(56)、event(85)、storage(34)、query(85)、sim(23) 等模块 |
| 4 | **多语言文档** | README.md 有中文/英文/日文三语说明，含 Mermaid 架构图；已有详细的 `fd-rdd-架构评审报告.md` 和 `fd-rdd-编年史.md` |
| 5 | **可恢复性设计** | WAL + stable snapshot + quarantine + recovery audit + startup repair 形成完整的崩溃恢复链 |
| 6 | **内存可解释** | stats 模块提供字节级内存占用拆解（L1/Base/L2/Disk/overlay/rebuild/process RSS/smaps），支持 full/light 两种采样深度 |
| 7 | **Feature flag 控制** | mimalloc/rkyv 通过 feature flag 控制，`--no-default-features` 可回退系统分配器 |
| 8 | **Release profile 优化** | LTO + codegen-units=1 + opt-level=3 |

---

## 3. 架构问题（按严重度分级）

### 🔴 严重（Critical）

#### 3.1 循环依赖：`index ↔ event`、`index ↔ query`、`index ↔ storage`

**现状**：`index` 模块与三个模块存在双向 `use crate::` 依赖：

| 循环对 | index → 方向 | 反向 |
|--------|-------------|------|
| index ↔ event | index 导入 `event::sync::{DirtyReason, DirtyScope}`、`event::tiered_watch::*`（6 处） | event 导入 `index::TieredIndex`、`index::tiered::ScanOutcome`（3 处） |
| index ↔ query | index 导入 `query::{Matcher, matcher, dsl}`（10 处）— `IndexLayer` trait 依赖 `Matcher` | query 导入 `index::{TieredIndex, tiered::*}`（7 处） |
| index ↔ storage | index 导入 `storage::{snapshot_v7, snapshot, wal, quarantine, recovery_audit, traits}`（8 处） | storage 导入 `index::{base_index, l2_partition, MmapIndex, file_entry_v2}`（9 处） |

**影响**：
- `index` 成为"上帝模块"（God Module），几乎所有其他模块都与之双向耦合
- 无法独立编译/测试任一模块
- 修改 index 内部实现会级联影响 event/query/storage
- `IndexLayer` trait 定义在 `index/mod.rs` 中却依赖 `query::Matcher`，违反了"核心抽象不依赖上层"原则

**建议**：
- 将 `DirtyReason`/`DirtyScope`/`DirtyQueue` 等共享类型提取到独立的 `sync` 或 `shared` 模块
- 将 `Matcher` trait 提取到独立的 `matcher` trait 模块（不依赖 query 实现），index 依赖 trait 而非实现
- 将 `FileEntry`/`BaseIndexData`/`IndexSnapshotV2-V5` 等存储序列化所需的数据类型提取到 `model` 或 `types` 模块
- 长期目标：建立 `types → core → index → event/query/storage` 的单向依赖链

#### 3.2 `main.rs` 巨型编排文件（2,091 行）

**现状**：`main.rs` 包含 2,091 行代码，远超合理的入口文件规模。它承担了：
- CLI 参数解析（Args struct，26 个字段）
- 配置加载/合并逻辑
- 索引初始化与 10+ 个 `apply_*` / `set_*` / `spawn_*` 调用链
- 启动修复（startup repair）决策
- 事件管道构建
- 5 个后台循环的 spawn（dirty queue loop、tiered scan loop、fast scan loop、rotating cold window loop、proc sampler loop）
- HTTP/UDS 查询服务启动
- 健康检查/统计/metrics provider 的闭包构建（`HealthTelemetry` 构造闭包就有 ~120 行字段映射）
- 优雅关闭流程

**影响**：
- 单文件混合了配置解析、编排、业务逻辑、后台循环实现
- `HealthTelemetry` 构造闭包（行 465-590）和 `MetricsHealthSnapshot` 构造闭包（行 707-815）有大量字段逐一复制，极难维护
- `spawn_dirty_queue_loop`（行 1238-1445，200+ 行）是核心业务逻辑，不应在 main.rs 中
- 函数 `build_tiered_watch_plan`（行 1057-1209）是纯业务逻辑，不属于入口

**建议**：
- 提取 `App` 或 `Runtime` 结构体封装启动编排逻辑
- 将 `spawn_*_loop` 函数移到 `event` 模块
- 将 `build_watch_plan` / `build_tiered_watch_plan` 移到 `event::tiered_watch` 或独立的 `watch_planner` 模块
- 将 `HealthTelemetry` / `MetricsSnapshot` 构造逻辑移到 `stats` 模块，使用 `From` trait 转换

---

### 🟠 高（High）

#### 3.3 巨型文件：`tiered_watch.rs`（5,822 行）

**现状**：`src/event/tiered_watch.rs` 单文件 5,822 行，是全项目最大文件。file_outline 显示它包含：
- 8 个 enum（WatchTier, Freshness, IndexResidency, FastScanLeaseKind, PromotionDecision, EphemeralWatchDecision, ...）
- `TieredWatchRuntime` 结构体及 60+ 个方法（lease 管理、fast scan、ephemeral watch、rotating cold window、proc sampler、registry persist/restore、debug dump）
- 持久化逻辑（restore_fast_scan_registry, persist_fast_scan_registry）
- 事件评分逻辑（record_event_paths, note_dirty_scope, observe_dirty_scope）

**影响**：单文件承担了过多职责（watcher 状态管理 + fast scan + ephemeral watch + cold window + 持久化 + 评分），极难理解和维护。

**建议**：按职责拆分为：
- `tiered_watch/types.rs`（enums, configs）
- `tiered_watch/runtime.rs`（TieredWatchRuntime 核心）
- `tiered_watch/fast_scan.rs`（fast scan lease 逻辑）
- `tiered_watch/ephemeral.rs`（ephemeral watch 逻辑）
- `tiered_watch/cold_window.rs`（rotating cold window）
- `tiered_watch/registry.rs`（持久化/恢复）
- `tiered_watch/scoring.rs`（事件评分）

#### 3.4 缺少 lint 配置文件

**现状**：CI 运行 `cargo clippy --all-targets -- -D warnings` 和 `cargo fmt --all -- --check`，但项目中**没有** `.clippy.toml`、`clippy.toml`、`rustfmt.toml` 或 `.rustfmt.toml`。

**影响**：
- 没有自定义 clippy lint 规则（如 `cognitive_complexity`、`too_many_arguments`、`large_enum_variant` 阈值）
- 没有自定义 rustfmt 格式化规则，依赖默认格式
- 无法在本地快速复现 CI 的格式/lint 检查标准

**建议**：添加 `rustfmt.toml`（如 `edition = "2021"`、`max_width = 100`）和 `.clippy.toml`（如 `cognitive-complexity-threshold = 30`、`too-many-arguments-threshold = 8`）。

#### 3.5 `index` 模块文件过多（13 文件 + tiered/ 15 文件 = 28 文件）

**现状**：`src/index/` 有 13 个直接文件 + `tiered/` 子目录 15 个文件，共 28 个文件。同时存在 `path_table_v2.rs` 和 `pathtable.rs` 两套路径表实现（V2 和旧版并存），以及 `file_entry_v2.rs`（暗示有 V1 历史遗留）。

**影响**：
- `pathtable.rs` vs `path_table_v2.rs` 命名不一致（下划线 vs 无分隔）且暗示旧代码未清理
- 文件数接近"胖目录"阈值，但考虑到索引系统的复杂性尚可接受

**建议**：
- 确认 `pathtable.rs` 是否仍有使用方，若无则删除
- 统一命名风格为 snake_case

---

### 🟡 中（Medium）

#### 3.6 硬编码路径与魔术数字

**现状**：
- `main.rs:706` 硬编码 `./reports/metrics` 作为 metrics 输出目录
- `config.rs` 中硬编码 `/tmp`、`/run/user/{uid}` 路径（虽然有 env var 回退，但 fallback 路径散落在多个函数中）
- `config.rs` 中大量魔术数字：`max_verify_per_query: 150`、`max_bytes: 64 * 1024 * 1024`、`wal_seal_bytes: 16 * 1024 * 1024`、`rotating_cold_window_budget: 128` 等

**影响**：配置值分散，难以统一调整和文档化。

**建议**：将关键常量提取为命名常量（部分已有 `DEFAULT_*` 常量，但覆盖不全），metrics 输出目录应可配置。

#### 3.7 `core` 模块 RDD 抽象疑似过度设计

**现状**：`src/core/` 包含 `rdd.rs`、`lineage.rs`、`partition.rs`、`adaptive.rs`、`dag.rs`，借鉴 Spark RDD 概念。但项目实际是一个单机文件索引器，仅 8 个内联测试。

**影响**：RDD/DAG/Partition 抽象对文件索引场景可能过度设计，增加了理解成本。需确认这些抽象是否被实际使用，还是遗留的架构实验。

**建议**：审查 `core` 模块的实际使用情况，若 RDD 抽象仅被 `l3_cold::IndexBuilder` 使用，考虑简化为直接的扫描/构建逻辑。

#### 3.8 `HealthTelemetry` 字段爆炸

**现状**：`main.rs` 中 `HealthTelemetry` 构造闭包有 ~80 个字段（行 486-588），`MetricsHealthSnapshot` 构造有 ~70 个字段（行 720-812），两者高度重复。

**影响**：
- 两个结构体字段几乎一一对应，但需要手动逐一复制
- 每新增一个监控指标需要在两处同步修改
- 极易遗漏导致不一致

**建议**：使用 `#[derive]` 或 `From<HealthTelemetry> for MetricsHealthSnapshot` 自动转换，或合并为一个结构体。

---

### 🟢 低（Low）

#### 3.9 TODO/FIXME 数量少但需跟进

仅 3 处 TODO，均在存储/索引核心路径：
- `src/index/tiered/mod.rs:429` — tombstone 查找优化（trie/sorted vec）
- `src/storage/snapshot_v7.rs:1726, 1732` — delta 归并去重未实现（简单追加）

**影响**：snapshot_v7 的 delta 合并未做真正归并去重，可能导致快照膨胀。

#### 3.10 测试分层命名良好但缺少基准测试集成

`tests/` 按 p0/p1/p2 分级命名清晰。有 `BENCHMARK.md` 和 `scripts/bench.sh`，但基准测试未集成到 CI 中（CI 有 stress test 但无 perf benchmark 回归）。

---

## 4. 依赖方向总结

```
                    ┌─────────────────────────────────┐
                    │           main.rs                │  (编排入口，依赖几乎所有模块)
                    └────────────┬────────────────────┘
                                 │
          ┌──────────────────────┼──────────────────────┐
          ▼                      ▼                      ▼
    ┌──────────┐  ←──双向──→  ┌─────────┐  ←──双向──→  ┌─────────┐
    │  index   │              │  event  │              │  query  │
    │ (上帝模块) │              └─────────┘              └─────────┘
    └────┬─────┘                                         ▲
         │ ←────────────双向─────────────────────────────┘
         ▼
    ┌──────────┐
    │ storage  │  (index ↔ storage 双向)
    └──────────┘

    acyclic (单向):
    core → fs_policy, io_governor, util
    config → event::proc_sampler, fs_policy, io_governor, util
    stats, sim, security, clock → 无反向依赖
```

**核心问题**：`index` 是依赖漩涡中心，与 event/query/storage 三者双向耦合。

---

## 5. 改进建议优先级

| 优先级 | 建议 | 预期收益 | 工作量 |
|--------|------|---------|--------|
| P0 | 拆解 `index ↔ query` 循环：提取 `Matcher` trait 到独立模块 | 解耦核心抽象 | 中 |
| P0 | 拆解 `index ↔ storage` 循环：提取共享数据类型到 `model`/`types` 模块 | 解耦持久化与索引 | 大 |
| P0 | 拆解 `index ↔ event` 循环：提取 `DirtyReason`/`DirtyScope`/`DirtyQueue` 到独立模块 | 解耦事件与索引 | 中 |
| P1 | 拆分 `main.rs`：提取 `Runtime`/`App` 结构体，移出 spawn_* 函数和 watch_plan 逻辑 | 降低入口复杂度 | 中 |
| P1 | 拆分 `tiered_watch.rs`（5,822 行）为 6-7 个子模块 | 降低单文件复杂度 | 中 |
| P1 | 添加 `rustfmt.toml` + `.clippy.toml` | 规范化 lint/fmt | 小 |
| P2 | `HealthTelemetry`/`MetricsHealthSnapshot` 使用 `From` trait 自动转换 | 消除字段重复 | 小 |
| P2 | 清理 `pathtable.rs` 旧实现，统一命名 | 减少技术债 | 小 |
| P2 | metrics 输出目录可配置化 | 灵活性 | 小 |
| P3 | 审查 `core` RDD 抽象的实际使用，考虑简化 | 减少过度设计 | 中 |
| P3 | 完成 snapshot_v7 delta 归并去重 TODO | 减少快照膨胀 | 中 |

---

## 6. 总结

`fd-rdd` 是一个**工程成熟度较高**的项目：CI/CD 完善、测试覆盖全面、文档详实、恢复性设计周到。它的架构方向（分层索引 + 事件驱动 + 预算受控 watcher）是合理的。

但在"屎山化"风险方面，最大的结构性问题是 **`index` 模块作为上帝模块与 event/query/storage 三个模块形成循环依赖**。这使得系统难以独立演进——任何对 index 的修改都可能级联影响其他模块。配合 `main.rs`（2,091 行）和 `tiered_watch.rs`（5,822 行）两个巨型文件，项目的可维护性正在接近临界点。

**一句话评价**：架构方向正确，工程实践扎实，但 `index` 模块的中心化耦合和两个巨型文件是当前最需要优先治理的结构性债务。若不加以拆解，后续功能迭代将越来越容易引入回归。
