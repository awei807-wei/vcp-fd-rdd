# v0.6.3 动机

## 背景

基于 `optimization-assessment-report.md` 的源码二次验证，确认了之前报告中的三处关键偏差：
1. `CompactMeta.mtime_ns` 早已是 `i64`（已优化）
2. compaction 后 `madvise(DONTNEED)` 已实施
3. `filekey_to_docid` 当前是 `BTreeMap` 而非 `HashMap`（两份报告方向矛盾）

## 优化动机

### 1. BTreeMap → HashMap（省 ~48 MB）
这两个 map 仅做 `.get()`、`.insert()`、`.remove()`、`.clear()`、`.len()` 操作，无任何有序迭代依赖。HashMap 每条目约 41B vs BTreeMap 约 59B，百万级文件差异显著。`FileKey` 已 derive `Hash + Eq`，`u64` 天然 `Hash`，零功能风险。

### 2. channel_size 统一（省 ~11 MB）
`EventPipeline::new()` 硬编码 `262_144` 是死代码（生产路径全部走 CLI 默认 `65_536`），降低它消除未来误用风险。

### 3. FAST_COMPACTION 默认启用
`compact_layers_fast` 已完整实现（位图 OR 合并）并通过 CI 验证，仅需将 `unwrap_or(false)` 改为 `unwrap_or(true)`。

### 4. short_component_index Box<[u8]> → u16（省 ~3 MB）
短路径组件仅 1-2 字节，每个 `Box<[u8]>` 有 16B 堆分配器元数据，元数据/数据比 21:1。改为栈上 u16 大端编码直接消除堆碎片。

### 5. L1 path_index O(1) 快速路径
`path_index` 字段早已存在并被维护（`insert()`/`remove_by_path()` 正确使用），但 `query()` 始终做 O(N) 全扫描。精确全路径查询时可跳过全扫描。

### 6. 动态目录监控修复
`notify::RecursiveMode::Recursive` 不会自动为新创建的目录添加 inotify watch，导致启动后新目录中的文件无法被实时索引。这是功能 bug，不仅影响测试，也影响生产使用。
