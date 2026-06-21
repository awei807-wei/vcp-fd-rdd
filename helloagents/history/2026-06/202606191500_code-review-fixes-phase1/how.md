# 代码审查修复第二轮（怎么做）

## 1) 修复快照输出非确定性

- **文件**：`src/storage/snapshot_v7.rs` — `snapshot_now_v7()`
- **方案**：将 HashMap 遍历改为收集到 Vec 后按 `FileKey` 排序再重建 `entries_by_key`，保证快照字节确定性。

## 2) 修复恒真测试断言

- **文件**：`src/index/l2_partition/tests.rs`
- **方案**：删除 `!is_empty() || len() == 0` 恒真断言，添加注释说明 arena 内容取决于 root 匹配逻辑。

## 3) 修复 unwrap() 崩溃风险

- **文件**：`src/query/dsl.rs`（2 处）
- **方案**：`unwrap()` 改为 `expect()` 带诊断信息，保留"显式失败"意图同时提供排查线索。

## 4) 修复 unwrap_or(0) 静默损坏

- **文件**：`src/runtime.rs`（4 处）
- **方案**：`u64::try_from(usize).unwrap_or(0)` 改为 `as u64` 直接转换，消除不可能溢出场景下的静默归零。

## 5) 修复 fsync_dir 日志级别

- **文件**：`src/storage/mod.rs`
- **方案**：目录打开失败日志级别从 `debug!` 提升至 `warn!`，与 `sync_all` 失败的日志级别一致。

## 6) 消除循环依赖

- **新建文件**：`src/storage/snapshot_common.rs`
- **方案**：提取 `MAGIC`、`STATE_COMMITTED`、`STATE_INCOMPLETE`、`HEADER_SIZE` 到独立模块，`snapshot.rs` 和 `snapshot_legacy.rs` 改为从 `snapshot_common` 导入，消除双向依赖。

## 7) 验证策略

- `cargo build` 编译通过
- 搜索确认所有修复点已正确合入
