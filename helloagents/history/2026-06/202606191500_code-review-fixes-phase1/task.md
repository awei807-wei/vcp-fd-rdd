# 代码审查修复第二轮：任务清单

- [√] 修复 `storage/snapshot_v7.rs` HashMap 非确定性：遍历前按 FileKey 排序
- [√] 修复 `index/l2_partition/tests.rs` 恒真断言：删除无意义 assert
- [√] 修复 `query/dsl.rs` 2 处 `unwrap()`：改为 `expect()` 带诊断信息
- [√] 修复 `runtime.rs` 多处 `unwrap_or(0)`：改为 `as u64` 直接转换
- [√] 修复 `storage/mod.rs` `fsync_dir` 日志级别：`debug!` 改为 `warn!`
- [√] 新建 `storage/snapshot_common.rs`：提取共享常量，消除循环依赖
- [√] 更新 CHANGELOG.md
- [√] 验证：`cargo build` 通过
