# 代码审查修复第二轮（为什么做）

目标：修复 2026-06-19 代码审查第二轮中发现的关键和中等问题，确保快照确定性、测试有效性和守护进程健壮性。

核心问题列表：

1. **快照输出非确定性（严重）**：`snapshot_v7.rs` 的 `snapshot_now_v7` 使用 HashMap 收集条目后直接遍历重建索引，HashMap 遍历顺序随机化导致相同数据产生不同快照字节，破坏可复现性。
2. **测试断言恒真（严重）**：`l2_partition/tests.rs` 中 `!is_empty() || len() == 0` 永远为真，零覆盖率。
3. **查询编译路径 unwrap() 崩溃风险（中等）**：`dsl.rs` 两处 `unwrap()` 在不变量被破坏时导致守护进程无信息 panic。
4. **unwrap_or(0) 静默损坏（中等）**：`runtime.rs` 多处 `u64::try_from(usize).unwrap_or(0)` 在不可能溢出的场景下使用 0 回退，导致预算计算静默错误。
5. **fsync_dir 日志级别过低（中等）**：`storage/mod.rs` 中目录打开失败用 `debug!` 记录，生产环境无法有效排查。
6. **snapshot.rs 与 snapshot_legacy.rs 循环依赖（中等）**：两个模块互相导入常量和类型，增加维护负担。

约束：

- 不改变任何函数签名或公共 API
- 不引入新依赖
- 修复必须通过 `cargo build`
