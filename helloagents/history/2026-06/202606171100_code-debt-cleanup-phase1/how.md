# 代码债务清理 Phase 1 设计

## 当前行号复核

- `src/query/dsl.rs:523-524`：`HardlinkDupe` / `ContentDupe` 编译为 `CompiledExpr::True`。
- `src/index/l2_partition.rs:1328-1620`：`export_segments_v6`、`export_segments_v6_to_writer`、`export_segments_v6_compacted_to_writer` 三方法重叠。
  - 重复常量：`TRIGRAM_SENTINEL`(1398/1555)、`FKM_MAGIC`(1440/1587)、`FKM_VERSION`(1441/1588)、`FKM_FLAG_LEGACY`(1443/1589)、`FKM_FLAG_RKYV`(1444/1591)。
- `src/main.rs:928`、`src/stats/metrics_reporter.rs:537`、`src/event/tiered_watch.rs:3936`、`src/index/tiered/snapshot.rs:239`、`src/index/tiered/mod.rs:318`、`src/index/tiered/events.rs:403`：`unix_secs()` 6 份相同副本。
- `src/util.rs:1`：工具函数模块，已有 `maybe_trim_rss`、`pathbuf_from_encoded_vec` 等，`unix_secs` 应放此处。
- `src/io_governor.rs:4`：`use std::sync::Mutex;`
- `src/io_governor.rs:227,249,250,269,273`：`.lock().unwrap()` 调用。

## 实现策略

### 1. 修复 HardlinkDupe / ContentDupe 桩代码（P0）

**文件**：`src/query/dsl.rs`

将：
```rust
Atom::HardlinkDupe => Ok(CompiledExpr::True),
Atom::ContentDupe => Ok(CompiledExpr::True),
```
改为：
```rust
Atom::HardlinkDupe => Err(QueryCompileError::UnsupportedAtom {
    atom: "hardlink_dupe",
    reason: "hardlink dedup is not yet implemented",
}),
Atom::ContentDupe => Err(QueryCompileError::UnsupportedAtom {
    atom: "content_dupe",
    reason: "content dedup is not yet implemented",
}),
```

- 检查 `QueryCompileError` 是否已有 `UnsupportedAtom` 变体；若无，新增一个带 `atom: &str` 和 `reason: &str` 字段的变体。
- 确认相关测试（如果有 `dupe:` 的测试用例）更新为期望编译错误。

### 2. 提取 unix_secs() 到 util.rs（P1）

**文件**：`src/util.rs` + 6 个调用文件

1. 在 `src/util.rs` 添加：
   ```rust
   pub fn unix_secs() -> u64 {
       std::time::SystemTime::now()
           .duration_since(std::time::UNIX_EPOCH)
           .map(|d| d.as_secs())
           .unwrap_or(0)
   }
   ```

2. 在 6 个文件中：
   - 删除本地 `fn unix_secs()` 定义
   - 添加 `use crate::util::unix_secs;`（或通过模块路径调用）
   - 注意 `index/tiered/tests.rs:55` 的 `unix_secs_for_test` 是测试变体，可保留或也复用

3. 检查 visibility：`event/tiered_watch.rs:3936` 的 `unix_secs` 在 `#[cfg(test)]` 模块内，需要确认 `util::unix_secs` 在测试中可见（pub 即可）。

### 3. 去重 l2_partition.rs 段导出逻辑（P1）

**文件**：`src/index/l2_partition.rs`

1. 提取常量到模块级：
   ```rust
   const TRIGRAM_SENTINEL: u32 = ...;
   const FKM_MAGIC: [u8; 4] = ...;
   const FKM_VERSION: u32 = ...;
   const FKM_FLAG_LEGACY: u32 = ...;
   const FKM_FLAG_RKYV: u32 = ...;
   ```

2. 抽取公共写入函数：
   ```rust
   fn write_v6_segments_to_writer<W: Write>(
       writer: &mut W,
       segments: &V6Segments,  // 已有的构建结果
       // 其他共享参数
   ) -> Result<...>
   ```

3. `export_segments_v6`、`export_segments_v6_to_writer`、`export_segments_v6_compacted_to_writer` 共享：
   - trigram/postings 构建 → `build_v6_segments(&self) -> V6Segments`
   - 写入逻辑 → `write_v6_segments_to_writer`
   - 各方法只需构建 segments 后调用写入，不再各自重复

4. 逐方法重构，确保逻辑等价。用 `git diff` 验证行为不变。

### 4. 统一 io_governor.rs 锁策略（P2）

**文件**：`src/io_governor.rs`

1. 将 `use std::sync::Mutex;` 改为 `use parking_lot::Mutex;`
2. 将所有 `.lock().unwrap()` 改为 `.lock()`（parking_lot 的 lock 返回 guard 而非 Result）
3. 检查 `Cargo.toml` 确认 `parking_lot` 已是依赖
4. 确认无其他 `std::sync::Mutex` 使用

## 边界

- 不新增依赖（parking_lot 已在项目中）。
- 不改变函数签名和公共 API。
- 不改变运行时行为（除 dupe 查询从静默 True 变为报错）。
- 每个改动独立可验证，逐步提交。
