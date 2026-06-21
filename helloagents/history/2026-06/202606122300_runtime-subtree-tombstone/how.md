# M1-2 运行时 subtree tombstone 方案

> 状态：已完成。

## 范围

- `src/index/tiered/mod.rs` / `load.rs`：新增运行时 subtree tombstone 状态。
- `src/index/tiered/events.rs`：应用事件时维护 tombstone。
- `src/index/tiered/query.rs`：冷层/base 候选进入验真前做 prefix filter。
- `src/index/tiered/tests.rs`：覆盖大目录删除、TTL 清理、同名目录重建。

## 运行时结构

```rust
struct RuntimeSubtreeTombstone {
    root_path: PathBuf,
    generation: u64,
    expires_at: Instant,
}
```

- `root_path`：被删除或被 rename-from 的 subtree 根。
- `generation`：事件序号，用于只让后续事件清理旧 tombstone。
- `expires_at`：运行时 TTL。第一版不持久化，不进入 snapshot。

## 事件维护

- `Delete(path)`：新增 subtree tombstone。
- `Rename { from, to }`：对 `from` 新增 subtree tombstone；对 `to` 清理覆盖路径下的旧 tombstone。
- `Create` / `Modify`：若路径位于 tombstone root 内，或是 tombstone root 的祖先/同路径，则清理该 tombstone，避免同名目录重建误伤。

## 查询过滤

- 在 parent query、base query、L2 warm query 中，候选路径先检查 `path.starts_with(tombstone.root_path)`。
- 命中则跳过候选并计入 stale hit，不消耗 `QueryVerifyBudget`，不执行同步 `metadata()`。

## 不做

- 不修改 snapshot 格式。
- 不把 subtree tombstone 写入 WAL。
- 不在查询线程同步 readdir 做补全；完整追平交给 M1-3 sliced repair。
