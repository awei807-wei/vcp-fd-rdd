# M1-2 运行时 subtree tombstone 需求

## 背景

M1-1 已把冷层/base 返回前验真改为默认同步 `stat`。这保证“返回结果是真的”，但删除大目录后，宽泛查询仍可能命中大量冷层旧候选；如果逐个 `stat`，会把一次查询放大成万级系统调用。

## 问题

- 当前 tombstone 主要是精确路径/DocId 语义，父目录 Delete 不屏蔽冷层子路径。
- 删除 `node_modules` 这类大目录后，base/cold 里仍有大量旧路径。M1-1 的同步验真会在预算内逐个 `stat`，虽然受 `query.max_verify_per_query` 限制，但仍浪费查询预算。
- 第一版要求只做运行时，不改变 snapshot 持久化格式。

## 成功标准

- Delete 父目录后，查询候选进入同步验真前先被 prefix filter 屏蔽。
- 宽泛查询不会对已删除 subtree 中的冷层旧候选执行大量 `stat`。
- Tombstone 有 TTL，可清理。
- 同名目录重建并产生 Create/Modify/Rename 事件后，不再被旧 subtree tombstone 误伤。
