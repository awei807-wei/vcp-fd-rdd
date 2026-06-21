# M1-2 运行时 subtree tombstone 任务

## 实施任务

- [√] 新增 `RuntimeSubtreeTombstone { root_path, generation, expires_at }`
- [√] Delete / RenameFrom 事件新增 subtree tombstone
- [√] Create / Modify / RenameTo 事件清理同名重建相关 tombstone
- [√] 查询冷层/base 候选验真前执行 prefix filter
- [√] TTL 清理仅保持运行时语义，不改 snapshot 格式
- [√] README/CHANGELOG/wiki 同步 subtree tombstone 口径

## 测试项

- [√] 删除父目录后，子路径 cold/base 候选不返回，且 `cold_validate_count` 不增加
- [√] Tombstone TTL 过期后可清理
- [√] 同名目录重建并产生 Create 事件后不被旧 tombstone 误伤
- [√] Runtime-only：snapshot/reload 不持久化 subtree tombstone

## 验收标准

- [√] `cargo test -q subtree_tombstone`
- [√] `cargo test -q query_verify`
- [√] `cargo test -q query`
- [√] `cargo test -q`
- [√] `cargo fmt --check`
