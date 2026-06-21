# 修复 snapshot_now_v7 冗余双重排序（怎么做）

## 1) 问题定位

文件：`src/storage/snapshot_v7.rs`，函数 `snapshot_now_v7`。

- 第 1666-1672 行（保留）：确定性排序——收集 `latest_by_key` 到 `Vec`，按 `FileKey` 排序后逐条 push 到 `entries_by_key`。
- 原第 1691-1693 行（删除目标）：冗余二次排序 `merged.entries_by_key.sort_by_key()` + 注释 `// 排序（key）`。

## 2) 修复操作

删除原第 1691-1693 行（空行 + 注释 + `sort_by_key()` 调用），使函数从 `merged.tombstones |= delta.tombstones.clone();` 直接流到 `write_v7_snapshot_atomic(path, &merged)`。

## 3) 验证

- `cargo check` 编译通过。
- 快照字节输出不变：`entries_by_key` 在第 1666-1672 行已按 `FileKey` 排序，末尾的 `sort_by_key()` 对已排序数据是幂等操作，删除后输出字节完全一致。

## 4) 影响范围

- 仅影响 `snapshot_now_v7` 函数，该函数在快照写入路径调用。
- 每次快照写入节省一次 O(n log n) 排序遍历，n 为索引中文档总数。
- 无 API 变更，无磁盘格式变更，无行为变更。
