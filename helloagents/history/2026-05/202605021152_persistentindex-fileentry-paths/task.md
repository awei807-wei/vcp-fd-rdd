# PersistentIndex FileEntry/Paths 迁移

- [√] 移除 `PersistentIndex` 运行时 `metas` / `arena` 字段，保留 `CompactMeta` / `PathArena` 类型作为旧快照与 v5/v6 导出兼容层。
- [√] 将 v4/v5 快照加载转换为 `FileEntry + Vec<Vec<u8>>`，并从该主存储重建 filekey、path hash、trigram、short component 索引。
- [√] 将 upsert、rename、delete、query、parent index、`IndexLayer` 读取全部切到 `entries` / `paths`。
- [√] `export_snapshot_v5`、`export_segments_v6`、streaming v6 export 在导出时临时构造 legacy `PathArena + CompactMeta`。
- [√] `to_base_index_data` 保持导出期批量构建 `PathTableV2 + FileEntryIndex`，不在热路径维护压缩路径表。
- [√] 更新超长路径单测：运行时可索引超出 legacy `u16 path_len` 的路径。
- [√] 验证：`cargo test -q` 通过。
