# v0.6.3 内存优化与功能修复

## 任务清单

- [x] `filekey_to_docid` 和 `path_hash_to_id` 从 `BTreeMap` 改为 `HashMap`，省 ~48 MB
- [x] `EventPipeline::new()` channel_size 从 262144 降至 131072，省 ~11 MB
- [x] `FAST_COMPACTION` 默认启用：`unwrap_or(false)` → `unwrap_or(true)`
- [x] `short_component_index` 键类型从 `Box<[u8]>` 改为 `u16`（大端编码），省 ~3 MB
- [x] L1 Cache `path_index` O(1) 快速路径：`Matcher` trait 新增 `exact_path()` 方法
- [x] 动态目录监控修复：`Create(Folder)` 事件触发 `watcher.watch()`
- [x] Cargo.toml 版本号 0.6.2 → 0.6.3
- [x] README.md 新增 v0.6.3 更新说明
- [x] CHANGELOG.md 新增 v0.6.2 和 v0.6.3 条目
- [x] `cargo check` 编译通过
- [x] helloagents 开发历史上下文维护

## 改动文件

| 文件 | 改动行数 | 说明 |
|------|---------|------|
| `src/index/l2_partition.rs` | ~24 | BTreeMap→HashMap + short_component_index u16 |
| `src/index/mmap_index.rs` | ~20 | short_component_index u16 同步修改 |
| `src/event/stream.rs` | ~15 | channel_size + 动态目录监控 |
| `src/index/tiered/compaction.rs` | 2 | FAST_COMPACTION 默认启用 |
| `src/query/matcher.rs` | ~12 | exact_path() trait 方法 |
| `src/index/l1_cache.rs` | ~12 | path_index O(1) 快速路径 |
| `Cargo.toml` | 1 | 版本号 0.6.3 |
| `README.md` | ~15 | v0.6.3 更新日志 |
| `CHANGELOG.md` | ~35 | v0.6.2 + v0.6.3 条目 |

**总计：约 136 行改动，省 ~62 MB 常驻内存 + 消除 compaction CPU 尖峰 + 修复动态目录监控 bug。**
