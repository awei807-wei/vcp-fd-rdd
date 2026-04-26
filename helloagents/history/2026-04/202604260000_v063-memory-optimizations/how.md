# v0.6.3 实现细节

## 1. BTreeMap → HashMap

### 文件：`src/index/l2_partition.rs`

- 类型声明：`RwLock<BTreeMap<FileKey, DocId>>` → `RwLock<HashMap<FileKey, DocId>>`
- 类型声明：`RwLock<BTreeMap<u64, OneOrManyDocId>>` → `RwLock<HashMap<u64, OneOrManyDocId>>`
- 所有 `BTreeMap::new()` 替换为 `HashMap::new()`（~6 处）
- `size_of::<BTreeMap<...>>()` 替换为 `size_of::<HashMap<...>>()`（2 处）
- 移除 `use std::collections::BTreeMap`（已无其他使用处）

HashMap 已在 scope 中（`trigram_index` 使用），无需新增导入。

## 2. channel_size 默认值

### 文件：`src/event/stream.rs`

```
- channel_size: 262_144,
+ channel_size: 131_072,
```

## 3. FAST_COMPACTION 默认启用

### 文件：`src/index/tiered/compaction.rs`

```
- .unwrap_or(false);
+ .unwrap_or(true);
```

用户仍可通过 `FAST_COMPACTION=0` 环境变量回退到慢路径。

## 4. short_component_index Box<[u8]> → u16

### 编码方案

```rust
fn encode_short_component(bytes: &[u8]) -> u16 {
    u16::from_be_bytes([bytes[0], bytes.get(1).copied().unwrap_or(0)])
}
```

- 1 字节: `[b]` → `[b, 0x00]`（大端 u16）
- 2 字节: `[b0, b1]` → `[b0, b1]`

由于 Linux 文件名不含 null 字节，1 字节编码的 `[b, 0x00]` 不会与 2 字节组件冲突。

### 修改范围

**`src/index/l2_partition.rs`：**
- 新增 `encode_short_component()` 函数
- `for_each_short_component`: `FnMut(&[u8])` → `FnMut(u16)`
- `short_component_matches`: `component: &[u8]` → `encoded: u16`，用 `to_be_bytes()` 解码后匹配
- 类型：`HashMap<Box<[u8]>, RoaringTreemap>` → `HashMap<u16, RoaringTreemap>`
- 删除所有 `Box::<[u8]>::from(component)` 调用，直接传 `component`

**`src/index/mmap_index.rs`：**
- 同步修改：`ShortComponentCache` 类型、`for_each_short_component`、`short_component_matches`、缓存构建逻辑

## 5. L1 Cache path_index O(1) 快速路径

### Matcher trait 扩展（`src/query/matcher.rs`）

```rust
pub trait Matcher: Send + Sync {
    // ... existing methods ...
    fn exact_path(&self) -> Option<&Path> { None }
}
```

`WfnMatcher` 重写：当 `scope == FullPath && case_sensitive` 时返回 `Some(Path::new(&self.pattern))`。

### L1Cache::query() 快速路径（`src/index/l1_cache.rs`）

```rust
if let Some(path) = matcher.exact_path() {
    if let Some(&fkey) = self.path_index.read().get(path) {
        if let Some(meta) = self.inner.read().get(&fkey) {
            lru.touch(fkey);
            return Some(vec![meta.clone()]);
        }
    }
    return None;
}
```

## 6. 动态目录监控修复

### 文件：`src/event/stream.rs`

- `let _watcher = watcher;` → `let mut watcher = watcher;`
- 新增 `use notify::Watcher;` 导入
- 在 debounce 收集后、fast path 前插入动态 watch 逻辑：

```rust
for ev in &raw_events {
    if matches!(ev.kind, notify::EventKind::Create(notify::event::CreateKind::Folder)) {
        for path in &ev.paths {
            if let Err(e) = watcher.watch(path, notify::RecursiveMode::Recursive) {
                tracing::debug!("Failed to add dynamic watch for {:?}: {}", path, e);
            }
        }
    }
}
```
