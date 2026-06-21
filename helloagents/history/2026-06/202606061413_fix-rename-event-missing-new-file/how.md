# 修复 rename 事件导致新下载文件搜不到的方案设计

> 状态：已完成。本版已按调研包 `202606061358` 的要求落地：只把确认的孤立 `RenameMode::To` 当 Create，`RenameMode::From` 不得误当 Create。

## 总体策略

在事件 merge 阶段保证所有 rename 目标路径都能以正确语义写入索引，无论 `RenameFrom` 是否成功配对。

## 修复点一：孤立 RenameTo 当 Create 处理

**位置**：`src/event/stream.rs:merge_events_in_place`

**现状**：

```rust
// 处理 Rename（双路径事件）
if matches!(kind, notify::EventKind::Modify(notify::event::ModifyKind::Name(_)))
    && paths.len() >= 2
{
    // 处理配对成功的 rename
}

// 普通事件
let mut it = paths.into_iter();
let Some(path) = it.next() else { continue; };
let event_type: EventType = kind.into();  // 单路径 rename 走到这里，变成 Modify
```

**修改**：

```rust
// 处理 Rename（双路径事件）
if matches!(kind, notify::EventKind::Modify(notify::event::ModifyKind::Name(_)))
    && paths.len() >= 2
{
    // ... 原有双路径逻辑
}

// 处理孤立 RenameTo（单路径、明确 To 模式）
// 注意：只匹配 RenameMode::To，不匹配 Name(_) 通配。
// RenameMode::From 的单路径事件表示"路径已离开"，当 Create 会为已消失的旧路径
// 制造幽灵索引项，必须保持普通分支语义。
if matches!(
    kind,
    notify::EventKind::Modify(notify::event::ModifyKind::Name(
        notify::event::RenameMode::To
    ))
) && paths.len() == 1
{
    let path = paths.into_iter().next().unwrap();
    *seq += 1;
    scratch.merged.insert(
        path,
        MergedEvent {
            seq: *seq,
            timestamp: now,
            event_type: EventType::Create,  // 孤立 RenameTo 当 Create 处理
            path_hint: None,
        },
    );
    continue;
}

// 普通事件
// ...
```

**语义**：孤立 `RenameTo` 说明目标文件在 watch 视角下是新出现的，应当以 Create 语义索引。

**各 RenameMode 的处理口径**：

- `RenameMode::To`（单路径）→ Create。Linux inotify 后端总是给出明确的 From/To，孤立 To 即"新路径出现"。
- `RenameMode::From`（单路径）→ 保持普通分支（转 Modify）。带 tracker 的 From 已被 `PendingMoveMap` 移出 `raw_events`，能走到这里的是无 tracker 或配对超时的孤立 From；其路径已不存在，索引层 stat/验真路径会自然清理，不在本方案内扩展。
- `RenameMode::Any` / `Other`（单路径）→ 保持普通分支。管道内合成的 `Any` 总是双路径，不会进入此分支；其他后端的单路径 `Any` 无法判定方向，不做激进假设。

## 修复点二：Rename 语义不被覆盖

**位置**：`src/event/stream.rs:merge_events_in_place`

**现状**：

```rust
// 合并策略：后到的事件覆盖先到的
*seq += 1;
scratch.merged.insert(path, MergedEvent { ... });
```

**修改**：

```rust
// 合并策略：后到的事件覆盖先到的，但 Rename 和 Create 优先级最高
match scratch.merged.entry(path.clone()) {
    std::collections::hash_map::Entry::Occupied(mut e) => {
        // 如果已存在是 Rename 或 Create，且当前事件是 Modify/Access/Other，不覆盖
        let existing_is_creation = matches!(
            e.get().event_type,
            EventType::Create | EventType::Rename { .. }
        );
        let new_is_minor = matches!(
            event_type,
            EventType::Modify | EventType::Access | EventType::Other
        );
        if existing_is_creation && new_is_minor {
            // 保留已有的 Create/Rename，忽略后续 Modify
            continue;
        }
        // 否则覆盖
        *seq += 1;
        e.insert(MergedEvent {
            seq: *seq,
            timestamp: now,
            event_type,
            path_hint: None,
        });
    }
    std::collections::hash_map::Entry::Vacant(e) => {
        *seq += 1;
        e.insert(MergedEvent {
            seq: *seq,
            timestamp: now,
            event_type,
            path_hint: None,
        });
    }
}
```

**语义**：Create/Rename 是结构性变化，Modify 是内容变化。同一批次内，结构性变化优先级更高，不应被内容变化覆盖。

## 修复点三：fast path 也处理 rename

**位置**：`src/event/stream.rs` 约 line 954

**现状**：

```rust
// Fast path: if all events are Create for distinct paths, apply immediately.
let all_create = raw_events.iter().all(|ev| matches!(ev.kind, notify::EventKind::Create(_)));
if all_create && raw_events.len() <= 10 {
    // ... 直接 apply
}
```

**建议保持现状**，因为：
- rename 需要配对逻辑，不适合 fast path 简化处理
- 修复点一已经让孤立 RenameTo 走 merge 路径并正确处理
- fast path 只优化纯 Create 批次（新文件夹展开等场景），rename 批次走 merge 路径性能开销可接受

## 不修改的部分

- `PendingMoveMap` 跨批次配对逻辑保持不变，它在 `RenameFrom` 和 `RenameTo` 都到达时能正确合并
- debounce 窗口不调整，priority channel 已经让 Create 事件用更短窗口（5ms vs 50ms）

## 收益

- 孤立 `RenameTo`（watch 刚启动窗口期）不再丢失，新文件立即可搜
- rename 语义不被后续 Modify 覆盖，避免静默失败
- 配对成功的 rename 保持原有行为（`EventType::Rename` 带 `from` 信息）
- 修改集中在 merge 阶段，风险可控

## 风险与验证

**风险**：把错误方向的单路径 rename 当 Create，会为已消失的旧路径写入幽灵索引项。

**缓解（本版已收紧口径）**：

- 只匹配 `ModifyKind::Name(RenameMode::To)`，不使用 `Name(_)` 通配。
- `RenameMode::From`：带 tracker 的已被 `PendingMoveMap` 处理并从 `raw_events` 移除；孤立 From 走普通分支转 Modify，旧路径由验真/修复路径清理，不会被当 Create。
- `RenameMode::Any`：管道合成的配对事件总是双路径，走双路径分支；单路径 Any 不做方向假设，走普通分支。
- 残余风险仅剩"孤立 To 的目标路径在事件处理前又被删除"，这与普通 Create 事件的竞态相同，由索引层既有的 stat 失败处理兜底。

**验证**：见 task.md 测试项
