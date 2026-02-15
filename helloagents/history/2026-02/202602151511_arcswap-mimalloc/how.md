# 方案：ArcSwap 原子切换 + 重建事件回放 + mimalloc 可选全局分配器

## 1) ArcSwap 作为 L2 胶水

- 将 `TieredIndex.l2: PersistentIndex` 替换为 `ArcSwap<PersistentIndex>`
- `query/apply_events/snapshot` 等读取 L2 的位置统一通过 `l2.load_full()` 拿到 `Arc<PersistentIndex>` 后调用方法
- L2 仍保持内部细粒度锁（`parking_lot::RwLock`），但“指向哪一版索引”的切换不再需要全局写锁

## 2) 后台重建：构建新索引并原子切换

- 重建线程创建 `new_l2 = PersistentIndex::new()`，由 `IndexBuilder.full_build(&new_l2)` 扫描灌入
- 构建完成后，将“重建期间收集到的事件”按批次回放到 `new_l2`
- 在持有重建互斥锁的情况下执行 `l2.store(Arc::new(new_l2))`，并清理重建状态
- 为避免返回过期缓存：切换时清空 L1

## 3) 事件一致性：pending 事件缓冲与回放

核心约束：**任何发生在 rebuild 期间的事件，必须在切换前回放到 new_l2**。

实现：

- 在 `TieredIndex` 内新增 `rebuild_state: Mutex<...>`
  - `in_progress: bool`
  - `pending_events: Vec<EventRecord>`（可按需改成 `VecDeque` + 上限）
- `apply_events()` 先进入互斥区：
  - 若 `in_progress`，则把本批事件 clone 追加到 `pending_events`
  - 退出互斥区后再对当前 `l2` 执行 `apply_events`（在线读写仍作用于旧索引，保证查询可用/尽可能新鲜）
- 重建完成后：
  - 循环从 `pending_events` 取出一批回放到 `new_l2`，直到在持锁检查时 `pending_events` 为空
  - 仍持锁的情况下执行 `ArcSwap.store()` 原子切换，随后把 `in_progress=false`

说明：该策略的关键是 `apply_events` 的“**先记录后应用**”顺序与切换时的“**持锁判空→切换**”，避免切换窗口丢事件。

## 4) mimalloc：可选全局分配器

- `Cargo.toml` 新增 `mimalloc` optional 依赖，并提供 `--features mimalloc`
- 在 `src/lib.rs`（或 `src/main.rs`）增加：
  - `#[cfg(feature="mimalloc")] #[global_allocator] static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;`

## 5) 验证点

- 单元测试：补充“重建切换不丢事件”的测试（模拟：开始 rebuild→期间 apply_events→切换→新索引可查询到变更）
- 行为验证：触发 `spawn_rebuild()` 时，查询链路不会出现“瞬间归零/不可用”
- 编译验证：`--features mimalloc` 可正常构建（如环境允许拉取依赖）

