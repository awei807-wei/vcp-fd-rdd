# 性能瓶颈报告源码级验证结论

## 总体验证结论

| 验证点 | 结论 | 备注 |
|--------|------|------|
| 1. watcher 背压机制 | ✅ 准确 | 行号、阈值、sleep 范围均正确 |
| 2. EventPipeline 主循环 | ✅ 准确 | 253-471 行为主循环，默认 50ms debounce |
| 3. 溢出恢复循环 | ✅ 准确 | 477-504 行，200ms 轮询，1s/5s 条件均正确 |
| 4. Hybrid Crawler | ⚠️ 部分准确 | 轮询间隔正确，但 DFS 仅适用于 degraded roots |
| 5. dyn_walk_and_enqueue spawn_blocking | ✅ 准确 | 37-43 行，Create(Folder) 时无限制 spawn |
| 6. reconcile_degraded_root | ✅ 准确 | 575-668 行，迭代 DFS，MAX_DEPTH=20 |
| 7. DirtyTracker max_dirty_dirs=524288 | ⚠️ 部分准确 | 值正确，但非 recovery.rs 硬编码，由 channel_size*4 计算 |
| 8. PendingMoveMap 10s TTL | ✅ 准确 | 698 行，Duration::from_secs(10) |

**整体结论：报告描述基本准确（5/8 完全准确，2/8 部分准确，0/8 不准确）。关键性能瓶颈点确实存在。**

---

## 详细验证

### 1. watcher 背压机制 — ✅ 准确

**文件**: `src/event/watcher.rs`  
**行号**: 第 45-94 行（`handle_notify_result` 函数）

**关键代码片段**（第 67-72 行）：
```rust
// 动态背压：channel 水位 >80% 时主动 sleep，避免事件堆积压垮下游
let remaining = tx.capacity();
if remaining < channel_size.saturating_mul(2) / 10 {
    let delay_ms = 10u64.saturating_add((channel_size - remaining) as u64 % 41);
    std::thread::sleep(Duration::from_millis(delay_ms));
}
```

**验证分析**:
- `channel_size.saturating_mul(2) / 10` = 20%，即 **剩余容量 < 20%** 时触发背压 ✅
- `delay_ms = 10 + (channel_size - remaining) % 41`，范围 **10-50ms** ✅
- 使用 `std::thread::sleep` 在 watcher 回调线程中阻塞（同步 sleep，非异步）✅
- 报告描述的瓶颈逻辑（inotify 内核队列可能因 watcher 线程 sleep 而溢出）成立

---

### 2. EventPipeline 主循环 — ✅ 准确

**文件**: `src/event/stream.rs`  
**行号**: 第 253-471 行

**关键代码片段**（第 106 行，第 253-265 行，第 306-334 行）：
```rust
// 默认配置
debounce_ms: 50,          // line 106
channel_size: 131_072,   // line 107

// 主循环 spawn（line 253）
tokio::spawn(async move {
    let mut raw_events: Vec<notify::Event> = Vec::with_capacity(256);
    loop {
        raw_events.clear();
        // 等待第一个事件（biased 优先 priority_rx）
        let (first_ev, is_priority) = tokio::select! { ... };
        
        // debounce 窗口（line 306-334）
        let debounce_ms = if is_priority { priority_debounce_ms } else { debounce_ms };
        let deadline = tokio::time::Instant::now() + Duration::from_millis(debounce_ms);
        loop {
            let timeout = deadline.saturating_duration_since(tokio::time::Instant::now());
            // 在窗口内继续收集事件
            ...
        }
    }
});
```

**验证分析**:
- 第 253-471 行确实是主事件处理循环 ✅
- 默认 debounce 为 50ms ✅
- Priority 事件（Create）使用 `priority_debounce_ms = debounce_ms.min(5)` = **5ms**（第 222 行），比报告说的更短
- 报告描述准确，但 priority 路径的 debounce 实际上是 5ms 而非 50ms（这是优化点，不是瓶颈）

---

### 3. 溢出恢复循环 — ✅ 准确

**文件**: `src/event/stream.rs`  
**行号**: 第 477-504 行

**关键代码片段**（第 481-503 行）：
```rust
tokio::spawn(async move {
    // 经验值：静默 1s 触发；持续风暴 5s 强制触发一次（避免饿死）。
    let cooldown_ns: u64 = 1_000_000_000;      // 1s 静默
    let max_staleness_ns: u64 = 5_000_000_000;  // 5s 超时
    let min_interval_ns: u64 = 5_000_000_000;   // 最小间隔 5s

    loop {
        tokio::time::sleep(std::time::Duration::from_millis(200)).await;  // 200ms 轮询
        if dirty.sync_in_progress() {
            continue;
        }
        let Some(scope) = dirty.try_begin_sync(cooldown_ns, max_staleness_ns, min_interval_ns)
        else {
            continue;
        };
        tracing::warn!("Event overflow recovery: triggering fast-sync ({:?})", scope);
        idx.spawn_fast_sync(scope, ignores.clone(), dirty.clone());
    }
});
```

**验证分析**:
- 第 477-504 行确实是溢出恢复调度循环 ✅
- 每 200ms 轮询一次 ✅
- 1s 静默（cooldown）或 5s 超时（max-staleness）触发 fast-sync ✅
- min_interval_ns = 5s 防止过于频繁触发 ✅
- 与报告描述完全一致

---

### 4. Hybrid Crawler — ⚠️ 部分准确

**文件**: `src/event/stream.rs`  
**行号**: 第 507-566 行

**关键代码片段**（第 518-565 行）：
```rust
tokio::spawn(async move {
    let failed_poll_interval = std::time::Duration::from_secs(60);      // 60s
    let degraded_reconcile_interval = std::time::Duration::from_secs(30);  // 30s

    loop {
        tokio::time::sleep(std::time::Duration::from_secs(5)).await;

        // 1. Poll failed roots every 60s（line 529-539）
        if !failed.is_empty() && last_failed_poll.elapsed() >= failed_poll_interval {
            let scope = DirtyScope::Dirs { cutoff_ns: 0, dirs: failed.clone() };
            crawl_idx.spawn_fast_sync(scope, crawl_ignores.clone(), crawl_dirty.clone());
            last_failed_poll = tokio::time::Instant::now();
        }

        // 2. Reconcile degraded roots every 30s（line 543-562）
        if !degraded.is_empty() && last_degraded_reconcile.elapsed() >= degraded_reconcile_interval {
            for root in &degraded {
                if let Err(e) = reconcile_degraded_root(root, &crawl_idx, &crawl_dirty, &crawl_ignores).await {
                    ...
                }
            }
            last_degraded_reconcile = tokio::time::Instant::now();
        }
    }
});
```

**验证分析**:
- 轮询间隔：**failed roots 每 60s**、**degraded roots 每 30s** ✅
- 但 "DFS 全遍历" 的描述**不完全准确**：
  - **failed roots**（第 534 行）：调用 `spawn_fast_sync`，**不是 DFS**
  - **degraded roots**（第 547 行）：调用 `reconcile_degraded_root`，**这才是 DFS**
- 报告将两者混为一谈，实际上只有 degraded roots 走 DFS 路径

---

### 5. dyn_walk_and_enqueue spawn_blocking 洪水 — ✅ 准确

**文件**: `src/event/stream.rs`  
**行号**: 第 37-43 行

**关键代码片段**：
```rust
/// 动态 watch 辅助：递归遍历新目录，将已有文件以合成 Create 事件入队
/// 使用 spawn_blocking 避免阻塞事件循环（新目录可能包含大量文件）。
fn dyn_walk_and_enqueue(tx: tokio::sync::mpsc::Sender<notify::Event>, dir: std::path::PathBuf) {
    tokio::task::spawn_blocking(move || {
        if let Err(e) = walk_dir_send(&tx, &dir) {
            tracing::debug!("dyn_walk {:?} error: {}", dir, e);
        }
    });
}
```

**调用点**（第 354-355 行）：
```rust
if matches!(ev.kind, notify::EventKind::Create(notify::event::CreateKind::Folder)) {
    for path in &ev.paths {
        ...
        dyn_walk_and_enqueue(priority_tx.clone(), path.clone());  // 每次创建目录都 spawn
    }
}
```

**验证分析**:
- 第 37-43 行确实是 `spawn_blocking` 调用 ✅
- **每次发现新目录（Create(Folder)）都会无条件 spawn_blocking** ✅
- 没有并发限制（semaphore 或 pool），大规模目录创建时确实可能产生 "spawn_blocking 洪水" ✅
- 报告描述的瓶颈成立

---

### 6. reconcile_degraded_root — ✅ 准确

**文件**: `src/event/stream.rs`  
**行号**: 第 575-668 行

**关键代码片段**（第 587-656 行）：
```rust
async fn reconcile_degraded_root(
    root: &std::path::PathBuf,
    _index: &Arc<TieredIndex>,
    dirty: &DirtyTracker,
    ignore_paths: &[PathBuf],
) -> anyhow::Result<()> {
    let cutoff_ns = dirty.last_sync_ns();
    let effective_cutoff_ns = cutoff_ns.saturating_sub(10_000_000_000);  // 安全 margin 10s
    let mut changed_dirs: Vec<PathBuf> = Vec::new();

    // Use a stack for iterative DFS to avoid deep recursion
    let mut stack: Vec<(PathBuf, usize)> = vec![(root.clone(), 0)];
    const MAX_DEPTH: usize = 20;

    while let Some((dir, depth)) = stack.pop() {
        if depth > MAX_DEPTH { continue; }
        // Skip hidden, ignored paths
        // Check mtime vs cutoff
        // Push subdirs to stack...
        let rd = match std::fs::read_dir(&dir) { ... };
        for ent in rd {
            if ft.is_dir() {
                stack.push((ent.path(), depth + 1));
            }
        }
    }
    dirty.record_overflow_paths(&changed_dirs);
    Ok(())
}
```

**验证分析**:
- 第 575-668 行确实是 `reconcile_degraded_root` 函数 ✅
- 使用 **迭代 DFS**（stack）而非递归 ✅
- `MAX_DEPTH = 20` ✅
- 每 30s 对 degraded root 执行一次完整遍历 ✅
- 报告描述完全准确

---

### 7. DirtyTracker max_dirty_dirs = 524288 — ⚠️ 部分准确

**文件**: `src/event/recovery.rs`（结构体定义）+ `src/event/stream.rs`（实例化）  
**行号**: recovery.rs 第 39-47 行（结构体）；stream.rs 第 195 行（实例化）

**关键代码片段**：

recovery.rs 第 39-52 行（结构体定义，`max_dirty_dirs` 为参数而非硬编码）：
```rust
pub struct DirtyTracker {
    max_dirty_dirs: usize,   // 传入参数，不是常量
    ...
}

impl DirtyTracker {
    pub fn new(max_dirty_dirs: usize, roots: Vec<PathBuf>) -> Arc<Self> {
        Arc::new(Self {
            max_dirty_dirs: max_dirty_dirs.max(1),
            ...
        })
    }
}
```

stream.rs 第 107 行和第 195 行（实际计算值）：
```rust
channel_size: 131_072,      // line 107

let dirty = DirtyTracker::new(
    self.channel_size.saturating_mul(4).max(1024),  // line 195: 131072 * 4 = 524288
    roots.clone()
);
```

**验证分析**:
- `max_dirty_dirs` 的值 **确实是 524288** ✅
- 但报告说在 `recovery.rs` 第 39-237 行硬编码，这是**不准确的** ❌
  - recovery.rs 中 `max_dirty_dirs` 是构造参数，没有硬编码值
  - 实际值 524288 是在 `stream.rs` 第 195 行通过 `channel_size * 4` 计算得出的
  - recovery.rs 总行数约 200 行，不存在 "第 39-237 行" 这样的范围
- **结论**: 值正确，来源和行号描述不准确

---

### 8. PendingMoveMap 10s TTL — ✅ 准确

**文件**: `src/event/stream.rs`  
**行号**: 第 696-698 行

**关键代码片段**：
```rust
/// 跨批次 Rename 事件配对表：cookie → (插入时间, From 事件)
type PendingMoveMap = HashMap<usize, (Instant, notify::Event)>;

const PENDING_MOVE_TIMEOUT: Duration = Duration::from_secs(10);  // line 698
```

**清理逻辑**（第 235-251 行）：
```rust
tokio::spawn(async move {
    loop {
        tokio::time::sleep(PENDING_MOVE_TIMEOUT).await;  // 每 10s 清理一次
        let mut pm = pending_moves_cleaner.lock().await;
        cleanup_pending_moves(&mut pm);  // 移除超时的 From 事件
        ...
    }
});
```

**验证分析**:
- 第 698 行 `Duration::from_secs(10)` → **10s TTL** ✅
- 第 703 行清理逻辑：`now.duration_since(*t) < PENDING_MOVE_TIMEOUT` ✅
- 清理任务每 10s 执行一次（第 237 行）✅
- 报告描述完全准确

---

## 补充发现：关键配置汇总

| 配置项 | 值 | 位置 |
|--------|-----|------|
| channel_size | 131,072 | stream.rs:107 |
| debounce_ms (normal) | 50ms | stream.rs:106 |
| debounce_ms (priority/Create) | 5ms | stream.rs:222 |
| max_dirty_dirs | 524,288 (= channel_size * 4) | stream.rs:195 |
| 背压触发阈值 | < 20% 剩余容量 | watcher.rs:69 |
| 背压 sleep 范围 | 10-50ms | watcher.rs:70-71 |
| 溢出恢复轮询间隔 | 200ms | stream.rs:488 |
| 溢出恢复 cooldown | 1s | stream.rs:483 |
| 溢出恢复 max-staleness | 5s | stream.rs:484 |
| Hybrid Crawler failed 轮询 | 60s | stream.rs:519 |
| Hybrid Crawler degraded 轮询 | 30s | stream.rs:520 |
| PendingMoveMap TTL | 10s | stream.rs:698 |
| reconcile_degraded_root MAX_DEPTH | 20 | stream.rs:589 |
| reconcile_degraded_root safety margin | 10s | stream.rs:584 |
