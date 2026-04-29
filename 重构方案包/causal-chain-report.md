# fd-rdd CPU/RAM 暴涨因果链路图

**日期**: 2026-04-26
**分支**: tests (当前) vs main

---

## 一、全景结构

fd-rdd 有四个并发运行的子系统，它们互相作用：

```
┌──────────────────────────────────────────────────────────────┐
│  fd-rdd 运行时全景                                            │
│                                                              │
│  [哨兵] watcher 线程                                          │
│    │ inotify 回调 → 推送事件到 channel                        │
│    │ 背压时 SLEEP → inotify 内核队列溢出                      │
│    v                                                         │
│  [心脏] EventPipeline (tokio 任务, 每 50ms 一个周期)           │
│    │ debounce → merge → apply → 更新 L2 索引                  │
│    │ 检测到溢出 → 标记 dirty → 触发 fast_sync                  │
│    │ 检测到新目录 → dyn_walk_and_enqueue → 洪水               │
│    v                                                         │
│  [大脑] L2 索引 (PersistentIndex)                              │
│    │ 800 万条 CompactMeta                                     │
│    │ 7 把 RwLock 保护                                        │
│    │ upsert_lock 全局串行化所有写入                           │
│    v                                                         │
│  [管家] 维护任务 (多个 tokio 任务)                              │
│    │ fast_sync:  溢出恢复 → O(N) 全量扫描 metas               │
│    │ rebuild:    全量重建 → 双倍内存                          │
│    │ snapshot:   序列化 → 阻塞事件处理                        │
│    │ compaction: 合并段 → 大量分配                            │
│    │ rss_trim:   每 300s 回收页                               │
│    │ hybrid_crawler: 每 30/60s 轮询失败根目录                 │
└──────────────────────────────────────────────────────────────┘
```

---

## 二、六条因果链路

### 链路 1：事件风暴 → inotify 溢出 → dirty_all → 全局 fast_sync → CPU/RAM 雪崩

```
大量文件操作 (git clone / npm install / 编译)
    │
    v
watcher 每秒收到数千个 inotify 事件
    │
    v
handle_notify_result() 逐事件推入 channel
    channel 剩余容量 < 20%
    │
    v
watcher 线程 SLEEP 10-50ms (背压机制)
    │
    v
inotify 内核队列爆满 → IN_Q_OVERFLOW
    │
    v
event.need_rescan() = true
    │
    ├─→ rescan_signals 计数器 +1
    └─→ dirty.mark_dirty_all()   ← dirty_all = true
    │
    v
溢出恢复循环 (每 200ms 轮询)
    │ 等 1s 静默 或 5s 超时
    │ 等 5s 最小间隔
    v
spawn_fast_sync(DirtyScope::All)
    │
    ├─→ Phase 1: collect_dirs_changed_since(全部 roots)
    │      DFS 遍历所有目录
    │      每个目录: symlink_metadata + modified() + read_dir + file_type
    │      收集所有 mtime 有变化的目录
    │
    ├─→ Phase 2: 扫描脏目录的直接子文件
    │      每个文件: metadata() + open() + ioctl() + close() = 4 syscall
    │      批量 upsert 到 L2 (upsert_lock 串行)
    │
    └─→ Phase 3: ★ 核心杀手 — for_each_live_meta_in_dirs
           遍历全部 800 万条 metas
           每条: 构造 PathBuf (分配内存) → 检查父目录是否脏
           如果是: symlink_metadata(路径) — 系统调用
           持有 3 把读锁 (metas + arena + tombstones)
           整个期间阻塞所有查询的写路径

为什么这条链是致命的一击：
- Phase 3 遍历全部 8M 条 metas，但实际脏目录可能只有几个
- 每条都分配 PathBuf，然后立刻丢弃
- 持锁期间做 syscall——锁被持有的时间 = 遍历 8M 条的时间
- 这个 fast_sync 每 5 秒可能触发一次（5s 最小间隔）
```

---

### 链路 2：冷启动全量扫描 → 每文件 4 次 syscall → 3200 万次系统调用

```
启动 → file_count() == 0 → spawn_full_build()
    │
    v
FsScanRDD.for_each_meta (std::thread)
    │
    ├─→ ignore::WalkBuilder 递归遍历全部 roots
    │      每个文件:
    │        stat()               ← 获取元数据
    │        open(O_PATH)         ← 打开文件
    │        ioctl(FS_IOC_GETVERSION)  ← 获取 ext4 generation (去重用)
    │        close()              ← 关闭
    │
    ├─→ upsert 到新 PersistentIndex
    │      upsert_lock.write() — 全局串行化
    │      并行扫描 -> 串行写入 = 多线程无效
    │
    └─→ finish_rebuild()
           swap L2 (原子替换)
           应用 rebuild 期间堆积的 pending_events

8M 文件 × 4 syscall = 3200 万次 syscall
CPU 100% 持续到扫描完成
```

---

### 链路 3：Rebuild 期间 pending_events 无界堆积 → 内存暴涨

```
rebuild 开始 (因为溢出 / 手动触发)
    │
    v
rebuild_state.in_progress = true
    │
    v
所有进入的事件 → capture_l2_for_apply()
    │
    v
pending_events: HashMap<FileIdentifier, PendingEvent>
    │
    │  key: FileIdentifier (PathBuf 或 dev+ino)
    │  value: PendingEvent {
    │      seq, timestamp,
    │      event_type: EventType::Rename { from_path_hint: PathBuf, ... }  ← 克隆
    │      path_hint: Option<PathBuf>  ← 克隆
    │  }
    │
    │  每个条目 = 事件克隆 + 路径克隆
    │  按 FileIdentifier 去重 (同一文件只保留最新事件)
    │  但: 新建/修改的不同文件各自独立，不解限
    │
    │  ★ 无上限 ★
    │  无 eviction

    v
重建 2-3 分钟 → 数十万条目 → 内存飙升 1GB+
```

---

### 链路 4：Snapshot 阻塞事件管线 + Compaction 双重内存峰值

```
overlay 路径数 >= 250,000 或 overlay 字节 >= 64MB
    │
    v
flush_requested = true
    │
    v
snapshot_loop 被唤醒 (最小间隔 10s)
    │
    v
snapshot_now():
    │
    ├─→ apply_gate.write()  ← ★ 阻塞所有事件处理
    │      持有期间: 所有 begin_apply_batch 被阻塞
    │      管道中事件堆积在 channel
    │
    ├─→ export_segments_v6()
    │      持有全部读锁 (arena + metas + tombstones + trigram_index + filekey_to_docid)
    │      分配 metas_bytes (metas.len() × 40 = ~320MB for 8M)
    │      分配 trigram_table_bytes
    │      分配 postings_blob (roaring bitmap 序列化)
    │      分配 filekey_map_bytes
    │      ★ 峰值 ~400MB 临时分配
    │
    ├─→ swap L2 → 新的空 PersistentIndex
    │      旧 index 被 ArcSwap 释放 (引用计数为 0 时 drop)
    │
    ├─→ append_delta_v6 → 写入磁盘
    │
    └─→ maybe_spawn_compaction()
           │
           v
        delta_count >= 8 且 cooldown >= 300s?
           │
           v (YES)
        compact_layers_fast():
           │
           ├─→ 遍历 base + 4 deltas, 收集 live metas
           ├─→ 合并 trigram postings (RoaringTreemap bitor_assign)
           ├─→ 构建新 PersistentIndex (fill_from_compaction)
           ├─→ export compacted segments
           ├─→ replace_base_v6 (写入磁盘)
           └─→ swap disk_layers

★ Snapshot ~400MB + Compaction new index ~300MB = 峰值 700MB+
```

---

### 链路 5：Hybrid Crawler → 周期性全量轮询 → 稳定的 CPU 脉冲

```
inotify max_user_watches 超限 / 部分 root 注册失败
    │
    v
watch_roots_enhanced() 返回 (failed_roots, degraded_roots)
    │
    v
Hybrid Crawler 启动两个周期任务:
    │
    ├─→ 每 30s: reconcile_degraded_root()
    │      DFS 遍历全部 root 目录树
    │      每个目录: symlink_metadata + modified() + read_dir
    │      收集 mtime 变化的目录 → dirty.record_overflow_paths()
    │
    └─→ 每 60s: spawn_fast_sync(DirtyScope::Dirs{failed_roots})
           完整 fast_sync 流程 → 回到链路 1

★ 每 30 秒一次 DFS 全遍历
★ 每 60 秒一次 fast_sync (含 for_each_live_meta_in_dirs)
```

---

### 链路 6：目录重命名 → 递归深扫 → 阻塞事件管线

```
mv old_dir/ new_dir/
    │
    v
EventPipeline 收到 Rename 事件 (目标是目录)
    │
    v
apply_events_inner() → 事件应用完毕后
    │
    v
检测到 Rename + 目标是目录
    │
    v
scan_dirs_immediate_deep(目标路径)
    │
    ├─→ ignore::WalkBuilder 无深度限制递归
    ├─→ 每个文件: metadata() + open + ioctl + close
    ├─→ 上限 10 个目录, 每个 50000 条目
    │
    └─→ 同步阻塞事件管线
           期间: channel 堆积事件 → 溢出 → 链路 1

★ 大目录重命名 = 管线暂停 + 可能触发溢出
```

---

## 三、所有因素按杀伤力排名

```
杀伤力   因素                       触发频率          主要消耗
────────────────────────────────────────────────────────────────────
 ★★★★★   for_each_live_meta_in_dirs  每次 fast_sync    CPU + RAM
         遍历全部 8M metas           每 5s 可能触发     分配 + syscall
────────────────────────────────────────────────────────────────────
 ★★★★★   冷启动每文件 4 syscall      一次性             CPU
          3200 万次                  (不可跳过)
────────────────────────────────────────────────────────────────────
 ★★★★    pending_events 无界堆积    重建期间            RAM
                                    (2-3 分钟)
────────────────────────────────────────────────────────────────────
 ★★★★    snapshot 序列化 +           每 300s            RAM + 阻塞
         apply_gate 写锁            或 overlay 超阈值
────────────────────────────────────────────────────────────────────
 ★★★★    compaction 双索引内存       每 snapshot 后     RAM
                                    每 8 delta
────────────────────────────────────────────────────────────────────
 ★★★     Hybrid Crawler DFS         每 30s             CPU
         全量轮询退化根目录
────────────────────────────────────────────────────────────────────
 ★★★     目录重命名递归深扫           每次 rename        CPU + 阻塞
         阻塞管线                    大目录
────────────────────────────────────────────────────────────────────
 ★★      remove_from_pending        每次事件批次       CPU
         O(n*m) retain              每 50ms
────────────────────────────────────────────────────────────────────
 ★★      collect_dirs_changed_since  每次 DirtyScope::All  CPU
         DFS 遍历所有目录                                   syscall
────────────────────────────────────────────────────────────────────
 ★★      export_segments_v6          每次 snapshot      RAM
         全量序列化                  每 300s
────────────────────────────────────────────────────────────────────
 ★       startup_reconcile          启动一次           CPU
         cutoff=0 全量比对           (有快照时)
────────────────────────────────────────────────────────────────────
 ★       upsert_lock 全局串行化      每个 upsert        CPU 利用率
         多线程扫描无效
────────────────────────────────────────────────────────────────────
 ★       dyn_walk_and_enqueue        每个新目录         CPU + 线程
         spawn_blocking 洪水
────────────────────────────────────────────────────────────────────
 ★       PendingMoveMap              每次 rename 风暴   RAM
         10s TTL 内堆积
────────────────────────────────────────────────────────────────────
 ★       watcher 背压 sleep          事件风暴时         CPU (被动)
         inotify 队列溢出
────────────────────────────────────────────────────────────────────
```

---

## 四、恶性循环放大图

```
                    ┌──────────────────┐
                    │  事件风暴开始     │
                    └────────┬─────────┘
                             │
                  ┌──────────┴──────────┐
                  v                     v
          ┌──────────────┐      ┌──────────────┐
          │ watcher sleep │      │ channel 堆积  │
          │ 背压阻塞      │      │ 事件积压      │
          └──────┬───────┘      └──────┬───────┘
                 │                     │
          ┌──────┴───────┐      ┌──────┴───────┐
          │ inotify 溢出  │      │ debounce      │
          │ Q_OVERFLOW   │      │ raw_events    │
          │ mark_dirty_all│     │ 无限增长      │
          └──────┬───────┘      └──────┬───────┘
                 │                     │
          ┌──────┴───────┐      ┌──────┴───────┐
          │ fast_sync    │      │ merge 大量    │
          │ 触发          │      │ HashMap 分配  │
          └──────┬───────┘      └──────┬───────┘
                 │                     │
          ┌──────┴─────────────────────┴───────┐
          │     apply_events → L2 写入          │
          │     upsert_lock 全局串行            │
          │     所有线程排队等锁                 │
          └──────────────┬─────────────────────┘
                         │
          ┌──────────────┴──────────────┐
          v                             v
  ┌──────────────┐              ┌──────────────┐
  │ for_each_live│              │ overlay 膨胀  │
  │ _meta_in_dirs│              │ 路径 > 250k   │
  │ O(8M) 遍历   │              │ → flush 触发  │
  └──────┬───────┘              └──────┬───────┘
         │                             │
  ┌──────┴───────┐              ┌──────┴───────┐
  │ 持锁 + syscall│             │ snapshot_now  │
  │ 每次分配 PathBuf│           │ apply_gate    │
  │ 8M 条 × 200B  │            │ WRITE — 阻塞  │
  │ ≈ 1.6GB 临时  │            │ 所有事件      │
  └──────┬───────┘              └──────┬───────┘
         │                             │
  ┌──────┴───────┐              ┌──────┴───────┐
  │ 查询被阻塞   │              │ channel 再次  │
  │ metas 读锁   │              │ 堆积         │
  │ 占满          │              │ → 再次溢出   │
  └──────────────┘              └──────┬───────┘
                                      │
                               ┌──────┴───────┐
                               │ 恶性循环     │
                               │ 回到开头     │
                               └──────────────┘
```

---

## 五、优化报告 vs 实际瓶颈

### 优化报告做的事（细枝末节）

| 优化 | 收益 | 占 1GB 峰值的比例 |
|------|------|-------------------|
| BTreeMap → HashMap ×2 | 省 ~48MB | 4.8% |
| channel_size 262k → 131k | 省 ~11MB | 1.1% |
| short_component Box<[u8]> → u16 | 省 ~3MB | 0.3% |
| set FAST_COMPACTION as default | 省 compaction 慢路径分配 | ~5% |
| **合计** | **~62MB** | **~6%** |

### 优化报告从未触及的大头

| 瓶颈 | 复杂度 | 触发频率 | 开销 |
|------|--------|----------|------|
| `for_each_live_meta_in_dirs` 遍历全部 metas | O(8M) | 每次 fast_sync | 8M 次迭代 + 8M 个 PathBuf 分配 |
| 冷启动每文件 open+ioctl+close 链 | O(8M) | 一次性 | 2400 万次 syscall |
| pending_events 无界累积 | 无上限 | 重建期间 | 每次 event clone + PathBuf |
| snapshot 全量序列化 | O(8M) | 每 300s | ~400MB 临时内存 |
| compaction 双索引 | O(8M) | 每 snapshot 后 | ~300MB 临时内存 |

### 一句话总结

> 优化报告和 tests 分支的开发，都把精力花在了**测量和减少数据结构的大小**上，
> 而没有花在**测量和减少这些数据被遍历的次数**上。
> 结果是省了 60MB 内存（6%），却完全没有解决 100% CPU 的核心原因。

---

## 六、关键源码位置索引

| 文件 | 行号 | 内容 |
|------|------|------|
| `src/index/l2_partition.rs` | 886-920 | `for_each_live_meta_in_dirs` — O(N) 全量 metas 遍历 |
| `src/index/tiered/sync.rs` | 373-531 | `fast_sync` — 完整的三阶段流程 |
| `src/index/tiered/sync.rs` | 506-528 | delete 对齐 — `for_each_live_meta_in_dirs` + `symlink_metadata` |
| `src/index/tiered/sync.rs` | 137-157 | `startup_reconcile` — cutoff=0 全量比对 |
| `src/index/tiered/sync.rs` | 264-297 | `spawn_full_build` — 冷启动全量构建 |
| `src/index/tiered/sync.rs` | 100-122 | `collect_dirs_changed_since` — DFS 遍历全部目录 |
| `src/index/tiered/events.rs` | 138-179 | `capture_l2_for_apply` — pending_events 无界累积 |
| `src/index/tiered/events.rs` | 316-335 | `apply_events_inner` — 目录重命名深扫 |
| `src/index/tiered/events.rs` | 366-389 | `remove_from_pending` — O(n*m) retain |
| `src/event/watcher.rs` | 45-94 | `handle_notify_result` — watcher 背压 + 溢出检测 |
| `src/event/stream.rs` | 253-471 | EventPipeline 主循环 |
| `src/event/stream.rs` | 477-504 | 溢出恢复循环 (每 200ms) |
| `src/event/stream.rs` | 507-566 | Hybrid Crawler (每 30/60s) |
| `src/event/stream.rs` | 37-43 | `dyn_walk_and_enqueue` — spawn_blocking 洪水 |
| `src/event/recovery.rs` | 39-237 | DirtyTracker — max_dirty_dirs = 524288 |
| `src/index/tiered/compaction.rs` | 21-87 | `maybe_spawn_compaction` — 8 delta 阈值 |
| `src/index/tiered/snapshot.rs` | 19-195 | `snapshot_now` — apply_gate.write() + 序列化 |
| `src/core/rdd.rs` | 52-93 | `FileKey::from_path_and_metadata` — open+ioctl+close |
| `src/core/rdd.rs` | 200-222 | `FsScanRDD::for_each_meta` — 并行扫描 + upsert_lock 串行 |
| `src/index/tiered/mod.rs` | 35-41 | 常量和阈值 |
| `src/main.rs` | 197-200 | 冷启动 spawn_full_build |
| `src/main.rs` | 232-234 | startup_reconcile 调用 |

---

## 七、所有常量阈值汇总

| 常量 | 值 | 位置 | 作用 |
|------|-----|------|------|
| `channel_size` | 131072 (默认) | `stream.rs:107` | Bounded channel capacity |
| `debounce_ms` | 50 | `stream.rs:106` | 事件批次窗口 |
| `priority_debounce_ms` | 5 | `stream.rs:222` | Create 事件优先窗口 |
| `max_dirty_dirs` | channel_size × 4, min 1024 = 524288 | `stream.rs:195` | 升级为 dirty_all 前上限 |
| `PENDING_MOVE_TIMEOUT` | 10s | `stream.rs:698` | Rename From 事件保留时间 |
| `cooldown_ns` (overflow) | 1s | `stream.rs:483` | fast_sync 前静默时间 |
| `max_staleness_ns` | 5s | `stream.rs:484` | 溢出最大过期时间 |
| `min_interval_ns` | 5s | `stream.rs:485` | fast_sync 最小间隔 |
| `failed_poll_interval` | 60s | `stream.rs:519` | 失败 root 轮询间隔 |
| `degraded_reconcile_interval` | 30s | `stream.rs:520` | 退化 root 遍历间隔 |
| `REBUILD_COOLDOWN` | 60s | `mod.rs:35` | rebuild 最小间隔 |
| `COMPACTION_DELTA_THRESHOLD` | 8 | `mod.rs:37` | 触发 compaction 的 delta 数 |
| `COMPACTION_MAX_DELTAS_PER_RUN` | 4 | `mod.rs:39` | 每次 compaction 合并 delta 上限 |
| `COMPACTION_COOLDOWN` | 300s | `mod.rs:41` | compaction 最小间隔 |
| `MIN_SNAPSHOT_INTERVAL` | 10s | `snapshot.rs:15` | snapshot 最小间隔 |
| `pending_events` cap | 4096 | `events.rs:278` | 待合并事件上限 |
| `upsert_events` batch | 2048 | `sync.rs:428,493` | fast_sync 中每批次数量 |
| `delete_events` chunk | 2048 | `sync.rs:526` | delete 应用每批次数量 |
| `scan_dirs_immediate` | 10 dirs, 10k entries | `sync.rs:629-632` | 即时扫描限制 |
| `scan_dirs_immediate_deep` | 10 dirs, 50k entries | `sync.rs:638-641` | 深扫限制 |

---

# 八、重构方案（基于新定位重新设计）

**来源**: 基于 "低占用、长待机、桌面文件搜索、API 响应、全文搜索铺垫" 的新定位
**调研**: plocate, fsearch, LMDB, Tantivy, Xapian, SQLite FTS5, Everything (voidtools)

---

## 8.1 调研结论：什么东西可以借鉴，什么东西不能用

### 可以借鉴的

| 工具 | 借鉴点 | 具体用在 fd-rdd 哪里 |
|------|--------|---------------------|
| **fsearch** | 排序数组 + 二分查找 FileEntry | 替换 HashMap<FileKey, DocId> |
| **plocate** | front-encoding 路径压缩 | 替换 PathArena，排序路径表+差量编码 |
| **plocate** | 排序 merge 更新 | 冷启动和 snapshot 时的批量归并 |
| **LMDB** | 单一 mmap 文件的持久化哲学 | 启发自定义持久化格式（不直接使用 LMDB）|
| **Everything** | io_uring 批量 stat 思想 | 冷启动用并行 walk 代替串行 open+ioctl+close |
| **Tantivy** | 全文搜索的 schema 设计 | 为后续全文搜索预留 ContentHint 字段 |

### 不能用的（过重或冲突新定位）

| 工具 | 原因 |
|------|------|
| **LMDB (heed)** | 引入 C 依赖，新定位追求零外部依赖和简单性。单一 mmap 文件 + 排序数组完全可以自己实现，1000 行代码无需 C 库 |
| **Xapian** | C++ 库，依赖重，全文搜索场景才需要 |
| **SQLite FTS5** | 对文件名搜索杀鸡用牛刀，trigram + Roaring 比 FTS5 快得多 |
| **Tantivy（目前）** | 同样面向全文搜索，对文件名级别的 trigram 搜索过重。**保留为未来全文搜索阶段的引擎候选** |
| **plocate 的全量替换更新** | fd-rdd 是实时监控引擎，不能像 plocate 那样定期 updatedb。需要用 inotify 增量更新 |

### fd-rdd 自身保留的部分

| 组件 | 保留原因 |
|------|---------|
| trigram + RoaringTreemap posting list | 对文件名搜索最优——AND 操作由 SIMD 优化的 C 代码完成 |
| short_component_index (u16 → RoaringTreemap) | 解决 1-2 字符查询，plocate/fsearch 都需要全扫描 |
| inotify/fanotify 实时监控 | fd-rdd 与 fsearch 的本质区别 |
| FileKey (dev, ino, generation) 去重 | inode 层面的文件身份追踪 |
| path_hash_to_id 用于路径反向查找 | 事件处理需要按路径查找 DocId |

---

## 8.2 新架构全景

```
┌──────────────────────────────────────────────────────────────┐
│  fd-rdd v2 架构                                              │
│                                                              │
│  ┌─ watcher ──────────────────────────────────────────────┐  │
│  │  inotify/fanotify → priority/normal channel             │  │
│  │  ★ 不管理的退化根目录 — 不轮询，只管理有 watch 的目录    │  │
│  └────────────────────────────────────────────────────────┘  │
│                            │                                 │
│  ┌─ EventPipeline ────────────────────────────────────────┐  │
│  │  debounce(10ms) → merge → apply → update delta buffer   │  │
│  │  ★ 无 overflow recovery loop                            │  │
│  │  ★ 无 Hybrid Crawler                                    │  │
│  │  ★ 事件使用 FileKey + CompSeq 替代 PathBuf              │  │
│  └────────────────────────────────────────────────────────┘  │
│                            │                                 │
│  ┌─ FileIndex (内存, 读写分离) ────────────────────────────┐  │
│  │                                                         │  │
│  │  ┌─ BaseIndex (ArcSwap, 只读) ──────────────────────┐  │  │
│  │  │  mmap 基础, 只读, 查询不持锁                       │  │  │
│  │  │  ┌─ PathTable ────────────────────────────────┐  │  │  │
│  │  │  │  排序路径表 + front-encoding 差量压缩      │  │  │  │
│  │  │  │  ~200MB (vs 当前 PathArena 800MB)         │  │  │  │
│  │  │  └───────────────────────────────────────────┘  │  │  │
│  │  │  ┌─ FileEntries (两个排序视图) ───────────────┐ │  │  │
│  │  │  │  by_filekey: [(FileKey, PathIdx)]          │ │  │  │
│  │  │  │  by_path:    [(PathIdx, FileKey)]          │ │  │  │
│  │  │  │  ~160MB × 2 = 320MB                        │ │  │  │
│  │  │  └───────────────────────────────────────────┘  │  │  │
│  │  │  ┌─ TrigramIndex ────────────────────────────┐ │  │  │
│  │  │  │  Trigram → RoaringTreemap (posting)       │ │  │  │
│  │  │  │  ~80MB                                     │ │  │  │
│  │  │  └───────────────────────────────────────────┘  │  │  │
│  │  │  ┌─ ParentIndex ─────────────────────────────┐ │  │  │
│  │  │  │  PathIdx → RoaringTreemap of DocIds       │ │  │  │
│  │  │  │  ~50MB                                     │ │  │  │
│  │  │  └───────────────────────────────────────────┘  │  │  │
│  │  │  ┌─ ContentHint (全文预留) ───────────────────┐ │  │  │
│  │  │  │  DocId → Option<ContentKey>               │ │  │  │
│  │  │  │  指向外部全文引擎 (Tantivy/SQLite FTS5)   │ │  │  │
│  │  │  │  ~8MB (8M × 1B Option)                    │ │  │  │
│  │  │  └───────────────────────────────────────────┘  │  │  │
│  │  └────────────────────────────────────────────────┘  │  │
│  │                                                       │  │
│  │  ┌─ DeltaBuffer (Mutex, 读写) ────────────────────┐  │  │
│  │  │  上次 snapshot 以来的变更                        │  │  │
│  │  │  最多 256K 条新增/修改项                         │  │  │
│  │  │  查询时: base ∩ delta → 最终结果                 │  │  │
│  │  │  snapshot 时: delta 归并到 base, 重写 mmap       │  │  │
│  │  └────────────────────────────────────────────────┘  │  │
│  └─────────────────────────────────────────────────────┘  │
│                                                              │
│  ┌─ Snapshot (每 300s) ──────────────────────────────────┐  │
│  │  base + delta → 排序 → 合并 → write_atomic mmap       │  │
│  │  ★ 无 LSM 段文件                                      │  │
│  │  ★ 无 compaction                                      │  │
│  │  ★ 无 WAL seal/checkpoint                             │  │
│  │  WAL 只记录自上次快照以来的事件, 崩溃恢复用            │  │
│  └───────────────────────────────────────────────────────┘  │
│                                                              │
│  ┌─ HTTP/UDS API ────────────────────────────────────────┐  │
│  │  /search, /status                                    │  │
│  │  ★ 查询不持锁, 直接读 BaseIndex + DeltaBuffer 快照    │  │
│  └──────────────────────────────────────────────────────┘  │
└──────────────────────────────────────────────────────────────┘
```

---

## 8.3 新数据结构定义

### 8.3.1 PathTable — 排序路径表 + front-encoding 差量压缩

```
原理 (学自 plocate/locate):
  路径按字典序排序后, 相邻路径共享长前缀:
  第1条: /home/shiyi/.bashrc           [完整: 20 bytes]
  第2条: /home/shiyi/.config/          [前11字节相同] → 存 (11, ".config/")
  第3条: /home/shiyi/.config/fd-rdd/   [前20字节相同] → 存 (20, "fd-rdd/")

存储格式:
  [anchor_interval: u16]  每 N 条存一个完整锚点 (建议 256)
  [entries: Entry × N]

  Entry:
    shared_len: varint    与前一条的共享前缀长度
    suffix_len: varint    新字节数
    suffix:     [u8; suffix_len]

随机访问:
  path[i] = 从最近的锚点开始, 依次应用差量直到条目 i
  最坏情况: 回溯 255 条 (~5KB 差量数据)
```

```rust
pub struct PathTable {
    /// mmap 的原始字节 (或 Vec<u8>)
    data: Vec<u8>,
    /// 每 256 个条目的锚点位置
    anchors: Vec<u32>,
    /// 条目数
    count: u32,
}

impl PathTable {
    /// 解析第 idx 个路径为绝对路径字节
    fn resolve(&self, idx: u32) -> &[u8];

    /// 第 idx 个路径的父目录路径索引
    fn parent_idx(&self, idx: u32) -> Option<u32>;

    /// 二分查找: 找到以 prefix 开头的路径范围
    fn find_prefix_range(&self, prefix: &[u8]) -> (u32, u32);
}
```

**内存**: 8M × 平均 25B (含差量编码) ≈ **200MB** (vs 当前 800MB)

### 8.3.2 FileEntry — 单文件条目

```rust
#[repr(C)]
#[derive(Copy, Clone, bytemuck::Pod, bytemuck::Zeroable)]
pub struct FileEntry {
    pub file_key: FileKey,   // 20 bytes (dev: u64, ino: u64, gen: u32)
    pub path_idx: u32,       // 4 bytes — in PathTable
    pub size: u64,           // 8 bytes
    pub mtime_ns: i64,       // 8 bytes
} // = 40 bytes

// 两个排序视图 (同一个 Vec<FileEntry>, 不同排序):
// by_filekey: 按 file_key 排序 → O(log N) 二分查找
// by_path:    按 path_idx 排序 → 路径字母序范围查询
```

**对比当前 CompactMeta**: 40B vs 44B。省了 root_id+seq_off+comp_count 的间接引用，直接存 path_idx。

### 8.3.3 TrigramIndex — trigram 倒排索引

```rust
pub struct TrigramIndex {
    /// trigram → posting list (RoaringTreemap)
    /// 热数据放内存, mmap 段保持只读
    entries: Vec<([u8; 3], RoaringTreemap)>,
    /// 短组件索引: u16 → RoaringTreemap
    short_components: Vec<(u16, RoaringTreemap)>,
}
```

保持当前实现。RoaringTreemap 的 AND 操作有 SIMD 优化，对 trigram 交集运算最优。

### 8.3.4 ParentIndex — 目录索引

```rust
pub struct ParentIndex {
    /// path_idx (目录) → 该目录下所有文件的 DocIds
    /// 用 RoaringTreemap 存储
    entries: Vec<(u32, RoaringTreemap)>,
}

impl ParentIndex {
    /// O(log N) 二分查找找到目录, 然后 O(1) 返回 bitmap
    fn files_in_dir(&self, dir_path_idx: u32) -> Option<&RoaringTreemap>;

    /// 多个目录的并集
    fn files_in_any_dir(&self, dirs: &[u32]) -> RoaringTreemap;
}
```

**消灭 `for_each_live_meta_in_dirs` 的 O(N) 遍历**: 直接返回 bitmap，复杂度 = O(脏目录数) + O(bitmap OR 操作)。

### 8.3.5 BaseIndex — 只读基础索引

```rust
pub struct BaseIndex {
    /// mmap 的文件内容
    mmap: Mmap,
    /// 路径表 (指向 mmap 内的 PathTable 区域)
    path_table: PathTableRef,
    /// 按 file_key 排序的文件条目 (指向 mmap)
    entries_by_key: &[FileEntry],
    /// 按 path_idx 排序的文件条目 (指向 mmap)
    entries_by_path: &[FileEntry],
    /// trigram 索引 (内存中, mmap 段 lazily 加载)
    trigram_index: TrigramIndex,
    /// 父目录索引 (指向 mmap)
    parent_index: ParentIndexRef,
    /// 墓碑 (RoaringTreemap, mmap'd)
    tombstones: RoaringTreemap,
}
```

### 8.3.6 DeltaBuffer — 运行时增量缓存

```rust
pub struct DeltaBuffer {
    /// 新增/修改的文件 (按 file_key 排序)
    added: Vec<FileEntry>,
    /// 已删除的文件 key
    removed: RoaringTreemap,
    /// 上次 snapshot 的文件计数
    base_file_count: u64,
    /// 容量上限 (超过则触发 snapshot)
    max_entries: usize,  // = 262144 (256K)
}

impl DeltaBuffer {
    fn file_count(&self) -> u64 {
        self.base_file_count + self.added.len() as u64 - self.removed.len() as u64
    }
}
```

**查询语义**: `base_index.entries_by_key ∩ delta.added ∪ delta.removed_complement`

### 8.3.7 ContentHint — 全文搜索预留

```rust
// 每个文件预留一个 Option<ContentKey>
// ContentKey 指向外部全文引擎中的文档 ID
pub type ContentKey = u64;

// 在 FileEntry 不存这个字段 — 另开一个数组:
// DocId → Option<ContentKey>
// 8M × 9 bytes (1B tag + 8B key) = ~72MB
// 若用 sparse: ~5-10MB (99% 文件无全文索引)

pub struct ContentMap {
    /// 稀疏存储: 只存有全文索引的文件
    entries: Vec<(u32, ContentKey)>,  // (DocId, ContentKey)
}
```

**交互方式**:
- 文件通过 inotify 发现 → fd-rdd 发通知 (RPC/回调) 给全文引擎 → 全文引擎提取内容索引
- fd-rdd 本身不处理文件内容
- `/search?q=content:hello` → fd-rdd 调用全文引擎的 `/search?q=hello` → 交集

---

## 8.4 简化后的组件清单

### KEEP (保留)

| 组件 | 位置 | 原因 |
|------|------|------|
| trigram + RoaringTreemap | l2_partition.rs | 文件名搜索最优，有 SIMD |
| short_component_index | l2_partition.rs | 短查询（1-2 字符）必须 |
| inotify/fanotify watcher | watcher.rs | fd-rdd 的核心差异 |
| FileKey 去重 | rdd.rs | 硬链接处理必须 |
| path_hash → DocId 反向索引 | l2_partition.rs | 事件处理按路径查找 |
| EventPipeline 主循环 | stream.rs | debounce + merge + apply |
| HTTP/UDS API | query_server | 核心功能 |
| IgnoreFilter (.gitignore) | ignore_filter.rs | 用户配置必须 |
| WAL (简化版) | wal.rs | 崩溃恢复 (只记录最近 300s 事件) |
| 快照循环 (简化版) | snapshot.rs | 持久化 (只写一个 mmap 文件) |

### REMOVE (删除)

| 组件 | 位置 | 原因 |
|------|------|------|
| 整个 LSM 多层段 (disk_layers) | mod.rs, snapshot.rs | 单一 mmap base + delta 替代 |
| 整个 compaction | compaction.rs | 无 LSM → 无 compact |
| 整个 DirtyTracker | recovery.rs | 无 overflow 恢复 → 无 dirty 跟踪 |
| Hybrid Crawler | stream.rs:506-566 | 冲突 "长待机、低占用" |
| reconcile_degraded_root | stream.rs:575-668 | 冲突 "长待机" |
| Overflow 恢复循环 | stream.rs:476-504 | 冲突 "长待机" |
| spawn_rebuild | sync.rs:300-334 | 无重建机制 |
| try_start_rebuild_with_cooldown | sync.rs:172-217 | 无重建机制 |
| finish_rebuild | sync.rs:219-261 | 无重建机制 |
| startup_reconcile | sync.rs:137-157 | 冷启动后直接构建，无需 reconcile |
| collect_dirs_changed_since | sync.rs:100-122 | DFS 遍历冲突 "低占用" |
| visit_dirs_since | sync.rs:12-84 | DFS 遍历冲突 "低占用" |
| for_each_live_meta_in_dirs | l2_partition.rs:886-920 | ParentIndex 替代 |
| OverlayState | events.rs | DeltaBuffer 替代 |
| apply_gate | mod.rs | 读写分离锁替代 |
| PendingMoveMap 复杂处理 | stream.rs | 简化 rename 跟踪 |
| RSS trim loop | main.rs | 低占用设计不需要 |

### SIMPLIFY (简化)

| 组件 | 当前 | 简化后 |
|------|------|--------|
| PersistentIndex | 7 把 RwLock + 10 个字段 | BaseIndex + DeltaBuffer = 2 个锁源 |
| EventPipeline | ~600 行 (含恢复) | ~250 行 |
| EventRecord | 含 PathBuf | 含 FileKey + CompSeq |
| snapshot_now | ~180 行 (含 LSM/compaction) | ~50 行 |
| WAL | LSM manifest seal | 简单事件日志 |
| TieredIndex | 19 个字段 | ~8 个字段 |
| CLI 参数 | 25 个 | ~12 个 |
| 总代码量 | ~15000 行 | ~7000-8000 行 |

---

## 8.5 稳态行为

```
空闲状态 (无文件系统活动):
  watcher:    inotify read() 阻塞, 0% CPU
  pipeline:   tokio 休眠, 0% CPU
  query:      HTTP/UDS 监听, 0% CPU
  snapshot:   每 300s, 仅 dirty 时执行 (通常 0% CPU)

  ★ 没有 Hybrid Crawler 的 30s DFS walk
  ★ 没有 DirtyTracker 的 200ms 轮询
  ★ 没有 compaction 的周期性合并
  ★ 没有 RSS trim 循环

  内存: base mmap (~20MB 冷页, 只访问时 fault in)
        + path_table 热页 (~30MB)
        + trigram_index 工作集 (~80MB)
        + parent_index 热页 (~20MB)
        + delta_buffer (~10MB)
        ≈ 150-180MB 常驻

  CPU:  ~0%

活跃状态 (文件操作):
  watcher:    事件推入 channel
  pipeline:   每 50ms 批处理
              ★ 批处理内部零 PathBuf 分配
              ★ 只用 path_idx + FileKey

  query:      读 BaseIndex (ArcSwap, 无锁)
              + 读 DeltaBuffer (Mutex 读锁, 短暂)
              ★ 读写不互斥

  CPU:  批处理期间 5-10% (峰值)
  内存: 稳定 (delta 有上限 256K)
```

---

## 8.6 四阶段实现计划

### 第一阶段: 砍代码 (1 周)

```
目标: 删除所有冲突"低占用、长待机"的代码

删除:
  - recovery.rs (整个文件)
  - compaction.rs (整个文件)
  - sync.rs 中: startup_reconcile, spawn_rebuild, try_start_rebuild_*,
    finish_rebuild, collect_dirs_changed_since, visit_dirs_since
  - stream.rs 中: 溢出恢复循环, Hybrid Crawler, reconcile_degraded_root
  - snapshot.rs 中: LSM bootstrap/append/compaction 触发

简化:
  - TieredIndex 字段从 19 个减到 ~8 个
  - CLI 参数从 25 个减到 ~12 个
  - 去掉 no_snapshot, no_watch, no_build 等无用参数

结果: 代码量 -40%, 编译更快, 没有周期性 O(N) 操作的代码路径
RSS: 不变 (数据结构还没改)
CPU: 空闲 0% (去掉所有周期性轮询)
```

### 第二阶段: 新数据结构 (2-3 周)

```
目标: PathTable + FileEntry + ParentIndex 替换当前 PersistentIndex

新增:
  - src/index/path_table.rs      # PathTable (排序路径表 + front-encoding)
  - src/index/file_entry.rs      # FileEntry + 排序视图
  - src/index/parent_index.rs    # ParentIndex (目录 → DocIds)
  - src/index/delta_buffer.rs    # DeltaBuffer (增量缓存)
  - src/index/base_index.rs      # BaseIndex (mmap 只读基础索引)
  - src/storage/snapshot_v7.rs   # v7 mmap 持久化格式

修改:
  - src/index/l2_partition.rs    # PersistentIndex → BaseIndex + DeltaBuffer
  - src/index/tiered/query.rs    # 适配新 path_idx 查询路径
  - src/index/tiered/sync.rs     # fast_sync delete 对齐用 ParentIndex
  - src/index/tiered/events.rs   # EventRecord 用 FileKey + path_idx
  - src/index/mmap_index.rs      # 适配 v7 段格式
  - src/util.rs                  # compose_abs_path 改为 path_table.resolve

结果:
  PathArena 800MB → PathTable 200MB
  HashMap 系列 → 排序数组 (200MB → 160MB)
  + ParentIndex 50MB
  ≈ 450MB (新) vs 700MB (当前)
CPU: 不变 (已在第一阶段降到 0%)
```

### 第三阶段: 事件管线零分配 (2 周)

```
目标: EventRecord 不再含 PathBuf, 全程用 path_idx + FileKey

修改:
  - src/core/lineage.rs          # EventRecordV2: 不含 PathBuf
  - src/event/stream.rs          # 适配无 PathBuf 事件流
  - src/event/watcher.rs         # 路径转 path_idx in pipeline
  - src/index/l1_cache.rs        # L1 用 path_idx 替 PathBuf key
  - src/index/tiered/events.rs   # apply 路径适配

结果:
  每事件: 0 个 PathBuf 分配 (vs 当前 1-4 个)
  每查询结果: path_table.resolve() 分配 (只在需要完整路径时)
  RSS: ~400MB
```

### 第四阶段: mmap 持久化 + 全文铺垫 (2-3 周)

```
目标: mmap 启动 + 路径差量编码存储 + ContentHint

新增:
  - src/storage/snapshot_v7.rs   # v7 格式正式实现
  - 启动时 mmap v7 文件, 冷页不占 RSS

修改:
  - src/main.rs                  # 简化启动流程
  - snapshot 循环                # 每 300s: delta → 排序 → 合并 → write_atomic

新增 (全文预留):
  - ContentMap (DocId → ContentKey) 稀疏存储
  - /search 支持 content: 查询语义
  - 全文引擎回调接口 (供外部集成)

结果:
  冷启动: mmap 文件 → 构建 trigram 索引 (内存) → 就绪 (无全量扫描)
  RSS: 100-180MB (mmap 冷页 + 内存热数据)

  重启恢复:
    加载 v7 mmap → 构建 trigram index → 回放 WAL (最近 300s 事件) → 就绪
    启动时间: 1-2 秒 (vs 当前 5-10 分钟的全量扫描)
```

---

## 8.7 内存效果总结

```
阶段                RSS (8M 文件)    CPU 空闲    启动时间     工作量    风险
───────────────────────────────────────────────────────────────────────
当前 (tests)        700MB + 尖峰     100% 峰值    5-10 分钟    -         -
第一阶段 (砍代码)   700MB            0%           5-10 分钟    1 周     低
第二阶段 (新结构)   450MB            0%           5-10 分钟    2-3 周   中
第三阶段 (零分配)   400MB            0%           5-10 分钟    2 周     中
第四阶段 (mmap)     100-180MB        0%           1-2 秒       2-3 周   高
───────────────────────────────────────────────────────────────────────
fsearch 参考值      150MB            -            N/A         -         -
```

**前两个阶段 (3-4 周)**: CPU 空闲 0% + RSS ~450MB。代码量 -40%。

**做完三个阶段 (5-7 周)**: RSS ~400MB + 批处理零分配。

**做完全部 (8-11 周)**: RSS 100-180MB，启动 1-2 秒，有全文搜索预留。接近 fsearch 水平但保留实时监控能力。

---

## 8.8 与 fsearch 的定位差异

| 维度 | fsearch | fd-rdd v2 |
|------|---------|-----------|
| 索引更新 | 手动 (用户触发重新扫描) | 实时 (inotify/fanotify) |
| 搜索 | GUI (GTK) | HTTP/UDS API |
| 持久化 | 停等模式, 写入完整文件 | 定期 snapshot + WAL |
| 全文搜索 | 不支持 | ContentHint 预留 |
| 内存 | 150MB / 百万文件 | 100-180MB / 8M 文件 |
| 适用场景 | 交互式文件查找 | 桌面守护进程 + API 服务 |

---

## 8.9 新持久化格式: v7

```
v7 文件格式 (单个 mmap 文件):

[ HEADER: 32 bytes ]
  magic:       u32 LE    [0xFDDD_0003]
  version:     u32 LE    [7]
  flags:       u32 LE
  file_count:  u64 LE
  path_table_off: u64 LE
  path_table_len: u64 LE

[ PathTable, 8B aligned ]
  anchor_interval: u16
  entry_count:     u32
  data_len:        u64
  anchors:  [(offset: u32)] × ceil(count / interval)
  entries:  [(shared_len: varint, suffix_len: varint, suffix: [u8])] × count

[ FileEntries (by_filekey), 8B aligned ]
  count: u32
  entries: [FileEntry; count]  (40B each, sorted by file_key)

[ FileEntries (by_path), 8B aligned ]
  count: u32
  entries: [FileEntry; count]  (40B each, sorted by path_idx)

[ TrigramIndex ]
  count: u32
  entries: [(tri: [u8;3], posting_off: u64, posting_len: u64)] × count
  postings: [u8]  (RoaringTreemap serialized blobs, contiguously)

[ ParentIndex ]
  count: u32
  entries: [(path_idx: u32, posting_off: u64, posting_len: u64)] × count
  postings: [u8]  (RoaringTreemap serialized blobs)

[ Tombstones ]
  RoaringTreemap serialized

[ ContentMap (optional) ]
  count: u32
  entries: [(docid: u32, content_key: u64)] × count

[ TRAILER: 32 bytes ]
  magic:          u32 LE    [0xFDDD_0003]
  checksum:       u32 LE    CRC32C of all content
  version:        u32 LE    [7]
  reserved:       u64
```

**特点:**
- 单文件, 直接 mmap, 不需要 MANIFEST.bin + seg-*.db + seg-*.del 三件套
- 所有排序数组可以直接 mmap 读取, 零拷贝二分查找
- 差量编码路径按需解码
- 启动: mmap 文件 → 构建 trigram 索引内存结构 → 回放 WAL → 就绪

---

## 8.10 关键文件变更清单

```
新增 (8 个文件):
  src/index/path_table.rs          # PathTable 实现
  src/index/file_entry.rs          # FileEntry + 排序视图
  src/index/parent_index.rs        # ParentIndex
  src/index/delta_buffer.rs        # DeltaBuffer
  src/index/base_index.rs          # BaseIndex
  src/storage/snapshot_v7.rs       # v7 格式读写

删除 (2 个文件):
  src/index/tiered/compaction.rs   # 无 LSM
  src/event/recovery.rs            # 无 DirtyTracker

大幅修改 (12 个文件):
  src/index/l2_partition.rs        # PersistentIndex → BaseIndex
  src/index/tiered/mod.rs          # TieredIndex 精简
  src/index/tiered/query.rs        # path_idx 查询
  src/index/tiered/sync.rs         # 去掉重建/恢复
  src/index/tiered/events.rs       # EventRecord 去 PathBuf
  src/index/tiered/snapshot.rs     # 简化 snapshot
  src/index/mmap_index.rs          # mmap 适配
  src/core/lineage.rs              # EventRecordV2
  src/core/rdd.rs                  # FileEntry, 扫描适配
  src/event/stream.rs              # 去恢复/爬虫
  src/event/watcher.rs             # 简化
  src/main.rs                      # 参数 + 启动精简

小幅修改 (5 个文件):
  src/index/l1_cache.rs            # L1 适配
  src/util.rs                      # 废弃 compose_*
  src/index/tiered/load.rs         # v7 加载
  src/index/tiered/rebuild.rs      # RebuildState 移除
  src/config.rs                    # 去无用参数
```

---

## 8.11 冷启动加速：io_uring 批量 statx

### 当前瓶颈

冷启动全量扫描（8M 文件），每个文件都走一遍 `get_file_generation()` (`src/core/rdd.rs:25-45`)：

```
每个文件:
  open(O_PATH)           ← 系统调用 1
  ioctl(FS_IOC_GETVERSION) ← 系统调用 2 (获取 ext4 generation)
  close()                ← 系统调用 3

8M 文件 = 2400 万次 syscall（仅 generation 部分）
加上 WalkBuilder 的 readdir + stat：~3200 万次 syscall 总计
```

### io_uring 方案

`FS_IOC_GETVERSION` 不能用 io_uring 批处理（io_uring 不支持 `ioctl` opcode）。但可以用 `statx` 批量获取元数据，用 `(stx_ino, stx_ctime_ns)` 作为 generation 替代——ctime 在 inode 复用时会变化，区分度足够。

**接入方式**：`io-uring` crate (tokio-rs)，feature flag `io-uring`，Linux 5.1+。

```
新增: src/core/uring.rs

IoUringScanner:
  batch_openat:   一次 io_uring_enter 批量打开 N 个文件
  batch_statx:    一次 io_uring_enter 批量获取 statx
  batch_getdents: Linux 5.15+ 批量读目录条目
```

### 预期效果

```
Phase 1 (batch statx):
  替换 open+ioctl+close 串行链
  2400 万 syscall → ~8000 次 io_uring_enter
  冷启动加速: 1.5-2x

Phase 2 (batch getdents, Linux 5.15+):
  替换 WalkBuilder 的 readdir 串行
  ~15K getdents64 → ~100 次 io_uring_enter
  冷启动加速: +1.2-1.5x

Phase 3 (full pipeline):
  所有 traversal + statx 在一个 ring 内完成
  冷启动加速: 总 2-4x
```

### 实现

```rust
// src/core/uring.rs (新文件, feature = "io-uring")
use io_uring::{IoUring, opcode, types};

pub struct IoUringScanner {
    ring: IoUring,
}

impl IoUringScanner {
    /// 批量获取 FileKey (无 open+ioctl+close)
    pub fn batch_file_keys(
        &self,
        paths: &[&std::path::Path],
    ) -> Vec<Option<FileKey>> {
        // 一次 io_uring_enter 提交所有 statx 请求
        // 用 stx_ino || stx_ctime_ns 代替 generation
    }
}
```

```toml
# Cargo.toml
[features]
io-uring = ["dep:io-uring"]

[dependencies]
io-uring = { version = "0.6", optional = true }
```

回退策略：`#[cfg(feature = "io-uring")]` — 非 Linux 或不支持时，回退到当前的串行 `open+ioctl+close`。
