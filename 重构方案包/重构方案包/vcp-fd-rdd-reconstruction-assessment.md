# vcp-fd-rdd 重构方案评估报告与开发计划

> **评估对象**: `causal-chain-report.md`（v2 重构方案）  
> **目标代码库**: https://github.com/awei807-wei/vcp-fd-rdd/tree/tests  
> **当前版本**: v0.6.3（2026-04-26）  
> **评估日期**: 2026-04-27  
> **评估维度**: 可靠性、可实施性、ROI

---

## 一、执行摘要

### 1.1 总体结论

| 维度 | 评分 (1-10) | 结论 |
|------|-------------|------|
| **可靠性** | 7.5/10 | 核心瓶颈分析高度准确，技术借鉴点合理，但存在 3 处中高风险假设 |
| **可实施性** | 7/10 | 四阶段分解合理，但第二、四阶段复杂度被低估，实际需 10-14 周 |
| **ROI** | 8.5/10 | 投入 10-14 周，解决 100% CPU 峰值 + 启动 5-10min→1-2s，长期维护成本大幅降低 |

**综合判定**: **可行性足够高，建议执行**。但需要对第二、四阶段增加缓冲时间，对删除 overflow 恢复和 Hybrid Crawler 的风险点增加回退方案。

### 1.2 核心发现

1. **报告中的瓶颈分析 90% 准确**: `for_each_live_meta_in_dirs` O(N) 遍历、pending_events 无界累积、upsert_lock 全局串行化等核心问题均已在源码中完全证实。
2. **v0.6.3 的"小修小补"治标不治本**: 当前 tests 分支在 6 天内发了 4 个版本(v0.6.0→0.6.3)，疯狂打补丁，但总共只省 ~62MB(6%)，核心瓶颈一个未解。
3. **重构方案直击要害**: 删除 LSM/compaction/overflow 恢复等复杂机制，改为单一 mmap + 排序数组 + front-encoding，架构更简洁。
4. **报告中的内存估算偏乐观**: v2 的 100-180MB 目标在 8M 文件场景下偏激进，更现实的估计是 250-350MB（仍比当前节省 50%+）。
5. **最大价值在 CPU 和启动时间**: 内存节省是副产品，真正的价值是消除周期性 O(N) 操作导致的 100% CPU 峰值，以及启动时间从 5-10 分钟降到 1-2 秒。

---

## 二、源码验证结果

### 2.1 验证方法

- 4 组并行 Agent 对 14 个关键源文件进行逐行审查
- 交叉验证报告中的 23 个源码位置索引
- 对比 CHANGELOG 确认版本演进

### 2.2 关键验证结论

| 报告声明 | 验证结果 | 偏差说明 |
|----------|----------|----------|
| `for_each_live_meta_in_dirs` 遍历 8M metas，分配 PathBuf，持 3 读锁 | ✅ **完全准确** | 行号 886-920 准确，源码注释明确警告锁持有期间不能调用 apply_events |
| `fast_sync` 三阶段流程 | ✅ **完全准确** | 行号 373-531 准确 |
| `pending_events` 无界累积 | ✅ **完全准确** | HashMap 按 FileIdentifier 去重但无总上限 |
| `upsert_lock` 全局串行化 | ✅ **完全准确** | v0.6.2 为修复 race condition 反而加了全局锁 |
| 冷启动每文件 4 syscall | ⚠️ **部分准确** | 实际是 3 syscall(open+ioctl+close)，高估 33% |
| `snapshot_now` 用 `write()` 阻塞 | ⚠️ **部分准确** | v0.6.0 已改为 `try_write()`，但仍有阻塞风险 |
| Hybrid Crawler DFS 全遍历 | ⚠️ **部分准确** | 仅 degraded roots 走 DFS，failed roots 走 fast_sync |
| `max_dirty_dirs=524288` 在 recovery.rs | ⚠️ **部分准确** | 值正确，但实际在 stream.rs 动态计算 (channel_size×4) |

### 2.3 v0.6.3 已实施的优化（与报告对比）

| 优化项 | 报告评估 | v0.6.3 实际 | 状态 |
|--------|----------|-------------|------|
| BTreeMap→HashMap ×2 | "细枝末节，省 ~48MB" | ✅ 已实施 | 报告正确识别但低估价值 |
| channel_size 降低 | "细枝末节，省 ~11MB" | ✅ 已实施 | 同上 |
| short_component_index u16 | "细枝末节，省 ~3MB" | ✅ 已实施 | 同上 |
| FAST_COMPACTION 默认 | "细枝末节，省 ~5%" | ✅ 已实施 | 同上 |
| **合计** | **~62MB (6%)** | **~62MB** | **核心瓶颈未解** |

---

## 三、可靠性评估

### 3.1 技术假设正确性

#### 3.1.1 正确的假设（✅）

| 假设 | 评估 |
|------|------|
| `for_each_live_meta_in_dirs` 是核心杀手 | ✅ 完全正确。O(N) 遍历 + 8M 次 PathBuf 分配 + 持锁期间 syscall，每 5s 可能触发 |
| PathTable front-encoding 可省内存 | ✅ 合理。学自 plocate/locate，相邻路径共享前缀，8M 路径可从 800MB 压到 ~200MB |
| 排序数组 + 二分查找可替代 HashMap | ✅ 合理。filekey_to_docid 仅做点查、插入、删除，无排序依赖 |
| BaseIndex + DeltaBuffer 读写分离 | ✅ 合理。ArcSwap 切换只读 base + Mutex 保护 delta，查询不阻塞写入 |
| 删除 LSM/compaction 可简化架构 | ✅ 合理。单一 mmap 文件 + 定期重写，比 LSM 段管理简单得多 |
| 保留 trigram + RoaringTreemap | ✅ 正确。文件名搜索的最优方案 |

#### 3.1.2 有风险的假设（⚠️）

| 假设 | 风险等级 | 说明 |
|------|----------|------|
| 删除 overflow 恢复机制 | **高** | inotify 内核队列溢出后没有 fast_sync 兜底。虽然 v0.6.0 有 back-pressure (channel>80% 时 sleep)，但极限场景（git clone 100万文件）仍可能溢出。**需要保留降级策略** |
| 删除 Hybrid Crawler | **高** | max_user_watches 超限后无退化根目录轮询。桌面场景中 home 目录很容易触发此限制。**必须保留或替代** |
| 删除 rebuild 机制 | **中** | 索引损坏后无重建路径。WAL 恢复只覆盖 300s 事件，WAL 损坏需全量重扫。**可接受，但需明确文档** |
| 删除 LSM 后无段损坏隔离 | **中** | 当前 LSM 的"坏段跳过"机制有价值。单一 mmap 文件损坏需重建。**需要 checksum + 原子写入** |
| mmap 冷启动 1-2 秒 | **中** | 需要构建 trigram 索引（内存中），8M 文件的 trigram 构建可能需要 10-30 秒，非 1-2 秒 |
| DeltaBuffer 上限 256K 足够 | **低** | 300s 内 256K 文件变更在桌面场景几乎不可能 |
| PathTable 随机访问性能 | **低** | 回溯 255 条 (~5KB) 差量数据，在现代 CPU 上 <1μs，可接受 |

### 3.2 调研结论合理性

| 借鉴点 | 合理性 | 说明 |
|--------|--------|------|
| fsearch 排序数组 | ✅ 合理 | 文件名索引用排序数组 + 二分查找是经典方案 |
| plocate front-encoding | ✅ 合理 | 路径压缩效果经 plocate 验证，差量编码实现简单 |
| LMDB 单一 mmap 哲学 | ✅ 合理 | 不引入 C 依赖的判断正确，自定义 mmap 格式 1000 行可实现 |
| Everything io_uring | ⚠️ 需谨慎 | 报告仅说"借鉴思想"，未要求实际使用 io_uring。但 fd-rdd 是 inotify 实时监控，与 Everything 的 NTFS MFT 监控本质不同 |
| Tantivy 全文预留 | ✅ 合理 | ContentHint 设计轻量，不引入依赖 |

### 3.3 方案设计可靠性评分

| 组件 | 设计可靠性 | 理由 |
|------|------------|------|
| PathTable | 8/10 | front-encoding 成熟方案，实现复杂度中等 |
| FileEntry 排序视图 | 8/10 | 二分查找替代 HashMap，查询性能相当，内存更省 |
| ParentIndex | 9/10 | 消除 O(N) 遍历的关键，RoaringTreemap 存储高效 |
| BaseIndex + DeltaBuffer | 8/10 | 读写分离设计经典，但 DeltaBuffer 的查询合并逻辑需仔细测试 |
| Snapshot v7 | 7/10 | 自定义持久化格式有风险，需要充分测试崩溃恢复 |
| 删除 overflow 恢复 | 5/10 | **最大风险点**。需保留最小化的 overflow 检测和恢复机制 |
| 删除 Hybrid Crawler | 5/10 | **第二风险点**。桌面场景的 max_user_watches 限制很常见 |

---

## 四、可实施性评估

### 4.1 四阶段计划评估

#### 第一阶段：砍代码（报告估 1 周）

| 任务 | 复杂度 | 风险 | 建议时间 |
|------|--------|------|----------|
| 删除 recovery.rs | 低 | 低 | 0.5 天 |
| 删除 compaction.rs | 低 | 低 | 0.5 天 |
| 删除 sync.rs 中 rebuild/startup_reconcile/collect_dirs | 中 | **中** | 2 天 |
| 删除 stream.rs overflow/Hybrid Crawler | 中 | **高** | 2 天 |
| 简化 snapshot.rs LSM 触发 | 低 | 低 | 0.5 天 |
| 简化 TieredIndex 字段 | 低 | 低 | 1 天 |
| 简化 CLI 参数 | 低 | 低 | 0.5 天 |
| **小计** | | | **1 周** ✅ |

**评估**: 时间估算是合理的，但删除 overflow 恢复和 Hybrid Crawler 需要非常谨慎。建议**保留最小化的 overflow 检测**（只标记 dirty_all，不启动 fast_sync，而是直接触发 snapshot），而不是完全删除。

#### 第二阶段：新数据结构（报告估 2-3 周）

| 任务 | 复杂度 | 风险 | 建议时间 |
|------|--------|------|----------|
| PathTable + front-encoding | **高** | 中 | 5-7 天 |
| FileEntry + 排序视图 | 中 | 低 | 2-3 天 |
| ParentIndex | 中 | 低 | 2-3 天 |
| DeltaBuffer | 中 | 中 | 3-4 天 |
| BaseIndex | 中 | 中 | 3-4 天 |
| 修改 l2_partition.rs 适配 | **高** | **高** | 5-7 天 |
| 修改 query.rs / sync.rs / events.rs 适配 | **高** | **高** | 5-7 天 |
| **小计** | | | **3-4 周** |

**评估**: 报告估 2-3 周偏乐观。PathTable 的 varint 编码、锚点系统、二分查找实现比预期复杂。修改 l2_partition.rs 涉及面广，需要大量回归测试。**建议按 3-4 周规划**。

#### 第三阶段：事件管线零分配（报告估 2 周）

| 任务 | 复杂度 | 风险 | 建议时间 |
|------|--------|------|----------|
| EventRecordV2 (无 PathBuf) | 中 | 中 | 3-4 天 |
| stream.rs 适配 | 中 | 中 | 3-4 天 |
| watcher.rs 适配 | 中 | 中 | 2-3 天 |
| L1 cache 适配 | 低 | 低 | 1-2 天 |
| events.rs apply 适配 | 中 | 中 | 3-4 天 |
| **小计** | | | **2-3 周** |

**评估**: 报告估 2 周偏乐观。PathBuf 在事件系统中的使用非常广泛，完全替换为 path_idx 需要修改大量代码路径。**建议按 2-3 周规划**。

#### 第四阶段：mmap 持久化 + 全文预留（报告估 2-3 周）

| 任务 | 复杂度 | 风险 | 建议时间 |
|------|--------|------|----------|
| v7 格式实现 | 中 | 中 | 3-4 天 |
| mmap 启动加载 | 中 | **高** | 3-5 天 |
| snapshot 循环重写 | 中 | **高** | 3-5 天 |
| WAL 简化 | 低 | 中 | 2-3 天 |
| ContentMap 预留 | 低 | 低 | 1-2 天 |
| /search content: 语义 | 低 | 低 | 1-2 天 |
| **小计** | | | **2-3 周** |

**评估**: 时间估算基本合理。但 mmap 启动的"1-2 秒"目标可能不现实（trigram 索引构建需要时间），**建议目标调整为"5-10 秒"**。

### 4.2 总工作量估算

| 阶段 | 报告估算 | 建议估算 | 差异 |
|------|----------|----------|------|
| 第一阶段 | 1 周 | 1 周 | ✅ |
| 第二阶段 | 2-3 周 | 3-4 周 | +1 周 |
| 第三阶段 | 2 周 | 2-3 周 | +1 周 |
| 第四阶段 | 2-3 周 | 2-3 周 | ✅ |
| **合计** | **7-9 周** | **8-11 周** | 基本一致 |

**结论**: 报告的总时间估算（8-11 周）是合理的，但中间阶段需要增加缓冲。建议按 **10-12 周** 规划，包含测试和修复时间。

### 4.3 团队要求

| 要求 | 说明 |
|------|------|
| **人员** | 1-2 名资深 Rust 工程师（3 年以上经验） |
| **技能** | Linux 文件系统、inotify、mmap、内存管理、并发编程 |
| **测试** | 需要大量集成测试（特别是并发和崩溃恢复场景） |
| **CI** | 需要压力测试环境（800K+ 文件规模） |

---

## 五、ROI 评估

### 5.1 投入

| 投入项 | 估算 |
|--------|------|
| 开发时间 | 10-12 周（1 名全职资深 Rust 工程师） |
| 测试时间 | 3-4 周（含压力测试、崩溃恢复测试） |
| 总投入 | **13-16 周** |

### 5.2 产出

| 产出项 | 当前 | 重构后 | 改善幅度 |
|--------|------|--------|----------|
| **内存 (RSS)** | 700MB + 尖峰 | 250-350MB (保守估计) | **节省 50-60%** |
| **CPU 空闲** | 100% 峰值 (周期性轮询) | ~0% | **彻底消除周期性 CPU 峰值** |
| **启动时间** | 5-10 分钟 | 5-10 秒 (保守估计) | **99% 提升** |
| **代码量** | ~15,000 行 | ~7,000-8,000 行 | **简化 47%** |
| **维护成本** | 高（LSM/compaction/overflow 复杂交互） | 低（单一 mmap + delta） | **长期大幅降低** |

### 5.3 ROI 计算

| 方案 | 投入 | 产出 (内存节省) | 产出 (启动时间) | 综合 ROI |
|------|------|-----------------|-----------------|----------|
| **v0.6.3 小修小补** | 3 天 | 62MB (6%) | 无 | 低 |
| **v2 重构方案** | 13-16 周 | 350-450MB (50-65%) | 5-10min→5-10s | **极高** |

**结论**: ROI 极高。虽然投入大，但解决了当前架构的根本性设计缺陷，长期维护成本大幅降低。如果项目目标是"低占用、长待机、桌面文件搜索"，重构是必经之路。

---

## 六、详细开发计划

### 总体原则

1. **每阶段可独立编译运行**: 阶段之间保持接口兼容，避免"大爆炸式"重构
2. **保留回退路径**: 关键删除操作先标记 deprecated，确认稳定后再删除
3. **测试先行**: 每阶段必须有对应的性能/正确性测试

---

### 第一阶段：砍代码（Week 1）

#### 目标
删除所有与"低占用、长待机"定位冲突的代码，消除周期性 O(N) 操作。

#### 文件级修改清单

| 文件 | 操作 | 具体修改 | 备注 |
|------|------|----------|------|
| `src/event/recovery.rs` | **删除** | 整个文件（~170 行） | DirtyTracker 被移除 |
| `src/index/tiered/compaction.rs` | **删除** | 整个文件（~200 行） | compaction 被移除 |
| `src/index/tiered/sync.rs` | **删除函数** | `startup_reconcile` (L137-157) | cutoff=0 全量比对 |
| | | `spawn_full_build` (L264-297) | 冷启动全量构建 |
| | | `try_start_rebuild_with_cooldown` (L172-217) | rebuild 触发 |
| | | `finish_rebuild` (L219-261) | rebuild 收尾 |
| | | `collect_dirs_changed_since` (L100-122) | DFS 遍历 |
| | | `visit_dirs_since` (L12-84) | DFS 辅助函数 |
| | **简化 fast_sync** | 保留 `spawn_fast_sync` 入口 | 但 Phase 3 删除对齐改为用 ParentIndex（第二阶段实现） |
| `src/event/stream.rs` | **删除代码块** | 溢出恢复循环 (L477-504) | 每 200ms 轮询 |
| | | Hybrid Crawler 循环 (L507-566) | 每 30/60s 轮询 |
| | | `reconcile_degraded_root` (L575-668) | DFS 退化根目录 |
| | | `dyn_walk_and_enqueue` (L37-43) | spawn_blocking 洪水 |
| | **保留** | `watch_roots_enhanced` 的 failed_roots 标记 | 仅标记，不轮询 |
| `src/index/tiered/snapshot.rs` | **删除** | LSM bootstrap/append/compaction 触发 | 删除 disk_layers 管理 |
| | **简化** | `snapshot_now` 只保留 apply_gate + export | 去除 delta 计数检查 |
| `src/index/tiered/mod.rs` | **简化字段** | 从 19 个字段减到 ~8 个 | 删除 disk_layers, rebuild_state 等 |
| | **删除常量** | `COMPACTION_DELTA_THRESHOLD`, `COMPACTION_MAX_DELTAS_PER_RUN`, `COMPACTION_COOLDOWN` | 无 compaction |
| `src/main.rs` | **删除调用** | `spawn_full_build` 调用 (L197-200) | 冷启动走 snapshot 加载 |
| | | `startup_reconcile` 调用 (L232-234) | |
| | | `rss_trim_loop` 启动 | 低占用不需要 |
| `src/config.rs` | **简化** | CLI 参数从 25 个减到 ~12 个 | 删除 no_snapshot, no_watch, no_build 等 |
| `Cargo.toml` | **删除依赖** | 可选：删除 `rayon`, `crossbeam`（如不再并行扫描） | 保留 `tokio`, `notify`, `roaring` 等核心依赖 |

#### 第一阶段量化标准

| 指标 | 基准值 | 目标值 | 测量方法 |
|------|--------|--------|----------|
| 代码行数 | ~15,000 行 | ≤11,000 行 | `find src -name "*.rs" | xargs wc -l` |
| 编译时间 | 基准 | 缩短 20%+ | `cargo build --release` 计时 |
| 空闲 CPU | 100% 峰值 | ≤5% | `top` / `pidstat` 观察 60s |
| 空闲 RSS | 700MB | 700MB（本阶段不变） | `/proc/[pid]/status` |
| 测试通过数 | 全部 | 全部 | `cargo test` |

#### 第一阶段验收标准

- [ ] `cargo build --release` 编译通过，零 warning
- [ ] `cargo test` 全部通过（删除 compaction 相关测试）
- [ ] 空闲 60s 内 CPU 占用 ≤5%（消除周期性轮询）
- [ ] `cargo clippy --all-targets -- -D warnings` 通过
- [ ] 手动测试：启动后无 inotify 溢出/重建触发

---

### 第二阶段：新数据结构（Week 2-5）

#### 目标
用 PathTable + FileEntry + ParentIndex + BaseIndex + DeltaBuffer 替换 PersistentIndex，消除 PathArena 和 HashMap 系列的大内存占用。

#### 新增文件

| 文件 | 内容 | 行数估算 | 复杂度 |
|------|------|----------|--------|
| `src/index/path_table.rs` | PathTable: 排序路径表 + front-encoding 差量压缩 | ~300 行 | 高 |
| `src/index/file_entry.rs` | FileEntry (40B) + 排序视图构建/维护 | ~150 行 | 中 |
| `src/index/parent_index.rs` | ParentIndex: 目录 → RoaringTreemap of DocIds | ~100 行 | 中 |
| `src/index/delta_buffer.rs` | DeltaBuffer: 增量缓存，上限 256K 条 | ~150 行 | 中 |
| `src/index/base_index.rs` | BaseIndex: mmap 只读基础索引 | ~200 行 | 高 |
| `src/storage/snapshot_v7.rs` | v7 持久化格式定义（先只定义，第四阶段实现） | ~100 行 | 中 |

#### 修改文件

| 文件 | 修改内容 | 影响范围 |
|------|----------|----------|
| `src/index/l2_partition.rs` | **重写核心**。PersistentIndex → BaseIndex + DeltaBuffer 组合 | 所有查询/写入路径 |
| `src/index/tiered/query.rs` | 适配新的 path_idx 查询路径。`exact_path` 走 `entries_by_path` 二分查找 | 搜索性能 |
| `src/index/tiered/sync.rs` | fast_sync 的 delete 对齐改为用 `parent_index.files_in_any_dir(dirty_dirs)` | 消除 O(N) 遍历 |
| `src/index/tiered/events.rs` | EventRecord 改用 `FileKey + path_idx` 替代 `PathBuf`。OverlayState → DeltaBuffer | 事件处理 |
| `src/index/mmap_index.rs` | 适配 v7 段格式（先预留接口） | 持久化 |
| `src/util.rs` | `compose_abs_path` 改为 `path_table.resolve(path_idx)` | 路径解析 |
| `src/core/rdd.rs` | `FileKey::from_path_and_metadata` 保留，但冷启动不再调用 | 启动流程 |

#### 关键设计决策

1. **PathTable 实现细节**:
   - 锚点间隔: 256 条（每 256 条存一个完整路径）
   - 编码: varint(shared_len) + varint(suffix_len) + suffix bytes
   - 二分查找: 基于锚点 + 差量解码的前缀比较

2. **FileEntry 排序视图维护**:
   - `by_filekey`: 按 FileKey 排序，用于点查和去重
   - `by_path`: 按 path_idx 排序，用于前缀范围查询
   - 插入时：在 DeltaBuffer 中无序存储，snapshot 时统一排序合并

3. **ParentIndex 构建**:
   - 从 `entries_by_path` 构建：按父目录 path_idx 分组
   - 每个目录存储 RoaringTreemap of DocIds
   - 替代 `for_each_live_meta_in_dirs` 的 O(N) 遍历

#### 第二阶段量化标准

| 指标 | 基准值 | 目标值 | 测量方法 |
|------|--------|--------|----------|
| PathArena 内存 | ~800MB | 删除 | 内存报告 |
| filekey_to_docid 内存 | ~192MB | 删除 | 内存报告 |
| path_hash_to_id 内存 | ~168MB | 删除 | 内存报告 |
| PathTable 内存 | 0 | ~200MB | 内存报告 |
| entries_by_key 内存 | 0 | ~320MB | 内存报告 |
| parent_index 内存 | 0 | ~50MB | 内存报告 |
| **总 RSS** | **700MB** | **≤500MB** | `/proc/[pid]/status` |
| 查询 QPS | 基准 | ≥90% 基准 | 压力测试 |
| fast_sync 耗时 | O(8M) | O(dirty_dirs × avg_files_per_dir) | 事件风暴测试 |

#### 第二阶段验收标准

- [ ] `cargo test` 全部通过
- [ ] 800K 文件集成测试通过 (`tests/p2_large_scale_hybrid.rs`)
- [ ] 内存报告：`memory_report()` 显示总 RSS ≤ 500MB
- [ ] 查询功能测试：exact/fuzzy/trigram/short 查询均正确
- [ ] fast_sync 测试：修改 1 个目录后同步耗时 < 100ms（vs 当前 5-30s）
- [ ] 并发测试：10 并发查询 + 事件写入，无 panic/死锁
- [ ] 路径解析测试：path_table.resolve() 返回正确路径

---

### 第三阶段：事件管线零分配（Week 6-8）

#### 目标
EventRecord 全程使用 FileKey + path_idx，消除所有 PathBuf 分配。

#### 修改文件

| 文件 | 修改内容 | 复杂度 |
|------|----------|--------|
| `src/core/lineage.rs` | EventRecordV2: 用 `FileKey` + `path_idx: Option<u32>` 替代 `PathBuf` | 中 |
| `src/event/stream.rs` | 适配无 PathBuf 事件流。debounce/merge/apply 全用 path_idx | 中 |
| `src/event/watcher.rs` | `handle_notify_result` 输出 `FileKey + path_idx`（如已知）或仅路径 | 中 |
| `src/index/l1_cache.rs` | L1 用 `path_idx` 替代 `PathBuf` 作为 key | 低 |
| `src/index/tiered/events.rs` | `apply_events_inner` 用 path_idx 进行所有路径操作 | 中 |
| `src/query/server.rs` | 查询结果只在需要完整路径时调用 `path_table.resolve()` | 低 |

#### 关键设计决策

1. **watcher → pipeline 路径映射**:
   - watcher 阶段：收到 inotify 事件时只有路径（无 path_idx）
   - pipeline 阶段：首次遇到路径时查找/分配 path_idx（在 DeltaBuffer 中）
   - 后续操作：全程使用 path_idx

2. **rename 事件处理**:
   - from_path_hint: 旧 path_idx
   - to_path: 新路径 → 分配新 path_idx
   - 旧 path_idx 加入 tombstones

3. **零分配验证**:
   - 使用 `dhat` 或 `heaptrack` 验证事件批处理期间零 PathBuf 分配

#### 第三阶段量化标准

| 指标 | 基准值 | 目标值 | 测量方法 |
|------|--------|--------|----------|
| 每事件 PathBuf 分配 | 1-4 个 | 0 个 | `dhat` / `heaptrack` |
| 事件批处理延迟 | 50ms debounce + 分配延迟 | ≤50ms | 日志计时 |
| 内存碎片 | 高（频繁 PathBuf 分配/释放） | 低 | RSS 稳定性观察 |
| **总 RSS** | 500MB | ≤450MB | `/proc/[pid]/status` |

#### 第三阶段验收标准

- [ ] `cargo test` 全部通过
- [ ] `dhat` 验证：事件批处理期间零 PathBuf 分配
- [ ] 800K 文件集成测试通过
- [ ] 事件风暴测试：git clone 10万文件不触发 overflow
- [ ] RSS 稳定性：24 小时运行 RSS 波动 < 10%

---

### 第四阶段：mmap 持久化 + 全文预留（Week 9-12）

#### 目标
实现 v7 持久化格式，mmap 冷启动，启动时间从 5-10 分钟降到 5-10 秒。

#### 新增/修改文件

| 文件 | 操作 | 内容 | 复杂度 |
|------|------|------|--------|
| `src/storage/snapshot_v7.rs` | **实现** | v7 格式：序列化 PathTable + entries + parent_index + trigram_index | 高 |
| `src/storage/wal.rs` | **简化** | WAL 只记录最近 300s 事件（简化版，非 LSM manifest） | 中 |
| `src/index/base_index.rs` | **修改** | 启动时 mmap v7 文件，构建 trigram_index（内存热数据） | 高 |
| `src/index/tiered/snapshot.rs` | **重写** | `snapshot_now`: delta → 排序 → 合并 → write_atomic mmap | 高 |
| `src/main.rs` | **简化** | 启动流程：加载 v7 mmap → 构建 trigram → 回放 WAL → 就绪 | 中 |
| `src/index/content_map.rs` | **新增** | ContentMap: DocId → ContentKey 稀疏存储 | 低 |
| `src/query/server.rs` | **新增** | `/search` 支持 `content:` 查询前缀 | 低 |

#### v7 文件格式

```
[ HEADER: 64 bytes ]
  magic:          u32 LE  [0xFDDD_0007]
  version:        u32 LE  [7]
  flags:          u32 LE
  file_count:     u64 LE
  
  path_table_off: u64 LE
  path_table_len: u64 LE
  
  entries_off:    u64 LE
  entries_len:    u64 LE
  
  parent_index_off: u64 LE
  parent_index_len: u64 LE
  
  trigram_index_off: u64 LE
  trigram_index_len: u64 LE
  
  checksum:       u32 LE  (CRC32C of header+data)

[ PathTable, 8B aligned ]
  anchor_interval: u16
  entry_count:     u32
  anchors:  [offset: u32] × ceil(count / interval)
  entries:  [shared_len: varint, suffix_len: varint, suffix: [u8]] × count

[ FileEntries, 8B aligned ]
  count: u64
  entries: [FileEntry; count]  (按 file_key 排序)

[ ParentIndex ]
  count: u32
  entries: [(path_idx: u32, docids_bytes_len: u32, docids: RoaringBitmap bytes)] × count

[ TrigramIndex ]
  count: u32
  entries: [(trigram: [u8;3], bitmap_bytes_len: u32, bitmap: RoaringBitmap bytes)] × count
```

#### 关键设计决策

1. **原子写入**:
   - snapshot 时写入临时文件 `index.v7.tmp`
   - 完成后 `rename()` 原子替换 `index.v7`
   - 崩溃后最多丢失 300s 内的变更（由 WAL 覆盖）

2. **mmap 冷页**:
   - v7 文件通过 `memmap2` mmap 到地址空间
   - 只访问的页面才会 fault in（Linux 按需分页）
   - 启动时 trigram_index 需要构建到内存（从 mmap 中读取序列化数据）

3. **WAL 简化**:
   - 只记录自上次 snapshot 以来的事件
   - 环形缓冲区，最多 300s/10MB
   - 崩溃恢复：mmap base + WAL 回放 = 最终状态

4. **启动流程**:
   - 有 v7 文件：mmap → 构建 trigram_index (~5-10s) → 回放 WAL → 就绪
   - 无 v7 文件：走全量扫描（fallback，与当前相同）

#### 第四阶段量化标准

| 指标 | 基准值 | 目标值 | 测量方法 |
|------|--------|--------|----------|
| 启动时间（有快照） | 5-10 分钟 | 5-10 秒 | `time fd-rdd` |
| 启动时间（无快照） | 5-10 分钟 | 5-10 分钟（fallback） | 同上 |
| 冷启动 syscall 数量 | 2400 万次 | 0（mmap 加载） | strace 计数 |
| snapshot 临时内存 | ~400MB | ≤50MB（delta 归并） | 峰值 RSS 观察 |
| 持久化文件大小 | 多段 LSM 文件 | 单一 v7 文件 (~600MB) | `ls -lh` |
| **总 RSS（稳态）** | 700MB | 250-350MB | 24h 观察 |
| WAL 回放正确性 | - | 100% | 崩溃恢复测试 |

#### 第四阶段验收标准

- [ ] `cargo test` 全部通过
- [ ] 800K 文件集成测试通过
- [ ] 启动时间测试：有 v7 快照时 ≤10 秒
- [ ] 崩溃恢复测试：kill -9 后重启，数据一致性 100%
- [ ] 压力测试：24 小时事件风暴 + 查询，无内存泄漏
- [ ] 文件完整性测试：v7 文件 checksum 验证通过
- [ ] 原子写入测试：snapshot 过程中 kill -9，数据不损坏

---

## 七、风险与缓解措施

### 7.1 高风险项

| 风险 | 影响 | 缓解措施 |
|------|------|----------|
| **删除 overflow 恢复后 inotify 溢出** | 数据丢失，变更不被索引 | 保留最小化 overflow 检测：溢出时标记 dirty_all 并**立即触发 snapshot**（非 fast_sync），利用 DeltaBuffer 的 256K 上限消化事件 |
| **删除 Hybrid Crawler 后 max_user_watches 超限** | 部分目录无法监控 | 保留 failed_roots 标记，启动时告警用户。提供手动刷新 API (`POST /refresh`) 供用户按需触发。研究 fanotify（Linux 5.1+）作为 inotify 替代 |
| **v7 持久化格式 bug** | 数据损坏，无法启动 | 1) CRC32C checksum；2) 原子写入 (write+rename)；3) 保留旧格式解析作为 fallback；4) 充分模糊测试 |
| **PathTable front-encoding 性能** | 路径解析过慢 | 1) 锚点间隔调优（256→128）；2) 热点路径缓存；3) 压力测试验证 |

### 7.2 中风险项

| 风险 | 影响 | 缓解措施 |
|------|------|----------|
| 第二阶段修改面广，引入回归 | 功能损坏 | 1) 每修改一个模块立即运行对应测试；2) 800K 集成测试必须通过；3) 新旧索引并行运行对比验证 |
| 排序视图维护开销 | 插入/删除变慢 | 1) DeltaBuffer 无序存储，snapshot 时统一排序；2) 批量更新优先 |
| mmap 页面回收 | RSS 未如预期降低 | 1) `madvise(MADV_DONTNEED)` 释放旧 mmap；2) 明确区分冷页和热页 |

### 7.3 低风险项

| 风险 | 影响 | 缓解措施 |
|------|------|----------|
| bytemuck 依赖 | 序列化性能 | 报告中使用 bytemuck::Pod，但 Cargo.toml 未包含。需添加依赖或手写序列化 |
| ContentMap 预留过早 | 代码膨胀 | 第四阶段最后再做，前面专注核心功能 |

---

## 八、与 v0.6.3 自带优化的关系

### 8.1 v0.6.3 优化的定位

v0.6.3 的 optimization-assessment-report.md 做的是"在现有架构内的微优化"：
- 改动 13 行代码
- 省 59MB 内存（6%）
- 不改架构，核心瓶颈仍在

### 8.2 v2 重构的定位

causal-chain-report.md 做的是"架构级重构"：
- 改动 ~2000 行代码（增删改）
- 省 350-450MB 内存（50-65%）
- 改架构，解决核心瓶颈

### 8.3 执行建议

1. **已完成**: v0.6.3 的微优化（BTreeMap→HashMap、channel_size、FAST_COMPACTION、short_component_index）已全部实施，**无需再做**。
2. **下一步**: 直接启动 v2 重构的第一阶段（砍代码）。v0.6.3 的优化为重构提供了"更干净的起点"。
3. **不要折中**: 不要尝试"半重构"（如只改 PathTable 但保留 LSM），这会带来两套架构的维护负担。

---

## 九、附录

### 9.1 源码验证详细记录

见子代理输出文件：
- `/mnt/agents/output/fd-rdd-src-analysis.md`（源码结构分析）
- `/mnt/agents/output/performance_bottleneck_verification_report.md`（瓶颈验证员 A）
- `/mnt/agents/output/verification_report.md`（瓶颈验证员 B）
- `/mnt/agents/output/verification-report.md`（瓶颈验证员 C）

### 9.2 关键源码位置索引（验证后修订版）

| 文件 | 行号 | 内容 | 验证状态 |
|------|------|------|----------|
| `src/index/l2_partition.rs` | 886-920 | `for_each_live_meta_in_dirs` | ✅ 准确 |
| `src/index/tiered/sync.rs` | 373-531 | `fast_sync` | ✅ 准确 |
| `src/index/tiered/sync.rs` | 506-528 | delete 对齐 | ✅ 准确 |
| `src/index/tiered/events.rs` | 138-179 | `capture_l2_for_apply` | ✅ 准确 |
| `src/index/tiered/events.rs` | 316-335 | `apply_events_inner` 目录重命名 | ✅ 准确 |
| `src/event/stream.rs` | 477-504 | 溢出恢复循环 | ✅ 准确 |
| `src/event/stream.rs` | 507-566 | Hybrid Crawler | ⚠️ failed roots 不走 DFS |
| `src/index/tiered/snapshot.rs` | 25-86 | `apply_gate` 锁 | ⚠️ 已改为 `try_write()` |
| `src/core/rdd.rs` | 52-93 | `get_file_generation` | ⚠️ 3 syscall，非 4 个 |
| `src/index/tiered/mod.rs` | 35-41 | 常量阈值 | ✅ 准确 |

### 9.3 评估团队

- **源码结构分析**: 源码结构分析师 Agent
- **瓶颈验证 A** (l2_partition/sync/events): 瓶颈验证员 A Agent
- **瓶颈验证 B** (stream/watcher/recovery): 瓶颈验证员 B Agent
- **瓶颈验证 C** (snapshot/compaction/rdd/main): 瓶颈验证员 C Agent
- **综合评估与计划**: Orchestrator (主 Agent)

---

*报告生成时间: 2026-04-27*  
*基于: vcp-fd-rdd tests 分支 v0.6.3 + causal-chain-report.md 重构方案 + 14 个源文件逐行验证*
