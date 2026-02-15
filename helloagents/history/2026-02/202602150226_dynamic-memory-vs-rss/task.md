# 动态内存优化：任务清单（mimalloc + 路径进一步压缩）

## 本次新增任务（来自你刚才提的 1/2）

- [√] 可选分配器：接入 mimalloc feature（不默认开启）
- [√] 路径压缩方案落地：相对 root 存储（`root_id + relative_path`，快照升级至 v5 并带 roots_hash 校验）

## 当前剩余开发任务（包含之前路线与本次新增）

### A+（仍属于阶段 A 的动态侧优化）

- [√] 事件风暴稳定性：优化 overflow→rebuild 的冷却/合并策略（避免频繁重建制造高水位）
- [√] reset/rebuild 内存回吐：重建时用“后台构建新索引 + ArcSwap 原子切换”整体替换关键结构，释放旧 capacity
- [√] 内存统计继续校准：补充 arena bytes / hashmap capacity / roaring serialized_size 的拆项展示

### 阶段 B（你最初的“胶水+持久化”目标）

- [√] ArcSwap：读链路无锁化（用 ArcSwap 原子替换新索引，后台构建不阻塞查询）
- [√] mmap 段式索引：替代 bincode 快照，冷启动 mmap 映射（posting lazy decode；manifest 目前为手写二进制，后续可替换为 rkyv）
- [√] 段式格式设计：Trigram 段/Metadata 段/Path 段 + per-segment 校验与版本迁移策略（v6）

### 明确抛弃/延后

- [-] fanotify：暂不做（后续结合 watcher 架构与阶段 B 一起评估）
