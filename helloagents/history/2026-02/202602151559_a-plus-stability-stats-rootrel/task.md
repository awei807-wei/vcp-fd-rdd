# 阶段 A+：任务清单（已执行）

- [√] rebuild 冷却与合并：在 `TieredIndex` 内实现 cooldown + coalesce，防止事件风暴频繁重扫
- [√] 内存统计校准：输出 arena/hashmap capacity/bytes 与 roaring serialized_size 拆项
- [√] root 相对路径压缩：`root_id + relative_path`，并升级快照到 v5（含 roots_hash 校验）
- [√] 验证：`cargo test --offline` 通过（15 tests）

