# fast scan SLA 回归修复方案

## 方案

1. 调整 `TieredWatchRuntime::should_bootstrap_fast_scan_dirs*`：
   - 保留 cooldown 与非 L0 目录检查。
   - 移除 `known_dirs + l0_dirs < candidate_limit` 的总量上限语义。
   - 当初始补扫队列积压达到单批预算时暂停 bootstrap，避免无限堆积。

2. 调整 `bootstrap_fast_scan_dirs`：
   - 成功注册 sentinel 后把目录加入 `changed_dir_queue`。
   - 复用现有 `fast_scan_tick` 的 changed-dir readdir 预算和 DirtyQueue 应用路径。

3. 更新测试：
   - 修改旧测试对首次 tick 无 changed dir 的过时假设。
   - 新增单元测试验证单批预算不是总量上限。
   - 新增单元测试验证 bootstrap 会触发初始补扫。

4. 更新知识库：
   - README / CHANGELOG 说明 bootstrap 预算与初始补扫语义。
   - 维护 `helloagents/wiki/tiered-watcher-runtime.md`。
