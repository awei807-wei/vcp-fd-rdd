# 搜索忽略感知、冷层轮转更新与增量快照方案原因

## 背景

本方案包来自一次真实运行态排查：

- `/home/shiyi/.local/share/fcitx5/rime/custom_phrase_double.txt` 文件存在，但 `/search?q=custom_phrase_double` 返回空。
- 直接原因不是索引没有更新，而是 `/home/shiyi/.local/share/fcitx5/rime/.gitignore` 明确写了 `custom_phrase_double.txt`，当前 `ignore_enabled = true`，fd-rdd 冷扫、手动 `/scan` 和事件过滤都会遵守 ignore 规则。
- 当前 `/search` 只查询已进入索引的可见集合，被 `.gitignore` 过滤的真实文件不会出现，也没有说明，用户会误判为“没更新”“漏索引”或 watcher 失效。
- 最近 metrics 还显示 tiered runtime 中部分目录处于 L3/frozen manifest only 口径，`Documents`、`Downloads` 因单根 watch 成本超过 `l0_max_cost_per_root=8192` 未进入实时 L0，依赖周期冷扫、query repair、fast scan hotset 和 DirtyQueue 保持最终一致。
- metrics 的 RSS 曲线呈“快照/flush 尖峰 + 回落”，最大约 880 MiB；代码路径 `snapshot_now()` 会通过 `materialize_snapshot_base()` 把当前可见集合重新物化成完整 `BaseIndexData` 再写 v7，快照阶段是临时分配大户。

## 用户诉求

1. 搜索被 `.gitignore` 忽略的文件时，至少给出明确提醒，不要静默空结果。
2. L1/L2/L3 非实时更新时，希望按“金字塔旋转”方式临时提高冷层的新鲜度：让 L3、L2 分时获得类似更高层的更新待遇，完成后恢复原 tier。
3. 快照不要每次都完整重建一份，可否使用 diff，降低内存尖峰和写放大。

## 问题本质

### 1. 被 ignore 的文件缺少“可解释不可见性”

fd-rdd 当前把 ignore 规则当作索引准入边界，规则本身是正确的；问题是查询层没有把“文件存在但被规则排除”反馈给用户。

这类反馈不能简单等同于把 ignored 文件纳入主索引，否则会破坏 `.gitignore` 的性能、隐私和语义边界。正确目标是建立低成本、可控、可关闭的“忽略提示”通道。

### 2. 冷层不是实时失效，而是缺少主动轮换的新鲜度加速

当前 L3 表示最终一致，不应被解释为实时 watcher 覆盖。直接把整个 L3 临时升为 L1/L0 会遇到两个硬约束：

- `notify::RecursiveMode::Recursive` 的真实 watch 成本可能非常高，不能对 `Documents`、`Downloads` 这类大树整棵注册。
- 直接轮换并降级现有热层会牺牲当前活跃目录的新鲜度，可能把“冷目录追平”变成“热目录漏更新”。

因此用户的“360 度旋转”应落地为：保持正式 L0/热 hotset 不被抢占，给 L2/L3 目录发放有预算的临时 watch/fast-scan/DirtyQueue 加速窗口，按时间片轮流覆盖冷层。

### 3. 当前快照是完整物化边界

当前快照语义偏向“完整可见集合的物化边界”：把 cold base、L2、overlay 折叠成新的完整 v7，再重新 cold remount。它简单可靠，但会在大索引下产生明显临时内存和 CPU 峰值。

diff snapshot 的本质不是在单个 v7 内写二进制差分，而是恢复“mmap base + delta segment + manifest + compaction”的 LSM 形态：普通 snapshot 只写增量段，达到阈值时再后台合并。

## 成功标准

- 查询被 `.gitignore` 排除的文件时，能返回结构化提示：命中路径、忽略来源、规则类型、建议动作；默认不把 ignored 文件混入主索引。
- 提示通道必须有预算保护：不在查询线程做全量 home 遍历；不读取文件内容；只在 configured roots 内工作；尊重安全边界和 exclude policy。
- L2/L3 新鲜度有可观测的轮转加速：冷层目录的最坏追平时间下降，且正式 L0/hotset SLA 不回退。
- 快照高峰 RSS 明显下降：常规增量 flush 不再全量物化 70 万级路径；完整 compaction 变为低频、可限速、可观测后台任务。
- `/health`、`/watch-state`、metrics 能解释：ignored hint 状态、轮转进度、冷层最坏新鲜度、delta segment 数量、compaction 状态和快照写放大。

## 非目标

- 不默认把 `.gitignore` 文件加入主索引。
- 不绕过 `.gitignore`、`.ignore`、`.git/info/exclude`、git global ignore 的语义。
- 不为追求冷层及时性而整棵注册超大目录 watcher。
- 不删除现有 stable v7 冷启动路径。
- 不一次性替换全部 snapshot/recovery 机制；增量快照必须可灰度、可回滚、可恢复审计。
