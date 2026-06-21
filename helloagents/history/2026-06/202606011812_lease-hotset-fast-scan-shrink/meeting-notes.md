# 沟通纪要

> 说明：本文件保留原始方案讨论过程。可执行结论已收敛到本方案包的 `why.md`、`how.md`、`task.md`；不再以“暂不定稿实施”作为当前状态。

## 1. 初始问题：新文件 5 秒 SLA 未达成

用户反馈 ComfyUI output 下新生成 PNG 搜不到，期望新文件 5 秒进入索引。排查后发现：

- 目标文件真实存在。
- 当前运行态不是文件不存在问题。
- fast scan bootstrap 曾把单批预算误当成 sentinel 总量上限，导致后续深目录没有覆盖。
- 新 sentinel 初始基线不补扫，可能吞掉覆盖前已发生的创建。

前一轮已修复：

- `l1_l2_fast_scan_bootstrap_budget_per_tick` 恢复为每 tick 注册预算。
- 新注册 sentinel 会进入一次 `FastScanChangedDir` 初始补扫。
- 目标 PNG 手动局部 `/scan` 后已可搜索。

## 2. 新问题：启动 CPU 峰值

用户提出每次启动项目 CPU 暴涨，怀疑是索引数据库不可信时触发重建。运行态检查显示：

- `snapshot_source = stable`
- `recovery_requires_rebuild = false`
- `startup_repair_ran = false`
- `is_rebuilding = false`

结论：不是每次启动 hard rebuild，而是 fast scan sentinel 是内存态，重启后重新 bootstrap，并因为新 sentinel 初始补扫产生启动 CPU 峰值。

## 3. 周期性 CPU 峰值

用户继续指出，即使启动问题解决，5 秒周期扫描新文件也会导致周期性 CPU 上升。讨论后确认：

- 如果非 L0 目录没有 watcher 覆盖，却要求 5 秒发现，就必须在 5 秒窗口内主动检查。
- 对几千目录做周期 stat/readdir，在低性能设备上会有可感知成本。
- 继续在 fast scan 内再做冷热分层，会形成“分层之上再分层”的复杂度套娃。

## 4. 业界参考讨论

外部反馈提到 Watchman、notify-rs、Syncthing、Nextcloud 等。

讨论结论：

- Watchman 的 cookie/barrier 适合 watcher 覆盖范围内的事件队列同步，但不能替代非 L0 目录扫描。
- Syncthing 的落盘 metadata + scan fallback 值得参考。
- notify-rs 的 PollWatcher 说明 polling 是显式 fallback，不应伪装成 native watcher 等价能力。
- 当前 fd-rdd 文件索引已落盘，真正未落盘的是 fast scan sentinel / runtime 覆盖状态。

## 5. 产品语义重新收敛

阶段性共识：

- 没有真正两全之策。
- 全局非 L0 5 秒发现不可避免地等价于轮询 watcher。
- 合理语义应转为“热点 5 秒，全局最终一致”。
- L0 / explicit hot roots / lease hotset 承诺 5 秒。
- 普通非 L0 best-effort + query repair + low-frequency scan。

## 6. 对“显式订阅路径”的质疑

用户认为手动指定哪些路径 5 秒订阅发现更新比较笨。讨论后调整为：

- 显式配置不应是主路径，只作为高级 override。
- 默认应是自动热点 lease hotset。
- 系统根据查询、stale hit、project marker、近期事件等自动给目录发放临时 5 秒 lease。

## 7. 关于多线程扫描

用户提出扫描只读，是否可以多线程并行，写索引 FIFO。

讨论后结论：

- 多线程扫描可以降低高性能设备上的发现延迟。
- 但它不能降低总 CPU/IO，低性能设备上可能更糟。
- 写入不能只靠 FIFO，因为旧扫描结果可能晚于新事件，覆盖更新状态。
- 正确模型应是并行 scan worker 产出 diff batch，apply actor 带 `event_seq` / `dir_epoch` / generation guard 串行写入。

## 8. 已定稿方向

已定稿为 M3：lease hotset 正式化 + fast scan 收缩。可执行方向为：

- 自动热点 lease hotset 为主。
- 显式 hot roots / subscription 作为高级 override。
- fast scan 只覆盖 lease hotset。
- sentinel registry 持久化只服务 hotset。
- 启动治理 Phase 2/4 并入 M3；Phase 3 收缩为 hotset-only sentinel。
- 多线程扫描只在有界并发、压力感知、apply 带 generation guard 的前提下评估。
- 普通非 L0 保持有界最终一致。

正式实施任务以本方案包 `task.md` 为准。
