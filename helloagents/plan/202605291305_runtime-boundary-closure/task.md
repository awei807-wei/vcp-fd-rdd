# Runtime Boundary Closure 任务清单

## A. Watcher / Scan 挂载边界

- [√] watcher dynamic scan 前执行 mount policy。
- [√] ephemeral watch 注册前执行 mount policy。
- [√] root 下子挂载点拒绝原因进入 watcher diagnostics reason matrix。
- [√] FUSE / SSHFS 初始化探测放入独立后台线程。
- [√] FUSE 探测配置 timeout，超时后扫描线程继续运行并记录 `FuseProbeTimeoutCount`。
- [√] scan thread 不直接对可疑 FUSE mount 执行可能永久挂起的 `readdir`。

## B. Quarantine Sidecar 后台验证

- [√] 实现 Sidecar Verify 后台线程或等价 runtime worker。
- [√] worker 校验 `root_path + mount_id/uuid + major:minor/source/fstype + affected_prefixes`。
- [√] 启动恢复时 Freeze Gate 先于 Sidecar Verify 生效。
- [√] 设备恢复时 append `ONLINE_ROOT`，解除 freeze，并入队 affected prefixes 局部 scan。
- [√] 设备仍离线时维持 freeze，禁止 DeltaBuffer 写入破坏性事件。
- [√] 查询默认隐藏 frozen root，debug/diagnostics 可观察冻结原因。

## C. Case Policy 探测补完

- [ ] 接入 pathconf 探测。
- [ ] pathconf 返回 `EINVAL` 时 fallback 到受控副作用探测。
- [ ] 只读 root / 无权限 root / 缺失 root 返回 `Unknown`。
- [ ] runtime detected policy / conflict count 只进入 runtime state 与 `/health.diagnostics`。
- [ ] Unicode fold trigram offset 使用 byte len，不使用 char len。
- [ ] 保留 `ß -> ss` 长度变化回归，防止索引越界 panic。

## D. Clock Skew Reconciliation

- [ ] 在 dirty loop / WAL flush loop 前接入 monotonic vs wall time detector。
- [ ] drift > 1s 时标记 mtime cutoff untrusted。
- [ ] 对受影响 root / prefixes 入队局部全量对账。
- [ ] 对账窗口内挂起增量 mtime 剪枝。
- [ ] 对账完成后恢复 cutoff trusted，并增加 reconciliation count。
- [ ] 不在 query hot path 执行时钟漂移检测。

## E. I/O Governor Diagnostics 与优雅降级

- [ ] full build / rebuild / fast-sync loop 接入 token bucket consume counters。
- [ ] PSI avg10、backoff count、当前 backoff 暴露到 `diagnostics.io`。
- [ ] ioprio 设置状态暴露到 `diagnostics.io`。
- [ ] Linux `IOPRIO_CLASS_IDLE` 使用 best-effort 调用。
- [ ] Docker / LXC 等沙箱中 `ioprio_set` 返回 `EPERM` 时不中断扫描。
- [ ] syscall 失败时回退到 token bucket + PSI 限制。

## F. Diagnostics / Health 收口

- [ ] `/health` 保持旧 summary 字段兼容。
- [√] `/health.diagnostics.watchers` 暴露 mount policy reason matrix。
- [√] `/health.diagnostics.storage` 暴露 quarantine sidecar verify 状态。
- [ ] `/health.diagnostics.clocks` 暴露 skew 与 reconciliation 状态。
- [ ] `/health.diagnostics.io` 暴露 ioprio / PSI / backoff / token bucket counters。
- [ ] config 文件不写入 runtime detected state。

## G. 单点测试

- [√] mount policy reason matrix 单元测试。
- [√] FUSE probe timeout helper 单元测试。
- [√] Sidecar identity confidence 单元测试。
- [√] Freeze Gate 阻止 Delete / Modify / Rename 单元测试。
- [ ] pathconf `EINVAL` fallback 单元测试。
- [ ] 只读 root `Unknown` 单元测试。
- [ ] Unicode fold byte offset 回归测试：`ß -> ss` 不 panic。
- [ ] ioprio `EPERM` / unsupported best-effort 单元测试。
- [ ] clock skew threshold 单元测试。

## H. 回归测试

- [ ] 普通 ext4/xfs root 默认扫描行为不变。
- [√] 显式 allow mount 后对应 mount 可扫描。
- [ ] 关闭 I/O governor 后扫描行为不变。
- [ ] 正常时间流逝时 fast-sync mtime 剪枝行为不变。
- [ ] root/system daemon 默认禁用无认证 HTTP query/scan。
- [ ] 旧 `roots = []` 与新 `[[roots]]` 配置继续可读。

## I. 集成与冒烟测试

- [ ] root 下被拒绝挂载点不会被扫描，并记录 skipped/reason 计数。
- [√] 离线 root 不产生破坏性 tombstone。
- [√] 离线 root 查询默认不可见，health 暴露 offline/quarantine/freeze 计数。
- [√] 挂载恢复后，存在路径通过局部 scan 复活。
- [ ] 回拨后创建 mtime 较旧的新文件，局部对账后可搜索。
- [ ] full build 扫描大量文件时 governor 计数递增，扫描仍完成。
- [√] daemon HTTP smoke：`/health` 返回 summary + diagnostics。
- [√] `scripts/smoke-search-syntax.sh` 通过。

## J. 文档与收尾

- [√] 更新 `README.md` 边界策略说明。
- [√] 更新 `CHANGELOG.md`。
- [√] 更新 `helloagents/wiki/runtime-boundary-state-contract.md`。
- [√] 更新 `helloagents/wiki/stage-progress.md`。
- [√] 更新本任务清单状态。
- [ ] 迁移方案包至 `helloagents/history/2026-05/`。
- [ ] 更新 `helloagents/history/index.md`。
