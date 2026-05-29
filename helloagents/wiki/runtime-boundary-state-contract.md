# Runtime Boundary State Contract

## 决策

fd-rdd 的远程/虚拟文件系统边界使用物理 root 状态契约，而不是 PathId/DocId 状态契约。

恢复顺序固定为：

```text
Load Snapshot
-> Attach WAL
-> Restore Quarantine Sidecar
-> Install Freeze Gates
-> Replay WAL Root State and File Events
-> Init DeltaBuffer/Event Pipeline
-> Sidecar Verify
```

WAL root state 会叠加到 sidecar 恢复出的 quarantine state 上；如果 WAL 中已有 `ONLINE_ROOT`，它会覆盖旧 sidecar 中仍 active 的离线 root。这样既保证 Freeze Gate 在事件管道启动前生效，又避免旧 sidecar 覆盖 WAL replay 的最终事实。

## Quarantine 主键

WAL 记录 root 级状态：

- `OFFLINE_ROOT`
- `ONLINE_ROOT`

sidecar 记录：

- `root_path`
- `mount_id`
- `major:minor`
- `fs_uuid?`
- `source`
- `fstype`
- `affected_prefixes`

`PathId` / `DocId` 禁止作为 quarantine 持久主键。原因是 snapshot 重写、LSM compaction、DocId 重排和 cold remount 都可能改变内存/段内身份；mount 物理身份才是跨恢复周期稳定锚点。

恢复置信度：

- `fs_uuid` 命中最强。
- `major:minor + source + fstype` 次之。
- `root_path` 只绑定用户配置，不单独证明设备恢复。

## Freeze Gate

Freeze Gate 必须先于 Sidecar Verify 安装。否则在校验空窗期内，新的 OS 事件可能进入 DeltaBuffer，造成离线 root 的 delete/modify/rename 脏写。

冻结状态下：

- Delete / Modify / Rename 不写 WAL、DeltaBuffer 或 L2。
- 查询默认隐藏 frozen path。
- Sidecar Verify 匹配当前 mount identity 后先 append `ONLINE_ROOT` 到 WAL。
- `ONLINE_ROOT` 写入成功后才解除 freeze，并把 affected prefixes 加入局部对账队列。
- 仍离线则维持 freeze，禁止破坏性 tombstone。

## 配置与运行时状态

用户配置只保存意图：

```toml
[[roots]]
path = "/mnt/samba"
case_policy = "Auto"
allow_remote = false
one_file_system = true
```

运行时探测状态不写回 `config.toml`：

- `detected_policy`
- `conflict_count`
- mount state
- freeze gate

这些状态只通过 runtime state 与 `/health.diagnostics` 暴露。

## Case Policy 探测

`case_policy = "Auto"` 的 root 使用分层探测：

- 先消费明确的 fstype hint。
- 支持 `_PC_CASE_SENSITIVE` 的平台先调用 `pathconf`。
- `pathconf` 返回 `EINVAL` 或平台不支持该常量时，回落到受控临时对象副作用探测。
- 只读、无权限、缺失或非目录 root 返回 `Unknown`，不把探测副作用写入 root。

Unicode fold 只用于 lookup key。原始 path bytes 仍是展示、PathTable 和持久化事实；fold 后 trigram 必须按 byte window 生成，避免 `ß -> ss` 这类长度变化污染原始 offset。

## Diagnostics

`/health` 保留旧 summary 字段，并新增强类型 `DiagnosticReport`：

- `system`
- `storage`
- `security`
- `clocks`
- `watchers`
- `io`

子模块实现 `DiagnosticSource`，由注册/收集机制写入固定板块，避免继续把 health 扁平字段扩成不可维护的大杂烩。

### Watcher Mount Policy

`diagnostics.watchers` 汇总同一组共享 mount policy 计数器：

- full build / rebuild walker。
- fast-sync / immediate scan walker。
- watcher dynamic watch 注册。
- ephemeral watch 注册。

动态 watcher 在调用 `watch()` 和触发后续深扫前必须先执行 mount policy。拒绝时回滚 tiered 或 ephemeral runtime reservation，并只记录拒绝原因，不进入被拒绝子树。

FUSE/SSHFS 可疑 mount 会进入后台 probe timeout cache。扫描线程只消费 `Pending` / `Ready` / `TimedOut` / `Failed` 缓存状态：`Ready` 后允许继续扫描，`Pending`、`TimedOut`、`Failed` 时保守拒绝；显式 `allow_mounts` 继续作为人工 override 放行，并记录 `allowed_override_count`。

字段含义：

- `denied_mount_count`：mount policy 拒绝总数。
- `fstype_blocked_count`：被 deny fstype 或 FUSE 类 fstype 拒绝的次数。
- `network_fs_ignored_count`：默认不允许远程文件系统时被拒绝的次数。
- `one_file_system_boundary_count`：跨 root 文件系统边界被拒绝的次数。
- `fuse_probe_timeout_count`：FUSE/SSHFS 探测超时次数。
- `allowed_override_count`：显式 allow mount 放行次数。

### Quarantine Verify

`diagnostics.storage` 暴露 sidecar verify 的运行状态：

- `quarantine_roots`：当前 active quarantine root 数。
- `freeze_gates`：当前 frozen root/prefix 数。
- `freeze_blocked_events`：Freeze Gate 拦截的破坏性事件数。
- `quarantine_verify_pending`：仍待确认 online 的 sidecar root 数。
- `quarantine_verified_roots`：本进程已验证并写入 `ONLINE_ROOT` 的 root 数。

## 产品安全策略

fd-rdd 坚持 per-user daemon。每个用户在自己的 session 里启动自己的进程，依赖进程权限天然隔离。

root/system daemon 不作为多用户查询入口；默认禁用未认证 HTTP query/scan。UDS 继续保持 same-user peer auth。
