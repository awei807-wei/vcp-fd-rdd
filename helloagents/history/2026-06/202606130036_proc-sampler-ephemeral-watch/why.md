# M2 proc sampler 提前触发 Ephemeral Watch 需求

## 背景

路线基准已收敛为“热点 5 秒 + 全局有界最终一致 + 返回前验真”。M0 和 M1 已完成 rename 可见性、查询返回前验真、运行时 subtree tombstone、分片 repair 与 `/health` 追平上界观测。下一步体验关键件是让持续写入目录在没有查询触发的情况下也能更快进入现有临时 watcher 覆盖。

## 问题

ComfyUI、下载器、渲染器等进程会长时间持有输出文件写句柄。此前 fd-rdd 只能依赖已有 watcher、fast scan、查询 stale/miss 或周期 repair 发现这些目录；对尚未进入 hotset 或还未被查询命中的目录，用户可能先感知到“候选集合暂时不全”。

## 目标

- 新增 Linux `/proc/<pid>/fd` + `fdinfo` 写句柄采样。
- 只扫描同用户进程，读取有明确预算，避免跨用户窥探或无界遍历。
- 采样结果只作为新鲜度线索，不能作为查询正确性来源。
- 发现正在写入的索引根内目录后，复用现有 Ephemeral Watch lease 机制触发临时 watcher，不新增独立 watcher 架构。
- 在 `/watch-state`、`/health`、diagnostics 与 metrics JSONL 暴露采样耗时、预算、命中目录和触发 watcher 计数。

## 验收

- 同用户写 fd 能采样到父目录，非同 uid 进程不会继续读取 fdinfo/fd link。
- 采样受 pid、fd、目录数预算限制，预算耗尽可观测。
- tiered 模式下 proc sampler 能把采样目录送入现有 Ephemeral Watch 入口。
- `/health` 与 metrics 输出 proc sampler 字段和预算/不可用 issue。
- `cargo test -q proc_sampler`、`cargo test -q health`、`cargo test -q tiered_watch`、完整 `cargo test -q` 无新增失败。
