# M2 proc sampler 提前触发 Ephemeral Watch 任务

## 实施任务

- [√] 新增 `src/event/proc_sampler.rs`，实现 `/proc/<pid>/fdinfo` 写 fd 采样
- [√] 配置新增 `[proc_sampler]`，支持 enabled、interval 和预算项
- [√] tiered runtime 增加 proc sampler 最近报告与 `/watch-state` 字段
- [√] `main.rs` 启动 proc sampler loop，并复用 Ephemeral Watch command 入口
- [√] `/health`、diagnostics、metrics JSONL 同步 proc sampler 字段与 issue
- [√] README、CHANGELOG、tests README、watcher wiki、stage progress 同步

## 测试项

- [√] fdinfo flags 解析和写权限判断
- [√] 同 uid 写 fd 采样出父目录，预算计数可观测
- [√] 非同 uid pid 不继续读取 fdinfo
- [√] watcher report 暴露 proc sampler 观测字段
- [√] health/metrics JSON 序列化 proc sampler 字段和 issue

## 验收标准

- [√] `cargo test -q proc_sampler`
- [√] `cargo test -q health`
- [√] `cargo test -q tiered_watch`
- [√] `cargo test -q`
- [√] `cargo fmt --check`
- [√] `git diff --check`
