# 任务清单：竞态修复、路径治理与 git 历史清理（2026-04-18）

状态符号:
- [ ] 待执行
- [√] 已完成
- [X] 执行失败
- [-] 已跳过
- [?] 待确认

## 背景

物理机上 fd-rdd RSS 超过 4GB，经排查根因是默认 snapshot 路径落在 tmpfs 上。
同时 CI 测试存在竞态问题，且 git 历史被 [Snow Team] 自动提交污染。

## 执行清单

### Critical

- [√] 修复 PersistentIndex upsert 竞态：引入 `upsert_lock` 写锁，保证
  `alloc_docid → insert_trigrams → insert_path_hash` 的原子性
- [√] 修复 Linux `default_snapshot_path()`：fallback 从 `/tmp` (tmpfs)
  改为 `dirs::data_local_dir()` (`~/.local/share/fd-rdd/`)，避免 LSM mmap 计入 RSS
- [√] 更新 systemd 服务文件 `scripts/fd-rdd.service`，引导使用 config.toml
- [√] 清理 git [Snow Team] 脏历史：使用 `git filter-branch` 批量移除所有
  `[Snow Team]` 前缀，删除遗留的 `snow-team/*` 分支与 `refs/original` 备份

### Medium

- [√] main 分支 README.md 添加 config.toml 使用说明与示例
- [√] tests 分支 README.md 同步 v0.5.6 更新日志（中文搜索修复）
- [√] tests 分支 Cargo.toml 版本号同步至 0.5.6

### Low

- [√] 排查确认 helloagents 中无 "Linux snapshot 路径改为持久化用户目录" 的
  历史记录（当时只改了 Windows 分支和配置文件路径）

## 涉及文件

- `src/index/l2_partition.rs` — 竞态修复
- `src/config.rs` — snapshot 默认路径修复
- `scripts/fd-rdd.service` — systemd 启动方式注释
- `README.md` — config.toml 使用说明
- `helloagents/CHANGELOG.md` — 已存在相关记录，本次未新增

## 验收

- [√] `cargo check` 通过
- [√] `cargo test` 全量通过（152 passed）
- [√] git 历史无 `[Snow Team]` 残留（`git log --oneline --all | grep "\[Snow Team\]`" 为空）
- [√] main 分支提交信息已规范重写
- [√] 未执行 git push
