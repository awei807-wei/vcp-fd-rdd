# 历史记录索引

## 2026-05

- `202605021152_persistentindex-fileentry-paths`：将 `PersistentIndex` 运行时主存储迁移到 `FileEntry + paths`，旧 `CompactMeta + PathArena` 降级为兼容读写格式。
- `20260502_wrapup-stabilization`：收敛 causal-chain 与 wrap-up 后续合并问题，恢复编译/测试，移除 `disk_layers` 热路径和目录 rename 同步深扫。
