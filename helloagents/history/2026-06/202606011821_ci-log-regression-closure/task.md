# CI 日志回归收敛任务

- [√] 扫描 `/home/shiyi/Downloads/vcp-FD/logs` 并按实际退出码区分失败与脚本分支文本。
- [√] 确认 `CI` workflow 多节点失败均来自 `index::tiered::tests::fast_sync_reconciles_add_and_delete`。
- [√] 确认 `fd-rdd Stress Tests` 实际失败节点为 `test-hardlink-crossmount` 的旧 hardlink 单路径断言。
- [√] 修正 fast-sync 测试断言，允许 Linux inode 复用下 single same-FileKey upsert 完成旧路径遮蔽。
- [√] 修正 stress CI hardlink 检查，对齐 PathEntry 多别名策略。
- [√] 同步 `CHANGELOG.md`、`helloagents/wiki/stage-progress.md` 与 `helloagents/history/index.md`。
- [√] 运行格式检查、定向测试与完整默认测试。
