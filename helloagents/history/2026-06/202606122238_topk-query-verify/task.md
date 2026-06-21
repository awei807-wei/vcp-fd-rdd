# M1-1 查询返回前 Top-K 验真任务

## 实施任务

- [√] 新增 `QueryConfig`：`max_verify_per_query`、`verify_timeout_ms`、`allow_sync_readdir`
- [√] 默认关闭 `lazy_validation_enabled`，保留显式配置开启后台补偿
- [√] `TieredIndex` 接入查询验真预算配置
- [√] 冷层/base 查询返回前使用 `QueryVerifyBudget` 约束同步 `stat`
- [√] 预算耗尽后停止冷层扫描并返回已确认结果
- [√] README/CHANGELOG/wiki 同步默认正确性口径和配置表

## 测试项

- [√] 删除的文件不返回，并写入 stale/tombstone 补偿路径
- [√] identity/mtime 变化不作为 `StaleChecked` 返回，返回时必须是 `Changed` 且 `validated=true`
- [√] 宽泛查询同步验证数量不超过 `query.max_verify_per_query`
- [√] lazy validation 默认关闭；显式开启时仍返回 `Unknown` 并由后台 worker 补偿
- [√] 配置解析支持 `[query]` 表并保持 `allow_sync_readdir=false` 默认

## 验收标准

- [√] `cargo test -q query_verify`
- [√] `cargo test -q lazy_validation`
- [√] `cargo test -q query`
- [√] `cargo test -q`
- [√] `cargo fmt --check`
