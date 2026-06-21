# 修复 rename 事件导致新下载文件搜不到的任务清单

> 状态：已完成。口径已按调研包要求修订并落地：只把孤立 `RenameMode::To` 当 Create。

## 实施任务

- [√] 修改 `src/event/stream.rs:merge_events_in_place`：孤立 `RenameMode::To`（单路径）当 Create 处理；`From`/`Any` 单路径保持普通分支
- [√] 修改 `src/event/stream.rs:merge_events_in_place`：同路径 Create/Rename 不被后续 Modify 覆盖（修复 merge 覆盖问题）

## 测试项

### 单元测试（src/event/stream.rs tests 模块）

- [√] `orphan_rename_to_treated_as_create`：单路径 `Modify(Name(To))` 事件走 merge，结果为 `EventType::Create`
- [√] `orphan_rename_from_not_treated_as_create`：单路径 `Modify(Name(From))` 事件走 merge，结果不是 `EventType::Create`（保持普通分支语义）
- [√] `orphan_rename_to_not_overwritten_by_modify`：孤立 RenameTo 后紧跟同路径 Modify，最终结果仍为 `EventType::Create`
- [√] `paired_rename_not_overwritten_by_modify`：配对成功的双路径 rename 后紧跟目标路径 Modify，最终结果仍为 `EventType::Rename`
- [√] `create_not_overwritten_by_modify`：同批次先 Create 后 Modify，最终结果为 `EventType::Create`（已有测试覆盖方向相反，需补此方向）
- [√] `modify_after_delete_still_wins`：Delete 后 Modify（异常但理论可能），验证 Delete 不被保护（Delete 不是"创建类"事件）

### 集成测试 / 行为验证

- [√] 下载场景：在已 watch 目录下模拟 `.part` 文件 → rename 到最终名，搜索最终文件名能命中
- [√] 窗口期场景：通过孤立 `RenameMode::To` 合成事件验证最终文件名能命中
- [√] 无回退验证：上述两个场景下，`RenameFrom` 原路径不出现在搜索结果中

## 验收标准

- [√] `cargo test -q rename` 全通过
- [√] `cargo test -q merge` 全通过
- [√] `cargo test -q stream` 全通过
- [√] `cargo test -q download_rename` 全通过
- [√] `cargo test -q` 无新增失败
