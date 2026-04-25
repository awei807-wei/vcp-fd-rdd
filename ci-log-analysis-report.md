# CI 日志分析报告

> **分析日期**：2026-04-25  
> **分析人**：docs-maintainer  
> **涉及 CI Run**：
> - `logs_66171761116` — stress-hybrid-large-scale workflow
> - `logs_66171761131` — 主 CI workflow（Format check job）

---

## 1. 压力测试（test-hybrid-large-scale）错误分析

### 1.1 具体报错信息

```
thread 'large_scale_hybrid_workspace_correctness' panicked at tests/p2_large_scale_hybrid.rs:45:5:
file with marker initial-18a99d8355e55f79 should be searchable at
/tmp/fd-rdd-hybrid-large-scale-.../probes/initial-initial-18a99d8355e55f79.txt
```

- **测试**：`p2_large_scale_hybrid::large_scale_hybrid_workspace_correctness`
- **失败位置**：`verify_file_searchable` 断言（line 45）
- **失败时间**：2026-04-25T13:45:46 UTC（测试耗时 438.77 秒）
- **表现**：初始索引完成后，使用 marker 搜索 probe 文件，返回空结果（假阴性）

### 1.2 根因分析

该错误是**查询假阴性（false negative）**——文件已被索引（或应当已被索引），但查询搜不到。结合代码和 git 历史，根因是 **v0.6.2 之前存在的 trigram_candidates 空 bitmap 短路 bug**：

1. **trigram_candidates 空 bitmap 短路**：`src/index/l2_partition.rs` 中，当查询词的任一 trigram 在索引中不存在，或 trigram 交集为空时，旧逻辑返回 `Some(空 bitmap)`。这阻断了 `short_hint_candidates` 回退和全量扫描回退，导致文件明明存在却搜不到。
2. **mmap_index.rs 同名短路**：`MmapIndex::trigram_candidates` 在交集为空时同样返回 `Some(空 bitmap)`，进一步阻断回退路径。
3. **upsert 竞态窗口**：rename / new-file 路径的 `alloc_docid → insert_trigrams → insert_path_hash` 不是原子的，查询可能在中间态执行。
4. **pending_events 不可见**：debounce 期间的事件尚未应用到 L2，且 `query_limit` 在 `execute_query_plan` 返回非空时直接 `return`，跳过 `pending_events` 扫描。

在 80 万文件的大规模场景下，上述问题叠加，导致初始索引后的 probe 文件搜索失败。

### 1.3 修复状态

**已在 v0.6.2 中修复**（commit `3da0b97`，2026-04-25 21:35:52 +0800）。

修复内容：
- trigram_candidates 空 bitmap 短路 → 统一返回 `None`，触发回退路径
- mmap_index.rs 同名短路修复
- `upsert_lock` 写锁保护，消除 upsert 竞态窗口
- `remove_from_pending` / `apply_events` 顺序交换（先 apply 后 remove）
- `query_limit` 始终扫描 `pending_events`，整合进 `execute_query_plan`
- `file_count()` 汇总全层计数 + `apply_gate` 读锁防不一致读取

CI 日志中的失败发生在 v0.6.2 修复**之前**（日志时间 13:45 UTC ≈ 21:45 +0800，修复提交 21:35:52 +0800，但 CI 实际运行的是更早的 commit）。当前代码已包含全部修复。

---

## 2. Format check 错误分析

### 2.1 具体报错信息

```
Diff in /home/runner/work/vcp-fd-rdd/vcp-fd-rdd/tests/p2_large_scale_hybrid.rs:171:
-    let process = FdRddProcess::spawn(&root, port, &snapshot, &["--debounce-ms", "10", "--event-channel-size", "524288"]);
+    let process = FdRddProcess::spawn(
+        &root,
+        port,
+        &snapshot,
+        &["--debounce-ms", "10", "--event-channel-size", "524288"],
+    );
```

- **失败位置**：`tests/p2_large_scale_hybrid.rs:171`
- **失败原因**：`FdRddProcess::spawn` 单行调用长度超过 rustfmt 默认限制（100 字符）
- **表现**：`cargo fmt --all -- --check` 不通过

### 2.2 根因分析

`p2_large_scale_hybrid.rs` 在 commit `2990512` 中被添加，`FdRddProcess::spawn` 初始调用为单行（含 `--debounce-ms "10"`）。后续 commit `5350bd1` 追加了 `--event-channel-size "524288"` 参数，使单行长度进一步增加，但**未运行 `cargo fmt` 格式化**。

### 2.3 修复状态

**当前代码中仍然存在该格式问题**。

验证：
```bash
$ cargo fmt --all -- --check
Diff in .../tests/p2_large_scale_hybrid.rs:171:
```

虽然 commit `95497a5`（ci: fix musl build, formatting, and clippy warnings）声称修复了 formatting，但其 diff 仅修改了 `p2_large_scale_hybrid.rs` 的后面部分（line 220+），**未触及 line 171 的 `FdRddProcess::spawn` 调用**。

### 2.4 修复建议

**立即执行**：
```bash
cargo fmt --all
git add tests/p2_large_scale_hybrid.rs
git commit -m "style: fix formatting in p2_large_scale_hybrid.rs"
```

或在下次代码修改时顺带运行 `cargo fmt --all`。

---

## 3. 总结

| CI 错误 | 根因 | 修复状态 | 建议 |
|---------|------|---------|------|
| 压力测试 `verify_file_searchable` panic | trigram_candidates 空 bitmap 短路 + upsert 竞态 + pending_events 不可见 | **已修复**（v0.6.2 `3da0b97`） | 无需进一步行动 |
| Format check `p2_large_scale_hybrid.rs:171` | `FdRddProcess::spawn` 单行超过 100 字符 | **未修复**（当前代码仍存在） | 立即运行 `cargo fmt --all` |

## 4. 附录：时间线

| 时间 (+0800) | 事件 |
|-------------|------|
| 2026-04-25 16:06 | `2990512` 添加 `p2_large_scale_hybrid.rs`（格式问题引入） |
| 2026-04-25 16:06 | `95497a5` 声称修复 formatting（实际未修 line 171） |
| 2026-04-25 17:48 | `5350bd1` 追加 `--event-channel-size` 参数（格式问题加剧） |
| 2026-04-25 21:35 | `3da0b97` 发布 v0.6.2（修复假阴性） |
| 2026-04-25 21:36 | Format check CI 失败（运行的是较早 commit） |
| 2026-04-25 21:45 | 压力测试 CI 失败（运行的是较早 commit） |
