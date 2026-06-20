# 周期扫描 stat() 风暴优化任务清单

> 状态：待确认。分三个 Phase，每 Phase 独立可合并。

## Phase 1：目录 mtime 预检

### 实施任务

- [ ] 修改 `src/index/tiered/directory_manifest.rs`：给 `DirectoryManifest` 添加 `dir_mtime_ns: i64` 字段
- [ ] 实现 `DirectoryManifestStore::dir_unchanged(&self, path, current_mtime_ns) -> bool`
- [ ] 实现 `DirectoryManifestStore::record_dir_mtime(&self, path, mtime_ns)`
- [ ] 修改 `src/index/tiered/sync.rs:scan_dir_repair_slice_with_project_markers`：在 manifest skip 检查后、sliced scan 前，插入目录 mtime 预检（~10 行）
- [ ] 修改同函数：sliced scan 完成后记录目录 mtime（~5 行）
- [ ] 确保首次扫描（无 manifest 记录）不被跳过——`dir_unchanged` 返回 false

### 测试项

- [ ] 新增单元测试：`dir_mtime_unchanged_skips_scan`——目录 mtime 不变时返回 manifest_skipped=true
- [ ] 新增单元测试：`dir_mtime_changed_triggers_scan`——目录 mtime 变化时正常扫描
- [ ] 新增单元测试：`first_scan_not_skipped`——无 manifest 记录时正常扫描
- [ ] 新增单元测试：`file_create_changes_dir_mtime`——在目录中创建文件后 dir_unchanged 返回 false
- [ ] `cargo test -q manifest` 全通过
- [ ] `cargo test -q scan` 全通过
- [ ] `cargo test -q` 无新增失败

### 验收标准

- [ ] `cargo check` 通过
- [ ] `cargo test` 全通过
- [ ] 基准验证：未变化目录的 PeriodicColdScan 耗时从秒级降到毫秒级

---

## Phase 2：readdir 批量删除对齐

### 实施任务

- [ ] 修改 `src/index/tiered/sync.rs` 的 `fast_sync` 删除对齐段（约 line 990-996）：用 `std::fs::read_dir` 获取当前文件名 `HashSet<OsString>`，替换逐文件 `symlink_metadata`
- [ ] 对每个 dirty 目录分别构建 name set，与该目录下的索引文件做差集
- [ ] 处理边界情况：目录不存在（read_dir 失败）→ 全部标记删除
- [ ] 处理边界情况：文件名非 UTF-8（OsString 直接比对，不转 String）

### 测试项

- [ ] 新增单元测试：`delete_alignment_uses_readdir`——验证删除对齐走 readdir 路径而非逐文件 stat
- [ ] 新增单元测试：`delete_alignment_detects_missing_files`——索引中有但 readdir 没有的文件被标记删除
- [ ] 新增单元测试：`delete_alignment_keeps_existing_files`——索引中和 readdir 都有的文件不被删除
- [ ] 新增单元测试：`delete_alignment_dir_gone`——目录不存在时全部标记删除
- [ ] `cargo test -q delete` 全通过
- [ ] `cargo test -q sync` 全通过
- [ ] `cargo test -q` 无新增失败

### 验收标准

- [ ] `cargo check` 通过
- [ ] `cargo test` 全通过
- [ ] 删除对齐不再对每个索引文件做 stat()

---

## Phase 3：修复 manifest 大目录上限

### 实施任务

- [ ] 新增 `directory_manifest_summary_unbounded` 函数（不限制条目数）
- [ ] 修改 `scan_dir_repair_slice_with_project_markers`：mtime 预检失败后，尝试 unbounded summary 比对
- [ ] summary 匹配则跳过，不匹配才进入 sliced scan
- [ ] 确保 unbounded summary 的内存安全（read_dir 迭代器不一次性收集全部条目）

### 测试项

- [ ] 新增单元测试：`manifest_skip_works_for_large_dir`——超过 512 条的目录 manifest skip 能生效
- [ ] 新增单元测试：`manifest_skip_fails_on_real_change`——目录内容变化时 manifest skip 不生效
- [ ] `cargo test -q manifest` 全通过
- [ ] `cargo test -q` 无新增失败

### 验收标准

- [ ] `cargo check` 通过
- [ ] `cargo test` 全通过
- [ ] 超过 512 条的目录能被 manifest skip 覆盖

---

## 整体验收标准

- [ ] 三个 Phase 全部合并后 `cargo test` 全通过
- [ ] 1M 文件基准测试中 canary create_visible 成功率显著提升（从 0/30 提升）
- [ ] 1M 文件基准测试中 cold_freshness_age_p95_max 下降
- [ ] L1→L2 降级速度加快（tier_distribution 中 L2/L3 目录数增加）
- [ ] 周期扫描 CPU 开销下降（process.cpu_pct_p95 下降）
- [ ] 查询正确性无退化（canary create/rename/delete 全通过）
- [ ] 快照保存/加载 round-trip 完整
