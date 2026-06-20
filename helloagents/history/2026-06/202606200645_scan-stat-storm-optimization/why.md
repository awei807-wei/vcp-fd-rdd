# 周期扫描 stat() 风暴优化——目录 mtime 预检 + readdir 批量对齐

## 背景

M2 真实规模基准测试（100 万文件）暴露出两个瓶颈：

1. **热层 canary 0/30**——在 Projects/（30 万文件）中创建文件后 30 次全部搜不到。原因是 Projects/ 因 `l0_max_cost_per_root=1` 未被 inotify watch 接纳，只能依赖 L1 周期扫描发现新文件，但扫描 30 万文件需要 15-60 秒，远超 5 秒扫描间隔。

2. **M2 旋转窗口受限**——虽然 passive canary 从 0% 提升到 66.7%，但大部分目录仍卡在 L1 无法降级到 L2/L3，旋转窗口可服务的冷目录有限。

两个问题有同一个根因：**周期扫描太慢**。

## 根因分析

### 主瓶颈：周期扫描对每个文件做一次 stat()

**位置**：`src/index/tiered/sync.rs:1302`，函数 `scan_dir_repair_slice_with_project_markers`

```rust
for child in &slice.entries {
    let meta = match std::fs::symlink_metadata(&path) {  // ← 每个文件一次 lstat
        Ok(meta) => meta,
        ...
    };
}
```

30 万文件的目录，分 586 个 slice（每 slice 512 条），每 slice 512 次 `lstat()`。冷缓存下单次 lstat 50-200μs，总计 15-60 秒。

同样的 stat 风暴存在于：
- `fast_sync`（sync.rs:920）：`ent.metadata()` 每文件一次
- `directory_manifest_summary_bounded`（sync.rs:1571）：计算 manifest summary 时每文件一次
- `delete_alignment`（sync.rs:992）：检查文件是否被删除时每文件一次

### 次瓶颈：manifest 跳过机制形同虚设

代码已有 `DirectoryManifestStore` 机制——存储目录内容摘要，摘要不变则跳过扫描。但有两个致命缺陷：

1. **计算摘要本身就要 stat 所有子文件**（sync.rs:1571: `child.metadata()`）。先付了 stat 成本才决定要不要跳过，等于没省。

2. **摘要有 512 条上限**（`REPAIR_SLICE_MAX_ENTRIES=512`）。超过 512 个文件的目录返回 `complete=false`，manifest 跳过直接失效。大目录是最需要跳过的，却偏偏被排除了。

### 根因总结

```
周期扫描触发
  → scan_dir_repair_slice_with_project_markers
    → 不检查目录 mtime，直接进入 slice 扫描
      → 每个 slice 512 个文件，每个文件一次 lstat()
        → 30 万文件 = 30 万次 syscall = 15-60 秒
          → 扫描间隔只有 5 秒，永远扫不完
            → 新文件搜不到，目录无法降级
```

Linux 内核保证：目录内有文件创建/删除/重命名时，目录 mtime 会变。`fast_sync` 路径已用此信号（`visit_dirs_since`），但**周期扫描路径未使用**。

## 影响范围

- **生产环境**：任何未被 inotify watch 的大目录（Downloads/、Pictures/、Projects/ 子目录等）都会遇到此问题。目录越大，扫描越慢，新文件可见延迟越高。
- **M2 冷层轮转**：L1→L2 降级需要空扫描，但扫描大目录太慢导致降级无法完成。
- **用户体验**：用户在 Downloads/ 里下载完文件后，如果 Downloads/ 不在 L0 watch 范围内，需要等几十秒甚至几分钟才能搜到。
- **资源浪费**：未变化的目录每次周期扫描都做 30 万次无意义的 stat()，浪费 CPU 和 IO。
