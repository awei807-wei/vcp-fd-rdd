# 轻量迭代：任务清单（v6 流式校验，降低冷启动 RSS）

## 背景

当前 v6 `load_v6_mmap_if_valid` 采用“先 mmap 再校验全段”的策略，会在启动时触碰大量页，导致进程 RSS（Private_Clean）偏高；这与“冷启动 mmap 但保持 cold”目标冲突。

## 目标

将 v6 加载链路改为“先流式校验（read/seek）再 mmap”，在保证完整性校验不退化的前提下，显著降低冷启动时的进程 RSS。

## 任务

- [√] Helper：实现 `compute_file_checksum(file, offset, len)`（64KB 栈缓冲区流式校验）
- [√] Loader：重构 `load_v6_mmap_if_valid`：
  - [√] 延迟 `Mmap::map` 到所有校验通过之后
  - [√] manifest/roots 使用 `read_exact` 临时 Vec（校验后丢弃）
  - [√] segment checksum 使用流式校验（按 `V6SegDesc.offset/len`，不包含 padding）
  - [√] TOCTOU：校验与 mmap 使用同一个已打开的 `File` 句柄
- [√] LSM：坏 delta 段校验失败时跳过并告警（base 段失败则整体回退）
- [√] 文档：补齐 wiki 对“流式校验 vs mmap 触页”的说明
- [√] 验证：`cargo test` 通过
