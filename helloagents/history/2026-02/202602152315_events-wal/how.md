# 方案：WAL + Seal Checkpoint（与 LSM manifest 绑定）

## WAL 文件组织

- WAL 目录：放在 `index.d/` 内（与段文件同目录，且 watcher 默认已忽略该目录）
- 当前 WAL：`index.d/events.wal`
- sealed WAL：`index.d/events.wal.seal-<id>`（snapshot 边界切分；id 为 u64 时间戳）

## WAL 记录格式（v1）

- Header：`magic(u32) + version(u32)`
- Records：重复写入
  - `len(u32) + crc(u32) + payload(len bytes)`
  - payload:
    - `kind(u8)`：Create/Delete/Modify/Rename
    - `ts_secs(u64) + ts_nanos(u32)`
    - `path_len(u32) + path_bytes`
    - rename 额外：`from_len(u32) + from_bytes`

解码时遇到尾部截断：忽略尾部不完整 record（允许崩溃中断）。

## 与 snapshot 的一致性边界

为避免“snapshot 期间 WAL 追加 -> snapshot 成功后 truncate 导致丢事件”的竞态：

- snapshot 开始（拿到 apply_gate 写锁，确保没有并发 apply）：
  - `seal()`：把当前 WAL 原子 rename 成 sealed 文件，并创建新的空 `events.wal`
  - 释放写锁后继续落盘旧 delta（段式/LSM 写入）
- snapshot 成功后：
  - LSM `MANIFEST.bin` 写入 `wal_seal_id` checkpoint
  - 清理 `seal_id <= checkpoint` 的 sealed WAL（best-effort）
- 启动恢复：
  - 读取 manifest 的 `wal_seal_id`
  - 仅回放 `seal_id > checkpoint` 的 sealed WAL + 当前 WAL

这样即使崩溃发生在“snapshot 已写入，但 sealed WAL 未清理”的窗口，也能通过 manifest checkpoint 避免重复回放。
