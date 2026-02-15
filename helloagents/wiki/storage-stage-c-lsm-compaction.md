# 阶段 C：LSM 演进与 Compaction（长期 mmap 基座 + 内存 Delta）

本页描述阶段 C 的“零拷贝常驻”方向：索引长期以 mmap 只读段为基座，在线更新只写入内存 Delta，并通过 Flush/Compaction 完成增量落盘与段合并。

## 目录化布局

以 `index.d/` 为根目录：

- `MANIFEST.bin`：段列表与 `next_id`（原子替换写入）
- `seg-{id}.db`：只读 Segment（复用 v6 单文件容器：roots/path_arena/metas/trigram_table/postings_blob/tombstones）
- `seg-{id}.del`：跨段删除墓碑（按“绝对路径 bytes”存储；用于屏蔽更老段中的同路径记录）
- `events.wal`：追加型事件日志（Append-only Log），用于 overflow/重启后的增量回放
- `events.wal.seal-<id>`：snapshot 边界切分后的 sealed WAL（`<id>` 为 u64 时间戳）

> 说明：v6 的 tombstones 是 DocId 维度，仅对单段内有效；`.del` 解决跨段 delete/rename-from 的语义。

## 启动加载优先级

1. 若存在 `index.d/MANIFEST.bin` 且校验通过：加载 base + delta segments（mmap）
2. 否则若存在 legacy `index.db` v6 且校验通过：作为 base（mmap）
3. 否则回退读取 v2~v5（bincode），落在内存索引

启动后增量恢复：

- 读取 manifest 的 `wal_seal_id` checkpoint
- 回放 `seal_id > wal_seal_id` 的 `events.wal.seal-*` + 当前 `events.wal`
- 回放事件写入内存 Delta 与 overlay，使查询结果包含“最后一次 snapshot 之后”的变更

## 查询合并语义（newest → oldest）

层级顺序：

1. 内存 Delta（可变）
2. 磁盘 Segments（按 newest→oldest 依次合并）

合并规则（核心：正确覆盖 + delete→recreate 语义）：

- 维护一个 `blocked` 集合（路径 bytes）
- 每层开始先把该层的 `.del` 加入 `blocked`（屏蔽更老层）
- 对该层查询结果：
  - 若路径在 `blocked`：跳过
  - 否则输出，并把该路径加入 `blocked`（屏蔽更老层旧版本）

## Flush（内存 Delta 刷盘）

触发条件：

- Delta 索引 dirty
- 或 overlay 记录到跨段 delete（删除/rename-from）

流程要点：

- 用 `apply_gate` 写锁短暂阻塞事件写入，ArcSwap 将当前 Delta 指针换成新的空 Delta
- 在写锁窗口内对 WAL 执行 `seal()`：把当前 `events.wal` rename 成 `events.wal.seal-<id>` 并创建新的空 WAL，保证“snapshot 前/后”的事件严格分流
- 旧 Delta 导出为 v6 bytes 写入 `seg-{id}.db`
- overlay delete（扣除同路径 upsert 的抵消项）写入 `seg-{id}.del`
- 更新 `MANIFEST.bin` 追加该段，并写入 `wal_seal_id` checkpoint（用于启动时判断哪些 sealed WAL 已经被持久化）

**bootstrap 特殊路径：**

若启动时还没有 manifest（只有 legacy base 或只有内存索引），首次 flush 会先构建一个新的 base 段，避免出现“写了 delta 但 base 被遗忘”的风险。

## Compaction（段合并）

触发阈值（当前默认）：

- delta segments 数量 ≥ 4

合并策略（后台）：

- oldest→newest 遍历段：
  1) 应用 `.del`（按路径 tombstone）
  2) 将 live metas 灌入新索引（使用 `upsert_rename`，确保路径可被新层覆盖）
- 导出为新的 base 段写入磁盘，并原子更新 manifest（base=新段，delta=[]）
- best-effort 清理旧段文件
