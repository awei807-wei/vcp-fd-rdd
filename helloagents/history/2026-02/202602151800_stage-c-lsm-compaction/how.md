# 方案设计：目录化 LSM（Manifest + 多 Segment 文件）

## 总体结构

存储路径采用“目录布局”：

- `index.d/`
  - `MANIFEST.bin`：段列表与 next_id（原子替换）
  - `seg-{id}.db`：Segment 主体（复用 v6 单文件容器：roots/path_arena/metas/trigram_table/postings_blob/tombstones）
  - `seg-{id}.del`：跨段删除墓碑（按路径字节存储；用于屏蔽更老段里的同路径记录）

> 说明：复用现有 v6 容器能最大化复用 mmap/lazy decode 的代码；`.del` 作为 sidecar 解决“DocId tombstone 仅对单段有效”的跨段删除问题。

## 查询合并（Merge）

层级顺序：`内存 Delta` → `磁盘 Segments（newest → oldest）`

合并规则（保证语义正确）：

1. 从 newest 到 oldest 遍历层，维护 `blocked` 集合（路径 bytes）。
2. 每到一层，先把该层的 `.del` 追加到 `blocked`（屏蔽更老层的同路径结果）。
3. 对该层查询结果：
   - 若路径在 `blocked` 中：跳过
   - 否则输出，并将其路径加入 `blocked`（屏蔽更老层的同路径旧版本）

这样可以自然支持“delete → recreate”：

- recreate 在更“新”的层先输出并加入 `blocked`
- 更“老”的 delete 不影响已输出的新记录

## Flush（内存 Delta 刷盘）

触发条件（任一满足）：

- Delta 索引 dirty
- 或存在跨段 delete 墓碑（删除/rename-from）

Flush 步骤：

1. 使用 `apply_gate`（写锁）短暂阻塞事件写入，ArcSwap 将当前 Delta 指针换成新的空 Delta。
2. 导出旧 Delta 的 v6 段 bytes，写入 `seg-{id}.db`。
3. 将本次 flush 收集的跨段删除墓碑写入 `seg-{id}.del`（原子写入）。
4. 原子更新 `MANIFEST.bin`，把该段追加到 delta 列表。
5. 在内存中 mmap 新段并加入 `disk_layers`，清空 L1（避免缓存旧结果）。

## Compaction（多段合并）

触发阈值（可配置常量，先用保守默认）：

- `delta_segments >= 4` → 触发一次合并为新的 Base

合并算法（后台线程）：

1. 取一份当前段列表快照（按 oldest→newest）。
2. 构建新的 `PersistentIndex`：
   - 对每个段：先应用 `.del`（按路径删除）
   - 再遍历段内 live metas（mmap）执行 `upsert_rename`（保证路径可被新层覆盖）
3. 导出新的 v6 段 bytes，写入新的 base 段文件。
4. 原子写入新 manifest（base=新段，delta=[]），再清理旧段文件。
5. 内存中替换 `disk_layers` 为新 base，并清空 L1。

## 与 rebuild 的关系

rebuild/overflow 仍保留，但重建的落点从“替换内存 L2 全量”升级为“重建并替换磁盘 Base”：

- 后台扫描构建新 `PersistentIndex`
- 写入新的 base 段
- 原子切换 base（读请求无中断）
- pending 事件回放写入新的 Delta

