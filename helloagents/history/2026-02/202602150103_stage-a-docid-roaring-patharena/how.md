# 阶段 A：实施方案（怎么做）

## 1) ID 模型调整

- 引入 `DocId = u32` 作为 L2 内部的稳定文档编号（posting 元素）。
- 保留文件身份 `FileKey(dev, ino)` 仅用于“去重/单路径策略/增量更新定位”，不再进入 posting。
- 维护映射：
  - `filekey_to_docid: HashMap<FileKey, DocId>`

## 2) L2 主表改为紧凑 Vec

- 将 `files: HashMap<FileId, FileMeta>` 改为：
  - `metas: Vec<CompactMeta>`（DocId 直接做下标）
- `CompactMeta` 仅保存必要字段：
  - `path_off: u32`
  - `path_len: u16`
  - `size: u64`
  - `mtime: Option<SystemTime>`（本阶段不做时间戳压缩）
- 删除与存在性：
  - `tombstones: RoaringBitmap`（DocId 集合）

## 3) Path Blob Arena

- 新增 `PathArena { data: Vec<u8> }`：
  - 插入路径时将 `path.to_string_lossy()` 的 UTF-8 bytes 追加到 arena，返回 `(off,len)`。
  - rename 时追加新路径并更新 meta 的 `(off,len)`（arena 允许碎片，后续阶段可做 compaction）。
- 路径反查：
  - `path_hash_to_id: HashMap<u64, OneOrManyDocId>`（hash(path_string) → 候选 DocId，最终二次校验真实字符串）

## 4) Trigram 倒排：RoaringBitmap posting

- `trigram_index: HashMap<Trigram, RoaringBitmap>`
  - upsert：按 basename 提取 trigram，bitmap.insert(docid)
  - delete/rename：bitmap.remove(docid)
- 查询候选集：
  - 从 query 提取 trigram → 取所有 bitmap 交集 → 迭代 docid 做 matcher 精确过滤
  - 锁顺序保持：先读取 trigram_index 计算候选 bitmap（克隆后释放锁），再读取 metas/arena

## 5) 快照版本升级与迁移

- 新增快照版本（例如 v4）：bincode body 结构改为“记录列表”：
  - `Vec<SnapshotRecord { file_key, path_string, size, mtime }>` + tombstones（若需要）
- loader 兼容：
  - 继续支持读取旧 v2/v3（FileId->FileMeta 的 map），加载后重建 DocId 紧凑结构并写回新版本。

## 6) 验证策略

- 单元测试：insert/rename/delete/query 的语义不变；path_hash 冲突二次校验仍正确。
- 快照兼容：构造 v3 快照→加载→能查询到同样结果→写出 v4→再次加载一致。

