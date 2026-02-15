# 阶段 B：任务清单（mmap 段式 + rkyv）

- [√] 协议：在 `snapshot.rs` 定义 v6 容器 header + manifest + segment descriptor（manifest 当前为手写二进制，预留后续替换为 rkyv）
- [√] writer：实现 v6 写入（segments + per-segment checksum + 原子替换）
- [√] reader：实现 v6 mmap 加载与校验（manifest 校验 + roots 校验 + 段校验）
- [√] 查询：实现 TrigramTable 二分查找 + PostingBlob Roaring lazy decode（按需解码）
- [√] 集成：启动链路优先加载 v6；失败则回退 v2~v5；并确保 watcher/快照循环兼容（base hydration 后再写 v6）
- [√] 测试：增加段式快照 roundtrip（写入→mmap加载→查询正确）与 roots 不一致拒绝加载
- [√] 文档：补齐 wiki（段式协议、校验与迁移策略、lazy decode 与后续演进）
- [ ] 后续：如需严格 rkyv archived manifest，再单独开 task 接入 rkyv（需要可联网环境拉取依赖）
- [√] 迁移：完成后迁移方案包到 history，并更新 history/index.md
