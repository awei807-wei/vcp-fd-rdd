# fd-rdd Review Checklist

> 基于静态代码审查整理。
>
> 说明：
> - `scripts/*` 视为早期遗留脚本，不纳入主线整改优先级。
> - `helloagents/*` 视为 AI 辅助上下文，不作为正式外部文档要求同步。
> - 本清单聚焦主线代码、默认行为、防呆和可维护性。

## P0：优先处理（防呆 / 交付闭环）

### 1. HTTP 默认只监听本机
- [ ] 给 HTTP 服务增加 `--bind` / `--host` 参数
- [ ] 默认绑定改为 `127.0.0.1`
- [ ] 仅在显式指定时才监听 `0.0.0.0`
- 相关位置：
  - `src/query/server.rs:80`
  - `src/main.rs:44-46`
- 验收标准：
  - 不带参数启动时，仅本机可访问
  - 显式传参时才对外暴露

### 2. 去掉“未传 root 就扫描 HOME”的默认行为
- [ ] `roots.is_empty()` 时明确报错退出
- [ ] 不再默认附带 `/tmp/vcp_test_data`
- [ ] 帮助信息里明确要求显式传 `--root`
- 相关位置：
  - `src/main.rs:17-19`
  - `src/main.rs:111-119`
- 验收标准：
  - 不传 `--root` 时不会悄悄扫描用户目录
  - 错误提示明确可操作

### 3. 修正 `fd-rdd-query --spawn` 的隐式启动行为
- [ ] 评估是否直接移除 `--spawn`
- [ ] 若保留，则补齐必要参数透传（root / snapshot / include-hidden 等）
- [ ] 保证 spawn 起的 daemon 配置是显式、可预期的
- 相关位置：
  - `src/bin/fd-rdd-query.rs:34-36`
  - `src/bin/fd-rdd-query.rs:59-64`
- 验收标准：
  - `--spawn` 不会在默认配置下意外启动“扫描 HOME”的 daemon

### 4. 修复 release workflow
- [ ] 去掉 workflow 中错误的 `cd fd-rdd`
- [ ] 校正打包和 artifact 路径
- [ ] 确认 tag 构建可直接从仓库根目录执行
- 相关位置：
  - `.github/workflows/release.yml:28-41`
- 验收标准：
  - tag release 可以正常构建并产出压缩包

### 5. 补基础 CI
- [ ] 新增 `push` / `pull_request` 触发的 CI workflow
- [ ] 接入 `cargo fmt --check`
- [ ] 接入 `cargo clippy --all-targets --all-features -- -D warnings`
- [ ] 接入 `cargo test`
- 验收标准：
  - 日常提交可自动发现格式、lint、测试问题

---

## P1：第二批处理（解耦 / 维护性）

### 6. 抽取短查询公共 helper
- [ ] 将短查询逻辑抽到公共模块，例如 `src/index/short_hint.rs`
- [ ] 统一以下逻辑的单一实现：
  - `normalize_short_hint`
  - `trigram_matches_short_hint`
  - `short_component_matches`
  - `for_each_short_component`
- 当前重复位置：
  - `src/index/l2_partition.rs:22-29`
  - `src/index/l2_partition.rs:31-37`
  - `src/index/l2_partition.rs:39-44`
  - `src/index/l2_partition.rs:89-99`
  - `src/index/mmap_index.rs:31-38`
  - `src/index/mmap_index.rs:40-46`
  - `src/index/mmap_index.rs:48-53`
  - `src/index/mmap_index.rs:55-65`
- 验收标准：
  - L2 和 mmap 层共用一套短查询规则
  - 后续修改不会出现两层语义漂移

### 7. 对 `TieredIndex` 做轻量拆分
- [ ] 先按职责拆文件，不急着大改状态模型
- [ ] 推荐拆分方向：
  - `tiered_query.rs`：查询相关
  - `tiered_snapshot.rs`：snapshot / flush / compaction 触发
  - `tiered_maintenance.rs`：memory report / trim
- 当前职责集中位置：
  - full build：`src/index/tiered.rs:895-929`
  - rebuild / fast-sync：`src/index/tiered.rs:931-1015`
  - query：`src/index/tiered.rs:1142-1261`
  - snapshot / flush：`src/index/tiered.rs:1546-1738`
  - memory report：`src/index/tiered.rs:1876-1922`
  - RSS trim：`src/index/tiered.rs:1928-1998`
- 验收标准：
  - `tiered.rs` 只保留核心状态和少量入口
  - 查询、快照、维护逻辑不再集中在一个超大文件里

### 8. 给 UDS 启动里的吞错操作补日志
- [ ] 为 `create_dir_all` / `remove_file` / `set_permissions` 的失败增加日志
- [ ] 关键失败使用 `warn!`，可忽略失败至少用 `debug!`
- 相关位置：
  - `src/query/socket.rs:119-128`
  - `src/query/socket.rs:147`
- 验收标准：
  - socket 启动或清理异常时可以快速定位原因

---

## P2：补齐型优化（非阻塞）

### 9. 给 `unsafe` 补 `// SAFETY:` 注释
- [ ] 为关键 `unsafe` 代码补充前提说明
- 参考位置：
  - `src/util.rs:10`
  - `src/util.rs:16`
  - `src/storage/mmap.rs:14`
  - `src/storage/snapshot.rs:702`
  - `src/query/socket.rs:40-41`
  - `src/index/mmap_index.rs:189`
- 验收标准：
  - 后续维护者能理解每个 `unsafe` 的安全边界

### 10. README 顶部补平台边界说明
- [ ] 在 README 开头明确标注 Linux-only 支持边界
- [ ] 说明 Windows 环境仅适合静态阅读，不支持本地运行验证
- 相关位置：
  - `README.md:1-2`
- 验收标准：
  - 用户能快速理解平台支持范围

---

## 推荐处理顺序
1. HTTP bind
2. 默认 root
3. `--spawn`
4. release workflow
5. CI
6. 短查询 helper 抽公共
7. `TieredIndex` 轻拆
8. socket 日志
9. `unsafe` 注释
10. README 平台说明

---

## 总结
当前最值得优先做的是两类：

1. **防呆 / 默认值收紧**
   - 防止服务在未显式配置时暴露过大范围或做出意外行为
2. **解耦 / 可维护性提升**
   - 降低后续继续演进查询、快照、恢复逻辑时的维护成本

建议先完成 P0，再处理 P1，P2 可穿插进行。
