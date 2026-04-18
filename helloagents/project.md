# fd-rdd 项目说明（SSOT）

## 项目定位

`fd-rdd` 是一个事件驱动的文件索引服务：

- L1：热缓存（短期命中）
- L2：内存常驻的持久索引（DocId + Roaring posting + PathArena）
- L3：离线/后台构建器（全量扫描灌入 L2）

对外提供 HTTP 查询接口，并支持周期性快照落盘（当前为 bincode 的原子替换快照）。

补充：阶段 B 引入 v6 段式快照（mmap + posting lazy decode），用于冷启动“秒开”。阶段 C 进一步演进为目录化 LSM：长期 mmap 基座 + 内存 Delta，通过 Flush/Compaction 追加段与合并段，启动不再做全量 hydration（详见 `wiki/storage-stage-c-lsm-compaction.md`）。

## 关键目标

- 查询链路低延迟、可用性优先
- 动态更新场景下内存可控、可观测（RSS/结构体拆项）
- 支持后台重建/滚动升级能力（通过 ArcSwap 等原子切换手段）

## 目录结构（概览）

- `src/index/`：三级索引实现（L1/L2/L3 + 组合层）
- `src/event/`：notify watcher + 事件管道（debounce/合并/批量应用）
- `src/storage/`：快照存储（header + checksum + 原子替换）
- `src/query/`：HTTP/Socket 查询服务
- `helloagents/wiki/`：项目知识库（架构/性能/可靠性笔记）
- `helloagents/plan/`：方案包（设计阶段产物）
- `helloagents/history/`：已执行方案包归档

## 当前持久化形态（2026-02-15）

- v6 段式容器：legacy `index.db`（兼容读取 v2~v6；v6 以 mmap 为主要加载方式）
- LSM 目录布局：`index.d/`（`MANIFEST.bin` + `seg-*.db` + `seg-*.del`）
- events.wal：`index.d/events.wal` + `events.wal.seal-*`，配合 manifest 的 `wal_seal_id` checkpoint 做启动回放

## v0.5.9（2026-04-18）：配置自动创建、路径展开与诊断输出

- **配置自动创建**：`Config::load()` 在缺失配置文件时自动创建默认配置（`~/.config/fd-rdd/config.toml`），创建失败则回退到默认值。
- **Tilde 路径展开**：新增 `expand_path()` 辅助函数，将 `~` / `~/` 展开为用户主目录；加载配置后对 `roots`、`socket_path`、`snapshot_path` 均应用此展开。
- **snapshot_path 配置项**：`Config` 结构体新增 `snapshot_path` 字段，用户可通过配置文件覆盖快照数据库路径（优先级：CLI > config.toml > 默认值）。
- **诊断输出**：新增 `--show-config` CLI 标志，打印生效配置及各参数的来源（CLI / config.toml / default），解决多配置源混用时的可审计性问题。
- **代码清理**：修复 clippy `manual_strip` 警告。
- **CI 修复**：修复 ThreadSanitizer ABI 兼容性问题（添加 `rust-src` 组件、`-Zbuild-std`）；修复 `musl-tools` 安装；增加 TSan thread-leak 抑制规则。
