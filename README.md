# fd-rdd

一个面向 Linux 的文件索引守护进程。常驻后台，通过 HTTP/UDS 提供低延迟的文件搜索查询，索引随文件系统事件增量更新。

> **实验性声明**：`fd-rdd` 处于探索型开发阶段，核心架构、存储格式与安全默认值仍在快速迭代，不适合直接用于关键生产环境。
>
> **平台声明**：fd-rdd 以 **Linux** 为主平台；macOS 编译为实验性支持，功能与性能不做保证。Windows 支持已移除。

## 它能做什么

- **低延迟搜索**：HTTP `/search` 接口，支持子串、glob、fuzzy、正则、短语、布尔组合与多种过滤器
- **增量索引**：基于 `inotify`/`fanotify` 监听文件事件，索引随变更自动更新
- **快速冷启动**：优先加载 mmap 段式快照，按需触页，避免每次重启都全量扫描
- **持久化与恢复**：LSM 目录结构 + WAL 增量日志，崩溃后回放事件即可恢复；损坏段可识别并跳过
- **大结果集流式查询**：UDS 文本协议边查边返，避免 Daemon/Client 端内存聚合

## 快速开始

### 从 AUR 安装

Arch Linux 用户可通过 AUR 安装：

```bash
yay -S fd-rdd-git
```

安装后二进制为 `fd-rdd` 与 `fd-rdd-query`，可直接使用。

### 从源码编译

```bash
cargo build --release
```

默认启用 `mimalloc`。如需系统分配器：

```bash
cargo build --release --no-default-features
```

### 启动守护进程

#### 方式一：配置文件（推荐）

创建 `~/.config/fd-rdd/config.toml`：

```toml
# ~/.config/fd-rdd/config.toml
roots = ["~"]
include_hidden = true
snapshot_interval_secs = 300
http_port = 6060
ignore_enabled = true
log_level = "info"
```

配置加载优先级：`CLI 参数 > config.toml > 默认值`。配置好后直接运行：

```bash
./target/release/fd-rdd
```

#### 方式二：命令行参数

如果未提供配置文件，或需要临时覆盖配置：

```bash
./target/release/fd-rdd \
  --root /path/to/scan \
  --root /another/path/to/scan \
  --http-port 6060 \
  --uds-socket /tmp/fd-rdd.sock
```

说明：

- `--root` 为必传参数（无配置文件时），可重复传入多个索引源
- 默认会跳过隐藏文件与 `.gitignore` 规则中的内容；可用 `--include-hidden` / `--no-ignore` 调整
- 默认 `--snapshot-path` / `--uds-socket` 优先使用 `$XDG_RUNTIME_DIR/fd-rdd/`，避免多用户冲突

### 搜索

```bash
# 基础搜索
curl -G "http://127.0.0.1:6060/search" --data-urlencode "q=main.rs" --data-urlencode "limit=20"

# 排除目录
curl -G "http://127.0.0.1:6060/search" --data-urlencode "q=server.js !node_modules"

# Fuzzy 模式
curl -G "http://127.0.0.1:6060/search" --data-urlencode "q=mdt" --data-urlencode "mode=fuzzy"

# 排序
curl "http://127.0.0.1:6060/search?q=test&sort=date_modified&order=desc"
```

### UDS 流式查询（推荐大结果集）

```bash
./target/release/fd-rdd-query --socket /tmp/fd-rdd.sock --limit 2000 "*.rs"
./target/release/fd-rdd-query --socket /tmp/fd-rdd.sock --spawn --root /path/to/scan --limit 2000 "main.rs"
```

### 即时扫描

主动触发目录扫描并立即更新索引：

```bash
curl -X POST http://127.0.0.1:6060/scan \
  -H 'Content-Type: application/json' \
  -d '{"paths":["/home/shiyi/Downloads"]}'
```

### 健康检查

```bash
curl "http://127.0.0.1:6060/health"
```

## 查询语法

### 逻辑运算符

- `VCP server` — AND（空格）
- `server.js|Plugin.js` — OR（竖线）
- `server.js !node_modules` — NOT（感叹号全局排除）

### 短语与通配

- `"New Folder"` — 短语匹配
- `*memoir*` — glob 通配
- `c/use/shi` — 路径段首匹配（自动展开为 OR 分支）

### 过滤器

| 过滤器                     | 示例                           | 说明             |
| -------------------------- | ------------------------------ | ---------------- |
| `parent:` / `infolder:`    | `parent:/home/shiyi/Downloads` | 父目录精确匹配   |
| `depth:`                   | `depth:<=3`                    | 路径深度限制     |
| `len:`                     | `len:>50`                      | 文件名字节长度   |
| `ext:`                     | `ext:js;py`                    | 后缀过滤         |
| `size:`                    | `size:>10mb`                   | 文件大小         |
| `dm:`                      | `dm:today`                     | 修改日期         |
| `dc:` / `da:`              | `dc:2024-01-01`                | 创建/访问日期    |
| `type:`                    | `type:file`                    | 类型过滤         |
| `doc:` / `pic:` / `video:` | `pic:十一`                     | 按扩展名集合过滤 |
| `wfn:`                     | `wfn:main.rs`                  | 完整文件名匹配   |
| `regex:`                   | `regex:"^VCP.*\\.(js\|ts)$"`   | 正则匹配         |

### 排序

可用 `sort` 值：`score`（默认）、`name`、`path`、`size`、`ext`、`date_modified`、`date_created`、`date_accessed`

## 核心特性

- **多级索引结构**：热缓存 → 内存 Delta（RoaringBitmap + PathArena）→ 只读磁盘段（mmap LSM）
- **事件管道**：bounded channel + debounce；溢出后走 dirty 标记 + fast-sync 增量恢复，必要时 rebuild 兜底
- **数据校验**：v7 快照/WAL 默认 CRC32C，段损坏可识别并隔离
- **内存可观测**：定期输出 MemoryReport（RSS、Private Dirty/Clean、page faults）
- **符号链接安全**：默认不跟随软链；开启时以 `(dev, ino)` 组合检测循环，避免跨文件系统误判

## 常用配置

### 完整配置示例

```toml
# ~/.config/fd-rdd/config.toml
roots = ["~/code", "~/documents"]
include_hidden = true
follow_symlinks = false
ignore_enabled = true
snapshot_interval_secs = 300
http_port = 6060
log_level = "info"
```

支持的字段：

| 字段                     | 类型              | 说明                                                           |
| ------------------------ | ----------------- | -------------------------------------------------------------- |
| `roots`                  | `Vec<PathBuf>`    | 索引根目录，**必填**                                           |
| `include_hidden`         | `bool`            | 是否索引隐藏文件（默认 `false`）                               |
| `follow_symlinks`        | `bool`            | 是否跟随符号链接（默认 `false`）                               |
| `ignore_enabled`         | `bool`            | 是否启用 `.gitignore`（默认 `true`）                           |
| `snapshot_interval_secs` | `u64`             | 自动快照落盘周期（默认 `300`）                                 |
| `http_port`              | `u16`             | HTTP 查询端口（默认 `6060`）                                   |
| `snapshot_path`          | `Option<PathBuf>` | 快照数据库路径（默认自动选择）                                 |
| `log_level`              | `String`          | 日志级别：`trace`/`debug`/`info`/`warn`/`error`（默认 `info`） |

配置加载优先级：`CLI 参数 > config.toml > 默认值`。配置好后直接运行 `./target/release/fd-rdd` 即可启动。

### 命令行覆盖

所有配置项均可通过命令行参数临时覆盖，适用于调试或一次性任务：

查看当前生效配置及来源：

```bash
./target/release/fd-rdd --show-config
```

```bash
./target/release/fd-rdd \
  --root /home/shiyi/code \
  --include-hidden \
  --follow-symlinks \
  --no-ignore \
  --http-port 6060 \
  --snapshot-interval-secs 300
```

更多参数见 `./target/release/fd-rdd --help`。

## 更新日志

- **v0.5.9** — 配置系统重构：自动创建、tilde 展开与诊断命令
  - **新增** `Config::load()` 自动创建默认配置：首次启动时若 `~/.config/fd-rdd/config.toml` 不存在，自动创建并写入默认配置（`roots = ["~"]` 等），创建失败则回退到内存默认值。
  - **新增** `~` / `~/` 路径展开：配置文件中 `roots`、`socket_path`、`snapshot_path` 均支持 tilde 展开，无需硬编码绝对路径。
  - **新增** `snapshot_path` 配置字段：用户可通过 `config.toml` 指定快照数据库路径。
  - **新增** `--show-config` 诊断命令：启动时打印每个配置项的最终生效值及其来源（`CLI`、`config.toml` 或 `default`），解决配置混用时的可审计性问题。
  - **修复** clippy `manual_strip` 警告。

完整历史更新与缺陷修复记录见 [CHANGELOG.md](./CHANGELOG.md)（如不存在则参考 Git 历史）。

## TODO

### 展望

- [ ] 提供 `systemd --user` 单元模板和更稳妥的守护进程自启约定；同时收口 `fd-rdd-query --spawn` 在”无显式 root/config”场景下的安全默认，避免误扫整个 `$HOME`。
- [ ] 完成 `content:` 全文索引，复用现有全局 `DocId`、事件增量链路以及 LSM + mmap 存储布局。
- [ ] 为全文索引补齐内容过滤策略：大文件阈值、二进制文件跳过、ignore 规则复用、内容哈希去重。
- [ ] 段级 Bloom 过滤器：为每个磁盘段构建 Bloom Filter，查询时提前跳过不包含目标路径的段，减少无效的段遍历和 mmap 触页开销。
- [ ] Leveled 代际 Compaction：实现更平滑的分层合并策略，替代当前简单的”最多合并 2 个旧段”逻辑，降低写放大并控制段数增长。
- [ ] 增强版 WAL 语义：支持可配置的 fsync 策略（每次写入/批量/异步）、事件去重、Gap 校验（检测 WAL 中的记录缺失或损坏），提升持久化可靠性和恢复能力。

### 已知缺陷

- [ ] DocId 上限无扩容方案：DocId 使用 u32 编码，超过 40 亿文件后无法分配新 ID，大规模场景下会导致索引失效。需要设计 ID 扩容方案或改用 u64。
- [ ] mmap 安全校验不足：当前仅通过文件修改时间检测外部篡改，攻击者可在修改内容后恢复时间戳绕过检测。需要增强校验机制（如定期重新计算 CRC）或考虑其他防护手段。

## 许可证

MIT License (c) 2026 fd-rdd Contributors
