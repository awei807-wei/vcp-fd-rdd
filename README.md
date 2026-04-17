# fd-rdd

一个面向 Linux 的文件索引守护进程。常驻后台，通过 HTTP/UDS 提供低延迟的文件搜索查询，索引随文件系统事件增量更新。

> **实验性声明**：`fd-rdd` 处于探索型开发阶段，核心架构、存储格式与安全默认值仍在快速迭代，不适合直接用于关键生产环境。

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
yay -S fd-rdd-fit
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

```bash
./target/release/fd-rdd \
  --root /path/to/scan \
  --root /another/path/to/scan \
  --http-port 6060 \
  --uds-socket /tmp/fd-rdd.sock
```

说明：

- `--root` 为必传参数，可重复传入多个索引源
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

```bash
./target/release/fd-rdd \
  --root /home/shiyi/code \
  --include-hidden \                    # 索引隐藏文件
  --follow-symlinks \                   # 跟随符号链接（默认 false）
  --no-ignore \                          # 忽略 .gitignore
  --snapshot-path /tmp/fd-rdd/index.db \
  --http-port 6060 \
  --uds-socket /tmp/fd-rdd.sock \
  --event-channel-size 65536 \           # 事件通道容量
  --snapshot-interval-secs 300 \         # 自动落盘周期
  --report-interval-secs 60              # 内存报告周期
```

更多参数见 `./target/release/fd-rdd --help`。

## 更新日志

- **v0.5.5** — 存储层健壮性加固、WAL 去重、事件溢出增量恢复、版本兼容重构
- **v0.5.4** — CRC32C 校验全面升级、错误处理强化、符号链接安全、测试覆盖补全
- **v0.5.3** — 默认本地 HTTP、CLI `--root` 必传、搜索排序重构

完整历史更新与缺陷修复记录见 [CHANGELOG.md](./CHANGELOG.md)（如不存在则参考 Git 历史）。

## 许可证

MIT License (c) 2026 fd-rdd Contributors
