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

### 配置文件（推荐）

推荐通过 `~/.config/fd-rdd/config.toml` 管理配置，避免长命令行参数：

```toml
# ~/.config/fd-rdd/config.toml
roots = ["/home/username"]
include_hidden = true
snapshot_interval_secs = 300
http_port = 6060
ignore_enabled = true
log_level = "info"
```

配置加载优先级：`CLI 参数 > config.toml > 默认值`。配置好后直接运行 `./target/release/fd-rdd` 即可启动。

更多参数见 `./target/release/fd-rdd --help`。

## 更新日志

- **v0.5.5** — 存储层健壮性加固、WAL 去重、事件溢出增量恢复、版本兼容重构
- **v0.5.4** — CRC32C 校验全面升级、错误处理强化、符号链接安全、测试覆盖补全
- **v0.5.3** — 默认本地 HTTP、CLI `--root` 必传、搜索排序重构

注：当 `--fd-metric=pd` 且脚本能解析到 fd-rdd 的 MemoryReport 时，`--max-growth-mb` 实际比较的是  
`unaccounted=max(0, pdΔ-disk_tomb_estΔ)`（把 tombstones 等可量化的结构性增长从“泄漏”里剔除）；否则回退为 `pdΔ`。

如果希望“一次运行就得到 PASS/FAIL 结论”（脚本自动启动 fd-rdd + warmup + plateau 检查）：

```bash
python3 scripts/fs-churn.py \
  --verdict --reset --cleanup \
  --populate 20000 --ops 200000 --max-files 20000 \
  --warmup-rounds 1 --rounds 10 --settle-secs 20 \
  --fd-metric pd --max-growth-mb 8
```

脚本会在 PASS/FAIL 时输出“归因摘要”（包含 event overflow 与 MemoryReport 的 heap/index 拆分）。如需输出 fd-rdd 的详细日志，可追加 `--fd-echo`；想减少 overflow 干扰可调 `--fd-event-channel-size` / `--fd-debounce-ms` 或给 churn 加 `--sleep-ms`。

注：部分系统对 `/proc/<pid>/smaps_rollup` 有权限限制（常见于 yama/ptrace_scope 策略）。遇到无法读取时：

- 只想做 RSS 平台检查：把 `--fd-metric` 改为 `rss`（会 fallback 到 `/proc/<pid>/statm`）
- 想继续检查 `pd/pc/pss`：用 `--spawn-fd` 让脚本启动 fd-rdd（成为父进程；该参数需要放在命令最后）

```bash
python3 scripts/fs-churn.py \
  --root /tmp/fd-rdd-churn --reset --populate 20000 \
  --ops 200000 --max-files 20000 \
  --rounds 10 --settle-secs 20 \
  --fd-metric pd --max-growth-mb 8 \
  --spawn-fd ./target/release/fd-rdd \
    --root /tmp/fd-rdd-churn \
    --snapshot-path /tmp/fd-rdd/index.db \
    --no-build \
    --snapshot-interval-secs 0 \
    --auto-flush-overlay-paths 5000 \
    --auto-flush-overlay-bytes 0 \
    --report-interval-secs 5 \
    --trim-interval-secs 0
```

## TODO

### 展望

- [ ] 补齐 `~/.config/fd-rdd/config.toml` 的全量字段接线；当前优先级已经是 `CLI > 配置文件 > 默认值`，但 `http_port`、`snapshot_interval_secs`、`include_hidden`、`log_level` 等仍未全部贯通到启动路径。
- [ ] 提供 `systemd --user` 单元模板和更稳妥的守护进程自启约定；同时收口 `fd-rdd-query --spawn` 在”无显式 root/config”场景下的安全默认，避免误扫整个 `$HOME`。
- [ ] 完成 `content:` 全文索引，复用现有全局 `DocId`、事件增量链路以及 LSM + mmap 存储布局。
- [ ] 为全文索引补齐内容过滤策略：大文件阈值、二进制文件跳过、ignore 规则复用、内容哈希去重。
- [ ] 段级 Bloom 过滤器：为每个磁盘段构建 Bloom Filter，查询时提前跳过不包含目标路径的段，减少无效的段遍历和 mmap 触页开销。
- [ ] Leveled 代际 Compaction：实现更平滑的分层合并策略，替代当前简单的”最多合并 2 个旧段”逻辑，降低写放大并控制段数增长。
- [ ] 增强版 WAL 语义：支持可配置的 fsync 策略（每次写入/批量/异步）、事件去重、Gap 校验（检测 WAL 中的记录缺失或损坏），提升持久化可靠性和恢复能力。

### v0.5.6 更新（中文搜索修复）

- **修复** `compute_substring_highlights` UTF-8 边界 panic：匹配后错误使用 `start = abs_pos + 1`，中文字符（3 字节）导致下一轮切片落在字符中间，触发 panic 并使 HTTP 查询线程崩溃；已改为按匹配子串实际字节长度推进。
- **修复** 中文路径边界加分失效：`is_boundary_char` 原仅检查 ASCII 边界字符，中文无法获得边界加分；改为以 `char` 为单位判断，非字母数字字符（含中文）均被视为边界。
- **修复** 中文短查询优化被跳过：`normalize_short_hint` 按字节长度判断 1-2 字符，单个中文字符 = 3 字节导致短组件索引优化失效；改为按字符数判断。
- **修复** 全角空格 `U+3000` 未识别为分隔符：`tokenize` 仅使用 `is_ascii_whitespace()`，导致以全角空格分隔的中文查询词被错误合并；新增 `is_token_separator()` 统一检测 ASCII 空白与全角空格。
- **测试**：在 `scoring.rs`、`l2_partition.rs`、`dsl_parser.rs`、`matcher.rs`、`dsl.rs`、`fzf.rs` 中新增 15+ 个中文搜索相关单元测试，全部通过。

完整历史更新与缺陷修复记录见 [CHANGELOG.md](./CHANGELOG.md)（如不存在则参考 Git 历史）。

### 待修复缺陷

- [x] 核心流程仍存在 unwrap 导致的崩溃风险：LSM 合并等关键路径中使用 `store.lsm_manifest_wal_seal_id().unwrap()`，manifest 读取失败会直接导致守护进程崩溃，需要改为错误传播或降级处理。（v0.5.5 已修复：snapshot.rs 中 v6 mmap 加载的 5 处 unwrap 改为 match 降级返回 `Ok(None)`）
- [x] 持久化缺少目录 fsync 保证：快照和 WAL 文件 rename 后未对父目录执行 fsync，Linux 下掉电可能导致元数据丢失，快照/WAL 文件消失，索引回退到旧状态。需要在关键写入路径补充目录 fsync。（v0.5.5 已修复：wal.rs 的 `seal()` 和 `open_or_init()` rename 后补充 `fsync(parent_dir)`）
- [x] WAL 回放缺少事件去重：回放 WAL 时未对重复事件做去重处理，若 WAL 中存在重复记录（如异常写入、部分 flush），会导致索引中出现重复条目或已删除文件重新出现。（v0.5.5 已修复：wal.rs `replay_since_seal` 中增加基于 (id, timestamp) 的去重逻辑）
- [ ] DocId 上限无扩容方案：DocId 使用 u32 编码，超过 40 亿文件后无法分配新 ID，大规模场景下会导致索引失效。需要设计 ID 扩容方案或改用 u64。
- [ ] mmap 安全校验不足：当前仅通过文件修改时间检测外部篡改，攻击者可在修改内容后恢复时间戳绕过检测。需要增强校验机制（如定期重新计算 CRC）或考虑其他防护手段。
- [x] 版本兼容代码重复度高：v1-v7 的快照/WAL 解码逻辑存在大量复制粘贴，维护时容易遗漏修改导致兼容性问题。建议重构为统一的版本分发框架。（v0.5.5 已修复：引入 `LegacySnapshot` trait + macro，统一 v2-v5 加载与 `into_persistent_index` 分发）
- [x] 符号链接循环检测存在跨文件系统误判：当前基于 inode 的 visited 集合在跨文件系统时可能因 inode 重复导致误判（正常软链被当作循环）或漏判（真实循环未检测到）。需要结合设备号（dev）一起判断。（已修复：`src/core/rdd.rs` 中 `FsScanRDD` 已使用 `(dev, ino)` 组合进行 visited 去重）
- [x] 事件溢出恢复依赖全量重建：事件通道溢出后直接标记脏区并触发全量扫描，中间丢失的事件无法增量恢复。大目录重建耗时长，影响索引可用性。建议优化为增量补偿机制。（v0.5.5 已修复：`DirtyTracker` 将溢出路径映射到 root 粒度，fast-sync 对 root 做增量 mtime 局部扫描）

### 缺陷（v0.5.4 已修复）

- [x] 旧版本快照、WAL 使用自研的 SimpleChecksum 做校验，本质是字节累加 + 简单旋转的玩具校验，碰撞概率极高，根本无法有效识别数据 corruption
- [x] mmap 快照存在严重的内存安全隐患：仅在加载时做了一次校验，加载完成后就不再管文件状态 ——mmap 是共享文件映射，外部程序修改快照文件会直接篡改进程的内存，随时可能触发越界访问、进程 panic，甚至更严重的内存安全问题，完全不符合 “可恢复” 能力。
- [x] WAL 的错误处理逻辑极端脆弱：读取 WAL 时只要碰到一条损坏记录，直接中断整个读取流程，后面所有的增量事件全部丢弃，一条坏记录就能丢光所有后续的变更数据，
- [x] 大量使用 unwrap/expect 莽错误：WAL 的文件锁直接 lock().unwrap()，锁一旦 poison（有线程 panic）直接把整个守护进程干崩；各种 IO、解析错误没有兜底，随便一个小错误就能把常驻服务搞崩，完全不符合长期运行的要求。
- [x] 大量静默吞错误：清理旧 sealed 文件、清理 socket 文件时，全用 let \_ = xxx 忽略错误，删除失败连警告日志都没有，用户碰到磁盘满、权限不足的情况，完全得不到提醒，旧文件堆着占满磁盘都不知道。
- [x] tokio 直接启用了 full 全量特性，把大量用不到的功能打包进二进制，完全不会按需启用特性，导致最终编译出的程序体积巨大,所以需要修改成按需引用。
- [x] 整个测试目录只有两个测试文件，核心的事件处理、WAL 恢复、mmap 安全校验全没有自动化测试。
  - [x] 基础开关测试：--follow-symlinks=false 时，扫描不会进入符号链接指向的目录，比如指向根目录的软链，不会递归索引全系统
  - [x] 软链本身的处理：禁用跟随的时候，符号链接文件本身会不会被正常索引（而不是直接跳过这个文件本身）
  - [x] 事件层同步：禁用跟随的时候，事件监听不会监听符号链接指向的目录的变更，避免监听到根目录的全系统事件
  - [x] 嵌套软链测试：嵌套的符号链接（a 链到 b，b 链到根），会不会正确阻断递归，不会穿透
  - [x] 场景复现测试：模拟 Steam Proton 的 dosdevices/z:指向 / 的场景，验证不会触发全根目录索引
  - [x] ignore 规则贯通测试：冷扫、增量补扫、事件过滤，三层是不是都用了同一套 ignore 规则（.gitignore/ 全局 ignore 是不是都生效）
  - [x] 隐藏文件开关：--include-hidden 的开关，默认是不是跳过隐藏文件，开启后能不能正常扫描
  - [x] 多 root 隔离：多个--root 参数的场景，多个索引目录能不能正常隔离，不会互相干扰
  - [x] 超长路径容错：超过 65535 字节的超长路径，会不会正确跳过告警，不会 panic
  - [x] 大目录扫描：十万级文件的大目录，扫描会不会正常完成，不会 OOM 或者崩溃
  - [x] 高负载事件处理：模拟 git clone 的批量创建事件，会不会触发事件溢出，溢出后会不会正确触发 fast-sync，不会丢事件、不会全量重建
  - [x] 重命名事件处理：文件重命名后，索引会不会正确更新，不会当成删除 + 新建丢数据
  - [x] 删除事件处理：文件 / 目录删除后，索引会不会正确清理对应的条目
  - [x] 事件损坏恢复：WAL 里有损坏的事件记录，会不会正确跳过坏记录，不会把后面的所有事件全丢了
  - [x] watch 降级测试：事件监听退化到轮询的时候，能不能正常工作，不会丢索引
  - [x] 崩溃恢复测试：进程崩溃后重启，能不能正确回放 WAL，恢复所有增量事件，不用重新全量扫描
  - [x] 版本兼容测试：从 v2 到 v7 的旧版本快照 / WAL，能不能正确加载兼容，不会丢用户的旧索引
  - [x] 坏文件处理：快照 / WAL 文件损坏的时候，会不会正确识别、跳过，不会 panic，能不能触发兜底重建
  - [x] LSM compaction 测试：大量小变更后，compaction 能不能正确合并段、回收磁盘空间，不会段数暴涨
  - [x] 旧文件清理：seal 后的旧 WAL、过期的 LSM 段，能不能正确清理，不会残留占磁盘
  - [x] 过滤器有效性：size / 日期 /ext/regex 这些过滤器，能不能正确生效，有没有匹配错误
  - [x] 模糊查询：fuzzy 模式的匹配是不是正常，有没有排序错误
  - [x] 流式查询：大结果集的 UDS 流式查询，能不能边查边返回，不会在内存聚合导致 OOM
  - [x] UDS 权限：别的用户 /root 能不能访问你的 UDS socket，权限拦截是不是正常
  - [x] 短查询优化：1-2 字符的短查询，会不会触发短组件索引优化，不会退化全扫
- [x] 添加配置项 --follow-symlinks（默认 false），在 walker 和事件监听层禁用符号链接跟随，防止 Steam Proton 的 dosdevices/z: 递归索引整个根目录。
  - [x] 顺便补个循环检测：就算开了跟随，也加个 inode 的 visited 集合，挡住 a->b->a 这种循环软链，避免扫描死循环，把鲁棒性拉满。

### 小一点的毛病（v0.5.4 已修复）

- [x] 很多地方用了 unwrap，还有 let \_ = xxx 吞错误，比如删文件的时候忽略错误，需要补错误处理
  - [x] 把那些 let \_ = xxx 吞错误的地方，加上带上下文的 warn 日志，比如清理旧段、删 socket 的时候，失败了打个带路径的告警；还有把锁的 unwrap 换成 poison 兼容的处理，用 poison_err.into_inner()拿到锁，避免单个线程 panic 直接把整个守护进程干崩，提升长期运行的韧性。
- 各个版本的快照、WAL 的兼容代码，写的有点重复，比如 v1 到 v7 的加载逻辑，堆了不少重复的判断
  - [x] 存储模块的 mmap unsafe 代码，补上标准的 SAFETY:注释，说明这个 unsafe 的安全边界是什么，比如 “我们已经校验过文件范围，不会越界”，符合 Rust 的安全编码规范，也方便后续维护。
- [x] 少部分 unsafe 没加安全注释：mmap 的 unsafe 代码，没有加注释说明 “这个 unsafe 为什么是安全的”，Rust 的 unsafe 惯例是要加注释说明安全边界的，这个地方有点随意。
- [x] legacy 把早期的 SimpleChecksum 的 legacy 逻辑，加个版本告警，碰到旧版本的快照提醒升级，后续慢慢把这个过渡的校验算法淘汰掉，全换成标准 CRC32C，统一数据校验的可靠性。

注：当 `--fd-metric=pd` 且脚本能解析到 fd-rdd 的 MemoryReport 时，`--max-growth-mb` 实际比较的是  
`unaccounted=max(0, pdΔ-disk_tomb_estΔ)`（把 tombstones 等可量化的结构性增长从“泄漏”里剔除）；否则回退为 `pdΔ`。

如果希望“一次运行就得到 PASS/FAIL 结论”（脚本自动启动 fd-rdd + warmup + plateau 检查）：

```bash
python3 scripts/fs-churn.py \
  --verdict --reset --cleanup \
  --populate 20000 --ops 200000 --max-files 20000 \
  --warmup-rounds 1 --rounds 10 --settle-secs 20 \
  --fd-metric pd --max-growth-mb 8
```

脚本会在 PASS/FAIL 时输出“归因摘要”（包含 event overflow 与 MemoryReport 的 heap/index 拆分）。如需输出 fd-rdd 的详细日志，可追加 `--fd-echo`；想减少 overflow 干扰可调 `--fd-event-channel-size` / `--fd-debounce-ms` 或给 churn 加 `--sleep-ms`。

注：部分系统对 `/proc/<pid>/smaps_rollup` 有权限限制（常见于 yama/ptrace_scope 策略）。遇到无法读取时：

- 只想做 RSS 平台检查：把 `--fd-metric` 改为 `rss`（会 fallback 到 `/proc/<pid>/statm`）
- 想继续检查 `pd/pc/pss`：用 `--spawn-fd` 让脚本启动 fd-rdd（成为父进程；该参数需要放在命令最后）

```bash
python3 scripts/fs-churn.py \
  --root /tmp/fd-rdd-churn --reset --populate 20000 \
  --ops 200000 --max-files 20000 \
  --rounds 10 --settle-secs 20 \
  --fd-metric pd --max-growth-mb 8 \
  --spawn-fd ./target/release/fd-rdd \
    --root /tmp/fd-rdd-churn \
    --snapshot-path /tmp/fd-rdd/index.db \
    --no-build \
    --snapshot-interval-secs 0 \
    --auto-flush-overlay-paths 5000 \
    --auto-flush-overlay-bytes 0 \
    --report-interval-secs 5 \
    --trim-interval-secs 0
```

## TODO

### 展望

- [ ] 补齐 `~/.config/fd-rdd/config.toml` 的全量字段接线；当前优先级已经是 `CLI > 配置文件 > 默认值`，但 `http_port`、`snapshot_interval_secs`、`include_hidden`、`log_level` 等仍未全部贯通到启动路径。
- [ ] 提供 `systemd --user` 单元模板和更稳妥的守护进程自启约定；同时收口 `fd-rdd-query --spawn` 在”无显式 root/config”场景下的安全默认，避免误扫整个 `$HOME`。
- [ ] 完成 `content:` 全文索引，复用现有全局 `DocId`、事件增量链路以及 LSM + mmap 存储布局。
- [ ] 为全文索引补齐内容过滤策略：大文件阈值、二进制文件跳过、ignore 规则复用、内容哈希去重。
- [ ] 段级 Bloom 过滤器：为每个磁盘段构建 Bloom Filter，查询时提前跳过不包含目标路径的段，减少无效的段遍历和 mmap 触页开销。
- [ ] Leveled 代际 Compaction：实现更平滑的分层合并策略，替代当前简单的”最多合并 2 个旧段”逻辑，降低写放大并控制段数增长。
- [ ] 增强版 WAL 语义：支持可配置的 fsync 策略（每次写入/批量/异步）、事件去重、Gap 校验（检测 WAL 中的记录缺失或损坏），提升持久化可靠性和恢复能力。

### v0.5.6 更新（中文搜索修复）

- **修复** `compute_substring_highlights` UTF-8 边界 panic：匹配后错误使用 `start = abs_pos + 1`，中文字符（3 字节）导致下一轮切片落在字符中间，触发 panic 并使 HTTP 查询线程崩溃；已改为按匹配子串实际字节长度推进。
- **修复** 中文路径边界加分失效：`is_boundary_char` 原仅检查 ASCII 边界字符，中文无法获得边界加分；改为以 `char` 为单位判断，非字母数字字符（含中文）均被视为边界。
- **修复** 中文短查询优化被跳过：`normalize_short_hint` 按字节长度判断 1-2 字符，单个中文字符 = 3 字节导致短组件索引优化失效；改为按字符数判断。
- **修复** 全角空格 `U+3000` 未识别为分隔符：`tokenize` 仅使用 `is_ascii_whitespace()`，导致以全角空格分隔的中文查询词被错误合并；新增 `is_token_separator()` 统一检测 ASCII 空白与全角空格。
- **测试**：在 `scoring.rs`、`l2_partition.rs`、`dsl_parser.rs`、`matcher.rs`、`dsl.rs`、`fzf.rs` 中新增 15+ 个中文搜索相关单元测试，全部通过。

### 待修复缺陷

- [x] 核心流程仍存在 unwrap 导致的崩溃风险：LSM 合并等关键路径中使用 `store.lsm_manifest_wal_seal_id().unwrap()`，manifest 读取失败会直接导致守护进程崩溃，需要改为错误传播或降级处理。（v0.5.5 已修复：snapshot.rs 中 v6 mmap 加载的 5 处 unwrap 改为 match 降级返回 `Ok(None)`）
- [x] 持久化缺少目录 fsync 保证：快照和 WAL 文件 rename 后未对父目录执行 fsync，Linux 下掉电可能导致元数据丢失，快照/WAL 文件消失，索引回退到旧状态。需要在关键写入路径补充目录 fsync。（v0.5.5 已修复：wal.rs 的 `seal()` 和 `open_or_init()` rename 后补充 `fsync(parent_dir)`）
- [x] WAL 回放缺少事件去重：回放 WAL 时未对重复事件做去重处理，若 WAL 中存在重复记录（如异常写入、部分 flush），会导致索引中出现重复条目或已删除文件重新出现。（v0.5.5 已修复：wal.rs `replay_since_seal` 中增加基于 (id, timestamp) 的去重逻辑）
- [ ] DocId 上限无扩容方案：DocId 使用 u32 编码，超过 40 亿文件后无法分配新 ID，大规模场景下会导致索引失效。需要设计 ID 扩容方案或改用 u64。
- [ ] mmap 安全校验不足：当前仅通过文件修改时间检测外部篡改，攻击者可在修改内容后恢复时间戳绕过检测。需要增强校验机制（如定期重新计算 CRC）或考虑其他防护手段。
- [x] 版本兼容代码重复度高：v1-v7 的快照/WAL 解码逻辑存在大量复制粘贴，维护时容易遗漏修改导致兼容性问题。建议重构为统一的版本分发框架。（v0.5.5 已修复：引入 `LegacySnapshot` trait + macro，统一 v2-v5 加载与 `into_persistent_index` 分发）
- [x] 符号链接循环检测存在跨文件系统误判：当前基于 inode 的 visited 集合在跨文件系统时可能因 inode 重复导致误判（正常软链被当作循环）或漏判（真实循环未检测到）。需要结合设备号（dev）一起判断。（已修复：`src/core/rdd.rs` 中 `FsScanRDD` 已使用 `(dev, ino)` 组合进行 visited 去重）
- [x] 事件溢出恢复依赖全量重建：事件通道溢出后直接标记脏区并触发全量扫描，中间丢失的事件无法增量恢复。大目录重建耗时长，影响索引可用性。建议优化为增量补偿机制。（v0.5.5 已修复：`DirtyTracker` 将溢出路径映射到 root 粒度，fast-sync 对 root 做增量 mtime 局部扫描）

### 缺陷（v0.5.4 已修复）

- [x] 旧版本快照、WAL 使用自研的 SimpleChecksum 做校验，本质是字节累加 + 简单旋转的玩具校验，碰撞概率极高，根本无法有效识别数据 corruption
- [x] mmap 快照存在严重的内存安全隐患：仅在加载时做了一次校验，加载完成后就不再管文件状态 ——mmap 是共享文件映射，外部程序修改快照文件会直接篡改进程的内存，随时可能触发越界访问、进程 panic，甚至更严重的内存安全问题，完全不符合 “可恢复” 能力。
- [x] WAL 的错误处理逻辑极端脆弱：读取 WAL 时只要碰到一条损坏记录，直接中断整个读取流程，后面所有的增量事件全部丢弃，一条坏记录就能丢光所有后续的变更数据，
- [x] 大量使用 unwrap/expect 莽错误：WAL 的文件锁直接 lock().unwrap()，锁一旦 poison（有线程 panic）直接把整个守护进程干崩；各种 IO、解析错误没有兜底，随便一个小错误就能把常驻服务搞崩，完全不符合长期运行的要求。
- [x] 大量静默吞错误：清理旧 sealed 文件、清理 socket 文件时，全用 let \_ = xxx 忽略错误，删除失败连警告日志都没有，用户碰到磁盘满、权限不足的情况，完全得不到提醒，旧文件堆着占满磁盘都不知道。
- [x] tokio 直接启用了 full 全量特性，把大量用不到的功能打包进二进制，完全不会按需启用特性，导致最终编译出的程序体积巨大,所以需要修改成按需引用。
- [x] 整个测试目录只有两个测试文件，核心的事件处理、WAL 恢复、mmap 安全校验全没有自动化测试。
  - [x] 基础开关测试：--follow-symlinks=false 时，扫描不会进入符号链接指向的目录，比如指向根目录的软链，不会递归索引全系统
  - [x] 软链本身的处理：禁用跟随的时候，符号链接文件本身会不会被正常索引（而不是直接跳过这个文件本身）
  - [x] 事件层同步：禁用跟随的时候，事件监听不会监听符号链接指向的目录的变更，避免监听到根目录的全系统事件
  - [x] 嵌套软链测试：嵌套的符号链接（a 链到 b，b 链到根），会不会正确阻断递归，不会穿透
  - [x] 场景复现测试：模拟 Steam Proton 的 dosdevices/z:指向 / 的场景，验证不会触发全根目录索引
  - [x] ignore 规则贯通测试：冷扫、增量补扫、事件过滤，三层是不是都用了同一套 ignore 规则（.gitignore/ 全局 ignore 是不是都生效）
  - [x] 隐藏文件开关：--include-hidden 的开关，默认是不是跳过隐藏文件，开启后能不能正常扫描
  - [x] 多 root 隔离：多个--root 参数的场景，多个索引目录能不能正常隔离，不会互相干扰
  - [x] 超长路径容错：超过 65535 字节的超长路径，会不会正确跳过告警，不会 panic
  - [x] 大目录扫描：十万级文件的大目录，扫描会不会正常完成，不会 OOM 或者崩溃
  - [x] 高负载事件处理：模拟 git clone 的批量创建事件，会不会触发事件溢出，溢出后会不会正确触发 fast-sync，不会丢事件、不会全量重建
  - [x] 重命名事件处理：文件重命名后，索引会不会正确更新，不会当成删除 + 新建丢数据
  - [x] 删除事件处理：文件 / 目录删除后，索引会不会正确清理对应的条目
  - [x] 事件损坏恢复：WAL 里有损坏的事件记录，会不会正确跳过坏记录，不会把后面的所有事件全丢了
  - [x] watch 降级测试：事件监听退化到轮询的时候，能不能正常工作，不会丢索引
  - [x] 崩溃恢复测试：进程崩溃后重启，能不能正确回放 WAL，恢复所有增量事件，不用重新全量扫描
  - [x] 版本兼容测试：从 v2 到 v7 的旧版本快照 / WAL，能不能正确加载兼容，不会丢用户的旧索引
  - [x] 坏文件处理：快照 / WAL 文件损坏的时候，会不会正确识别、跳过，不会 panic，能不能触发兜底重建
  - [x] LSM compaction 测试：大量小变更后，compaction 能不能正确合并段、回收磁盘空间，不会段数暴涨
  - [x] 旧文件清理：seal 后的旧 WAL、过期的 LSM 段，能不能正确清理，不会残留占磁盘
  - [x] 过滤器有效性：size / 日期 /ext/regex 这些过滤器，能不能正确生效，有没有匹配错误
  - [x] 模糊查询：fuzzy 模式的匹配是不是正常，有没有排序错误
  - [x] 流式查询：大结果集的 UDS 流式查询，能不能边查边返回，不会在内存聚合导致 OOM
  - [x] UDS 权限：别的用户 /root 能不能访问你的 UDS socket，权限拦截是不是正常
  - [x] 短查询优化：1-2 字符的短查询，会不会触发短组件索引优化，不会退化全扫
- [x] 添加配置项 --follow-symlinks（默认 false），在 walker 和事件监听层禁用符号链接跟随，防止 Steam Proton 的 dosdevices/z: 递归索引整个根目录。
  - [x] 顺便补个循环检测：就算开了跟随，也加个 inode 的 visited 集合，挡住 a->b->a 这种循环软链，避免扫描死循环，把鲁棒性拉满。

### 小一点的毛病（v0.5.4 已修复）

- [x] 很多地方用了 unwrap，还有 let \_ = xxx 吞错误，比如删文件的时候忽略错误，需要补错误处理
  - [x] 把那些 let \_ = xxx 吞错误的地方，加上带上下文的 warn 日志，比如清理旧段、删 socket 的时候，失败了打个带路径的告警；还有把锁的 unwrap 换成 poison 兼容的处理，用 poison_err.into_inner()拿到锁，避免单个线程 panic 直接把整个守护进程干崩，提升长期运行的韧性。
- 各个版本的快照、WAL 的兼容代码，写的有点重复，比如 v1 到 v7 的加载逻辑，堆了不少重复的判断
  - [x] 存储模块的 mmap unsafe 代码，补上标准的 SAFETY:注释，说明这个 unsafe 的安全边界是什么，比如 “我们已经校验过文件范围，不会越界”，符合 Rust 的安全编码规范，也方便后续维护。
- [x] 少部分 unsafe 没加安全注释：mmap 的 unsafe 代码，没有加注释说明 “这个 unsafe 为什么是安全的”，Rust 的 unsafe 惯例是要加注释说明安全边界的，这个地方有点随意。
- [x] legacy 把早期的 SimpleChecksum 的 legacy 逻辑，加个版本告警，碰到旧版本的快照提醒升级，后续慢慢把这个过渡的校验算法淘汰掉，全换成标准 CRC32C，统一数据校验的可靠性。

## 许可证

MIT License (c) 2026 fd-rdd Contributors
