# fd-rdd 项目评审整改清单

> 结论一句话：核心索引与恢复链路做得不差，但默认暴露面、发布链路、文档一致性和核心模块复杂度是当前最该先收拾的几坨坑。

## 处理优先级约定

- **P0**：必须先处理；不建议继续扩大使用范围或对外分发
- **P1**：建议下个迭代完成；不处理会持续拖累维护和协作
- **P2**：体验/交付层优化；适合在主风险收敛后补齐

---

## P0：必须先处理

### 1. 收紧 HTTP 暴露面
- [ ] 将 HTTP 默认监听地址从 `0.0.0.0` 改为 `127.0.0.1`
- [ ] 或新增 `--http-host` / `--listen` 参数，并让默认值仅绑定本机
- [ ] README / API 文档明确标注：当前 HTTP 接口**无内建认证**，不能直接暴露到局域网或公网
- [ ] 如需远程访问，至少给出反向代理/认证的推荐部署方式

**影响**
- 当前实现默认接受任意可达客户端访问，可能暴露被索引文件的路径信息。

**相关位置**
- `src/query/server.rs:75-82`
- `helloagents/wiki/api.md:5-8`

**验收标准**
- 默认启动后只能从本机访问 `/search` 与 `/status`
- 文档明确写清安全边界与部署前提

---

### 2. 收紧默认索引范围与 daemon 拉起行为
- [ ] 不再在未传 `--root` 时默认扫描 `$HOME`
- [ ] 清理 `/tmp/vcp_test_data` 这类测试残留默认行为
- [ ] `fd-rdd-query --spawn` 不应在缺少显式根目录配置时直接拉起一个“扫家目录”的 daemon
- [ ] systemd service 示例必须改成带显式 `--root` / `--snapshot-path` 的形式

**影响**
- 当前默认行为过于激进，容易在用户不知情时索引整个家目录，带来隐私、性能和磁盘占用风险。

**相关位置**
- `src/main.rs:17-18`
- `src/main.rs:111-128`
- `src/bin/fd-rdd-query.rs:57-84`
- `scripts/fd-rdd.service:4-13`

**验收标准**
- 未显式指定根目录时，程序 fail fast 或进入明确的安全默认模式
- `--spawn` 行为不会隐式扩大索引范围
- service 示例可直接作为安全起点使用

---

### 3. 修复发布工作流并补上基础 CI
- [ ] 修复 release workflow 中错误的 `cd fd-rdd`
- [ ] 确认 tag 构建、打包、上传 release artifact 能在当前仓库结构下执行
- [ ] 新增至少一条日常 CI：`cargo fmt --check`、`cargo test`、必要时 `cargo clippy`
- [ ] 让 PR / push 也能跑基础质量闸门，而不是只在打 tag 时才跑

**影响**
- 现在的发布工作流大概率直接失败；同时缺少日常 CI，回归风险全靠人肉兜底。

**相关位置**
- `.github/workflows/release.yml:2-41`

**验收标准**
- PR 阶段能看到 build/test 结果
- tag release 在当前仓库布局下可正常产物发布

---

## P1：下个迭代建议完成

### 4. 增补“结果质量治理”专项（排序 / 脏数据 / 正确性分层处理）
- [ ] 明确区分三类问题：**索引正确性问题**、**排序问题**、**噪音/脏结果问题**
- [ ] 为日常查询建立一组人工可复现的基准 case，分别覆盖：
  - 已删除文件仍然命中
  - rename 后旧路径残留 / 新旧路径同时出现
  - 重启前后同一查询结果不一致
  - 目标文件存在但总被更深路径、隐藏目录、构建产物压在后面
  - 非目标噪音目录（如 cache / build / vendor / target）占据前排
- [ ] 给 exact / fuzzy 两条查询链路分别建立结果质量回归样例
- [ ] 将“搜得到”和“排得对”拆开评估，避免把所有问题都混成“脏数据”
- [ ] 把结果质量治理列为主线工作，而不是仅继续扩 DSL/过滤器语法

**影响**
- 当前项目已具备 HTTP 接入各类快速启动器/日常工作流的能力，真正影响体验的是结果质量：一部分是索引脏，一部分是排序傻，还有一部分是噪音结果没被压下去。如果不分层诊断，后续会一直在“感觉不对劲”里兜圈子。

**相关位置**
- `src/index/tiered.rs:1142-1261`
- `src/query/fzf.rs:33-91`
- `src/query/dsl.rs:324-343`

**验收标准**
- 能明确区分“结果错误”和“结果排序不理想”
- 有一组固定查询能稳定复现/验证结果质量改进
- 后续改排序或去噪策略时，能快速知道体感是否真的变好

---

### 5. 建立第一版“人脑友好”的排序策略
- [ ] 为 exact / fuzzy 结果增加统一排序信号设计，而不是仅按命中或 fuzzy score 裸排
- [ ] 至少评估并组合以下排序信号：
  - basename 完全匹配优先
  - basename 前缀匹配优先
  - basename 子串匹配高于仅 fullpath 命中
  - 路径更短优先
  - 路径层级更浅优先
  - 非隐藏路径优先于隐藏路径
  - 精准匹配优先于宽泛噪音命中
  - 最近修改/最近命中可作为次级加权（如后续需要）
- [ ] fuzzy 模式不要只按 `SkimMatcherV2` score 裸排序，至少叠加 basename / path depth / hidden 等纠偏信号
- [ ] 为排序策略补一组对照样例，证明“更像用户真正要点的那个结果”

**影响**
- 当前 fuzzy 排序基本是按 matcher score 倒序，`src/query/fzf.rs:56-67`；exact 结果则主要按索引扫描命中顺序返回，`src/index/tiered.rs:1180-1261`。这会导致大量“能搜到但不好点”的情况，尤其在同名文件、深层目录、构建产物较多时特别明显。

**相关位置**
- `src/query/fzf.rs:56-67`
- `src/query/fzf.rs:70-90`
- `src/index/tiered.rs:1180-1261`

**验收标准**
- 常见日常查询中，用户真正想要的结果明显更稳定地出现在前几位
- basename 命中、浅层路径、非隐藏结果不会长期被深层噪音压住
- fuzzy 与 exact 至少在“高优先结果长什么样”上达成基本一致

---

### 6. 加一层“去噪/降权”策略，而不是把所有非目标结果都当成索引 bug
- [ ] 先区分“真脏数据”和“噪音结果”：
  - 真脏数据：已删除仍命中、rename 残留、重启前后不一致
  - 噪音结果：cache、build、vendor、隐藏目录、超深路径等虽然存在，但通常不该排前
- [ ] 为常见噪音目录和文件模式建立第一版降权规则，而不是一上来全部硬过滤
- [ ] 评估是否对以下路径特征做降权：
  - 隐藏目录/隐藏文件
  - 构建输出目录
  - 缓存目录
  - 依赖/vendor 目录
  - 路径层级过深
  - 明显临时文件/日志文件模式
- [ ] 保留显式查询这些路径的能力，避免“为了干净把功能做死”

**影响**
- 当前很多“脏数据”感受，未必是索引错了，而是噪音结果权重过高。若不先做降权层，用户会持续感知到结果页很脏，即使索引本身是对的。

**相关位置**
- `src/index/tiered.rs:1180-1261`
- `src/query/fzf.rs:70-90`
- `src/main.rs:76-80`

**验收标准**
- 常见噪音目录不会长期占据前排
- 目标结果在不牺牲可搜性的前提下更容易进入首屏
- 显式搜隐藏/构建/缓存类路径时，仍然能正常命中

---

### 7. 统一 API 文档与实际实现
- [ ] 更新 HTTP 响应文档，和当前 `{ path, size }` 实现保持一致
- [ ] 更新 UDS 协议文档，明确它是“逐行 path 文本流”，不是 `ndjson/filemeta`
- [ ] 更新 UDS 安全文档，写清 peer credential 校验，而不是只写 socket 文件权限
- [ ] 将 `/status`、`mode=fuzzy`、超时返回等行为补进文档

**影响**
- 当前文档和实现已经漂移，调用方会被文档误导。

**相关位置**
- `helloagents/wiki/api.md:5-64`
- `src/query/server.rs:23-27`
- `src/query/server.rs:98-145`
- `src/query/socket.rs:47-65`
- `src/query/socket.rs:229-242`
- `README.md:157-178`

**验收标准**
- 文档样例能直接映射到真实接口返回和协议行为
- 新同学仅看文档即可正确调用 HTTP / UDS

---

### 8. 拆分 `TieredIndex` 这个超大核心对象
- [ ] 将查询执行、事件应用、snapshot 管理、后台重建/fast-sync、观测/RSS trim 拆成更明确的内部组件
- [ ] 降低 `tiered.rs` 的单文件复杂度，避免继续堆逻辑
- [ ] 为拆分后的边界补单测或集成测试

**影响**
- 目前 `TieredIndex` 同时承担过多职责，后续改 compaction、query、recovery 任一块都容易互相污染。

**相关位置**
- `src/index/tiered.rs:896`
- `src/index/tiered.rs:974`
- `src/index/tiered.rs:1142`
- `src/index/tiered.rs:1273`
- `src/index/tiered.rs:1546`
- `src/index/tiered.rs:1693`
- `src/index/tiered.rs:1876`
- `src/index/tiered.rs:1928`

**验收标准**
- 查询、持久化、恢复、观测至少能分到独立子模块/结构
- 核心模块职责边界可以被一句话讲清楚

---

### 9. 去掉短查询逻辑的双份实现
- [ ] 抽取 `normalize_short_hint` / `trigram_matches_short_hint` / `short_component_matches` / `for_each_short_component` 公共 helper
- [ ] 确保 L2 和 mmap 层共享相同语义与测试用例
- [ ] 为 1~2 字符查询建立回归测试，防止未来两套逻辑跑偏

**影响**
- 现在热路径和冷路径各维护一份近似实现，后续修 bug 很容易只修一边。

**相关位置**
- `src/index/l2_partition.rs:21-43`
- `src/index/l2_partition.rs:88-98`
- `src/index/mmap_index.rs:30-52`
- `src/index/mmap_index.rs:54-65`

**验收标准**
- 短查询逻辑只有一套权威实现
- 同一查询在 L2 / mmap 命中语义一致

---

### 10. 补齐真正的系统级回归测试
- [ ] 增加 HTTP 接口集成测试：`/search`、`/status`、timeout、limit clamp
- [ ] 增加 UDS 认证/流式返回测试
- [ ] 增加 watcher overflow → dirty tracker → fast-sync 的集成测试
- [ ] 将现有 smoke 脚本纳入 CI 或提供可自动执行的 smoke job

**影响**
- 目前模块内单测不少，但真正跨模块的服务级验证偏弱，容易出现“每块都对、拼一起出锅”。

**相关位置**
- `tests/p0_allocator.rs:1-9`
- `scripts/smoke-search-syntax.sh:1-205`
- `src/query/server.rs:98-145`
- `src/query/socket.rs:152-243`
- `src/event/stream.rs:124-270`

**验收标准**
- 关键对外接口和恢复路径具备自动回归保障
- PR 阶段能发现协议、服务启动、overflow 恢复类回归

---

## P2：体验和交付层补齐

### 11. 补齐安装脚本交付物
- [ ] `install.sh` 一并安装 `fd-rdd-query`
- [ ] 视交付方式决定是否安装/生成 service 模板
- [ ] 安装完成后的提示信息与 README 用法保持一致

**影响**
- README 推荐的客户端工具没有被安装脚本一并交付，用户体验割裂。

**相关位置**
- `scripts/install.sh:5-15`
- `README.md:166-172`

**验收标准**
- 按安装脚本安装后，README 中的常用命令都能直接执行

---

### 12. 补一份“安全默认部署”示例
- [ ] 提供最小可用的 systemd / shell 启动示例
- [ ] 示例中显式包含 `--root`、`--snapshot-path`、监听地址与忽略路径
- [ ] 写明本地开发、单机自用、局域网代理三种推荐姿势

**影响**
- 当前项目有不错的核心实现，但缺少一份真正能让用户安全落地的标准示例。

**相关位置**
- `scripts/fd-rdd.service:1-15`
- `README.md:139-178`

**验收标准**
- 用户复制官方示例后，不会默认把服务暴露错地方，也不会误扫整块家目录

---

## 整改时建议保留的设计亮点

这些东西别手一抖给回退了：

- [ ] 保留 UDS peer credential 校验
  - `src/query/socket.rs:47-65`
- [ ] 保留 snapshot / WAL 的损坏检测与长度上限保护
  - `src/storage/snapshot.rs:26-29`
  - `src/storage/wal.rs:10-12`
- [ ] 保留 overflow 后 dirty tracker + fast-sync 的恢复思路
  - `src/event/stream.rs:239-265`
  - `src/event/recovery.rs:117-191`
- [ ] 保留 MemoryReport / smaps / page faults 这套观测闭环
  - `src/stats/mod.rs:2-37`
  - `src/stats/mod.rs:155-225`

---

## 推荐执行顺序

1. 先做 **P0-1 / P0-2 / P0-3**，把安全默认值和发布链路拉回正轨
2. 再做 **P1-4 / P1-5 / P1-6**，先把“结果质量”这条主线立起来，明确区分正确性、排序和噪音问题
3. 接着做 **P1-7 / P1-10**，让文档与系统级回归保障追上实现
4. 最后做 **P1-8 / P1-9 / P2**，逐步降低维护成本并补齐交付体验
