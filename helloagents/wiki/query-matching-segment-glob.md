# 查询语义开发文档：Glob 按“文件名/路径段”匹配

## 背景与问题

当前 `fd-rdd` 的通配符查询（`*`/`?`）使用 `wildmatch` 对**完整路径字符串**进行匹配：

- 例：索引条目路径为 `/tmp/vcptest/test_123`
- 用户查询 `test_*` 的直觉含义是“全局文件名以 `test_` 开头”
- 但在“全路径匹配”语义下，`test_*` 只会匹配以 `t` 开头的路径字符串，因此无法命中上述绝对路径

这与对标 Everything 的目标（更符合用户直觉的“模糊/全局”查找）相悖。

## 目标（Goals）

- 让 `test_*` 表达“全局匹配任意目录下，文件名以 `test_` 开头”
- 通配符模式默认按**文件名**或**任意路径段（path segment）**匹配，而不是按完整路径从头匹配
- 保留“可精确限定目录”的能力（当用户在模式中显式包含路径分隔符时）

## 非目标（Non-goals）

- 本文不覆盖 L2 缓存/增量构建、事件风暴等性能问题（另案处理）
- 不引入复杂的查询语法（如 Everything 的完整 DSL），仅调整 glob 默认语义与兼容规则

## 术语与现状

### 现状代码位置（便于定位）

- 匹配器：`src/query/matcher.rs`
- RDD 过滤入口：`src/core/rdd.rs`（`RDD::collect_with_filter` 调用 `filter_item`）
- FileEntry 过滤实现：`src/core/rdd.rs`（`FileIndexRDD::filter_item` 将 `path` 转为字符串参与匹配）

### 关键语义点

当前 GlobMatcher 的 `matches(path: &str)` 输入是“完整路径字符串”（例如 `/tmp/vcptest/test_123`）。
因此 `test_*` 不会命中，除非用户写 `*test_*` 或显式加目录前缀 `/tmp/vcptest/test_*`。

## 新语义设计（按文件名/路径段匹配）

### 规则 R1：是否按“完整路径”匹配的判定

当查询模式包含路径分隔符时，视为用户在描述路径结构，使用“完整路径 glob”：

- 分隔符判定建议同时支持 `/` 与 `\\`（兼容 Windows/跨平台输入）
- 示例：
  - `/tmp/vcptest/test_*`：限定目录 + 文件名 glob（完整路径匹配）
  - `tmp/*/test_??`：结构化路径匹配（完整路径匹配）

当查询模式**不包含**路径分隔符时，视为“全局名称匹配”，使用“段匹配”：

- 示例：
  - `test_*`：全局文件名/段匹配（能命中 `/tmp/vcptest/test_123`）
  - `target`：若未来扩展到非 glob 的段匹配，也可命中任意名为 `target` 的目录段（本次可不改）

### 规则 R2：段匹配的匹配域（match domain）

段匹配时，对每个索引条目路径，构造候选字符串集合并逐个尝试匹配：

建议最小实现（优先满足用户诉求）：

1) `basename`（文件名）：`Path::file_name()`

可选增强（更贴近 Everything 的“路径段”直觉）：

2) 任意路径段：遍历 `Path::components()`，对每个段的显示形式做匹配

推荐顺序：先匹配 basename（命中率高、成本低），再匹配其他段（成本略高）。

### 规则 R3：目录与文件

当前索引侧 `scan_partition` 只收集文件（过滤 `is_file()`），因此段匹配主要命中“文件名段”。
若未来要支持目录搜索，需要调整索引收集逻辑与 API 返回结构（本次不做）。

## 兼容性与用户迁移

### 兼容性策略

- 旧行为（完整路径 glob）仍可用：只要用户在模式里写出 `/`（或 `\\`），就走完整路径匹配
- 旧用法 `*test_*` 仍有效：在段匹配模式下，`*test_*` 也能命中文件名（等价但冗余）

### 行为差异示例（建议加入 README/FAQ）

假设存在 `/tmp/vcptest/test_123`：

- `q=test_*`
  - 旧：不命中
  - 新：命中（basename= `test_123`）
- `q=/tmp/vcptest/test_*`
  - 旧：命中
  - 新：命中（完整路径匹配）

## 实现指南（不写代码，但给出落点与步骤）

### 步骤 S1：扩展 Matcher 能力（推荐方案）

保持现有 `Matcher::matches(&self, path: &str)` 不变的情况下，引入一个“段匹配适配层”：

- 为 glob matcher 增加一个模式字段：
  - `GlobMode::FullPath`
  - `GlobMode::Segment`
- `create_matcher(pattern)`：
  - 若包含 `*`/`?`：
    - 若 pattern 含 `/` 或 `\\` => `FullPath`
    - 否则 => `Segment`
  - 否则沿用现有 contains 语义（本次不必修改）

### 步骤 S2：在 FileIndexRDD::filter_item 中落实“段匹配”

落点：`src/core/rdd.rs` 的 `FileIndexRDD::filter_item`

- FullPath：维持现状，对完整路径字符串调用 `matcher.matches(...)`
- Segment：
  - 先取 `file_name`（无法转换为字符串则跳过或按 lossy 处理）
  - 可选：遍历 components（排除根/盘符等非名称段），对每个段做匹配
  - 任一候选命中则返回 true

注意：这里是过滤链路的热路径；即便是“只匹配 basename”，也比全路径通配符更符合直觉且更快。

### 步骤 S3：调整 L1 的 prefix 启发式（如启用 Segment）

当前 L1 查询会用 `prefix()` 做一次 `contains(prefix)` 的快速过滤。
当 glob 进入 Segment 模式时，prefix 更接近“文件名固定前缀”，建议把这层启发式也改为对 basename/段做判断，否则可能出现：

- 性能不达预期（仍要对很多全路径做 `contains`/`matches`）
- 误导性的过滤（比如 prefix 在目录段出现导致大量 false positive）

最小可行策略：

- Segment 模式下：只对 basename 做 prefix 判断（不再对全路径 contains）

### 步骤 S4：补充测试用例（强烈建议）

建议新增单元测试覆盖 `create_matcher` 与 glob 语义（不依赖真实磁盘扫描）：

- `test_*` 命中 `/tmp/vcptest/test_123`
- `test_*` 不应命中 `/tmp/vcptest/attest_123`（basename 不以 test_ 开头）
- `/tmp/vcptest/test_*` 只命中该目录下的文件
- Windows 风格 `C:\\tmp\\vcptest\\test_*`（至少保证分隔符判定不出错）

集成测试（可选）：

- 在临时目录创建少量文件，跑一次 query handler，验证返回包含预期路径

## 运行与使用注意事项（FAQ）

- 如果通过命令行调用 HTTP 查询，务必给 `q` 加引号，避免 shell 先展开 `*`：
  - 例：`curl 'http://localhost:6060/search?q=test_*'`
- roots 配置需包含目标目录，否则即使匹配语义正确也搜不到。
  - 当前主程序默认 roots 不包含 `/tmp/vcptest`（需要显式加入或通过配置/参数提供）

## 风险与回滚策略

- 风险：用户曾依赖“无分隔符 glob = 全路径匹配”的旧语义（概率较低，但存在）
- 缓解：保留“包含路径分隔符 => 全路径匹配”的通道，必要时在 README 里给出迁移提示
- 回滚：将 `create_matcher` 的判定恢复为“glob 一律全路径匹配”

