# vcp-fd-rdd 重构方案评估与开发计划

## 项目概述
- **目标**: 评估 `causal-chain-report.md` 中对 vcp-fd-rdd (tests 分支) 的重构方案
- **源项目**: https://github.com/awei807-wei/vcp-fd-rdd/tree/tests
- **评估维度**: 可靠性、可实施性、ROI
- **输出**: 如果可行性足够高，输出每阶段的开发计划（明确到文件）、量化标准、验收标准

---

## Stage 1: 源码深度调研
**目标**: 从源码出发，验证报告中的六条因果链路和关键瓶颈点
**技能**: 无需特定技能，Orchestrator 直接调度

### 并行子任务:
1. **源码结构分析师**: 查看 src/ 下各模块结构，确认文件组织
2. **瓶颈验证员 A**: 查看 l2_partition.rs (for_each_live_meta_in_dirs), sync.rs (fast_sync), events.rs (pending_events)
3. **瓶颈验证员 B**: 查看 stream.rs (EventPipeline, Hybrid Crawler, overflow), watcher.rs (handle_notify_result)
4. **瓶颈验证员 C**: 查看 snapshot.rs, compaction.rs, rdd.rs (FileKey/syscall), main.rs (启动流程)
5. **数据验证员**: 查看 Cargo.toml (依赖), 确认当前数据结构和内存使用

### 输出:
- 源码关键位置确认报告
- 报告中的分析是否准确的验证结论
- 当前架构的真实瓶颈数据

---

## Stage 2: 重构方案评估
**目标**: 基于 Stage 1 的源码验证，评估重构方案的三大维度
**技能**: 无需特定技能

### 评估内容:
1. **可靠性**: 方案的技术假设是否正确？调研结论是否合理？借鉴点是否适用？
2. **可实施性**: 四阶段计划是否现实？文件修改清单是否完整？风险点有哪些？
3. **ROI**: 投入（8-11周）vs 产出（内存 700MB→100-180MB，CPU 0%，启动 5-10min→1-2s）

### 输出:
- 三维度评分与详细论证
- 风险点与缓解措施
- 是否继续的决策

---

## Stage 3: 详细开发计划（仅当可行性足够高时执行）
**目标**: 将四阶段计划细化到具体文件级别，制定量化标准和验收标准
**技能**: 无需特定技能

### 输出:
- 每阶段的文件级修改清单
- 每阶段的量化标准（性能指标、代码量变化）
- 每阶段的验收标准（测试要求、回归标准）
- 风险应对预案
- 最终 .md 报告

---

## 执行顺序
Stage 1 (并行调研) → Stage 2 (评估) → Stage 3 (开发计划)
