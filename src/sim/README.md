# fd-rdd-sim

`fd-rdd-sim` 是 tiered watcher 策略调优用的离线 synthetic 竞技场。它不会启动 daemon，也不会注册真实 inotify watcher；它只在模拟目录图和事件流上运行候选 L0/L1/L2/L3 策略，并输出可与 runtime `/watch-state` 趋势对照的报告。

## 常用命令

```bash
# 跨 developer/burst/dormant/adversarial/home-desktop workload 收敛推荐参数。
cargo run --bin fd-rdd-sim -- optimize \
  --dirs 3000 \
  --events 30000 \
  --generations 60 \
  --population 64 \
  --patience 10 \
  --checkpoint reports/optimized-tiered-watch.checkpoint.json \
  --output reports/optimized-tiered-watch.json

# 单策略基线诊断，不代表最优。
cargo run --bin fd-rdd-sim -- single --profile developer --dirs 1000 --events 10000

# 模拟日常 HOME 桌面工作负载：Downloads burst、文档/桌面、媒体、Code 和冷 NAS。
cargo run --bin fd-rdd-sim -- single --profile home-desktop --dirs 1200 --events 12000

# 参数网格搜索 baseline。
cargo run --bin fd-rdd-sim -- grid --profile burst --top-n 5

# 固定 seed small workloads 回归。
cargo run --bin fd-rdd-sim -- regression \
  --output reports/sim-regression.json \
  --markdown-output reports/sim-regression.md

# 遗传搜索单 workload baseline。
cargo run --bin fd-rdd-sim -- evolve --generations 16 --population 32

# 对抗鲁棒性报告。
cargo run --bin fd-rdd-sim -- adversarial --output reports/adversarial.json

# 从一个或多个报告生成保守 config.toml patch。
cargo run --bin fd-rdd-sim -- emit-config \
  --input reports/optimized-tiered-watch.json \
  --input reports/adversarial.json \
  --output reports/optimized-tiered-watch.toml

# 预览推荐参数合并到完整 config 后的结果，不写入用户配置。
cargo run --bin fd-rdd-sim -- apply \
  --input reports/optimized-tiered-watch.json \
  --config ~/.config/fd-rdd/config.toml \
  --dry-run
```

## 策略输入

可用 `--policy policies/tiered-default.toml` 读取策略基线。CLI 中显式传入的预算、TTL、扫描周期和 `--l3-scan-policy interval|validate_on_query|disabled` 会覆盖本次运行的 policy。`grid`、`evolve` 和 `optimize` 会搜索三种 L3 策略模式，不会默认把初始 seed 模式视为正确答案。

`optimize` 报告会暴露 `convergence.phase`、`current_generation`、`current_generation_trials`、`convergence.trace` 和 `recommendation`。推荐结果映射到 runtime 字段，例如 `watch_mode = "tiered"`、`max_watch_dirs`、L1/L2/L3 scan policy 和 TTL。

## 回归

`regression` 子命令内置 5 个固定 seed small workload：

- `developer-small`
- `burst-small`
- `dormant-small`
- `adversarial-small`
- `home-desktop-small`

每个 case 会检查 `p95_discovery_delay_secs`、`promotion_budget_blocked`、`watch_cost_peak`、`scanned_files`、`final_l0_dirs` 和 `final_l3_dirs`。任一阈值失败都会在写出 JSON/Markdown 报告后以非零退出码结束。

## Runtime 映射

runtime/sim 字段映射维护在 `../../helloagents/wiki/runtime-sim-report-mapping.md`。DirtyQueue 长度、directory manifest 计数、fast scan 计数、freshness 状态、mount policy 拒绝和 metrics JSONL diagnostics 等 runtime-only 字段，不应强行纳入 sim parity 阈值；除非模拟器显式建模对应成本。

长时间 `optimize` 可用 `Ctrl-C` 中断；checkpoint 会在每代结束后写出。继续迭代时，把同一个文件同时传给 `--resume` 和 `--checkpoint`。
