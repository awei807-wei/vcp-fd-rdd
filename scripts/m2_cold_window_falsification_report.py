"""M2 快速证伪 suite 的 Markdown 报告渲染。"""

from __future__ import annotations

from typing import Any


def _ratio_text(value: Any) -> str:
    return "n/a" if value is None else f"{float(value):.4f}"


def _number_text(value: Any, digits: int = 3) -> str:
    if value is None:
        return "n/a"
    if isinstance(value, float):
        return f"{value:.{digits}f}"
    return str(value)


def _paired_benefit_lines(gate: dict[str, Any]) -> list[str]:
    lines = [
        "| block | benefit | recovered positive paths | benefited bursts | benefited workloads | positive gain | visibility gain | poll-load symmetry |",
        "|---:|---|---:|---:|---|---:|---:|---:|",
    ]
    for row in gate["paired"]:
        lines.append(
            f"| {row['block']} | {row['benefited']} | "
            f"{row['recovered_primary_paths']} | "
            f"{row['benefited_burst_count']} | "
            f"{','.join(row['benefited_workloads']) or '-'} | "
            f"{row['positive_success_rate_gain']:.4f} | "
            f"{row['visibility_success_rate_gain']:.4f} | "
            f"{_ratio_text(row['query_poll_load_ratio'])} |"
        )
    return lines


def _paired_cost_lines(gate: dict[str, Any]) -> list[str]:
    lines = [
        "| block | CPU A/B (s) | CPU ratio | incremental CPU / recovered path (s) | read A/B (B) | read ratio | incremental read / recovered path (B) | write A/B (B) | write ratio | incremental write / recovered path (B) | RSS p95 delta (B) |",
        "|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|",
    ]
    for row in gate["paired"]:
        lines.append(
            f"| {row['block']} | {row['a_cpu_core_seconds']:.3f}/"
            f"{row['b_cpu_core_seconds']:.3f} | {_ratio_text(row['cpu_ratio'])} | "
            f"{_number_text(row['cpu_seconds_per_recovered_path'], 6)} | "
            f"{row['a_read_bytes']}/{row['b_read_bytes']} | "
            f"{_ratio_text(row['read_ratio'])} | "
            f"{_number_text(row['read_bytes_per_recovered_path'], 3)} | "
            f"{row['a_write_bytes']}/{row['b_write_bytes']} | "
            f"{_ratio_text(row['write_ratio'])} | "
            f"{_number_text(row['write_bytes_per_recovered_path'], 3)} | "
            f"{row['rss_p95_delta_bytes']} |"
        )
    return lines


def _leg_result_lines(legs: list[dict[str, Any]]) -> list[str]:
    lines = [
        "| block | order | leg | valid | bursts/expected records | unique assertions | positive/negative | visibility (p95 s / transport errors) | polls | target M2 seen/active/causal | sampling coverage/max gap | tier before | soft ratio/end | errors / watcher-remove / dirty-drop / rebuild total:bootstrap:quiesce:unattributed / window |",
        "|---:|---|---|---|---:|---:|---:|---:|---:|---:|---:|---|---:|---|",
    ]
    for leg in legs:
        correctness = leg["correctness"]
        protocol = leg["protocol"]
        stability = leg["stability"]
        tiers = ",".join(
            f"{key}:{value}" for key, value in protocol["tier_before"].items()
        )
        lines.append(
            f"| {leg['block']} | {leg['order']} | {leg['variant'].upper()} | "
            f"{leg['valid']} | {protocol['physical_bursts']}/"
            f"{protocol['events_total']} | {correctness['primary_unique_paths']} | "
            f"{correctness['positive_success_rate']:.4f}/"
            f"{correctness['negative_success_rate']:.4f} | "
            f"{correctness['visibility_success_rate']:.4f} "
            f"({correctness['visibility_p95_secs']:.3f} / "
            f"{correctness['visibility_transport_failures']}) | "
            f"{protocol['visibility_poll_count']} | "
            f"{protocol['target_m2_seen_bursts']}/"
            f"{protocol['target_m2_active_bursts']}/"
            f"{protocol['target_m2_causal_bursts']} | "
            f"{leg['resources']['sample_coverage_ratio']:.4f}/"
            f"{leg['resources']['sample_max_gap_secs']:.3f} | {tiers} | "
            f"{stability['waterline_soft_degraded_ratio']:.4f}/"
            f"{stability['waterline_soft_degraded_last']} | "
            f"{stability['log_error_count']}/"
            f"{stability.get('watch_remove_failure_count', 0)}/"
            f"{stability.get('dirty_queue_retry_drop_count', 0)}/"
            f"{stability.get('background_rebuild_count', 0)}:"
            f"{stability.get('bootstrap_background_rebuild_count', 0)}:"
            f"{stability.get('snapshot_quiesce_background_rebuild_count', 0)}:"
            f"{stability.get('unattributed_post_ready_background_rebuild_count', 0)}/"
            f"{stability.get('snapshot_log_window_valid', False)} |"
        )
    return lines


def _gate_summary_lines(gate: dict[str, Any]) -> list[str]:
    return [
        f"- 配对 CPU 中位比：`{_ratio_text(gate['median_cpu_ratio'])}`（中位及每个 block 均 ≤ 1.10）",
        f"- 配对 read bytes / syscall 中位比：`{_ratio_text(gate['median_read_bytes_ratio'])}` / `{_ratio_text(gate['median_read_syscalls_ratio'])}`（中位及每个 block 均 ≤ 1.10）",
        f"- 配对 write bytes / syscall 中位比：`{_ratio_text(gate['median_write_bytes_ratio'])}` / `{_ratio_text(gate['median_write_syscalls_ratio'])}`（中位及每个 block 均 ≤ 1.25）",
        f"- 查询轮询负载对称比：`{_ratio_text(gate['median_query_poll_load_ratio'])}`（中位及每个 block 均 ≤ 1.02）",
        f"- RSS p95 中位比 / 增量：`{_ratio_text(gate['median_rss_p95_ratio'])}` / `{_number_text(gate['median_rss_p95_delta_bytes'], 0)}` B（中位或任一 block 同时 >1.10 且 >32 MiB 即失败）",
        f"- minor fault 中位比 / 增量：`{_ratio_text(gate['median_minor_faults_ratio'])}` / `{_number_text(gate['median_minor_faults_delta_count'], 0)}`（中位或任一 block 同时 >1.25 且 >10000 即失败）",
        f"- major fault 中位增量：`{_number_text(gate['median_major_faults_delta_count'], 0)}`（中位及每个 block 均 ≤ 8）",
    ]


def render_report(summary: dict[str, Any]) -> str:
    """将 suite 汇总转换为可审计的 Markdown 报告。"""
    gate = summary["gate"]
    lines = [
        "# M2 快速证伪 A/B 报告",
        "",
        f"- 判定：`{gate['decision']}`",
        f"- 收益复现：`{gate['benefit_blocks']}/4` 个配对块",
        "- 实验单位：4 个配对 block（2×AB、2×BA），共 8 腿",
        "- 单腿协议：6 个 burst、806 个唯一主断言、38 个可见性探针",
        "- 正确性硬门：所有腿负向最终状态、主断言传输和 visibility 传输 100% 正确；A 正向断言和 visibility 100% 正确，visibility 总体及各 workload p95 均不超过 35 秒",
        *_gate_summary_lines(gate),
        "",
        "## 门禁原因",
        "",
        *[f"- {reason}" for reason in gate["reasons"] or ["无"]],
        "",
        "## 配对收益与测量负载",
        "",
        *_paired_benefit_lines(gate),
        "",
        "## 配对成本与单位收益成本",
        "",
        *_paired_cost_lines(gate),
        "",
        "> 成本硬门使用 daemon 从启动到持久化静默结束的完整运行窗口。CPU 使用 service time，I/O 使用 `/proc/<pid>/io` 增量；启动、240 秒预热、事件生成、轮询、后台扫描和安全 snapshot quiesce rebuild 均计入成本。",
        "",
        "> `process_after_event_storm_start` 事件风暴窗口包含起点前一个基准样本，用于诊断注入阶段；它不包含预热期 M2 成本，因此不替代完整运行门禁。旧 `process_after_first_burst` 仅保留兼容观测。",
        "",
        "> 主收益端点只使用 `recovered_primary_paths`：A 相对 B 至少多找回 5% 的正向主断言路径，并分布在至少 3 个 burst、2 类 workload，才算该 block 有收益。visibility 成功率与延迟仅作正确性/SLA 诊断，不会单独把 block 判成有收益。单位收益成本用完整运行 A-B 增量除以找回路径数，负值表示 A 同时节省资源。",
        "",
        "## 腿级结果",
        "",
        *_leg_result_lines(summary["legs"]),
        "",
        "## 解释边界",
        "",
        "- 固定目录调度按 tick/TTL 选择同一逻辑路径；每个 A burst 必须在写入时持有未过期 M2 租约，并证明写入后发生对应扫描或 watcher 事件，历史 `seen` 本身不构成归因。B 不得出现任一 M2 目标证据。visibility 探针即使已经可见也继续轮询，最终记录包含整个窗口的传输失败；每个配对 block 的总轮询量差异不得超过 2%。",
        "- 每腿 process 采样覆盖率必须 ≥95%、最大间隔 ≤3 秒且单调计数器不得回退；同一 block 两腿实际启动间隔不得超过 30 分钟，断点续跑按整块重跑，禁止跨会话拼接。",
        "- 负向断言只证明查询时的最终状态正确，不能单独证明旧路径曾进入 stable.v7，也不应被表述为 Tombstone 内部实现证明。",
        "- snapshot quiesce 若因结构移动触发一次可归因的安全 rebuild，只有最终 `ready=true`、无 ERROR 且次数不超过一次才允许通过；该 rebuild 的成本不会从完整运行窗口中剔除。",
        "- guest 内不能恢复 hypervisor VM/磁盘快照；fixture 重建、同一构建收据、腿私有执行副本、跨腿指纹和 AB/BA 平衡顺序用于降低偏差。",
        "- 当前阈值是工程筛查线，尚未由独立 B/B 或 A/A 噪声基线标定；4 个 block 只能作为进入大样本长跑前的快速证伪证据，不能单独建立生产环境因果结论，通过也不等价于生产发布结论。",
        "",
        "## 产物",
        "",
        *[
            f"- block {leg['block']} {leg['variant'].upper()}: `{leg['run_dir']}`"
            for leg in summary["legs"]
        ],
        "",
    ]
    return "\n".join(lines)
