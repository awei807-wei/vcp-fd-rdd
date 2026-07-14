"""Shared fail-closed evaluation for target-level M2 causal evidence."""

from __future__ import annotations

from typing import Any


VALID_M2_ACTIONS = frozenset({"ephemeral_watch", "fast_scan_lease", "scan_only"})


def _integer(value: Any, default: int = 0) -> int:
    try:
        return int(value or 0)
    except (TypeError, ValueError):
        return default


def evaluate_target_m2_causality(
    burst: dict[str, Any],
    check: dict[str, Any],
) -> dict[str, Any]:
    """Evaluate before→fence→after progress within one rotating lease cycle."""
    action = str(burst.get("target_m2_action", ""))
    cycle_id = _integer(burst.get("target_m2_cycle_id"), -1)
    before_scan = _integer(burst.get("target_m2_scan_seq"))
    before_event = _integer(burst.get("target_m2_event_seq"))
    fence_scan = _integer(burst.get("target_m2_fence_scan_seq"))
    fence_event = _integer(burst.get("target_m2_fence_event_seq"))
    after_scan = _integer(check.get("target_m2_after_scan_seq"))
    after_event = _integer(check.get("target_m2_after_event_seq"))

    fence_reasons: list[str] = []
    if burst.get("target_m2_debug_ok") is not True:
        fence_reasons.append("before debug 失败")
    if burst.get("target_m2_entry_present") is not True:
        fence_reasons.append("before exact entry 缺失")
    if burst.get("target_m2_active") is not True:
        fence_reasons.append("before lease 未激活")
    if action not in VALID_M2_ACTIONS:
        fence_reasons.append(f"before action 非法：{action or 'missing'}")
    if burst.get("target_m2_fence_debug_ok") is not True:
        fence_reasons.append("fence debug 失败")
    if burst.get("target_m2_fence_entry_present") is not True:
        fence_reasons.append("fence exact entry 缺失")
    if burst.get("target_m2_fence_active") is not True:
        fence_reasons.append("fence lease 未激活")
    if str(burst.get("target_m2_fence_action", "")) != action:
        fence_reasons.append("fence action 与 before 不一致")
    if _integer(burst.get("target_m2_fence_cycle_id"), -2) != cycle_id:
        fence_reasons.append("fence cycle 与 before 不一致")
    if fence_scan < before_scan or fence_event < before_event:
        fence_reasons.append("fence 序列发生回退")

    after_reasons: list[str] = []
    if check.get("target_m2_after_debug_ok") is not True:
        after_reasons.append("after debug 失败")
    if check.get("target_m2_after_entry_present") is not True:
        after_reasons.append("after exact entry 缺失")
    if check.get("target_m2_after_active") is not True:
        after_reasons.append("after lease 未激活")
    if str(check.get("target_m2_after_action", "")) != action:
        after_reasons.append("after action 与 before 不一致")
    if _integer(check.get("target_m2_after_cycle_id"), -2) != cycle_id:
        after_reasons.append("after cycle 与 before 不一致")
    if after_scan < fence_scan or after_event < fence_event:
        after_reasons.append("after 序列发生回退")

    scan_during = (
        fence_scan > before_scan
        and _integer(burst.get("target_m2_fence_scan_cycle_id"), -2) == cycle_id
    )
    scan_after = (
        after_scan > fence_scan
        and _integer(check.get("target_m2_after_scan_cycle_id"), -2) == cycle_id
    )
    event_during = (
        fence_event > before_event
        and _integer(burst.get("target_m2_fence_event_cycle_id"), -2) == cycle_id
    )
    event_after = (
        after_event > fence_event
        and _integer(check.get("target_m2_after_event_cycle_id"), -2) == cycle_id
    )
    scan_advanced = scan_during or scan_after
    event_advanced = event_during or event_after
    fence_valid = not fence_reasons
    after_valid = not after_reasons
    causal = (
        fence_valid
        and after_valid
        and (scan_advanced or (action == "ephemeral_watch" and event_advanced))
    )
    reasons = [*fence_reasons, *after_reasons]
    if fence_valid and after_valid and not causal:
        reasons.append("同一 lease cycle 内没有符合 action 的 scan/event 推进")
    return {
        "causal": causal,
        "fence_valid": fence_valid,
        "after_valid": after_valid,
        "reasons": reasons,
        "action": action,
        "cycle_id": cycle_id,
        "before_scan_seq": before_scan,
        "fence_scan_seq": fence_scan,
        "after_scan_seq": after_scan,
        "before_event_seq": before_event,
        "fence_event_seq": fence_event,
        "after_event_seq": after_event,
        "scan_advanced_during_mutation": scan_during,
        "scan_advanced_after_mutation": scan_after,
        "event_advanced_during_mutation": event_during,
        "event_advanced_after_mutation": event_after,
        "scan_advanced": scan_advanced,
        "event_advanced": event_advanced,
    }


def target_m2_fence_valid(burst: dict[str, Any]) -> bool:
    """Return whether before/fence evidence is internally consistent."""
    return bool(evaluate_target_m2_causality(burst, {})["fence_valid"])


def target_m2_causal(burst: dict[str, Any], check: dict[str, Any]) -> bool:
    """Return whether the evidence proves M2 progress for the burst."""
    return bool(evaluate_target_m2_causality(burst, check)["causal"])
