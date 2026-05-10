#!/usr/bin/env bash
# Start a long-running fd-rdd-sim optimize job with checkpoint/resume enabled.

if [ -z "${BASH_VERSION:-}" ]; then
  exec bash "$0" "$@"
fi

set -euo pipefail

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd -- "$SCRIPT_DIR/.." && pwd)"
cd "$REPO_ROOT"

REPORT_DIR="${FD_RDD_SIM_REPORT_DIR:-reports}"
POLICY="${FD_RDD_SIM_POLICY:-policies/tiered-default.toml}"
SIM_RUST_LOG="${RUST_LOG:-fd_rdd::sim=info}"
DIRS="${FD_RDD_SIM_DIRS:-3000}"
EVENTS="${FD_RDD_SIM_EVENTS:-30000}"
DURATION_SECS="${FD_RDD_SIM_DURATION_SECS:-7200}"
SEED="${FD_RDD_SIM_SEED:-42}"
GENERATIONS="${FD_RDD_SIM_GENERATIONS:-120}"
POPULATION="${FD_RDD_SIM_POPULATION:-96}"
PATIENCE="${FD_RDD_SIM_PATIENCE:-20}"
TOP_N="${FD_RDD_SIM_TOP_N:-20}"
FRESH=0
BACKGROUND=0

usage() {
  cat <<'EOF'
Usage:
  scripts/start-fd-rdd-sim-optimize.sh [--fresh] [--background] [--report-dir <dir>] [-- <extra fd-rdd-sim args>]

Env overrides:
  FD_RDD_SIM_REPORT_DIR       default: reports
  FD_RDD_SIM_POLICY           default: policies/tiered-default.toml when present, otherwise built-in default
  RUST_LOG                    default: fd_rdd::sim=info
  FD_RDD_SIM_DIRS             default: 3000
  FD_RDD_SIM_EVENTS           default: 30000
  FD_RDD_SIM_DURATION_SECS    default: 7200
  FD_RDD_SIM_SEED             default: 42
  FD_RDD_SIM_GENERATIONS      default: 120
  FD_RDD_SIM_POPULATION       default: 96
  FD_RDD_SIM_PATIENCE         default: 20
  FD_RDD_SIM_TOP_N            default: 20

Examples:
  scripts/start-fd-rdd-sim-optimize.sh
  scripts/start-fd-rdd-sim-optimize.sh --background
  FD_RDD_SIM_GENERATIONS=300 FD_RDD_SIM_POPULATION=128 scripts/start-fd-rdd-sim-optimize.sh

Progress:
  jq '{phase:.convergence.phase,generation:.convergence.current_generation,trials:.convergence.trials,best_score:.convergence.best_score,recommendation:.recommendation}' reports/optimized-tiered-watch.checkpoint.json
EOF
}

EXTRA_ARGS=()
while [[ $# -gt 0 ]]; do
  case "$1" in
    --fresh)
      FRESH=1
      shift
      ;;
    --background)
      BACKGROUND=1
      shift
      ;;
    --report-dir)
      REPORT_DIR="${2:-}"
      shift 2
      ;;
    --help|-h)
      usage
      exit 0
      ;;
    --)
      shift
      EXTRA_ARGS+=("$@")
      break
      ;;
    *)
      echo "Unknown arg: $1" >&2
      usage >&2
      exit 2
      ;;
  esac
done

[[ -n "$REPORT_DIR" ]] || { echo "Missing --report-dir value" >&2; exit 2; }
command -v cargo >/dev/null || { echo "Missing dependency: cargo" >&2; exit 2; }
if [[ ! -f src/bin/fd-rdd-sim.rs ]]; then
  echo "fd-rdd-sim binary source is missing: src/bin/fd-rdd-sim.rs" >&2
  echo "Please run this script from a checkout that includes the sim work, for example the latest tests branch." >&2
  echo "Current repo: $REPO_ROOT" >&2
  exit 2
fi

mkdir -p "$REPORT_DIR"

CHECKPOINT="$REPORT_DIR/optimized-tiered-watch.checkpoint.json"
OUTPUT="$REPORT_DIR/optimized-tiered-watch.json"
LOG="$REPORT_DIR/optimized-tiered-watch.log"

cmd=(
  cargo run --release --bin fd-rdd-sim -- optimize
  --dirs "$DIRS"
  --events "$EVENTS"
  --duration-secs "$DURATION_SECS"
  --seed "$SEED"
  --generations "$GENERATIONS"
  --population "$POPULATION"
  --patience "$PATIENCE"
  --top-n "$TOP_N"
  --checkpoint "$CHECKPOINT"
  --output "$OUTPUT"
)

if [[ -f "$POLICY" ]]; then
  cmd+=(--policy "$POLICY")
else
  echo "Policy file not found: $POLICY; using fd-rdd-sim built-in default policy."
fi

if [[ "$FRESH" -eq 0 && -f "$CHECKPOINT" ]]; then
  cmd+=(--resume "$CHECKPOINT")
fi

cmd+=("${EXTRA_ARGS[@]}")

echo "fd-rdd-sim optimize"
echo "  report:     $OUTPUT"
echo "  checkpoint: $CHECKPOINT"
echo "  log:        $LOG"
echo "  fresh:      $FRESH"
echo "  RUST_LOG:   $SIM_RUST_LOG"
printf '  command:'
printf ' %q' "${cmd[@]}"
printf '\n'

if [[ "$BACKGROUND" -eq 1 ]]; then
  nohup env RUST_LOG="$SIM_RUST_LOG" "${cmd[@]}" >"$LOG" 2>&1 &
  pid=$!
  echo "$pid" >"$REPORT_DIR/optimized-tiered-watch.pid"
  echo "Started in background: pid=$pid"
  echo "Follow log: tail -f $LOG"
else
  env RUST_LOG="$SIM_RUST_LOG" "${cmd[@]}" 2>&1 | tee "$LOG"
fi
