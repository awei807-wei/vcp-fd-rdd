#!/usr/bin/env bash

set -euo pipefail

OUTPUT_DIR=""
JOB_NAME=""
WORKFLOW_NAME=""
HTTP_PORT=""
declare -a LOG_PATHS=()
declare -a PATH_PATTERNS=()

usage() {
  cat <<'EOF'
Usage:
  scripts/ci-collect-debug.sh \
    --output <dir> \
    --job <job-name> \
    --workflow <workflow-name> \
    [--port <http-port>] \
    [--log <file>]... \
    [--path <file-or-dir-or-glob>]...

Examples:
  scripts/ci-collect-debug.sh \
    --output /tmp/fd-rdd-debug/smoke \
    --job smoke \
    --workflow ci \
    --port 6060 \
    --log /tmp/fd-rdd-daemon.log \
    --path /tmp/fd-rdd-smoke-root \
    --path /tmp/fd-rdd-smoke-data*
EOF
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --output)
      OUTPUT_DIR="${2:-}"
      shift 2
      ;;
    --job)
      JOB_NAME="${2:-}"
      shift 2
      ;;
    --workflow)
      WORKFLOW_NAME="${2:-}"
      shift 2
      ;;
    --port)
      HTTP_PORT="${2:-}"
      shift 2
      ;;
    --log)
      LOG_PATHS+=("${2:-}")
      shift 2
      ;;
    --path)
      PATH_PATTERNS+=("${2:-}")
      shift 2
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      echo "Unknown arg: $1" >&2
      usage >&2
      exit 2
      ;;
  esac
done

[[ -n "$OUTPUT_DIR" ]] || { echo "Missing --output" >&2; exit 2; }
[[ -n "$JOB_NAME" ]] || { echo "Missing --job" >&2; exit 2; }
[[ -n "$WORKFLOW_NAME" ]] || { echo "Missing --workflow" >&2; exit 2; }

mkdir -p "$OUTPUT_DIR" "$OUTPUT_DIR/logs" "$OUTPUT_DIR/paths" "$OUTPUT_DIR/http"

SUMMARY_FILE="$OUTPUT_DIR/summary.md"
METADATA_FILE="$OUTPUT_DIR/metadata.txt"
SYSTEM_FILE="$OUTPUT_DIR/system.txt"

append_summary() {
  printf '%s\n' "$*" >> "$SUMMARY_FILE"
}

sanitize_name() {
  printf '%s' "$1" | tr '/\\:*?"<>| ' '_' | tr -cd '[:alnum:]_.-'
}

collect_http_endpoint() {
  local endpoint="$1"
  local base="http://127.0.0.1:${HTTP_PORT}/${endpoint}"
  local header_file="$OUTPUT_DIR/http/${endpoint}.headers.txt"
  local body_file="$OUTPUT_DIR/http/${endpoint}.body.txt"
  local status_file="$OUTPUT_DIR/http/${endpoint}.curl-status.txt"

  if [[ -z "$HTTP_PORT" ]]; then
    return 0
  fi

  if curl -fsS --max-time 3 -D "$header_file" "$base" -o "$body_file" >"$status_file" 2>&1; then
    append_summary "- HTTP /${endpoint}: collected"
  else
    append_summary "- HTTP /${endpoint}: unavailable"
    printf 'curl failed for %s\n' "$base" >> "$status_file"
  fi
}

collect_log_file() {
  local src="$1"
  local idx="$2"
  local name
  name="$(sanitize_name "$(basename "$src")")"
  local dst="$OUTPUT_DIR/logs/${idx}_${name}"
  local tail_file="$OUTPUT_DIR/logs/${idx}_${name}.tail.txt"

  if [[ -e "$src" ]]; then
    cp -R "$src" "$dst" 2>/dev/null || cp "$src" "$dst" 2>/dev/null || true
    tail -200 "$src" > "$tail_file" 2>/dev/null || true
    append_summary "- log: ${src}"
  else
    printf 'missing log: %s\n' "$src" >> "$OUTPUT_DIR/logs/missing.txt"
    append_summary "- missing log: ${src}"
  fi
}

collect_path_item() {
  local src="$1"
  local idx="$2"
  local label
  label="$(sanitize_name "$src")"
  local report="$OUTPUT_DIR/paths/${idx}_${label}.txt"

  {
    echo "path: $src"
    if [[ -e "$src" ]]; then
      echo
      echo "== stat =="
      ls -lad "$src" || true
      echo
      echo "== du =="
      du -sh "$src" 2>/dev/null || true
      echo
      echo "== tree/ls =="
      ls -laR "$src" 2>/dev/null || true
    else
      echo "missing"
    fi
  } > "$report"
}

expand_and_collect_paths() {
  local idx=0
  local pattern
  shopt -s nullglob
  for pattern in "${PATH_PATTERNS[@]}"; do
    local matches=()
    if [[ "$pattern" == *'*'* || "$pattern" == *'?'* || "$pattern" == *'['* ]]; then
      matches=( $pattern )
    else
      matches=( "$pattern" )
    fi

    if [[ ${#matches[@]} -eq 0 ]]; then
      idx=$((idx + 1))
      printf 'missing path pattern: %s\n' "$pattern" >> "$OUTPUT_DIR/paths/missing.txt"
      append_summary "- missing path: ${pattern}"
      continue
    fi

    local match
    for match in "${matches[@]}"; do
      idx=$((idx + 1))
      collect_path_item "$match" "$idx"
      append_summary "- path: ${match}"
    done
  done
  shopt -u nullglob
}

{
  echo "# fd-rdd CI Debug Bundle"
  echo
  echo "- workflow: ${WORKFLOW_NAME}"
  echo "- job: ${JOB_NAME}"
  echo "- collected_at_utc: $(date -u +'%Y-%m-%dT%H:%M:%SZ')"
  echo "- workspace: $(pwd)"
  if [[ -n "$HTTP_PORT" ]]; then
    echo "- http_port: ${HTTP_PORT}"
  fi
  echo
  echo "## Collected Items"
} > "$SUMMARY_FILE"

{
  echo "workflow=${WORKFLOW_NAME}"
  echo "job=${JOB_NAME}"
  echo "collected_at_utc=$(date -u +'%Y-%m-%dT%H:%M:%SZ')"
  echo "pwd=$(pwd)"
  echo "runner_os=${RUNNER_OS:-unknown}"
  echo "runner_temp=${RUNNER_TEMP:-unknown}"
  echo "github_run_id=${GITHUB_RUN_ID:-unknown}"
  echo "github_sha=${GITHUB_SHA:-unknown}"
} > "$METADATA_FILE"

{
  echo "== uname =="
  uname -a || true
  echo
  echo "== rustc -vV =="
  rustc -vV || true
  echo
  echo "== cargo --version =="
  cargo --version || true
  echo
  echo "== jq --version =="
  jq --version || true
  echo
  echo "== df -h =="
  df -h || true
  echo
  echo "== free -m =="
  free -m || true
  echo
  echo "== ulimit -a =="
  ulimit -a || true
  echo
  echo "== ps aux =="
  ps aux || true
} > "$SYSTEM_FILE" 2>&1

collect_http_endpoint "status"
collect_http_endpoint "health"

if [[ ${#LOG_PATHS[@]} -gt 0 ]]; then
  idx=0
  for log_path in "${LOG_PATHS[@]}"; do
    idx=$((idx + 1))
    collect_log_file "$log_path" "$idx"
  done
fi

if [[ ${#PATH_PATTERNS[@]} -gt 0 ]]; then
  expand_and_collect_paths
fi

if [[ -n "${GITHUB_STEP_SUMMARY:-}" && -f "$SUMMARY_FILE" ]]; then
  cat "$SUMMARY_FILE" >> "$GITHUB_STEP_SUMMARY"
fi

echo "Collected debug bundle at $OUTPUT_DIR"
