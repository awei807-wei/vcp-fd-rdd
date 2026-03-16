#!/usr/bin/env bash
# smoke-search-syntax.sh - 搜索 DSL 冒烟测试（创建示例文件 + 调用 HTTP API）

set -euo pipefail

ROOT=""
BASE_URL="${FD_RDD_SMOKE_BASE_URL:-http://127.0.0.1:6060}"
TIMEOUT_SECS="${FD_RDD_SMOKE_TIMEOUT_SECS:-20}"
KEEP=1

usage() {
  cat <<'EOF'
Usage:
  scripts/smoke-search-syntax.sh --root <fd-rdd-root> [--base-url <url>] [--timeout <secs>] [--cleanup]

Env:
  FD_RDD_SMOKE_BASE_URL        default: http://127.0.0.1:6060
  FD_RDD_SMOKE_TIMEOUT_SECS    default: 20
EOF
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --root) ROOT="${2:-}"; shift 2 ;;
    --base-url) BASE_URL="${2:-}"; shift 2 ;;
    --timeout) TIMEOUT_SECS="${2:-}"; shift 2 ;;
    --cleanup) KEEP=0; shift ;;
    -h|--help) usage; exit 0 ;;
    *) echo "Unknown arg: $1" >&2; usage; exit 2 ;;
  esac
done

[[ -n "$ROOT" ]] || { echo "Missing --root" >&2; usage; exit 2; }
[[ -d "$ROOT" ]] || { echo "Root 不是目录：$ROOT" >&2; exit 2; }
command -v curl >/dev/null || { echo "Missing dependency: curl" >&2; exit 2; }
command -v jq >/dev/null || { echo "Missing dependency: jq" >&2; exit 2; }

curl -fsS "${BASE_URL%/}/status" >/dev/null || {
  echo "fd-rdd HTTP 不可用：${BASE_URL%/}/status" >&2
  echo "请确认 daemon 已启动且端口正确（默认 6060）。" >&2
  exit 2
}

RUN_ID="$(date +%Y%m%d%H%M%S)_$$"
BASE_DIR="${ROOT%/}/fd_rdd_smoke_${RUN_ID}"
EXCLUDE_DIR="exclude_${RUN_ID}"

cleanup() {
  if [[ "$KEEP" -eq 0 ]]; then
    rm -rf "$BASE_DIR"
  else
    echo "保留样例目录：$BASE_DIR"
  fi
}
trap cleanup EXIT

api_search_paths() {
  local q="$1"
  curl -fsS -G "${BASE_URL%/}/search" --data-urlencode "q=$q" --data-urlencode "limit=200" \
    | jq -r '.[].path'
}

wait_until_indexed() {
  local q="$1"
  local expect="$2"
  local deadline=$((SECONDS + TIMEOUT_SECS))
  while (( SECONDS < deadline )); do
    local out
    out="$(api_search_paths "$q" || true)"
    if printf '%s\n' "$out" | grep -Fq "$expect"; then
      return 0
    fi
    sleep 0.2
  done
  echo "等待索引超时（${TIMEOUT_SECS}s）：未命中 $expect" >&2
  echo "提示：请确认 daemon 的 --root 覆盖了该目录，且未使用 --no-watch。" >&2
  return 1
}

mkdir -p "$BASE_DIR"

# 1) Smart-Case / case:
mkdir -p "$BASE_DIR/case"
printf '0123456789abcdef\n' > "$BASE_DIR/case/VCP_${RUN_ID}.txt"
printf '0123456789abcdef\n' > "$BASE_DIR/case/vcp_${RUN_ID}.txt"

# 2) phrase + NOT
mkdir -p "$BASE_DIR/phrase/VCP_server/New Folder"
mkdir -p "$BASE_DIR/phrase/${EXCLUDE_DIR}/VCP_server/New Folder"
printf 'hello from include\n' > "$BASE_DIR/phrase/VCP_server/New Folder/readme_${RUN_ID}.md"
printf 'hello from exclude\n' > "$BASE_DIR/phrase/${EXCLUDE_DIR}/VCP_server/New Folder/readme_${RUN_ID}.md"

# 3) OR
mkdir -p "$BASE_DIR/or"
printf 'console.log(\"server\");\n' > "$BASE_DIR/or/server_${RUN_ID}.js"
printf 'console.log(\"plugin\");\n' > "$BASE_DIR/or/plugin_${RUN_ID}.js"
printf 'console.log(\"myserver\");\n' > "$BASE_DIR/or/myserver_${RUN_ID}.js"

# 4) glob (Segment)
mkdir -p "$BASE_DIR/glob"
printf '0123456789abcdef\n' > "$BASE_DIR/glob/test_${RUN_ID}_123.txt"
printf '0123456789abcdef\n' > "$BASE_DIR/glob/attest_${RUN_ID}_123.txt"

# 5) ext / pic:
mkdir -p "$BASE_DIR/media"
printf '0123456789abcdef\n' > "$BASE_DIR/media/十一_${RUN_ID}.jpg"
printf '0123456789abcdef\n' > "$BASE_DIR/media/十一_${RUN_ID}.png"
printf '0123456789abcdef\n' > "$BASE_DIR/media/十一_${RUN_ID}.txt"

# 6) size:
mkdir -p "$BASE_DIR/size"
printf '123456789' > "$BASE_DIR/size/small_${RUN_ID}.bin"     # 9B
printf '1234567890' > "$BASE_DIR/size/big_${RUN_ID}.bin"      # 10B

# 7) dm:
mkdir -p "$BASE_DIR/dm"
printf '0123456789abcdef\n' > "$BASE_DIR/dm/today_${RUN_ID}.txt"
printf '0123456789abcdef\n' > "$BASE_DIR/dm/old_${RUN_ID}.txt"
touch -d '2000-01-01 00:00:00' "$BASE_DIR/dm/old_${RUN_ID}.txt" 2>/dev/null || true

# 8) regex:
mkdir -p "$BASE_DIR/regex"
printf 'console.log(\"plugin\");\n' > "$BASE_DIR/regex/VCP${RUN_ID}Plugin.js"
printf 'console.log(\"tool\");\n' > "$BASE_DIR/regex/VCP${RUN_ID}Tool.ts"

# ready marker（避免用 wfn/regex/glob 做等待条件）
READY="READY_smoke_${RUN_ID}.txt"
printf 'ready\n' > "$BASE_DIR/${READY}"
wait_until_indexed "$READY" "$READY"

fail() { echo "FAIL: $*" >&2; exit 1; }
assert_has() {
  local out="$1" want="$2" msg="$3"
  if ! printf '%s\n' "$out" | grep -Fq "$want"; then
    fail "$msg (missing: $want)"
  fi
}
assert_not_has() {
  local out="$1" bad="$2" msg="$3"
  if printf '%s\n' "$out" | grep -Fq "$bad"; then
    fail "$msg (unexpected: $bad)"
  fi
}

echo "== fd-rdd search DSL smoke test =="
echo "base_url=${BASE_URL%/}"
echo "root=$ROOT"
echo "sample_dir=$BASE_DIR"

out="$(api_search_paths "vcp_${RUN_ID}")"
assert_has "$out" "VCP_${RUN_ID}.txt" "smart-case: vcp 应命中 VCP"
assert_has "$out" "vcp_${RUN_ID}.txt" "smart-case: vcp 应命中 vcp"

out="$(api_search_paths "VCP_${RUN_ID}")"
assert_has "$out" "VCP_${RUN_ID}.txt" "smart-case: VCP 应命中 VCP"
assert_not_has "$out" "vcp_${RUN_ID}.txt" "smart-case: VCP 不应命中 vcp"

out="$(api_search_paths "case: vcp_${RUN_ID}")"
assert_has "$out" "vcp_${RUN_ID}.txt" "case:: vcp 应命中 vcp"
assert_not_has "$out" "VCP_${RUN_ID}.txt" "case:: vcp 不应命中 VCP"

out="$(api_search_paths "VCP server \"New Folder\" readme_${RUN_ID} !${EXCLUDE_DIR}")"
assert_has "$out" "phrase/VCP_server/New Folder/readme_${RUN_ID}.md" "phrase+NOT: include 应命中"
assert_not_has "$out" "phrase/${EXCLUDE_DIR}/VCP_server/New Folder/readme_${RUN_ID}.md" "phrase+NOT: exclude 不应命中"

out="$(api_search_paths "server_${RUN_ID}.js|plugin_${RUN_ID}.js")"
assert_has "$out" "or/server_${RUN_ID}.js" "OR: server 分支应命中"
assert_has "$out" "or/plugin_${RUN_ID}.js" "OR: plugin 分支应命中"

out="$(api_search_paths "test_${RUN_ID}_*")"
assert_has "$out" "glob/test_${RUN_ID}_123.txt" "glob: test_* 应命中 test"
assert_not_has "$out" "glob/attest_${RUN_ID}_123.txt" "glob: test_* 不应命中 attest"

out="$(api_search_paths "ext:jpg;png 十一_${RUN_ID}")"
assert_has "$out" "media/十一_${RUN_ID}.jpg" "ext: jpg 应命中"
assert_has "$out" "media/十一_${RUN_ID}.png" "ext: png 应命中"
assert_not_has "$out" "media/十一_${RUN_ID}.txt" "ext: 不应命中 txt"

out="$(api_search_paths "pic:十一_${RUN_ID}")"
assert_has "$out" "media/十一_${RUN_ID}.jpg" "pic: 应命中 jpg"
assert_has "$out" "media/十一_${RUN_ID}.png" "pic: 应命中 png"
assert_not_has "$out" "media/十一_${RUN_ID}.txt" "pic: 不应命中 txt"

out="$(api_search_paths "size:<10b small_${RUN_ID}")"
assert_has "$out" "size/small_${RUN_ID}.bin" "size:<10b 应命中 9B"
assert_not_has "$out" "size/big_${RUN_ID}.bin" "size:<10b 不应命中 10B"

out="$(api_search_paths "dm:today today_${RUN_ID}")"
assert_has "$out" "dm/today_${RUN_ID}.txt" "dm:today 应命中 today"
assert_not_has "$out" "dm/old_${RUN_ID}.txt" "dm:today 不应命中 old"

out="$(api_search_paths "wfn:server_${RUN_ID}.js")"
assert_has "$out" "or/server_${RUN_ID}.js" "wfn: basename 应命中 server.js"
assert_not_has "$out" "or/plugin_${RUN_ID}.js" "wfn: basename 不应命中 plugin.js"
assert_not_has "$out" "or/myserver_${RUN_ID}.js" "wfn: basename 不应命中 myserver.js"

server_path="$(api_search_paths "/or/server_${RUN_ID}.js" | head -n 1)"
[[ -n "$server_path" ]] || fail "wfn: fullpath 准备失败：找不到 or/server_${RUN_ID}.js"
out="$(api_search_paths "wfn:\"$server_path\"")"
assert_has "$out" "$server_path" "wfn: fullpath 应命中精确路径"

out="$(api_search_paths "regex:\"^VCP${RUN_ID}.*\\\\.(js|ts)$\"")"
assert_has "$out" "regex/VCP${RUN_ID}Plugin.js" "regex: 应命中 js"
assert_has "$out" "regex/VCP${RUN_ID}Tool.ts" "regex: 应命中 ts"

echo "PASS"
