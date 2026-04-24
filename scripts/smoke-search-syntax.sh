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

api_search_json() {
  local q="$1"
  shift
  curl -fsS -G "${BASE_URL%/}/search" --data-urlencode "q=$q" "$@"
}

api_scan_json() {
  local path="$1"
  jq -nc --arg path "$path" '{paths:[$path]}' \
    | curl -fsS -X POST "${BASE_URL%/}/scan" \
        -H 'Content-Type: application/json' \
        --data-binary @-
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

path_depth() {
  local path="$1"
  local trimmed="${path//[^\/]/}"
  echo "${#trimmed}"
}

mkdir -p "$BASE_DIR"

# 1) Smart-Case / case:
mkdir -p "$BASE_DIR/case"
printf '0123456789abcdef\n' > "$BASE_DIR/case/VCPsmoke_${RUN_ID}_upper.txt"
printf '0123456789abcdef\n' > "$BASE_DIR/case/vcpsmoke_${RUN_ID}_lower.txt"

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

# 9) additional coverage: parent/infolder/depth/len/type/fuzzy/sort/highlights/initials/CJK
mkdir -p "$BASE_DIR/filter_parent/target_parent" "$BASE_DIR/filter_parent/other_parent"
printf 'parent match\n' > "$BASE_DIR/filter_parent/target_parent/parent_probe_${RUN_ID}.txt"
printf 'other parent\n' > "$BASE_DIR/filter_parent/other_parent/parent_probe_${RUN_ID}.txt"

mkdir -p "$BASE_DIR/filter_depth"
printf 'depth shallow\n' > "$BASE_DIR/filter_depth/depthprobe_${RUN_ID}_shallow.txt"
mkdir -p "$BASE_DIR/filter_depth/alpha/beta/gamma"
printf 'depth deep\n' > "$BASE_DIR/filter_depth/alpha/beta/gamma/depthprobe_${RUN_ID}_deep.txt"

mkdir -p "$BASE_DIR/filter_len"
printf 'len short\n' > "$BASE_DIR/filter_len/lenprobe_${RUN_ID}.txt"
printf 'len long\n' > "$BASE_DIR/filter_len/lenprobe_filename_with_significantly_long_name_${RUN_ID}.txt"

mkdir -p "$BASE_DIR/filter_type"
printf 'type file\n' > "$BASE_DIR/filter_type/typeprobe_${RUN_ID}.txt"

mkdir -p "$BASE_DIR/segment/client/user/search"
printf 'initials\n' > "$BASE_DIR/segment/client/user/search/initials_probe_${RUN_ID}.txt"

mkdir -p "$BASE_DIR/fuzzy"
printf 'fuzzy target\n' > "$BASE_DIR/fuzzy/main_document_target_${RUN_ID}.txt"

mkdir -p "$BASE_DIR/sort"
printf '123456789' > "$BASE_DIR/sort/sortprobe_small_${RUN_ID}.txt"
printf '12345678901234567890' > "$BASE_DIR/sort/sortprobe_large_${RUN_ID}.txt"

mkdir -p "$BASE_DIR/highlight"
printf 'highlight\n' > "$BASE_DIR/highlight/highlightprobe_${RUN_ID}.txt"

mkdir -p "$BASE_DIR/cjk"
printf '中文\n' > "$BASE_DIR/cjk/中文检索_${RUN_ID}.txt"

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
assert_json_expr() {
  local json="$1" expr="$2" msg="$3"
  if ! printf '%s' "$json" | jq -e "$expr" >/dev/null; then
    fail "$msg (jq: $expr)"
  fi
}

# Force a full directory refresh before assertions. Waiting for READY alone only
# proves one file is visible; the rest of the sample tree may still be settling
# on slower CI runners while the first smart-case query starts.
json="$(api_scan_json "$BASE_DIR")"
assert_json_expr "$json" ".scanned >= 1" "POST /scan 应至少扫描样本目录"
wait_until_indexed "vcpsmoke_${RUN_ID}" "VCPsmoke_${RUN_ID}_upper.txt"
wait_until_indexed "vcpsmoke_${RUN_ID}" "vcpsmoke_${RUN_ID}_lower.txt"

echo "== fd-rdd search DSL smoke test =="
echo "base_url=${BASE_URL%/}"
echo "root=$ROOT"
echo "sample_dir=$BASE_DIR"

out="$(api_search_paths "vcpsmoke_${RUN_ID}")"
assert_has "$out" "VCPsmoke_${RUN_ID}_upper.txt" "smart-case: 小写查询应命中大写前缀文件"
assert_has "$out" "vcpsmoke_${RUN_ID}_lower.txt" "smart-case: 小写查询应命中小写前缀文件"

out="$(api_search_paths "VCPsmoke_${RUN_ID}")"
assert_has "$out" "VCPsmoke_${RUN_ID}_upper.txt" "smart-case: 大写查询应命中大写前缀文件"
assert_not_has "$out" "vcpsmoke_${RUN_ID}_lower.txt" "smart-case: 大写查询不应命中小写前缀文件"

out="$(api_search_paths "case: vcpsmoke_${RUN_ID}")"
assert_has "$out" "vcpsmoke_${RUN_ID}_lower.txt" "case:: 小写查询应命中小写前缀文件"
assert_not_has "$out" "VCPsmoke_${RUN_ID}_upper.txt" "case:: 小写查询不应命中大写前缀文件"

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

parent_dir="${BASE_DIR}/filter_parent/target_parent"
out="$(api_search_paths "parent:${parent_dir} parent_probe_${RUN_ID}")"
assert_has "$out" "filter_parent/target_parent/parent_probe_${RUN_ID}.txt" "parent: 应命中目标父目录"
assert_not_has "$out" "filter_parent/other_parent/parent_probe_${RUN_ID}.txt" "parent: 不应命中其他父目录"

out="$(api_search_paths "infolder:${parent_dir} parent_probe_${RUN_ID}")"
assert_has "$out" "filter_parent/target_parent/parent_probe_${RUN_ID}.txt" "infolder: 应作为 parent: 别名生效"
assert_not_has "$out" "filter_parent/other_parent/parent_probe_${RUN_ID}.txt" "infolder: 不应命中其他父目录"

depth_shallow_path="${BASE_DIR}/filter_depth/depthprobe_${RUN_ID}_shallow.txt"
depth_limit="$(path_depth "$depth_shallow_path")"
out="$(api_search_paths "depthprobe_${RUN_ID} depth:<=${depth_limit}")"
assert_has "$out" "filter_depth/depthprobe_${RUN_ID}_shallow.txt" "depth:<= 应命中浅层文件"
assert_not_has "$out" "filter_depth/alpha/beta/gamma/depthprobe_${RUN_ID}_deep.txt" "depth:<= 不应命中更深层文件"

out="$(api_search_paths "lenprobe len:>40")"
assert_has "$out" "filter_len/lenprobe_filename_with_significantly_long_name_${RUN_ID}.txt" "len:>40 应命中长文件名"
assert_not_has "$out" "filter_len/lenprobe_${RUN_ID}.txt" "len:>40 不应命中短文件名"

out="$(api_search_paths "type:file typeprobe_${RUN_ID}")"
assert_has "$out" "filter_type/typeprobe_${RUN_ID}.txt" "type:file 应命中文件"

out="$(api_search_paths "c/u/s/initials_probe_${RUN_ID}")"
assert_has "$out" "segment/client/user/search/initials_probe_${RUN_ID}.txt" "路径段首匹配应命中 c/u/s/initials_probe"

json="$(api_search_json "maindoctarget" --data-urlencode "mode=fuzzy" --data-urlencode "limit=200")"
assert_json_expr "$json" ". | map(.path) | any(contains(\"main_document_target_${RUN_ID}.txt\"))" "mode=fuzzy 应命中文件"

json="$(api_search_json "sortprobe" --data-urlencode "sort=size" --data-urlencode "order=desc" --data-urlencode "limit=20")"
assert_json_expr "$json" ".[0].path | contains(\"sortprobe_large_${RUN_ID}.txt\")" "sort=size&order=desc 首项应为大文件"

json="$(api_search_json "highlightprobe_${RUN_ID}" --data-urlencode "limit=20")"
assert_json_expr "$json" ".[0].highlights | length > 0" "搜索结果应返回 highlights"

out="$(api_search_paths "中文检索_${RUN_ID}")"
assert_has "$out" "cjk/中文检索_${RUN_ID}.txt" "中文查询应命中文件"

mkdir -p "$BASE_DIR/scan/on_demand"
printf 'scan now\n' > "$BASE_DIR/scan/on_demand/scan_trigger_${RUN_ID}.txt"
json="$(api_scan_json "$BASE_DIR/scan/on_demand")"
assert_json_expr "$json" ".scanned >= 1" "POST /scan 应返回扫描条目数"
wait_until_indexed "scan_trigger_${RUN_ID}" "scan_trigger_${RUN_ID}.txt"

echo "PASS"
