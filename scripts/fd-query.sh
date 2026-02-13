#!/bin/bash
# fd-query.sh - 命令行查询脚本

set -e

QUERY="$1"
if [ -z "$QUERY" ]; then
    echo "Usage: $0 <keyword>"
    exit 1
fi

curl -s "http://localhost:6060/search?q=$QUERY" | jq -r '.[].path'