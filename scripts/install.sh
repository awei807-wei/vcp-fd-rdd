#!/bin/bash
# fd-rdd 安装脚本

set -e

BIN_DIR="$HOME/.vcp/bin"
mkdir -p "$BIN_DIR"

echo "Building fd-rdd in release mode..."
cargo build --release

echo "Installing binary to $BIN_DIR..."
cp target/release/fd-rdd "$BIN_DIR/"
chmod +x "$BIN_DIR/fd-rdd"

echo "Installation complete. You can start the daemon with: $BIN_DIR/fd-rdd"