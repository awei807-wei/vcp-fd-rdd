.PHONY: static build run

# 目标：全静态链接二进制文件
static:
	@echo "正在进行全静态编译 (Target: x86_64-unknown-linux-musl)..."
	rustup target add x86_64-unknown-linux-musl
	cargo build --release --target x86_64-unknown-linux-musl
	@echo "编译完成！二进制文件位于: target/x86_64-unknown-linux-musl/release/fd-rdd"
	@echo "验证依赖情况:"
	-ldd target/x86_64-unknown-linux-musl/release/fd-rdd || echo "确认：这是一个全静态二进制文件，无动态依赖！"

build:
	cargo build --release

run:
	cargo run --release