// `Parser` trait 为 `Args::parse()` 提供命令行参数解析能力。
use clap::Parser;
// `Args` 描述命令行参数，`run` 负责启动完整的 fd-rdd 守护进程。
use fd_rdd::runtime::{run, Args};

// 把普通的 main 函数包装进 Tokio 异步运行时，以便使用 `.await`。
#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // 初始化 tracing 的文本日志输出；后续 info!/warn! 等日志会输出到终端。
    tracing_subscriber::fmt::init();

    // 读取进程命令行并转换为 Args；参数错误、--help 和 --version 由 clap 处理。
    let args = Args::parse();

    // 启动索引、文件监听和查询服务，并等待其结束。
    // 这里不加分号，run 的 Result 会直接成为 main 的返回值。
    run(args).await
}
