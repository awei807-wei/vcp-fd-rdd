use clap::Parser;
use fd_rdd::runtime::{run, Args};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt::init();
    let args = Args::parse();
    run(args).await
}
