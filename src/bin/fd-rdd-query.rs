use clap::Parser;
use std::path::PathBuf;

/// fd-rdd-query：通过 Unix Domain Socket 向 fd-rdd Daemon 发起查询（流式输出）
#[derive(Parser, Debug)]
#[command(name = "fd-rdd-query", version, about)]
struct Args {
    /// UDS socket 路径（需与 fd-rdd 的 --uds-socket 一致）
    #[arg(long, value_name = "PATH", default_value = "/tmp/fd-rdd.sock")]
    socket: PathBuf,

    /// 最大返回条数（0 表示使用服务端默认值）
    #[arg(long, default_value_t = 2000)]
    limit: usize,

    /// 若连不上 socket，尝试自动拉起 fd-rdd Daemon（`fd-rdd --uds-socket <PATH>`）
    #[arg(long)]
    spawn: bool,

    /// 查询表达式（支持 AND/OR/NOT/过滤器；详见 README）
    query: String,
}

#[cfg(unix)]
#[tokio::main]
async fn main() -> anyhow::Result<()> {
    use std::time::Duration;
    use tokio::io::{self, AsyncWriteExt};
    use tokio::net::UnixStream;

    let args = Args::parse();
    let req = format!("q:{}\nlimit:{}\n", args.query, args.limit);

    let mut stream = match UnixStream::connect(&args.socket).await {
        Ok(s) => s,
        Err(e) if args.spawn => {
            // best-effort：按需拉起 daemon；失败则继续返回原错误。
            let _ = std::process::Command::new("fd-rdd")
                .arg("--uds-socket")
                .arg(args.socket.to_string_lossy().to_string())
                .spawn();

            // 等待 socket 可用（最多 5s）
            let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
            loop {
                match UnixStream::connect(&args.socket).await {
                    Ok(s) => break s,
                    Err(e) => {
                        if tokio::time::Instant::now() >= deadline {
                            return Err(anyhow::anyhow!(
                                "failed to connect to {} after spawn: {}",
                                args.socket.display(),
                                e
                            ));
                        }
                        tokio::time::sleep(Duration::from_millis(100)).await;
                    }
                }
            }
        }
        Err(e) => return Err(e.into()),
    };

    stream.write_all(req.as_bytes()).await?;
    stream.shutdown().await?;

    let mut stdout = io::stdout();
    tokio::io::copy(&mut stream, &mut stdout).await?;
    Ok(())
}

#[cfg(not(unix))]
fn main() -> anyhow::Result<()> {
    anyhow::bail!("fd-rdd-query is only supported on unix platforms")
}
