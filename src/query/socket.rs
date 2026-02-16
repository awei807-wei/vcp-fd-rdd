use crate::index::TieredIndex;
use std::sync::Arc;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::UnixListener;

/// Unix Socket 查询服务（供 fd-query.sh / fzf 使用）
pub struct SocketServer {
    pub index: Arc<TieredIndex>,
}

impl SocketServer {
    pub fn new(index: Arc<TieredIndex>) -> Self {
        Self { index }
    }

    pub async fn run(self, path: &str) -> anyhow::Result<()> {
        let _ = std::fs::remove_file(path);
        let listener = UnixListener::bind(path)?;
        tracing::info!("Unix Socket Server listening on {}", path);

        loop {
            let (mut socket, _) = listener.accept().await?;
            let index = self.index.clone();

            tokio::spawn(async move {
                let mut buf = [0u8; 1024];
                if let Ok(n) = socket.read(&mut buf).await {
                    let request = String::from_utf8_lossy(&buf[..n]);
                    let parts: Vec<&str> = request.trim().splitn(2, ':').collect();

                    if parts.len() == 2 {
                        let keyword = parts[1];
                        let results = index.query(keyword);
                        let mut response = String::new();
                        for meta in results.iter().take(200) {
                            response.push_str(&meta.path.to_string_lossy());
                            response.push('\n');
                        }
                        let _ = socket.write_all(response.as_bytes()).await;
                    }
                }
            });
        }
    }
}
