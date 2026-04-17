use crate::index::TieredIndex;
use std::sync::Arc;

#[cfg(unix)]
mod imp {
    use super::*;
    use crate::query::{execute_query, QueryMode, SortColumn, SortOrder};
    use std::path::{Path, PathBuf};
    use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt, BufWriter};
    use tokio::net::{unix::UCred, UnixListener, UnixStream};
    use tokio::sync::oneshot;

    #[derive(Clone, Copy, Debug, Eq, PartialEq)]
    struct PeerIdentity {
        pid: Option<libc::pid_t>,
        uid: libc::uid_t,
        gid: libc::gid_t,
    }

    impl From<UCred> for PeerIdentity {
        fn from(value: UCred) -> Self {
            Self {
                pid: value.pid(),
                uid: value.uid(),
                gid: value.gid(),
            }
        }
    }

    #[derive(Clone, Copy, Debug)]
    pub struct PeerAuthPolicy {
        pub owner_uid: libc::uid_t,
        pub owner_gid: libc::gid_t,
        pub allow_root: bool,
        pub allow_same_gid: bool,
    }

    impl PeerAuthPolicy {
        fn current_process() -> Self {
            Self {
                // SAFETY: libc::geteuid() and libc::getegid() are simple syscalls that return
                // the effective user/group ID. They have no failure mode and require no preconditions.
                owner_uid: unsafe { libc::geteuid() },
                owner_gid: unsafe { libc::getegid() },
                allow_root: true,
                allow_same_gid: false,
            }
        }

        fn authorize(self, peer: PeerIdentity) -> anyhow::Result<()> {
            let allowed = peer.uid == self.owner_uid
                || (self.allow_root && peer.uid == 0)
                || (self.allow_same_gid && peer.gid == self.owner_gid);
            if allowed {
                return Ok(());
            }

            anyhow::bail!(
                "unauthorized uds peer: pid={:?} uid={} gid={} (owner_uid={} owner_gid={} allow_root={} allow_same_gid={})",
                peer.pid,
                peer.uid,
                peer.gid,
                self.owner_uid,
                self.owner_gid,
                self.allow_root,
                self.allow_same_gid
            );
        }
    }

    #[derive(Clone, Copy, Debug)]
    pub struct SocketConfig {
        pub default_limit: usize,
        pub max_limit: usize,
        pub flush_every: usize,
        pub max_request_bytes: usize,
        pub peer_auth: PeerAuthPolicy,
    }

    impl Default for SocketConfig {
        fn default() -> Self {
            Self {
                default_limit: 2000,
                max_limit: 200_000,
                flush_every: 1000,
                max_request_bytes: 8 * 1024,
                peer_auth: PeerAuthPolicy::current_process(),
            }
        }
    }

    /// Unix Socket 查询服务（供 fd-query.sh / fzf 使用）
    pub struct SocketServer {
        pub index: Arc<TieredIndex>,
        pub config: SocketConfig,
    }

    impl SocketServer {
        pub fn new(index: Arc<TieredIndex>) -> Self {
            Self {
                index,
                config: SocketConfig::default(),
            }
        }

        pub fn with_config(mut self, config: SocketConfig) -> Self {
            self.config = config;
            self
        }

        pub async fn run(self, path: &Path) -> anyhow::Result<()> {
            let (_tx, rx) = oneshot::channel::<()>();
            self.run_until_shutdown(path, rx).await
        }

        pub async fn run_until_shutdown(
            self,
            path: &Path,
            mut shutdown: oneshot::Receiver<()>,
        ) -> anyhow::Result<()> {
            let path: PathBuf = path.to_path_buf();
            if let Some(parent) = path.parent() {
                if let Err(e) = std::fs::create_dir_all(parent) {
                    tracing::warn!(
                        "Failed to create socket parent dir {}: {e}",
                        parent.display()
                    );
                }
            }
            if let Err(e) = std::fs::remove_file(&path) {
                if e.kind() != std::io::ErrorKind::NotFound {
                    tracing::warn!("Failed to remove old socket file {}: {e}", path.display());
                }
            }
            let listener = UnixListener::bind(&path)?;
            #[cfg(unix)]
            {
                use std::os::unix::fs::PermissionsExt;
                if let Err(e) =
                    std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o600))
                {
                    tracing::warn!(
                        "Failed to set socket permissions on {}: {e}",
                        path.display()
                    );
                }
            }
            tracing::info!("Unix Socket Server listening on {}", path.display());

            loop {
                tokio::select! {
                    _ = &mut shutdown => break,
                    accept = listener.accept() => {
                        let (socket, _) = accept?;
                        let index = self.index.clone();
                        let cfg = self.config;
                        tokio::spawn(async move {
                            if let Err(e) = handle_connection(index, cfg, socket).await {
                                tracing::debug!("Unix Socket handler error: {}", e);
                            }
                        });
                    }
                }
            }

            if let Err(e) = std::fs::remove_file(&path) {
                if e.kind() != std::io::ErrorKind::NotFound {
                    tracing::warn!(
                        "Failed to remove socket file on shutdown {}: {e}",
                        path.display()
                    );
                }
            }
            Ok(())
        }
    }

    async fn handle_connection(
        index: Arc<TieredIndex>,
        cfg: SocketConfig,
        socket: UnixStream,
    ) -> anyhow::Result<()> {
        let peer = PeerIdentity::from(socket.peer_cred()?);
        cfg.peer_auth.authorize(peer)?;
        handle_connection_io(index, cfg, socket).await
    }

    async fn handle_connection_io(
        index: Arc<TieredIndex>,
        cfg: SocketConfig,
        mut socket: impl AsyncRead + AsyncWrite + Unpin,
    ) -> anyhow::Result<()> {
        let mut req: Vec<u8> = Vec::with_capacity(256);
        let n = socket.read_to_end(&mut req).await?;
        if n == 0 {
            return Ok(());
        }
        if req.len() > cfg.max_request_bytes {
            anyhow::bail!("request too large: {} bytes", req.len());
        }

        let request = String::from_utf8_lossy(&req);
        let mut keyword: Option<&str> = None;
        let mut limit: Option<usize> = None;
        let mut mode = QueryMode::Exact;

        for line in request.lines() {
            let line = line.trim();
            if line.is_empty() {
                continue;
            }

            if let Some(rest) = line.strip_prefix("q:").or_else(|| line.strip_prefix("q=")) {
                keyword = Some(rest.trim());
                continue;
            }
            if let Some(rest) = line
                .strip_prefix("limit:")
                .or_else(|| line.strip_prefix("limit="))
            {
                if let Ok(v) = rest.trim().parse::<usize>() {
                    limit = Some(v);
                }
                continue;
            }
            if let Some(rest) = line
                .strip_prefix("mode:")
                .or_else(|| line.strip_prefix("mode="))
            {
                mode = QueryMode::parse_label(Some(rest.trim())).map_err(anyhow::Error::msg)?;
                continue;
            }

            // 兼容旧协议："x:keyword"（例如 "q:hello"）
            if keyword.is_none() {
                if let Some((_, rhs)) = line.split_once(':') {
                    keyword = Some(rhs.trim());
                    continue;
                }
                keyword = Some(line);
            }
        }

        let keyword = match keyword.map(str::trim).filter(|s| !s.is_empty()) {
            Some(k) => k,
            None => return Ok(()),
        };

        let mut limit = limit.unwrap_or(cfg.default_limit);
        if limit == 0 {
            limit = cfg.default_limit;
        }
        limit = limit.min(cfg.max_limit).max(1);

        let results = execute_query(
            index.as_ref(),
            keyword,
            limit,
            mode,
            SortColumn::default(),
            SortOrder::default(),
        );

        // 流式写回：不要在内存里拼接巨大 String/JSON。
        let mut w = BufWriter::new(&mut socket);
        for (i, meta) in results.iter().enumerate() {
            w.write_all(meta.path.as_os_str().as_encoded_bytes())
                .await?;
            w.write_all(b"\n").await?;
            if cfg.flush_every > 0 && (i + 1) % cfg.flush_every == 0 {
                w.flush().await?;
            }
        }
        w.flush().await?;
        let _ = socket.shutdown().await;
        Ok(())
    }

    #[cfg(test)]
    mod tests {
        use super::*;
        use crate::core::{EventRecord, EventType, FileIdentifier};
        use crate::event::recovery::DirtyScope;
        use std::time::{SystemTime, UNIX_EPOCH};
        use tokio::io::duplex;

        fn unique_tmp_dir(prefix: &str) -> PathBuf {
            let ns = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap_or_default()
                .as_nanos();
            let mut p = std::env::temp_dir();
            p.push(format!(
                "fd-rdd-test-{}-{}-{}",
                prefix,
                std::process::id(),
                ns
            ));
            std::fs::create_dir_all(&p).unwrap();
            p
        }

        fn ev(seq: u64, ty: EventType, path: PathBuf) -> EventRecord {
            EventRecord {
                seq,
                timestamp: SystemTime::now(),
                event_type: ty,
                id: FileIdentifier::Path(path),
                path_hint: None,
            }
        }

        #[tokio::test]
        async fn socket_handler_respects_limit() -> anyhow::Result<()> {
            let root = unique_tmp_dir("socket-limit");

            let index = Arc::new(TieredIndex::empty(vec![root.clone()]));
            let mut events: Vec<EventRecord> = Vec::new();
            for i in 0..10u64 {
                let p = root.join(format!("match_{}.txt", i));
                std::fs::write(&p, b"hello")?;
                events.push(ev(i + 1, EventType::Create, p));
            }
            index.apply_events(&events);

            let (mut client, server) = duplex(64 * 1024);
            let cfg = SocketConfig {
                default_limit: 2000,
                max_limit: 200_000,
                flush_every: 2,
                max_request_bytes: 8 * 1024,
                ..SocketConfig::default()
            };
            let server_task = tokio::spawn(handle_connection_io(index.clone(), cfg, server));

            client.write_all(b"q:match\nlimit:2\n").await?;
            client.shutdown().await?;

            let mut out: Vec<u8> = Vec::new();
            client.read_to_end(&mut out).await?;
            let s = String::from_utf8_lossy(&out);
            let lines: Vec<&str> = s.lines().filter(|l| !l.trim().is_empty()).collect();
            assert_eq!(lines.len(), 2);

            server_task.await??;

            let _ = std::fs::remove_dir_all(&root);
            Ok(())
        }

        #[tokio::test]
        async fn allocator_and_socket_handler_work_together() -> anyhow::Result<()> {
            let expected = if cfg!(feature = "mimalloc") {
                "mimalloc"
            } else {
                "system"
            };
            assert_eq!(crate::ALLOCATOR_KIND, expected);

            let root = unique_tmp_dir("socket-p0p1");
            let p = root.join("match_one.txt");
            std::fs::write(&p, b"hello")?;

            let index = Arc::new(TieredIndex::empty(vec![root.clone()]));
            index.apply_events(&[ev(1, EventType::Create, p.clone())]);

            let (mut client, server) = duplex(64 * 1024);
            let cfg = SocketConfig::default();
            let server_task = tokio::spawn(handle_connection_io(index.clone(), cfg, server));

            client.write_all(b"q:match\nlimit:10\n").await?;
            client.shutdown().await?;

            let mut out: Vec<u8> = Vec::new();
            client.read_to_end(&mut out).await?;
            let s = String::from_utf8_lossy(&out);
            assert!(s.contains("match_one.txt"));

            server_task.await??;

            let _ = std::fs::remove_dir_all(&root);
            Ok(())
        }

        #[tokio::test]
        async fn socket_handler_supports_fuzzy_mode() -> anyhow::Result<()> {
            let root = unique_tmp_dir("socket-fuzzy");
            let target = root.join("main_document.txt");
            let other = root.join("beta.rs");
            std::fs::write(&target, b"hello")?;
            std::fs::write(&other, b"world")?;

            let index = Arc::new(TieredIndex::empty(vec![root.clone()]));
            index.apply_events(&[
                ev(1, EventType::Create, target.clone()),
                ev(2, EventType::Create, other),
            ]);

            let (mut client, server) = duplex(64 * 1024);
            let cfg = SocketConfig::default();
            let server_task = tokio::spawn(handle_connection_io(index.clone(), cfg, server));

            client.write_all(b"q:mdt\nmode:fuzzy\nlimit:10\n").await?;
            client.shutdown().await?;

            let mut out: Vec<u8> = Vec::new();
            client.read_to_end(&mut out).await?;
            let s = String::from_utf8_lossy(&out);
            assert!(s.contains("main_document.txt"));

            server_task.await??;

            let _ = std::fs::remove_dir_all(&root);
            Ok(())
        }

        #[tokio::test]
        async fn p0_p1_p2_integration_fast_sync_affects_streaming_query() -> anyhow::Result<()> {
            // P0：分配器选择可观测
            let expected = if cfg!(feature = "mimalloc") {
                "mimalloc"
            } else {
                "system"
            };
            assert_eq!(crate::ALLOCATOR_KIND, expected);

            let root = unique_tmp_dir("socket-p0p1p2");
            let old = root.join("old_match.txt");
            std::fs::write(&old, b"old")?;

            let index = Arc::new(TieredIndex::empty(vec![root.clone()]));
            index.apply_events(&[ev(1, EventType::Create, old.clone())]);

            // 离线变更：删旧加新，不经事件管道
            std::fs::remove_file(&old)?;
            let new = root.join("new_match.txt");
            std::fs::write(&new, b"new")?;

            let _r = index.fast_sync(
                DirtyScope::Dirs {
                    cutoff_ns: 0,
                    dirs: vec![root.clone()],
                },
                &[],
            );

            // fast_sync 后索引可能有极短异步窗口，先直接 poll 确认 new_match 已入索引
            let deadline = tokio::time::Instant::now() + tokio::time::Duration::from_secs(5);
            let mut new_found = false;
            loop {
                let direct = index.query("match");
                if direct
                    .iter()
                    .any(|m| m.path.to_string_lossy().contains("new_match.txt"))
                {
                    new_found = true;
                    break;
                }
                if tokio::time::Instant::now() >= deadline {
                    break;
                }
                tokio::time::sleep(tokio::time::Duration::from_millis(50)).await;
            }
            assert!(new_found, "new_match.txt should appear after fast_sync");

            // P1：socket handler 通过 query_limit 流式输出，且结果应反映 fast-sync 后的状态
            let (mut client, server) = duplex(64 * 1024);
            let cfg = SocketConfig::default();
            let server_task = tokio::spawn(handle_connection_io(index.clone(), cfg, server));

            client.write_all(b"q:match\nlimit:100\n").await?;
            client.shutdown().await?;

            let mut out: Vec<u8> = Vec::new();
            client.read_to_end(&mut out).await?;
            let s = String::from_utf8_lossy(&out);
            assert!(s.contains("new_match.txt"));
            assert!(!s.contains("old_match.txt"));

            server_task.await??;

            let _ = std::fs::remove_dir_all(&root);
            Ok(())
        }

        #[test]
        fn peer_auth_policy_defaults_to_same_uid_or_root() {
            let policy = PeerAuthPolicy {
                owner_uid: 1000,
                owner_gid: 100,
                allow_root: true,
                allow_same_gid: false,
            };

            assert!(policy
                .authorize(PeerIdentity {
                    pid: Some(1),
                    uid: 1000,
                    gid: 999,
                })
                .is_ok());
            assert!(policy
                .authorize(PeerIdentity {
                    pid: Some(1),
                    uid: 0,
                    gid: 0,
                })
                .is_ok());
            assert!(policy
                .authorize(PeerIdentity {
                    pid: Some(1),
                    uid: 2000,
                    gid: 100,
                })
                .is_err());
        }
    }
}

#[cfg(not(unix))]
mod imp {
    use super::*;

    /// Unix Socket 查询服务（Windows 等平台不可用）
    pub struct SocketServer {
        pub index: Arc<TieredIndex>,
    }

    impl SocketServer {
        pub fn new(index: Arc<TieredIndex>) -> Self {
            Self { index }
        }

        pub async fn run(self, _path: &std::path::Path) -> anyhow::Result<()> {
            anyhow::bail!("Unix domain socket server is not supported on this platform")
        }
    }
}

pub use imp::SocketServer;
