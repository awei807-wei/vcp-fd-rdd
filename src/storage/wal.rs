use std::ffi::OsString;
use std::fs::{File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::os::unix::ffi::{OsStrExt, OsStringExt};
use std::path::{Path, PathBuf};
use std::sync::Mutex;

use crate::core::{EventRecord, EventType};

const WAL_MAGIC: u32 = 0x314C_4157; // "WAL1"
const WAL_VERSION: u32 = 1;

fn now_seal_id() -> u64 {
    use std::time::{SystemTime, UNIX_EPOCH};
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_nanos() as u64)
        .unwrap_or(0)
}

fn crc32_simple(data: &[u8]) -> u32 {
    // 复用 snapshot.rs 的 SimpleChecksum 语义：轻量、足够发现截断/随机翻转。
    // 不是强校验（非 cryptographic）。
    let mut s: u32 = 0;
    for &b in data {
        s = s.wrapping_add(b as u32);
        s = s.rotate_left(3);
    }
    s
}

fn encode_path(path: &Path) -> Vec<u8> {
    path.as_os_str().as_bytes().to_vec()
}

fn decode_path(bytes: &[u8]) -> PathBuf {
    PathBuf::from(OsString::from_vec(bytes.to_vec()))
}

fn system_time_to_unix(ts: std::time::SystemTime) -> (u64, u32) {
    use std::time::UNIX_EPOCH;
    match ts.duration_since(UNIX_EPOCH) {
        Ok(d) => (d.as_secs(), d.subsec_nanos()),
        Err(_) => (0, 0),
    }
}

fn unix_to_system_time(secs: u64, nanos: u32) -> std::time::SystemTime {
    use std::time::{Duration, UNIX_EPOCH};
    UNIX_EPOCH + Duration::new(secs, nanos)
}

#[derive(Clone, Debug)]
pub struct WalReplayResult {
    pub events: Vec<EventRecord>,
    pub sealed_used: usize,
    pub truncated_tail_records: usize,
}

/// Append-only 事件日志（WAL）。
///
/// - current: events.wal
/// - sealed: events.wal.seal-<id>（snapshot 边界切分）
pub struct WalStore {
    dir: PathBuf,
    current: PathBuf,
    file: Mutex<File>,
}

impl WalStore {
    pub fn open_in_dir(dir: PathBuf) -> anyhow::Result<Self> {
        std::fs::create_dir_all(&dir)?;
        let current = dir.join("events.wal");
        let f = open_or_init(&current)?;
        Ok(Self {
            dir,
            current,
            file: Mutex::new(f),
        })
    }

    pub fn dir(&self) -> &Path {
        &self.dir
    }

    pub fn append(&self, events: &[EventRecord]) -> anyhow::Result<()> {
        if events.is_empty() {
            return Ok(());
        }
        let mut f = self.file.lock().unwrap();
        for ev in events {
            let payload = encode_event(ev);
            let len: u32 = payload.len().try_into().unwrap_or(u32::MAX);
            let crc = crc32_simple(&payload);
            f.write_all(&len.to_le_bytes())?;
            f.write_all(&crc.to_le_bytes())?;
            f.write_all(&payload[..len as usize])?;
        }
        f.flush()?;
        Ok(())
    }

    /// seal：把当前 WAL rename 成 sealed 文件，并创建新的空 WAL。
    /// 返回 seal_id（用于与 manifest checkpoint 关联）。
    pub fn seal(&self) -> anyhow::Result<u64> {
        let mut f = self.file.lock().unwrap();
        f.flush()?;

        let id = now_seal_id();
        let sealed = self.dir.join(format!("events.wal.seal-{id:016x}"));
        // 关闭当前句柄后再 rename（避免平台差异）。
        drop(f);

        if self.current.exists() {
            std::fs::rename(&self.current, &sealed)?;
        }

        let newf = open_or_init(&self.current)?;
        *self.file.lock().unwrap() = newf;
        Ok(id)
    }

    pub fn cleanup_sealed_up_to(&self, seal_id: u64) -> anyhow::Result<()> {
        if seal_id == 0 {
            return Ok(());
        }
        for ent in std::fs::read_dir(&self.dir)? {
            let Ok(ent) = ent else { continue };
            let p = ent.path();
            if let Some(id) = parse_seal_id(&p) {
                if id <= seal_id {
                    let _ = std::fs::remove_file(p);
                }
            }
        }
        Ok(())
    }

    /// 回放：只读取 seal_id > checkpoint 的 sealed WAL + 当前 WAL。
    pub fn replay_since_seal(&self, checkpoint_seal_id: u64) -> anyhow::Result<WalReplayResult> {
        let mut sealed = Vec::new();
        for ent in std::fs::read_dir(&self.dir)? {
            let Ok(ent) = ent else { continue };
            let p = ent.path();
            if let Some(id) = parse_seal_id(&p) {
                if id > checkpoint_seal_id {
                    sealed.push((id, p));
                }
            }
        }
        sealed.sort_by_key(|(id, _)| *id);

        let mut events: Vec<EventRecord> = Vec::new();
        let mut truncated = 0usize;
        for (_, p) in sealed.iter() {
            let (mut evs, t) = read_wal_file(p)?;
            truncated += t;
            events.append(&mut evs);
        }
        let (mut cur, t) = read_wal_file(&self.current)?;
        truncated += t;
        events.append(&mut cur);

        // 统一为单调 seq（WAL 内部 seq 只用于排序/回放稳定性）。
        for (i, e) in events.iter_mut().enumerate() {
            e.seq = i as u64 + 1;
        }

        Ok(WalReplayResult {
            events,
            sealed_used: sealed.len(),
            truncated_tail_records: truncated,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn unique_tmp_dir(tag: &str) -> PathBuf {
        let nanos = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        std::env::temp_dir().join(format!("fd-rdd-wal-{}-{}", tag, nanos))
    }

    #[test]
    fn wal_append_seal_replay_respects_checkpoint() {
        let dir = unique_tmp_dir("basic");
        std::fs::create_dir_all(&dir).unwrap();

        let wal = WalStore::open_in_dir(dir.clone()).unwrap();

        let p1 = dir.join("a.txt");
        let p2 = dir.join("b.txt");

        wal.append(&[EventRecord {
            seq: 1,
            timestamp: std::time::SystemTime::now(),
            event_type: EventType::Create,
            path: p1.clone(),
        }])
        .unwrap();

        let seal1 = wal.seal().unwrap();

        wal.append(&[EventRecord {
            seq: 2,
            timestamp: std::time::SystemTime::now(),
            event_type: EventType::Delete,
            path: p2.clone(),
        }])
        .unwrap();

        // checkpoint=0：回放 sealed+current
        let r = wal.replay_since_seal(0).unwrap();
        assert_eq!(r.events.len(), 2);

        // checkpoint=seal1：只回放 current
        let r2 = wal.replay_since_seal(seal1).unwrap();
        assert_eq!(r2.events.len(), 1);
    }
}

fn open_or_init(path: &Path) -> anyhow::Result<File> {
    let exists = path.exists();
    let mut f = OpenOptions::new()
        .create(true)
        .read(true)
        .append(true)
        .open(path)?;

    if !exists {
        f.write_all(&WAL_MAGIC.to_le_bytes())?;
        f.write_all(&WAL_VERSION.to_le_bytes())?;
        f.flush()?;
        return Ok(f);
    }

    // 快速校验 header；不匹配则重建（避免历史垃圾文件导致读崩）。
    let mut hdr = [0u8; 8];
    f.seek(SeekFrom::Start(0))?;
    if f.read_exact(&mut hdr).is_err() {
        // 空文件/截断：重写 header
        f = OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(path)?;
        f.write_all(&WAL_MAGIC.to_le_bytes())?;
        f.write_all(&WAL_VERSION.to_le_bytes())?;
        f.flush()?;
        f = OpenOptions::new()
            .create(true)
            .read(true)
            .append(true)
            .open(path)?;
        return Ok(f);
    }

    let magic = u32::from_le_bytes(hdr[0..4].try_into()?);
    let ver = u32::from_le_bytes(hdr[4..8].try_into()?);
    if magic != WAL_MAGIC || ver != WAL_VERSION {
        // 不兼容：truncate 重新开始
        let mut nf = OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(path)?;
        nf.write_all(&WAL_MAGIC.to_le_bytes())?;
        nf.write_all(&WAL_VERSION.to_le_bytes())?;
        nf.flush()?;
        drop(nf);
        f = OpenOptions::new()
            .create(true)
            .read(true)
            .append(true)
            .open(path)?;
    }

    Ok(f)
}

fn parse_seal_id(path: &Path) -> Option<u64> {
    let name = path.file_name()?.to_str()?;
    let prefix = "events.wal.seal-";
    if !name.starts_with(prefix) {
        return None;
    }
    u64::from_str_radix(&name[prefix.len()..], 16).ok()
}

fn read_wal_file(path: &Path) -> anyhow::Result<(Vec<EventRecord>, usize)> {
    if !path.exists() {
        return Ok((Vec::new(), 0));
    }
    let mut f = File::open(path)?;

    let mut hdr = [0u8; 8];
    if f.read_exact(&mut hdr).is_err() {
        return Ok((Vec::new(), 0));
    }
    let magic = u32::from_le_bytes(hdr[0..4].try_into()?);
    let ver = u32::from_le_bytes(hdr[4..8].try_into()?);
    if magic != WAL_MAGIC || ver != WAL_VERSION {
        return Ok((Vec::new(), 0));
    }

    let mut out = Vec::new();
    let mut truncated_tail = 0usize;
    loop {
        let mut lb = [0u8; 8];
        if let Err(_) = f.read_exact(&mut lb) {
            break;
        }
        let len = u32::from_le_bytes(lb[0..4].try_into()?) as usize;
        let crc = u32::from_le_bytes(lb[4..8].try_into()?);
        let mut buf = vec![0u8; len];
        if let Err(_) = f.read_exact(&mut buf) {
            truncated_tail += 1;
            break;
        }
        if crc32_simple(&buf) != crc {
            // 校验失败：视为截断/损坏，停止读取（保守）。
            truncated_tail += 1;
            break;
        }
        if let Some(ev) = decode_event(&buf) {
            out.push(ev);
        }
    }
    Ok((out, truncated_tail))
}

fn encode_event(ev: &EventRecord) -> Vec<u8> {
    let mut out = Vec::new();
    let (secs, nanos) = system_time_to_unix(ev.timestamp);
    let (kind, from_opt): (u8, Option<&Path>) = match &ev.event_type {
        EventType::Create => (1, None),
        EventType::Delete => (2, None),
        EventType::Modify => (3, None),
        EventType::Rename { from } => (4, Some(from.as_path())),
    };

    let path = encode_path(&ev.path);
    out.push(kind);
    out.extend_from_slice(&secs.to_le_bytes());
    out.extend_from_slice(&nanos.to_le_bytes());
    let plen: u32 = path.len().try_into().unwrap_or(u32::MAX);
    out.extend_from_slice(&plen.to_le_bytes());
    out.extend_from_slice(&path[..plen as usize]);

    if let Some(from) = from_opt {
        let fb = encode_path(from);
        let flen: u32 = fb.len().try_into().unwrap_or(u32::MAX);
        out.extend_from_slice(&flen.to_le_bytes());
        out.extend_from_slice(&fb[..flen as usize]);
    } else {
        out.extend_from_slice(&0u32.to_le_bytes());
    }

    out
}

fn decode_event(buf: &[u8]) -> Option<EventRecord> {
    if buf.len() < 1 + 8 + 4 + 4 + 4 {
        return None;
    }
    let mut off = 0usize;
    let kind = buf[off];
    off += 1;
    let secs = u64::from_le_bytes(buf.get(off..off + 8)?.try_into().ok()?);
    off += 8;
    let nanos = u32::from_le_bytes(buf.get(off..off + 4)?.try_into().ok()?);
    off += 4;
    let plen = u32::from_le_bytes(buf.get(off..off + 4)?.try_into().ok()?) as usize;
    off += 4;
    let pbytes = buf.get(off..off + plen)?;
    off += plen;
    let flen = u32::from_le_bytes(buf.get(off..off + 4)?.try_into().ok()?) as usize;
    off += 4;
    let fbytes = if flen > 0 {
        buf.get(off..off + flen)?
    } else {
        &[]
    };

    let path = decode_path(pbytes);
    let ts = unix_to_system_time(secs, nanos);
    let event_type = match kind {
        1 => EventType::Create,
        2 => EventType::Delete,
        3 => EventType::Modify,
        4 => EventType::Rename {
            from: decode_path(fbytes),
        },
        _ => EventType::Modify,
    };

    Some(EventRecord {
        seq: 0,
        timestamp: ts,
        event_type,
        path,
    })
}
