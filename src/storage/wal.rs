use std::fs::{File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::sync::Mutex;
use std::time::{Duration, Instant};

use crate::core::{EventRecord, EventType, FileIdentifier};
use crate::storage::checksum::crc32c_checksum;
use crate::storage::quarantine::{MountIdentity, RootStateKind, RootStateRecord};

const WAL_MAGIC: u32 = 0x314C_4157; // "WAL1"
const WAL_VERSION: u32 = 4;

// Safety guard: WAL records are expected to be small (path + metadata). Treat any huge length as
// corruption to avoid memory DoS via `vec![0u8; len]`.
const MAX_WAL_RECORD_BYTES: usize = 8 * 1024 * 1024; // 8 MiB

fn now_seal_id() -> u64 {
    use std::time::{SystemTime, UNIX_EPOCH};
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_nanos() as u64)
        .unwrap_or(0)
}

fn wal_checksum(data: &[u8]) -> u32 {
    crc32c_checksum(data)
}

/// Legacy WAL checksum (used in v1/v2 WAL files).
fn crc32_simple(data: &[u8]) -> u32 {
    let mut s: u32 = 0;
    for &b in data {
        s = s.wrapping_add(b as u32);
        s = s.rotate_left(3);
    }
    s
}

fn encode_path(path: &Path) -> Vec<u8> {
    path.as_os_str().as_encoded_bytes().to_vec()
}

fn pathbuf_from_encoded_bytes(bytes: &[u8]) -> PathBuf {
    #[cfg(unix)]
    {
        use std::ffi::OsString;
        use std::os::unix::ffi::OsStringExt;
        PathBuf::from(OsString::from_vec(bytes.to_vec()))
    }
    #[cfg(not(unix))]
    {
        PathBuf::from(String::from_utf8_lossy(bytes).into_owned())
    }
}

fn decode_path(bytes: &[u8]) -> PathBuf {
    pathbuf_from_encoded_bytes(bytes)
}

fn encode_file_id(id: &FileIdentifier) -> Vec<u8> {
    let mut out = Vec::new();
    match id {
        FileIdentifier::Path(p) => {
            out.push(1);
            let pb = encode_path(p.as_path());
            let plen: u32 = pb.len().try_into().unwrap_or(u32::MAX);
            out.extend_from_slice(&plen.to_le_bytes());
            out.extend_from_slice(&pb[..plen as usize]);
        }
        FileIdentifier::Fid { dev, ino } => {
            out.push(2);
            out.extend_from_slice(&dev.to_le_bytes());
            out.extend_from_slice(&ino.to_le_bytes());
        }
    }
    out
}

fn decode_file_id(buf: &[u8], off: &mut usize) -> Option<FileIdentifier> {
    let tag = *buf.get(*off)?;
    *off += 1;
    match tag {
        1 => {
            let plen = u32::from_le_bytes(buf.get(*off..*off + 4)?.try_into().ok()?) as usize;
            *off += 4;
            let pbytes = buf.get(*off..*off + plen)?;
            *off += plen;
            Some(FileIdentifier::Path(decode_path(pbytes)))
        }
        2 => {
            let dev = u64::from_le_bytes(buf.get(*off..*off + 8)?.try_into().ok()?);
            *off += 8;
            let ino = u64::from_le_bytes(buf.get(*off..*off + 8)?.try_into().ok()?);
            *off += 8;
            Some(FileIdentifier::Fid { dev, ino })
        }
        _ => None,
    }
}

fn encode_path_opt(p: &Option<PathBuf>) -> Vec<u8> {
    let mut out = Vec::new();
    if let Some(path) = p {
        let pb = encode_path(path.as_path());
        let plen: u32 = pb.len().try_into().unwrap_or(u32::MAX);
        out.extend_from_slice(&plen.to_le_bytes());
        out.extend_from_slice(&pb[..plen as usize]);
    } else {
        out.extend_from_slice(&0u32.to_le_bytes());
    }
    out
}

fn decode_path_opt(buf: &[u8], off: &mut usize) -> Option<Option<PathBuf>> {
    let plen = u32::from_le_bytes(buf.get(*off..*off + 4)?.try_into().ok()?) as usize;
    *off += 4;
    if plen == 0 {
        return Some(None);
    }
    let pbytes = buf.get(*off..*off + plen)?;
    *off += plen;
    Some(Some(decode_path(pbytes)))
}

fn encode_bytes(bytes: &[u8]) -> Vec<u8> {
    let mut out = Vec::new();
    let len: u32 = bytes.len().try_into().unwrap_or(u32::MAX);
    out.extend_from_slice(&len.to_le_bytes());
    out.extend_from_slice(&bytes[..len as usize]);
    out
}

fn decode_bytes<'a>(buf: &'a [u8], off: &mut usize) -> Option<&'a [u8]> {
    let len = u32::from_le_bytes(buf.get(*off..*off + 4)?.try_into().ok()?) as usize;
    *off += 4;
    let bytes = buf.get(*off..*off + len)?;
    *off += len;
    Some(bytes)
}

fn encode_string(value: &str) -> Vec<u8> {
    encode_bytes(value.as_bytes())
}

fn decode_string(buf: &[u8], off: &mut usize) -> Option<String> {
    Some(String::from_utf8_lossy(decode_bytes(buf, off)?).into_owned())
}

fn encode_string_opt(value: &Option<String>) -> Vec<u8> {
    match value {
        Some(value) => {
            let mut out = vec![1];
            out.extend_from_slice(&encode_string(value));
            out
        }
        None => vec![0],
    }
}

fn decode_string_opt(buf: &[u8], off: &mut usize) -> Option<Option<String>> {
    let tag = *buf.get(*off)?;
    *off += 1;
    match tag {
        0 => Some(None),
        1 => Some(Some(decode_string(buf, off)?)),
        _ => None,
    }
}

fn encode_paths(paths: &[PathBuf]) -> Vec<u8> {
    let mut out = Vec::new();
    let count: u32 = paths.len().try_into().unwrap_or(u32::MAX);
    out.extend_from_slice(&count.to_le_bytes());
    for path in paths.iter().take(count as usize) {
        let bytes = encode_path(path);
        out.extend_from_slice(&encode_bytes(&bytes));
    }
    out
}

fn decode_paths(buf: &[u8], off: &mut usize) -> Option<Vec<PathBuf>> {
    let count = u32::from_le_bytes(buf.get(*off..*off + 4)?.try_into().ok()?) as usize;
    *off += 4;
    let mut paths = Vec::with_capacity(count.min(1024));
    for _ in 0..count {
        paths.push(decode_path(decode_bytes(buf, off)?));
    }
    Some(paths)
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
    pub root_events: Vec<RootStateRecord>,
    pub events_replayed: usize,
    pub root_events_replayed: usize,
    pub sealed_used: usize,
    pub truncated_tail_records: usize,
    pub gap_detected: bool,
    pub checkpoint_used: u64,
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum WalDurability {
    #[default]
    FlushOnly,
    SyncInterval {
        interval_ms: u64,
        batch_records: usize,
    },
    SyncDataAlways,
}

impl WalDurability {
    pub fn flush_only() -> Self {
        Self::FlushOnly
    }

    pub fn sync_interval(interval_ms: u64, batch_records: usize) -> Self {
        Self::SyncInterval {
            interval_ms: interval_ms.max(1),
            batch_records: batch_records.max(1),
        }
    }

    pub fn sync_always() -> Self {
        Self::SyncDataAlways
    }

    pub fn label(self) -> &'static str {
        match self {
            Self::FlushOnly => "flush-only",
            Self::SyncInterval { .. } => "sync-interval",
            Self::SyncDataAlways => "sync-always",
        }
    }

    pub fn sync_interval_ms(self) -> u64 {
        match self {
            Self::SyncInterval { interval_ms, .. } => interval_ms,
            _ => 0,
        }
    }

    pub fn sync_batch_records(self) -> usize {
        match self {
            Self::SyncInterval { batch_records, .. } => batch_records,
            _ => 0,
        }
    }
}

#[derive(Debug)]
struct WalSyncState {
    records_since_sync: usize,
    last_sync: Instant,
}

/// Append-only 事件日志（WAL）。
///
/// - current: events.wal
/// - sealed: events.wal.seal-<id>（snapshot 边界切分）
pub struct WalStore {
    dir: PathBuf,
    current: PathBuf,
    file: Mutex<File>,
    durability: Mutex<WalDurability>,
    sync_state: Mutex<WalSyncState>,
    last_seal_id: Mutex<u64>,
}

impl WalStore {
    pub fn open_in_dir(dir: PathBuf) -> anyhow::Result<Self> {
        std::fs::create_dir_all(&dir)?;
        let current = dir.join("events.wal");
        let f = open_or_init(&current)?;
        let last_seal_id = max_existing_seal_id(&dir).max(now_seal_id());
        Ok(Self {
            dir,
            current,
            file: Mutex::new(f),
            durability: Mutex::new(WalDurability::FlushOnly),
            sync_state: Mutex::new(WalSyncState {
                records_since_sync: 0,
                last_sync: Instant::now(),
            }),
            last_seal_id: Mutex::new(last_seal_id),
        })
    }

    pub fn dir(&self) -> &Path {
        &self.dir
    }

    pub fn append(&self, events: &[EventRecord]) -> anyhow::Result<()> {
        if events.is_empty() {
            return Ok(());
        }
        let mut f = self.file.lock().unwrap_or_else(|e| e.into_inner());
        for ev in events {
            let payload = encode_event(ev);
            let len: u32 = payload.len().try_into().unwrap_or(u32::MAX);
            let crc = wal_checksum(&payload);
            f.write_all(&len.to_le_bytes())?;
            f.write_all(&crc.to_le_bytes())?;
            f.write_all(&payload[..len as usize])?;
        }
        f.flush()?;
        let durability = *self.durability.lock().unwrap_or_else(|e| e.into_inner());
        match durability {
            WalDurability::FlushOnly => {}
            WalDurability::SyncDataAlways => {
                f.sync_data()?;
                self.mark_synced();
            }
            WalDurability::SyncInterval {
                interval_ms,
                batch_records,
            } => {
                let mut state = self.sync_state.lock().unwrap_or_else(|e| e.into_inner());
                state.records_since_sync = state.records_since_sync.saturating_add(events.len());
                let due_by_batch = state.records_since_sync >= batch_records.max(1);
                let due_by_time =
                    state.last_sync.elapsed() >= Duration::from_millis(interval_ms.max(1));
                if due_by_batch || due_by_time {
                    f.sync_data()?;
                    state.records_since_sync = 0;
                    state.last_sync = Instant::now();
                }
            }
        }
        Ok(())
    }

    pub fn append_root_events(&self, records: &[RootStateRecord]) -> anyhow::Result<()> {
        if records.is_empty() {
            return Ok(());
        }
        let mut f = self.file.lock().unwrap_or_else(|e| e.into_inner());
        for record in records {
            let payload = encode_root_state_record(record);
            let len: u32 = payload.len().try_into().unwrap_or(u32::MAX);
            let crc = wal_checksum(&payload);
            f.write_all(&len.to_le_bytes())?;
            f.write_all(&crc.to_le_bytes())?;
            f.write_all(&payload[..len as usize])?;
        }
        f.flush()?;
        Ok(())
    }

    pub fn set_durability(&self, durability: WalDurability) {
        *self.durability.lock().unwrap_or_else(|e| e.into_inner()) = durability;
        self.mark_synced();
    }

    pub fn durability(&self) -> WalDurability {
        *self.durability.lock().unwrap_or_else(|e| e.into_inner())
    }

    fn mark_synced(&self) {
        let mut state = self.sync_state.lock().unwrap_or_else(|e| e.into_inner());
        state.records_since_sync = 0;
        state.last_sync = Instant::now();
    }

    /// seal：把当前 WAL rename 成 sealed 文件，并创建新的空 WAL。
    /// 返回 seal_id（用于与 manifest checkpoint 关联）。
    pub fn seal(&self) -> anyhow::Result<u64> {
        let mut f = self.file.lock().unwrap_or_else(|e| e.into_inner());
        f.flush()?;

        let id = {
            let mut last = self.last_seal_id.lock().unwrap_or_else(|e| e.into_inner());
            let id = now_seal_id().max(last.saturating_add(1));
            *last = id;
            id
        };
        let sealed = self.dir.join(format!("events.wal.seal-{id:016x}"));
        // 关闭当前句柄后再 rename（避免平台差异）。
        drop(f);

        if self.current.exists() {
            std::fs::rename(&self.current, &sealed)?;
        }

        // fsync(dir) after rename to ensure the directory entry is persisted.
        if let Ok(dir) = std::fs::File::open(&self.dir) {
            let _ = dir.sync_all();
        }

        let newf = open_or_init(&self.current)?;
        *self.file.lock().unwrap_or_else(|e| e.into_inner()) = newf;
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
                    if let Err(e) = std::fs::remove_file(&p) {
                        tracing::warn!("Failed to remove sealed WAL {}: {e}", p.display());
                    }
                }
            }
        }
        Ok(())
    }

    /// 回放：只读取 seal_id > checkpoint 的 sealed WAL + 当前 WAL。
    pub fn replay_since_seal(&self, checkpoint_seal_id: u64) -> anyhow::Result<WalReplayResult> {
        let mut sealed = Vec::new();
        let mut sealed_ids = Vec::new();
        for ent in std::fs::read_dir(&self.dir)? {
            let Ok(ent) = ent else { continue };
            let p = ent.path();
            if let Some(id) = parse_seal_id(&p) {
                sealed_ids.push(id);
                if id > checkpoint_seal_id {
                    sealed.push((id, p));
                }
            }
        }
        sealed_ids.sort_unstable();
        sealed.sort_by_key(|(id, _)| *id);
        let gap_detected = sealed_id_gap_detected(&sealed_ids);

        let mut events: Vec<EventRecord> = Vec::new();
        let mut root_events: Vec<RootStateRecord> = Vec::new();
        let mut truncated = 0usize;
        for (_, p) in sealed.iter() {
            let (mut evs, mut roots, t) = read_wal_file(p)?;
            truncated += t;
            events.append(&mut evs);
            root_events.append(&mut roots);
        }
        let (mut cur, mut cur_roots, t) = read_wal_file(&self.current)?;
        truncated += t;
        events.append(&mut cur);
        root_events.append(&mut cur_roots);

        // Deduplicate by (id, timestamp), keeping the last occurrence.
        // This prevents duplicate index entries when WAL contains duplicate
        // records from abnormal writes or partial flushes.
        let mut last_pos = std::collections::HashMap::new();
        for (idx, ev) in events.iter().enumerate() {
            last_pos.insert((ev.id.clone(), ev.timestamp), idx);
        }
        let mut keep = vec![false; events.len()];
        for &idx in last_pos.values() {
            keep[idx] = true;
        }
        let mut retained = Vec::with_capacity(last_pos.len());
        for (idx, ev) in events.drain(..).enumerate() {
            if keep[idx] {
                retained.push(ev);
            }
        }
        events = retained;

        // 统一为单调 seq（WAL 内部 seq 只用于排序/回放稳定性）。
        for (i, e) in events.iter_mut().enumerate() {
            e.seq = i as u64 + 1;
        }

        let events_replayed = events.len();
        let root_events_replayed = root_events.len();
        Ok(WalReplayResult {
            events,
            root_events,
            events_replayed,
            root_events_replayed,
            sealed_used: sealed.len(),
            truncated_tail_records: truncated,
            gap_detected,
            checkpoint_used: checkpoint_seal_id,
        })
    }
}

// ---------------------------------------------------------------------------
// WriteAheadLog trait impl
// ---------------------------------------------------------------------------

impl crate::storage::traits::WriteAheadLog for WalStore {
    fn dir(&self) -> &Path {
        self.dir()
    }

    fn append(&self, events: &[EventRecord]) -> anyhow::Result<()> {
        self.append(events)
    }

    fn append_root_events(&self, records: &[RootStateRecord]) -> anyhow::Result<()> {
        self.append_root_events(records)
    }

    fn seal(&self) -> anyhow::Result<u64> {
        self.seal()
    }

    fn cleanup_sealed_up_to(&self, seal_id: u64) -> anyhow::Result<()> {
        self.cleanup_sealed_up_to(seal_id)
    }

    fn replay_since_seal(&self, checkpoint_seal_id: u64) -> anyhow::Result<WalReplayResult> {
        self.replay_since_seal(checkpoint_seal_id)
    }

    fn set_durability(&self, durability: WalDurability) {
        self.set_durability(durability)
    }

    fn durability(&self) -> WalDurability {
        self.durability()
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
    if magic == WAL_MAGIC && (ver == 1 || ver == 2 || ver == 3) && ver != WAL_VERSION {
        // v1/v2/v3 -> latest：非破坏性升级
        // 关键点：绝不能 truncate，否则会丢事件。
        drop(f);
        let id = now_seal_id();
        let suffix = match ver {
            1 => ".v1",
            2 => ".v2",
            3 => ".v3",
            _ => "",
        };
        let sealed = path
            .parent()
            .unwrap_or_else(|| Path::new("."))
            .join(format!("events.wal.seal-{id:016x}{suffix}"));
        std::fs::rename(path, &sealed)?;

        // fsync(dir) after rename to ensure the directory entry is persisted.
        if let Some(parent) = path.parent() {
            if let Ok(dir) = std::fs::File::open(parent) {
                let _ = dir.sync_all();
            }
        }

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
    } else if magic != WAL_MAGIC || !(1..=WAL_VERSION).contains(&ver) {
        // 不兼容：truncate 重新开始（保守）。已知版本以外视为垃圾文件。
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
    } else if ver != WAL_VERSION {
        // 读 legacy sealed 是允许的；但 current WAL 只写最新版本。
        // 若出现 v2->未来版本等情况，会在上面的分支被 truncate。
    }

    Ok(f)
}

fn parse_seal_id(path: &Path) -> Option<u64> {
    let name = path.file_name()?.to_str()?;
    let prefix = "events.wal.seal-";
    if !name.starts_with(prefix) {
        return None;
    }
    let rest = &name[prefix.len()..];
    let hex: String = rest.chars().take_while(|c| c.is_ascii_hexdigit()).collect();
    if hex.is_empty() {
        return None;
    }
    u64::from_str_radix(&hex, 16).ok()
}

fn max_existing_seal_id(dir: &Path) -> u64 {
    let Ok(entries) = std::fs::read_dir(dir) else {
        return 0;
    };
    entries
        .flatten()
        .filter_map(|entry| parse_seal_id(entry.path().as_path()))
        .max()
        .unwrap_or(0)
}

fn sealed_id_gap_detected(ids: &[u64]) -> bool {
    if ids.len() < 2 {
        return false;
    }
    let mut prev = None;
    for &id in ids {
        if prev == Some(id) {
            return true;
        }
        prev = Some(id);
    }
    let Some(max_id) = ids.iter().copied().max() else {
        return false;
    };
    if max_id > 1_000_000 {
        return false;
    }
    ids.windows(2).any(|pair| pair[1] > pair[0] + 1)
}

fn read_wal_file(path: &Path) -> anyhow::Result<(Vec<EventRecord>, Vec<RootStateRecord>, usize)> {
    if !path.exists() {
        return Ok((Vec::new(), Vec::new(), 0));
    }
    let mut f = File::open(path)?;
    let file_len = f.metadata().map(|m| m.len()).unwrap_or(u64::MAX);

    let mut hdr = [0u8; 8];
    if f.read_exact(&mut hdr).is_err() {
        return Ok((Vec::new(), Vec::new(), 0));
    }
    let magic = u32::from_le_bytes(hdr[0..4].try_into()?);
    let ver = u32::from_le_bytes(hdr[4..8].try_into()?);
    if magic != WAL_MAGIC || !(1..=WAL_VERSION).contains(&ver) {
        return Ok((Vec::new(), Vec::new(), 0));
    }

    if ver < WAL_VERSION {
        tracing::warn!(
            "Loading legacy WAL v{} from {}; current writer is v{}",
            ver,
            path.display(),
            WAL_VERSION
        );
    }

    let mut out = Vec::new();
    let mut root_out = Vec::new();
    let mut truncated_tail = 0usize;
    let mut pos: u64 = 8; // header consumed
    loop {
        let mut lb = [0u8; 8];
        if f.read_exact(&mut lb).is_err() {
            if pos < file_len {
                truncated_tail += 1;
            }
            break;
        }
        pos = pos.saturating_add(8);
        let len = u32::from_le_bytes(lb[0..4].try_into()?) as usize;
        let crc = u32::from_le_bytes(lb[4..8].try_into()?);
        if len > MAX_WAL_RECORD_BYTES || pos.saturating_add(len as u64) > file_len {
            truncated_tail += 1;
            break;
        }
        let mut buf = vec![0u8; len];
        if f.read_exact(&mut buf).is_err() {
            // Truncated payload: real IO error, stop reading
            truncated_tail += 1;
            break;
        }
        pos = pos.saturating_add(len as u64);

        // CRC verification: v3 uses CRC32C, v1/v2 use legacy crc32_simple
        let crc_ok = if ver >= 3 {
            crc32c_checksum(&buf) == crc
        } else {
            crc32_simple(&buf) == crc
        };

        if !crc_ok {
            // CRC mismatch: skip this record and continue to the next one
            truncated_tail += 1;
            continue;
        }

        if let Some(record) = decode_wal_record(ver, &buf) {
            match record {
                DecodedWalRecord::File(ev) => out.push(ev),
                DecodedWalRecord::Root(root) => root_out.push(root),
            }
        }
    }
    Ok((out, root_out, truncated_tail))
}

fn encode_event(ev: &EventRecord) -> Vec<u8> {
    // 当前写入的 WAL 永远使用最新版本（WAL_VERSION）。
    encode_event_v2(ev)
}

#[cfg(test)]
fn encode_event_v1(ev: &EventRecord) -> Vec<u8> {
    let mut out = Vec::new();
    let (secs, nanos) = system_time_to_unix(ev.timestamp);
    let (kind, from_opt): (u8, Option<&Path>) = match &ev.event_type {
        EventType::Create => (1, None),
        EventType::Delete => (2, None),
        EventType::Modify => (3, None),
        EventType::Rename {
            from,
            from_path_hint,
        } => (4, from_path_hint.as_deref().or_else(|| from.as_path())),
    };

    let path = match ev.best_path() {
        Some(p) => encode_path(p),
        None => Vec::new(),
    };
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

fn encode_event_v2(ev: &EventRecord) -> Vec<u8> {
    let mut out = Vec::new();
    let (secs, nanos) = system_time_to_unix(ev.timestamp);
    let kind: u8 = match &ev.event_type {
        EventType::Create => 1,
        EventType::Delete => 2,
        EventType::Modify => 3,
        EventType::Rename { .. } => 4,
    };

    out.push(kind);
    out.extend_from_slice(&secs.to_le_bytes());
    out.extend_from_slice(&nanos.to_le_bytes());
    out.extend_from_slice(&encode_file_id(&ev.id));
    out.extend_from_slice(&encode_path_opt(&ev.path_hint));

    if let EventType::Rename {
        from,
        from_path_hint,
    } = &ev.event_type
    {
        out.extend_from_slice(&encode_file_id(from));
        out.extend_from_slice(&encode_path_opt(from_path_hint));
    }

    out
}

fn encode_root_state_record(record: &RootStateRecord) -> Vec<u8> {
    let mut out = Vec::new();
    let kind = match record.kind {
        RootStateKind::OfflineRoot => 101,
        RootStateKind::OnlineRoot => 102,
    };
    let (secs, nanos) = system_time_to_unix(record.timestamp);
    out.push(kind);
    out.extend_from_slice(&secs.to_le_bytes());
    out.extend_from_slice(&nanos.to_le_bytes());
    out.extend_from_slice(&record.seq.to_le_bytes());
    let root_path = encode_path(&record.root_path);
    out.extend_from_slice(&encode_bytes(&root_path));
    out.extend_from_slice(&record.identity.mount_id.to_le_bytes());
    out.extend_from_slice(&encode_string(&record.identity.major_minor));
    out.extend_from_slice(&encode_string_opt(&record.identity.fs_uuid));
    out.extend_from_slice(&encode_string(&record.identity.source));
    out.extend_from_slice(&encode_string(&record.identity.fstype));
    out.extend_from_slice(&encode_paths(&record.affected_prefixes));
    out.extend_from_slice(&encode_string_opt(&record.reason));
    out
}

enum DecodedWalRecord {
    File(EventRecord),
    Root(RootStateRecord),
}

fn decode_wal_record(ver: u32, buf: &[u8]) -> Option<DecodedWalRecord> {
    match *buf.first()? {
        101 | 102 if ver >= 3 => decode_root_state_record(buf).map(DecodedWalRecord::Root),
        _ => {
            let decode_ver = if ver >= 3 { 2 } else { ver };
            decode_event(decode_ver, buf).map(DecodedWalRecord::File)
        }
    }
}

fn decode_event(ver: u32, buf: &[u8]) -> Option<EventRecord> {
    match ver {
        1 => decode_event_v1(buf),
        2 => decode_event_v2(buf),
        _ => None,
    }
}

fn decode_root_state_record(buf: &[u8]) -> Option<RootStateRecord> {
    if buf.len() < 1 + 8 + 4 + 8 + 4 {
        return None;
    }
    let mut off = 0usize;
    let kind = match buf[off] {
        101 => RootStateKind::OfflineRoot,
        102 => RootStateKind::OnlineRoot,
        _ => return None,
    };
    off += 1;
    let secs = u64::from_le_bytes(buf.get(off..off + 8)?.try_into().ok()?);
    off += 8;
    let nanos = u32::from_le_bytes(buf.get(off..off + 4)?.try_into().ok()?);
    off += 4;
    let seq = u64::from_le_bytes(buf.get(off..off + 8)?.try_into().ok()?);
    off += 8;
    let root_path = decode_path(decode_bytes(buf, &mut off)?);
    let mount_id = u32::from_le_bytes(buf.get(off..off + 4)?.try_into().ok()?);
    off += 4;
    let major_minor = decode_string(buf, &mut off)?;
    let fs_uuid = decode_string_opt(buf, &mut off)?;
    let source = decode_string(buf, &mut off)?;
    let fstype = decode_string(buf, &mut off)?;
    let affected_prefixes = decode_paths(buf, &mut off)?;
    let reason = decode_string_opt(buf, &mut off)?;

    Some(RootStateRecord {
        kind,
        root_path,
        identity: MountIdentity {
            mount_id,
            major_minor,
            fs_uuid,
            source,
            fstype,
        },
        affected_prefixes,
        reason,
        seq,
        timestamp: unix_to_system_time(secs, nanos),
    })
}

fn decode_event_v1(buf: &[u8]) -> Option<EventRecord> {
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
        4 => {
            let p = decode_path(fbytes);
            EventType::Rename {
                from: FileIdentifier::Path(p.clone()),
                from_path_hint: if flen > 0 { Some(p) } else { None },
            }
        }
        _ => EventType::Modify,
    };

    Some(EventRecord {
        seq: 0,
        timestamp: ts,
        event_type,
        id: FileIdentifier::Path(path.clone()),
        path_hint: Some(path),
    })
}

fn decode_event_v2(buf: &[u8]) -> Option<EventRecord> {
    // kind(1) + secs(8) + nanos(4) + id(tag+payload) + hint(4+payload) ...
    if buf.len() < 1 + 8 + 4 + 1 + 4 {
        return None;
    }
    let mut off = 0usize;
    let kind = buf[off];
    off += 1;
    let secs = u64::from_le_bytes(buf.get(off..off + 8)?.try_into().ok()?);
    off += 8;
    let nanos = u32::from_le_bytes(buf.get(off..off + 4)?.try_into().ok()?);
    off += 4;
    let id = decode_file_id(buf, &mut off)?;
    let path_hint = decode_path_opt(buf, &mut off)?;

    let ts = unix_to_system_time(secs, nanos);
    let event_type = match kind {
        1 => EventType::Create,
        2 => EventType::Delete,
        3 => EventType::Modify,
        4 => {
            let from = decode_file_id(buf, &mut off)?;
            let from_path_hint = decode_path_opt(buf, &mut off)?;
            EventType::Rename {
                from,
                from_path_hint,
            }
        }
        _ => EventType::Modify,
    };

    Some(EventRecord {
        seq: 0,
        timestamp: ts,
        event_type,
        id,
        path_hint,
    })
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
            id: FileIdentifier::Path(p1.clone()),
            path_hint: Some(p1.clone()),
        }])
        .unwrap();

        let seal1 = wal.seal().unwrap();

        wal.append(&[EventRecord {
            seq: 2,
            timestamp: std::time::SystemTime::now(),
            event_type: EventType::Delete,
            id: FileIdentifier::Path(p2.clone()),
            path_hint: Some(p2.clone()),
        }])
        .unwrap();

        // checkpoint=0：回放 sealed+current
        let r = wal.replay_since_seal(0).unwrap();
        assert_eq!(r.events.len(), 2);

        // checkpoint=seal1：只回放 current
        let r2 = wal.replay_since_seal(seal1).unwrap();
        assert_eq!(r2.events.len(), 1);
    }

    #[test]
    fn wal_v1_file_is_sealed_and_replayed_after_upgrade_to_latest() {
        let dir = unique_tmp_dir("upgrade");
        std::fs::create_dir_all(&dir).unwrap();

        let wal_path = dir.join("events.wal");

        // 手工构造一个 v1 WAL（header ver=1 + 1 条记录）
        {
            let mut f = OpenOptions::new()
                .create(true)
                .write(true)
                .truncate(true)
                .open(&wal_path)
                .unwrap();
            f.write_all(&WAL_MAGIC.to_le_bytes()).unwrap();
            f.write_all(&1u32.to_le_bytes()).unwrap();

            let p = dir.join("legacy.txt");
            let ev = EventRecord {
                seq: 1,
                timestamp: std::time::SystemTime::now(),
                event_type: EventType::Create,
                id: FileIdentifier::Path(p.clone()),
                path_hint: Some(p),
            };
            let payload = encode_event_v1(&ev);
            let len: u32 = payload.len().try_into().unwrap();
            let crc = crc32_simple(&payload);
            f.write_all(&len.to_le_bytes()).unwrap();
            f.write_all(&crc.to_le_bytes()).unwrap();
            f.write_all(&payload).unwrap();
            f.flush().unwrap();
        }

        // 打开时应触发 v1 -> latest 非破坏性升级（rename 为 sealed-*.v1）
        let wal = WalStore::open_in_dir(dir.clone()).unwrap();

        let sealed_v1 = std::fs::read_dir(&dir)
            .unwrap()
            .filter_map(|e| e.ok())
            .map(|e| e.path())
            .find(|p| {
                p.file_name()
                    .and_then(|n| n.to_str())
                    .map(|s| s.starts_with("events.wal.seal-") && s.contains(".v1"))
                    .unwrap_or(false)
            });
        assert!(sealed_v1.is_some());

        // 回放应能读到 v1 sealed 中的事件
        let r = wal.replay_since_seal(0).unwrap();
        assert_eq!(r.events.len(), 1);
    }

    #[test]
    fn wal_replays_offline_and_online_root_state_records() {
        let dir = unique_tmp_dir("root-state");
        std::fs::create_dir_all(&dir).unwrap();
        let wal = WalStore::open_in_dir(dir.clone()).unwrap();
        let identity = MountIdentity {
            mount_id: 7,
            major_minor: "8:1".to_string(),
            fs_uuid: Some("uuid-a".to_string()),
            source: "/dev/sda1".to_string(),
            fstype: "ext4".to_string(),
        };
        let offline = RootStateRecord::offline(
            1,
            PathBuf::from("/mnt/offline"),
            identity.clone(),
            vec![PathBuf::from("/mnt/offline/project")],
            Some("probe_timeout".to_string()),
        );
        let online = RootStateRecord::online(
            2,
            PathBuf::from("/mnt/offline"),
            identity,
            vec![PathBuf::from("/mnt/offline/project")],
        );

        wal.append_root_events(&[offline.clone(), online.clone()])
            .unwrap();
        let replay = wal.replay_since_seal(0).unwrap();

        assert_eq!(replay.events_replayed, 0);
        assert_eq!(replay.root_events_replayed, 2);
        assert_eq!(replay.root_events[0].kind, RootStateKind::OfflineRoot);
        assert_eq!(replay.root_events[0].root_path, offline.root_path);
        assert_eq!(replay.root_events[1].kind, RootStateKind::OnlineRoot);
        assert_eq!(
            replay.root_events[1].affected_prefixes,
            online.affected_prefixes
        );
    }
}
