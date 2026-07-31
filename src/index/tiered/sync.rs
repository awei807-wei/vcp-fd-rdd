use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::{Duration, Instant, UNIX_EPOCH};

use crate::config::L3ScanPolicy;
use crate::core::{EventRecord, EventType, FileIdentifier, FileKey, FileKind, FileMeta, Task};
use crate::event::sync::{
    now_ns, DirectoryFingerprint, DirtyPriority, DirtyQueueEntry, DirtyReason, DirtyRepairCursor,
    DirtyRepairProgress, DirtyScope,
};
use crate::fs_policy::{FsPolicy, SharedMountPolicyCounters};
use crate::index::delta_buffer::SubtreeInvalidationSnapshot;
use crate::index::l2_partition::{mtime_to_ns, PersistentIndex};
use crate::index::PathFreshness;
use crate::io_governor::IoGovernor;
use crate::util::{maybe_trim_rss, path_has_excluded_component};

use super::{
    directory_manifest::{DirectoryManifestBuilder, DirectoryManifestSummary},
    pathbuf_from_bytes, DirtyProcessReport, DirtyScanOutcome, ScanOutcome, StartupRepairStats,
    TieredIndex,
};

const REPAIR_SLICE_MAX_ENTRIES: usize = 512;

/// 修复切片的线程局部 scratch：批内跨目录复用两个按 entry 规模分配的大缓冲，
/// 消除每目录全新分配簇带来的次缺页与分配 CPU（任务5b）。
///
/// take/return 模式：主流程单一归还点；任何提前 return 只会让本次取出的
/// 缓冲随栈丢弃（下次取到空 Vec，仅损失一次复用，不影响正确性）。
struct RepairSliceScratch {
    upsert_events: Vec<EventRecord>,
    upsert_metas: Vec<FileMeta>,
    manifest: DirectoryManifestBuilder,
    dir_children: DirChildBuffer,
}

thread_local! {
    static REPAIR_SLICE_SCRATCH: std::cell::RefCell<RepairSliceScratch> =
        std::cell::RefCell::new(RepairSliceScratch {
            upsert_events: Vec::new(),
            upsert_metas: Vec::new(),
            manifest: DirectoryManifestBuilder::default(),
            dir_children: DirChildBuffer::default(),
        });
}

fn take_repair_slice_scratch() -> (
    Vec<EventRecord>,
    Vec<FileMeta>,
    DirectoryManifestBuilder,
    DirChildBuffer,
) {
    REPAIR_SLICE_SCRATCH.with(|scratch| {
        let mut scratch = scratch.borrow_mut();
        (
            std::mem::take(&mut scratch.upsert_events),
            std::mem::take(&mut scratch.upsert_metas),
            std::mem::take(&mut scratch.manifest),
            std::mem::take(&mut scratch.dir_children),
        )
    })
}

fn return_repair_slice_scratch(
    mut events: Vec<EventRecord>,
    mut metas: Vec<FileMeta>,
    mut manifest: DirectoryManifestBuilder,
    mut dir_children: DirChildBuffer,
) {
    events.clear();
    metas.clear();
    manifest.reset();
    dir_children.reset();
    REPAIR_SLICE_SCRATCH.with(|scratch| {
        let mut scratch = scratch.borrow_mut();
        scratch.upsert_events = events;
        scratch.upsert_metas = metas;
        scratch.manifest = manifest;
        scratch.dir_children = dir_children;
    });
}

#[derive(Default)]
struct AlignmentScratch {
    current_names: DirectoryNameBuffer,
    indexed_children: Vec<PathBuf>,
    missing_children: Vec<PathBuf>,
    delete_events: Vec<EventRecord>,
}

thread_local! {
    static ALIGNMENT_SCRATCH: std::cell::RefCell<AlignmentScratch> =
        std::cell::RefCell::new(AlignmentScratch::default());
}

fn take_alignment_scratch() -> AlignmentScratch {
    ALIGNMENT_SCRATCH.with(|scratch| std::mem::take(&mut *scratch.borrow_mut()))
}

fn return_alignment_scratch(mut scratch: AlignmentScratch) {
    const MAX_RETAINED_NAMES: usize = 65_536;
    const MAX_RETAINED_EVENTS: usize = 16_384;

    if scratch.current_names.slot_count() > MAX_RETAINED_NAMES {
        scratch.current_names = DirectoryNameBuffer::default();
    } else {
        scratch.current_names.reset();
    }
    if scratch.missing_children.capacity() > MAX_RETAINED_EVENTS {
        scratch.missing_children = Vec::new();
    } else {
        scratch.missing_children.clear();
    }
    if scratch.indexed_children.capacity() > MAX_RETAINED_EVENTS {
        scratch.indexed_children = Vec::new();
    } else {
        scratch.indexed_children.clear();
    }
    if scratch.delete_events.capacity() > MAX_RETAINED_EVENTS {
        scratch.delete_events = Vec::new();
    } else {
        scratch.delete_events.clear();
    }
    ALIGNMENT_SCRATCH.with(|slot| *slot.borrow_mut() = scratch);
}

#[cfg(test)]
struct RepairSlicePreApplyTestHook {
    dir: PathBuf,
    entered: Arc<std::sync::Barrier>,
    release: Arc<std::sync::Barrier>,
}

#[cfg(test)]
fn repair_slice_pre_apply_test_hook(
) -> &'static std::sync::Mutex<Option<RepairSlicePreApplyTestHook>> {
    static HOOK: std::sync::OnceLock<std::sync::Mutex<Option<RepairSlicePreApplyTestHook>>> =
        std::sync::OnceLock::new();
    HOOK.get_or_init(|| std::sync::Mutex::new(None))
}

#[cfg(test)]
pub(super) fn install_repair_slice_pre_apply_test_hook(
    dir: PathBuf,
) -> (Arc<std::sync::Barrier>, Arc<std::sync::Barrier>) {
    let entered = Arc::new(std::sync::Barrier::new(2));
    let release = Arc::new(std::sync::Barrier::new(2));
    *repair_slice_pre_apply_test_hook()
        .lock()
        .unwrap_or_else(|error| error.into_inner()) = Some(RepairSlicePreApplyTestHook {
        dir,
        entered: Arc::clone(&entered),
        release: Arc::clone(&release),
    });
    (entered, release)
}

#[cfg(test)]
fn run_repair_slice_pre_apply_test_hook(dir: &Path) {
    let hook = {
        let mut slot = repair_slice_pre_apply_test_hook()
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        if slot.as_ref().is_some_and(|hook| hook.dir == dir) {
            slot.take()
        } else {
            None
        }
    };
    if let Some(hook) = hook {
        hook.entered.wait();
        hook.release.wait();
    }
}
const REPAIR_SLICE_MAX_MS: u64 = 20;

/// readdir 批量删除对齐（v2）。
///
/// 将候选 `(doc_id, path)` 条目按 parent dir 分组，每个 dirty 目录做一次
/// `std::fs::read_dir` 构建 `HashSet<OsString>`，与索引文件名做差集：文件名不在
/// readdir 结果中即视为已删除。`read_dir` 或迭代失败时放弃该目录的负事实，避免将
/// 挂载点断联、权限抖动或部分读取误判为整目录删除。
///
/// 内存权衡：`HashSet<OsString>` 在每个 parent dir 处理完后立即 drop，不跨目录累积。
/// 30 万文件目录的 HashSet 峰值约 20-30MB，远小于逐文件 stat 的 15-60s 开销。
pub(super) fn readdir_delete_alignment(
    to_delete: Vec<(u64, PathBuf)>,
    io_governor: Option<&IoGovernor>,
) -> Vec<PathBuf> {
    // 按 parent dir 分组，每个 dirty 目录构建一次 name set，循环外不累积。
    let mut by_parent: HashMap<PathBuf, Vec<(u64, PathBuf)>> = HashMap::new();
    for (doc_id, path) in to_delete {
        if let Some(parent) = path.parent() {
            by_parent
                .entry(parent.to_path_buf())
                .or_default()
                .push((doc_id, path));
        }
    }

    let mut deleted: Vec<PathBuf> = Vec::new();
    for (parent_dir, files) in by_parent {
        // 每个 dirty 目录构建一次 name set，用完立即 drop（作用域在本循环迭代内）。
        if let Some(gov) = io_governor {
            gov.before_io();
        }
        let entries = match std::fs::read_dir(&parent_dir) {
            Ok(entries) => entries,
            Err(err) => {
                tracing::debug!(
                    "delete alignment skipped unreadable dir {}: {}",
                    parent_dir.display(),
                    err
                );
                continue;
            }
        };
        let mut current_names = HashSet::new();
        let mut complete = true;
        for entry in entries {
            match entry {
                Ok(entry) => {
                    current_names.insert(entry.file_name());
                }
                Err(err) => {
                    tracing::debug!(
                        "delete alignment abandoned incomplete readdir for {}: {}",
                        parent_dir.display(),
                        err
                    );
                    complete = false;
                    break;
                }
            }
        }
        if !complete {
            continue;
        }

        for (_doc_id, path) in files {
            // 文件名不在完整 readdir 结果中 = 被删除；OsString 直接比对，支持非 UTF-8。
            let should_delete = path
                .file_name()
                .map(|name| !current_names.contains(name))
                .unwrap_or(false);
            if should_delete {
                deleted.push(path);
            }
        }
        // current_names 在此处 drop，避免跨目录累积内存峰值。
    }
    deleted
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum RebuildAdmission {
    StartNow,
    Scheduled(Duration),
    Coalesced,
}

#[derive(Debug)]
struct BudgetedScanOutcome {
    outcome: ScanOutcome,
    budget_exhausted: bool,
    dropped_stale_batch: bool,
}

#[derive(Debug)]
pub(super) struct SlicedScanOutcome {
    outcome: ScanOutcome,
    manifest_skipped: bool,
    pub(super) completed: bool,
    next_cursor: Option<DirtyRepairCursor>,
    child_dirs: Vec<PathBuf>,
    pub(super) dropped_stale_batch: bool,
    failed: bool,
    dir_start_stamp: Option<DirectoryFingerprint>,
    invalidation_epoch: Option<u64>,
}

type DirectoryStamp = (PathBuf, DirectoryFingerprint);

struct RepairContinuationState {
    cursor: Option<DirtyRepairCursor>,
    completion_stamps: Option<Vec<DirectoryStamp>>,
    invalidation_epoch: Option<u64>,
    completed_outcome: Option<ScanOutcome>,
}

fn repair_continuation_cursor(
    entry: &mut DirtyQueueEntry,
    sliced: &mut SlicedScanOutcome,
    recursive_subtree_repair: bool,
) -> RepairContinuationState {
    if sliced.dropped_stale_batch || sliced.failed {
        return RepairContinuationState {
            cursor: None,
            completion_stamps: None,
            invalidation_epoch: None,
            completed_outcome: None,
        };
    }
    if !recursive_subtree_repair {
        return RepairContinuationState {
            cursor: sliced.next_cursor.clone(),
            completion_stamps: None,
            invalidation_epoch: sliced.invalidation_epoch,
            completed_outcome: None,
        };
    }

    let invalidation_epoch = sliced.invalidation_epoch;
    let (current_dir, mut pending_dirs, mut completed_dir_stamps, mut progress) = entry
        .repair_cursor
        .take()
        .map(DirtyRepairCursor::into_recursive_collections)
        .unwrap_or_else(|| {
            (
                entry.scope.dir_paths()[0].clone(),
                Default::default(),
                Vec::new(),
                DirtyRepairProgress::default(),
            )
        });
    progress.accumulate(
        sliced.outcome.scanned,
        sliced.outcome.changed,
        sliced.outcome.elapsed_ms,
        std::mem::take(&mut sliced.outcome.project_roots),
    );
    pending_dirs.extend(std::mem::take(&mut sliced.child_dirs));
    if sliced.completed {
        let Some(dir_start_stamp) = sliced.dir_start_stamp else {
            return RepairContinuationState {
                cursor: None,
                completion_stamps: None,
                invalidation_epoch: None,
                completed_outcome: None,
            };
        };
        completed_dir_stamps.push((current_dir, dir_start_stamp));
    }

    if let Some(cursor) = &sliced.next_cursor {
        return RepairContinuationState {
            cursor: Some(
                DirtyRepairCursor::from_recursive_collections(
                    cursor.dir.clone(),
                    cursor.offset,
                    pending_dirs,
                    completed_dir_stamps,
                    sliced.dir_start_stamp,
                    progress,
                )
                .with_scan_invalidation_epoch(
                    invalidation_epoch.expect("recursive scan tracks invalidation epoch"),
                ),
            ),
            completion_stamps: None,
            invalidation_epoch,
            completed_outcome: None,
        };
    }
    if !sliced.completed || pending_dirs.is_empty() {
        let completed_outcome = if sliced.completed {
            let (scanned, changed, elapsed_ms, project_roots) = progress.into_parts();
            Some(ScanOutcome {
                scanned,
                changed,
                elapsed_ms,
                project_roots,
            })
        } else {
            None
        };
        return RepairContinuationState {
            cursor: None,
            completion_stamps: sliced.completed.then_some(completed_dir_stamps),
            invalidation_epoch,
            completed_outcome,
        };
    }

    let next_dir = pending_dirs
        .pop_first()
        .expect("pending dir checked non-empty");
    RepairContinuationState {
        cursor: Some(
            DirtyRepairCursor::from_recursive_collections(
                next_dir,
                0,
                pending_dirs,
                completed_dir_stamps,
                None,
                progress,
            )
            .with_scan_invalidation_epoch(
                invalidation_epoch.expect("recursive scan tracks invalidation epoch"),
            ),
        ),
        completion_stamps: None,
        invalidation_epoch,
        completed_outcome: None,
    }
}

fn recursive_completion_stable(stamps: &[DirectoryStamp]) -> bool {
    stamps.iter().all(|(dir, expected_fingerprint)| {
        std::fs::symlink_metadata(dir)
            .ok()
            .filter(|meta| meta.is_dir())
            .and_then(|meta| directory_read_fingerprint(dir, &meta))
            .is_some_and(|fingerprint| fingerprint == *expected_fingerprint)
    })
}

#[derive(Debug)]
struct ImmediateScanReconcileOutcome {
    outcome: ScanOutcome,
    deleted: usize,
    stable: bool,
}

#[derive(Clone, Debug)]
struct DirChildEntry {
    path: PathBuf,
}

#[derive(Debug, Default)]
struct DirectoryNameBuffer {
    names: Vec<Vec<u8>>,
    active_len: usize,
}

impl DirectoryNameBuffer {
    fn reset(&mut self) {
        self.active_len = 0;
    }

    fn push(&mut self, name: &[u8]) {
        if let Some(slot) = self.names.get_mut(self.active_len) {
            slot.clear();
            slot.extend_from_slice(name);
        } else {
            self.names.push(name.to_vec());
        }
        self.active_len = self.active_len.saturating_add(1);
    }

    fn finish(&mut self) {
        self.names[..self.active_len].sort_unstable();
    }

    fn contains(&self, name: &std::ffi::OsStr) -> bool {
        self.names[..self.active_len]
            .binary_search_by(|candidate| candidate.as_slice().cmp(name.as_encoded_bytes()))
            .is_ok()
    }

    fn slot_count(&self) -> usize {
        self.names.len()
    }
}

#[derive(Debug, Default)]
struct DirChildBuffer {
    entries: Vec<DirChildEntry>,
    active_len: usize,
}

impl DirChildBuffer {
    fn reset(&mut self) {
        self.active_len = 0;
    }

    fn len(&self) -> usize {
        self.active_len
    }

    fn active(&self) -> &[DirChildEntry] {
        &self.entries[..self.active_len]
    }

    fn push_joined(&mut self, dir: &Path, name: &std::ffi::OsStr) {
        if let Some(entry) = self.entries.get_mut(self.active_len) {
            entry.path.clear();
            entry.path.push(dir);
            entry.path.push(name);
        } else {
            self.entries.push(DirChildEntry {
                path: dir.join(name),
            });
        }
        self.active_len = self.active_len.saturating_add(1);
    }

    #[cfg(not(unix))]
    fn push_path(&mut self, path: &Path) {
        if let Some(entry) = self.entries.get_mut(self.active_len) {
            entry.path.clear();
            entry.path.push(path);
        } else {
            self.entries.push(DirChildEntry {
                path: path.to_path_buf(),
            });
        }
        self.active_len = self.active_len.saturating_add(1);
    }
}

fn visit_dirs_since(
    roots: &[PathBuf],
    ignore_prefixes: &[PathBuf],
    exclude_dirs: &[String],
    cutoff_ns: u64,
    log_prefix: &str,
    io_governor: &IoGovernor,
    mut on_dir: impl FnMut(&std::path::Path, bool) -> bool,
) -> bool {
    use std::time::Duration;

    let cutoff = UNIX_EPOCH
        + Duration::new(
            cutoff_ns / 1_000_000_000,
            (cutoff_ns % 1_000_000_000) as u32,
        );

    let should_skip = |p: &std::path::Path| -> bool {
        ignore_prefixes
            .iter()
            .any(|ig| !ig.as_os_str().is_empty() && p.starts_with(ig))
    };

    let mut stack: Vec<PathBuf> = roots.to_vec();
    while let Some(dir) = stack.pop() {
        if should_skip(&dir) {
            continue;
        }
        if path_has_excluded_component(&dir, exclude_dirs) {
            continue;
        }

        io_governor.before_io();
        let md = match std::fs::symlink_metadata(&dir) {
            Ok(m) => m,
            Err(_) => continue,
        };
        if !md.is_dir() {
            continue;
        }

        let changed = if let Ok(modified) = md.modified() {
            cutoff_ns == 0 || modified > cutoff
        } else {
            true // 保守地认为已变化（部分文件系统不支持 mtime）
        };
        if on_dir(&dir, changed) {
            return true;
        }

        io_governor.before_io();
        let rd = match std::fs::read_dir(&dir) {
            Ok(rd) => rd,
            Err(e) => {
                // 权限/竞态等错误不应导致"永远判 stale"；保守地跳过不可读子树。
                tracing::debug!(
                    "{} mtime crawl: skip unreadable dir {:?}: {}",
                    log_prefix,
                    dir,
                    e
                );
                continue;
            }
        };
        for ent in rd {
            let ent = match ent {
                Ok(e) => e,
                Err(_) => continue,
            };
            let ft = match ent.file_type() {
                Ok(ft) => ft,
                Err(_) => continue,
            };
            if ft.is_dir() {
                stack.push(ent.path());
            }
        }
    }
    false
}

fn collect_dirs_changed_since(
    roots: &[PathBuf],
    ignore_prefixes: &[PathBuf],
    exclude_dirs: &[String],
    cutoff_ns: u64,
    io_governor: &IoGovernor,
) -> Vec<PathBuf> {
    let mut out: Vec<PathBuf> = Vec::new();
    visit_dirs_since(
        roots,
        ignore_prefixes,
        exclude_dirs,
        cutoff_ns,
        "fast-sync",
        io_governor,
        |dir, changed| {
            if changed {
                out.push(dir.to_path_buf());
            }
            false
        },
    );

    out.sort();
    out.dedup();
    out
}

struct ReadDirSlice {
    completed: bool,
    next_offset: Option<i64>,
}

// telldir/seekdir behavior across closedir/opendir cycles is filesystem-dependent:
// some filesystems (e.g. NFS, FUSE) may invalidate or reorder cookies after the
// directory handle is closed and reopened. This is acceptable for cold repair because
// the repair loop only requires eventual consistency -- missed or duplicated entries
// are corrected by subsequent repair passes, and the hot watcher provides a fallback
// for actively changing directories.
#[cfg(unix)]
fn read_dir_slice(
    dir: &Path,
    start_offset: i64,
    max_entries: usize,
    max_elapsed: Duration,
    entries: &mut DirChildBuffer,
) -> std::io::Result<ReadDirSlice> {
    use std::ffi::{CStr, CString};
    use std::os::unix::ffi::OsStrExt;

    struct DirHandle(*mut libc::DIR);

    impl Drop for DirHandle {
        fn drop(&mut self) {
            unsafe {
                libc::closedir(self.0);
            }
        }
    }

    let start = Instant::now();
    entries.reset();
    let mut completed = true;
    let c_path = CString::new(dir.as_os_str().as_bytes())
        .map_err(|_| std::io::Error::new(std::io::ErrorKind::InvalidInput, "path has NUL byte"))?;
    let raw = unsafe { libc::opendir(c_path.as_ptr()) };
    if raw.is_null() {
        return Err(std::io::Error::last_os_error());
    }
    let handle = DirHandle(raw);
    if start_offset > 0 {
        unsafe {
            libc::seekdir(handle.0, start_offset as libc::c_long);
        }
    }
    let mut next_offset = start_offset;

    loop {
        if entries.len() >= max_entries || start.elapsed() >= max_elapsed {
            completed = false;
            break;
        }
        let entry = unsafe { libc::readdir(handle.0) };
        if entry.is_null() {
            break;
        }
        let name = unsafe { CStr::from_ptr((*entry).d_name.as_ptr()) }.to_bytes();
        if name == b"." || name == b".." {
            next_offset = unsafe { libc::telldir(handle.0) as i64 };
            continue;
        }
        entries.push_joined(dir, std::ffi::OsStr::from_bytes(name));
        next_offset = unsafe { libc::telldir(handle.0) as i64 };
    }

    Ok(ReadDirSlice {
        completed,
        next_offset: (!completed).then_some(next_offset),
    })
}

#[cfg(not(unix))]
fn read_dir_slice(
    dir: &Path,
    start_offset: i64,
    max_entries: usize,
    max_elapsed: Duration,
    entries: &mut DirChildBuffer,
) -> std::io::Result<ReadDirSlice> {
    let start = Instant::now();
    entries.reset();
    let mut skipped = 0i64;
    let mut completed = true;

    for child in std::fs::read_dir(dir)? {
        if start.elapsed() >= max_elapsed {
            completed = false;
            break;
        }
        if skipped < start_offset {
            skipped = skipped.saturating_add(1);
            continue;
        }
        if entries.len() >= max_entries || start.elapsed() >= max_elapsed {
            completed = false;
            break;
        }
        let child = child?;
        entries.push_path(child.path().as_path());
    }

    let consumed = start_offset.saturating_add(entries.len() as i64);
    Ok(ReadDirSlice {
        completed,
        next_offset: (!completed).then_some(consumed),
    })
}

#[cfg(target_os = "linux")]
fn read_dir_names(dir: &Path, names: &mut DirectoryNameBuffer) -> std::io::Result<()> {
    use std::ffi::{CStr, CString};
    use std::os::unix::ffi::OsStrExt;

    struct DirHandle(*mut libc::DIR);

    impl Drop for DirHandle {
        fn drop(&mut self) {
            unsafe {
                libc::closedir(self.0);
            }
        }
    }

    names.reset();
    let c_path = CString::new(dir.as_os_str().as_bytes())
        .map_err(|_| std::io::Error::new(std::io::ErrorKind::InvalidInput, "path has NUL byte"))?;
    let raw = unsafe { libc::opendir(c_path.as_ptr()) };
    if raw.is_null() {
        return Err(std::io::Error::last_os_error());
    }
    let handle = DirHandle(raw);

    loop {
        unsafe {
            *libc::__errno_location() = 0;
        }
        let entry = unsafe { libc::readdir(handle.0) };
        if entry.is_null() {
            let errno = unsafe { *libc::__errno_location() };
            return if errno == 0 {
                names.finish();
                Ok(())
            } else {
                Err(std::io::Error::from_raw_os_error(errno))
            };
        }
        let name = unsafe { CStr::from_ptr((*entry).d_name.as_ptr()) }.to_bytes();
        if name != b"." && name != b".." {
            names.push(name);
        }
    }
}

#[cfg(not(target_os = "linux"))]
fn read_dir_names(dir: &Path, names: &mut DirectoryNameBuffer) -> std::io::Result<()> {
    names.reset();
    for entry in std::fs::read_dir(dir)? {
        let entry = entry?;
        names.push(entry.file_name().as_encoded_bytes());
    }
    names.finish();
    Ok(())
}

fn should_skip_dirty_dir(
    dir: &std::path::Path,
    ignore_prefixes: &[PathBuf],
    exclude_dirs: &[String],
) -> bool {
    ignore_prefixes
        .iter()
        .any(|ig| !ig.as_os_str().is_empty() && dir.starts_with(ig))
        || path_has_excluded_component(dir, exclude_dirs)
}

fn project_root_for_marker(path: &Path, markers: &[String]) -> Option<PathBuf> {
    if markers.is_empty() {
        return None;
    }
    let file_name = path.file_name()?.to_string_lossy();
    if markers.iter().any(|marker| marker == file_name.as_ref()) {
        return path.parent().map(Path::to_path_buf);
    }
    None
}

fn path_has_hidden_component_after_root(path: &Path, root: &Path) -> bool {
    path.strip_prefix(root)
        .unwrap_or(path)
        .components()
        .any(|component| match component {
            std::path::Component::Normal(name) => name.to_string_lossy().starts_with('.'),
            _ => false,
        })
}

#[derive(Debug, Default)]
pub(crate) struct FastSyncReport {
    pub(crate) dirs_scanned: usize,
    pub(crate) upsert_events: usize,
    pub(crate) delete_events: usize,
}

impl TieredIndex {
    #[cfg(test)]
    pub(super) fn try_start_rebuild_force(&self) -> bool {
        let mut st = self.rebuild_state.lock();
        if st.in_progress {
            return false;
        }
        st.in_progress = true;
        st.requested = false;
        st.scheduled = false;
        st.last_started_at = Some(Instant::now());
        true
    }

    pub(super) fn reserve_rebuild_with_cooldown(&self, reason: &'static str) -> RebuildAdmission {
        let mut st = self.rebuild_state.lock();
        if self.is_shutting_down() {
            st.requested = false;
            st.scheduled = false;
            tracing::debug!("Rebuild request rejected during shutdown ({})", reason);
            return RebuildAdmission::Coalesced;
        }
        if self.rebuild_snapshot_pending.load(Ordering::Acquire) {
            tracing::debug!(
                "Rebuild request coalesced into owned generation awaiting snapshot ({})",
                reason
            );
            return RebuildAdmission::Coalesced;
        }
        st.requested = true;

        if st.in_progress {
            tracing::debug!(
                "Rebuild merge: already in progress, coalescing ({})",
                reason
            );
            return RebuildAdmission::Coalesced;
        }

        let now = Instant::now();
        if let Some(last) = st.last_started_at {
            let elapsed = now.saturating_duration_since(last);
            let cooldown =
                Duration::from_secs(self.rebuild_cooldown_secs.load(Ordering::Relaxed).max(1));
            if elapsed < cooldown {
                if st.scheduled {
                    tracing::debug!(
                        "Rebuild merge: cooldown already scheduled, coalescing ({})",
                        reason
                    );
                    return RebuildAdmission::Coalesced;
                }

                let wait = cooldown - elapsed;
                st.scheduled = true;
                return RebuildAdmission::Scheduled(wait);
            }
        }

        // 立即开始：复位合并标记。
        st.in_progress = true;
        st.requested = false;
        st.scheduled = false;
        st.last_started_at = Some(now);
        RebuildAdmission::StartNow
    }

    fn try_start_rebuild_with_cooldown(self: &Arc<Self>, reason: &'static str) -> bool {
        match self.reserve_rebuild_with_cooldown(reason) {
            RebuildAdmission::StartNow => {
                self.run_rebuild_background(reason);
                true
            }
            RebuildAdmission::Scheduled(wait) => {
                let idx = self.clone();
                std::thread::spawn(move || {
                    std::thread::sleep(wait);
                    let _ = idx.try_start_rebuild_with_cooldown("cooldown elapsed (merged)");
                });
                false
            }
            RebuildAdmission::Coalesced => false,
        }
    }

    pub(super) fn finish_rebuild(self: &Arc<Self>, new_l2: Arc<PersistentIndex>) -> bool {
        enum FinishStep {
            Complete,
            Apply(Vec<EventRecord>),
            RetryIncomplete,
            WaitForReaders,
            RetrySharedGeneration,
        }

        let _event_boundary = self.snapshot_event_gate.lock();
        let mut new_l2 = Some(new_l2);
        let mut shared_generation_waits = 0usize;
        loop {
            let step = {
                // Global lock order is snapshot_event_gate →
                // rebuild_state → delta_buffer. The dual lock makes the replay
                // completeness check and base/L2 switch indivisible.
                let mut st = self.rebuild_state.lock();
                let mut db = self.delta_buffer.lock();
                let structural_replay_unproven = db.structural_replay_unproven();
                let subtree_requires_retry = if db.has_subtree_invalidations() {
                    let prefixes = db.snapshot_subtree_invalidations();
                    new_l2
                        .as_ref()
                        .expect("rebuild generation is present")
                        .snapshot_prefixes_match_descendants(&prefixes)
                } else {
                    false
                };
                if !db.is_complete() || structural_replay_unproven || subtree_requires_retry {
                    // A path was rejected after this rebuild's start boundary.
                    // A directory rename/recreate or a subtree invalidated
                    // after it was scanned is likewise unproven. Retain
                    // delta+WAL and retry instead of publishing stale or
                    // missing descendants. Exact file deletes have no
                    // descendants and are replayed below without restarting.
                    self.l2.store(Arc::new(PersistentIndex::new_with_roots(
                        self.roots.clone(),
                    )));
                    self.invalidate_memory_report_cache();
                    st.in_progress = false;
                    st.requested = false;
                    st.scheduled = false;
                    FinishStep::RetryIncomplete
                } else {
                    db.clear_subtree_invalidations();
                    if !db.is_empty() {
                        let mut events: Vec<EventRecord> = db.live_records().cloned().collect();
                        for path_bytes in db.deleted_paths() {
                            let path = pathbuf_from_bytes(path_bytes);
                            events.push(EventRecord {
                                seq: 0,
                                timestamp: std::time::SystemTime::UNIX_EPOCH,
                                event_type: EventType::Delete,
                                id: FileIdentifier::Path(path.clone()),
                                path_hint: Some(path),
                            });
                        }
                        events.sort_by_key(|e| e.seq);
                        db.clear();
                        FinishStep::Apply(events)
                    } else {
                        // 切换点：持锁判空 -> 原子切换，避免丢事件窗口。
                        self.l1.clear();
                        let current_l2 = self.l2.load_full();
                        if Arc::ptr_eq(
                            &current_l2,
                            new_l2.as_ref().expect("rebuild generation is present"),
                        ) {
                            self.l2.store(Arc::new(PersistentIndex::new_with_roots(
                                self.roots.clone(),
                            )));
                        }
                        drop(current_l2);
                        match Arc::try_unwrap(new_l2.take().expect("rebuild generation is present"))
                        {
                            Ok(generation) => {
                                self.note_pending_flush_rebuild(&generation);
                                let generation_stats = generation.memory_stats();
                                *self.pending_snapshot_generation.lock() = Some(generation);
                                {
                                    let mut telemetry = self.owned_snapshot_telemetry.lock();
                                    telemetry.lifecycle = super::OwnedSnapshotLifecycle::Pending;
                                    telemetry.l2 = generation_stats;
                                }
                                db.finish_full_rebuild_generation();
                                self.rebuild_snapshot_pending.store(true, Ordering::Release);
                                self.invalidate_memory_report_cache();
                                if !self.flush_requested.swap(true, Ordering::AcqRel) {
                                    self.flush_notify.notify_one();
                                }
                                st.in_progress = false;
                                // A complete scan plus complete boundary replay subsumes
                                // requests coalesced while this generation was building.
                                st.requested = false;
                                st.scheduled = false;
                                FinishStep::Complete
                            }
                            Err(shared) => {
                                new_l2 = Some(shared);
                                if shared_generation_waits < 500 {
                                    FinishStep::WaitForReaders
                                } else {
                                    st.in_progress = false;
                                    st.requested = false;
                                    st.scheduled = false;
                                    FinishStep::RetrySharedGeneration
                                }
                            }
                        }
                    }
                }
            };

            match step {
                FinishStep::Complete => {
                    self.mark_rebuild_recovery_complete();
                    return false;
                }
                FinishStep::Apply(batch) => new_l2
                    .as_ref()
                    .expect("rebuild generation is present")
                    .apply_events(&batch),
                FinishStep::RetryIncomplete => {
                    tracing::warn!(
                        "rebuild replay generation incomplete or subtree-stale; retaining delta/WAL and retrying"
                    );
                    return true;
                }
                FinishStep::WaitForReaders => {
                    shared_generation_waits += 1;
                    std::thread::sleep(Duration::from_millis(10));
                }
                FinishStep::RetrySharedGeneration => {
                    tracing::warn!(
                        "rebuild generation remained shared at publication; retaining WAL and retrying"
                    );
                    return true;
                }
            }
        }
    }

    fn run_rebuild_background(self: &Arc<Self>, reason: &'static str) {
        let idx = self.clone();
        std::thread::spawn(move || {
            idx.set_current_thread_idle_io_priority_for_scan();
            let strategy = {
                let mut sched = idx.scheduler.lock();
                sched.adjust_parallelism();
                sched.select_strategy(&Task::ColdBuild {
                    total_dirs: idx.roots.len(),
                })
            };

            tracing::warn!(
                "Starting background rebuild: {} (strategy={:?})",
                reason,
                strategy
            );
            let new_l2 = Arc::new(PersistentIndex::new_with_roots(idx.roots.clone()));
            {
                let _snapshot_boundary = idx.snapshot_event_gate.lock();
                idx.delta_buffer.lock().begin_full_rebuild_generation();
                idx.l2.store(new_l2.clone());
                idx.invalidate_memory_report_cache();
            }
            idx.l3.full_build_with_strategy(&new_l2, strategy);
            let again = idx.finish_rebuild(new_l2);
            tracing::warn!("Rebuild complete, triggering manual RSS trim...");
            maybe_trim_rss();
            tracing::warn!(
                "Background rebuild complete: {} files",
                idx.base.load_full().file_count()
            );
            if again {
                let _ = idx.try_start_rebuild_with_cooldown("merged rebuild request after rebuild");
            }
        });
    }

    /// 后台全量构建
    pub fn spawn_full_build(self: &Arc<Self>) {
        if !self.try_start_rebuild_with_cooldown("full build requested") {
            tracing::debug!("Background full build request coalesced or scheduled");
        }
    }

    /// Start the one expected rebuild for an empty or untrusted startup snapshot.
    pub(crate) fn spawn_startup_full_build(self: &Arc<Self>) {
        if !self.try_start_rebuild_with_cooldown("startup bootstrap") {
            tracing::debug!("Startup bootstrap rebuild request coalesced or scheduled");
        }
    }

    /// Recover a durable snapshot generation that cannot be merged directly.
    pub(crate) fn spawn_snapshot_recovery_full_build(self: &Arc<Self>) {
        if !self.try_start_rebuild_with_cooldown("snapshot recovery") {
            tracing::debug!("Snapshot recovery rebuild request coalesced or scheduled");
        }
    }

    /// overflow 兜底：dirty region + cooldown/max-staleness 触发后执行一次 fast-sync（best-effort）。
    ///
    /// 设计目标：
    /// - 避免 "overflow → 立刻全盘 rebuild" 在风暴中触发大分配/高水位；
    /// - 允许查询短暂陈旧，但不阻塞查询、不 OOM；
    /// - fast-sync 以"目录为单位"做对齐：只需要 read_dir + 必要的 metadata，不假设 mtime 冒泡。
    pub fn spawn_fast_sync(self: &Arc<Self>, scope: DirtyScope, ignore_prefixes: Vec<PathBuf>) {
        let permit = match self.fast_sync_semaphore.clone().try_acquire_owned() {
            Ok(p) => p,
            Err(_) => {
                tracing::debug!("Fast-sync already in progress, skipping duplicate spawn");
                return;
            }
        };

        let idx = self.clone();
        std::thread::spawn(move || {
            idx.set_current_thread_idle_io_priority_for_scan();
            let _permit = permit;
            let report = idx.fast_sync(scope, &ignore_prefixes);
            tracing::warn!(
                "Fast-sync complete: dirs={} upserts={} deletes={}",
                report.dirs_scanned,
                report.upsert_events,
                report.delete_events
            );
            tracing::warn!("Fast-sync complete, triggering manual RSS trim...");
            crate::util::maybe_trim_rss_throttled();
        });
    }

    pub fn enqueue_dirty(&self, scope: DirtyScope, reason: DirtyReason) {
        self.enqueue_dirty_with_priority(scope, reason, reason.default_priority());
    }

    pub fn enqueue_dirty_dirs(&self, dirs: Vec<PathBuf>, reason: DirtyReason) {
        if dirs.is_empty() {
            return;
        }
        self.enqueue_dirty(DirtyScope::dirs(now_ns(), dirs), reason);
    }

    pub fn enqueue_recursive_dirty_dirs(&self, dirs: Vec<PathBuf>, reason: DirtyReason) {
        if dirs.is_empty() {
            return;
        }
        {
            let mut queue = self.dirty_queue.lock();
            for dir in dirs {
                queue.enqueue_recursive(
                    DirtyScope::dirs(now_ns(), vec![dir]),
                    reason,
                    reason.default_priority(),
                    now_ns(),
                );
            }
        }
        self.dirty_notify.notify_one();
    }

    pub(crate) fn enqueue_paced_recursive_dirty_dirs(
        &self,
        dirs: Vec<PathBuf>,
        reason: DirtyReason,
    ) {
        if dirs.is_empty() {
            return;
        }
        {
            let mut queue = self.dirty_queue.lock();
            for dir in dirs {
                queue.enqueue_paced_recursive(
                    DirtyScope::dirs(now_ns(), vec![dir]),
                    reason,
                    reason.default_priority(),
                    now_ns(),
                );
            }
        }
        self.dirty_notify.notify_one();
    }

    pub fn set_cold_sweep_period_estimate_from_tiered_policy(
        &self,
        l2_scan_interval_secs: u64,
        l3_scan_policy: L3ScanPolicy,
        l3_scan_interval_secs: u64,
    ) {
        let mut estimate = l2_scan_interval_secs.max(1);
        if l3_scan_policy.schedules_periodic_scan() {
            estimate = estimate.max(l3_scan_interval_secs.max(1));
        }
        self.set_cold_sweep_period_estimate(estimate);
    }

    pub fn enqueue_dirty_with_priority(
        &self,
        scope: DirtyScope,
        reason: DirtyReason,
        priority: DirtyPriority,
    ) {
        {
            let mut queue = self.dirty_queue.lock();
            queue.enqueue(scope, reason, priority, now_ns());
        }
        self.dirty_notify.notify_one();
    }

    pub fn dirty_queue_len(&self) -> usize {
        self.dirty_queue.lock().len()
    }

    pub fn deferred_repair_queue_len(&self) -> usize {
        self.dirty_queue
            .lock()
            .count_by_reason(DirtyReason::StartupRepairDeferred)
    }

    pub fn enqueue_startup_deferred_repair(&self) {
        let report = self.recovery_status().report;
        if !report.deferred_repair {
            return;
        }
        if report.deferred_unknown_scope {
            self.enqueue_dirty_dirs(self.roots.clone(), DirtyReason::StartupRepairDeferred);
        }
        if !report.deferred_dirty_dirs.is_empty() {
            self.enqueue_dirty_dirs(
                report.deferred_dirty_dirs,
                DirtyReason::StartupRepairDeferred,
            );
        }
    }

    pub fn dirty_queue_ready_batch(&self, limit: usize) -> Vec<DirtyQueueEntry> {
        self.dirty_queue.lock().pop_ready(now_ns(), limit)
    }

    /// 仅当全部 ready 项都是低优先级 paced 轮转 continuation 时取出一批。
    ///
    /// 供 dirty loop 的 blocking 线程内联续跑使用；任何更高优先级或非 paced
    /// 的 ready 项存在时返回空，交回主循环按优先级调度。
    pub fn dirty_queue_ready_paced_low_batch(&self, limit: usize) -> Vec<DirtyQueueEntry> {
        self.dirty_queue.lock().pop_ready_paced_low(now_ns(), limit)
    }

    /// Returns the precise wake delay for a paced dirty-queue continuation.
    pub fn dirty_queue_next_ready_delay(&self) -> Option<Duration> {
        self.dirty_queue.lock().next_ready_delay(now_ns())
    }

    pub fn retry_dirty_entry(&self, entry: DirtyQueueEntry) -> bool {
        let retry = {
            let mut queue = self.dirty_queue.lock();
            queue.retry(entry, now_ns())
        };
        if retry {
            self.dirty_notify.notify_one();
        }
        retry
    }

    pub async fn wait_for_dirty_queue(&self) {
        self.dirty_notify.notified().await;
    }

    pub fn process_dirty_entry(
        &self,
        entry: DirtyQueueEntry,
        ignore_prefixes: &[PathBuf],
    ) -> DirtyProcessReport {
        self.process_dirty_entry_with_project_markers(entry, ignore_prefixes, &[])
    }

    pub fn process_dirty_entry_with_project_markers(
        &self,
        entry: DirtyQueueEntry,
        ignore_prefixes: &[PathBuf],
        project_markers: &[String],
    ) -> DirtyProcessReport {
        self.process_dirty_entry_with_project_markers_and_manifest_skip_dirs(
            entry,
            ignore_prefixes,
            project_markers,
            &HashSet::new(),
        )
    }

    pub fn process_dirty_entry_with_project_markers_and_manifest_skip_dirs(
        &self,
        mut entry: DirtyQueueEntry,
        ignore_prefixes: &[PathBuf],
        project_markers: &[String],
        manifest_skip_dirs: &HashSet<PathBuf>,
    ) -> DirtyProcessReport {
        let mut report = DirtyProcessReport {
            entries_processed: 1,
            ..DirtyProcessReport::default()
        };

        match &entry.scope {
            DirtyScope::All { .. } => {
                let sync = self.fast_sync(entry.scope.clone(), ignore_prefixes);
                report.dirs_scanned = sync.dirs_scanned;
                report.fast_sync_upserts = sync.upsert_events;
                report.fast_sync_deletes = sync.delete_events;
                report.changed = sync.upsert_events.saturating_add(sync.delete_events);
                return report;
            }
            DirtyScope::Dirs { dirs, .. } => {
                let recursive_subtree_repair = entry.requires_recursive_subtree_repair();
                if entry.reason == DirtyReason::StartupRepairDeferred && !recursive_subtree_repair {
                    let sync = self.fast_sync(entry.scope.clone(), ignore_prefixes);
                    report.dirs_scanned = sync.dirs_scanned;
                    report.fast_sync_upserts = sync.upsert_events;
                    report.fast_sync_deletes = sync.delete_events;
                    report.changed = sync.upsert_events.saturating_add(sync.delete_events);
                    return report;
                }
                if entry.reason == DirtyReason::FastScanChangedDir && !recursive_subtree_repair {
                    let sync = self.fast_sync(entry.scope.clone(), ignore_prefixes);
                    report.dirs_scanned = sync.dirs_scanned;
                    report.fast_sync_upserts = sync.upsert_events;
                    report.fast_sync_deletes = sync.delete_events;
                    report.changed = sync.upsert_events.saturating_add(sync.delete_events);
                    for dir in entry.scope.dir_paths() {
                        report.outcomes.push(DirtyScanOutcome {
                            dir: dir.clone(),
                            outcome: ScanOutcome {
                                scanned: sync.dirs_scanned,
                                changed: report.changed,
                                elapsed_ms: 0,
                                project_roots: Vec::new(),
                            },
                            reason: entry.reason,
                            manifest_skipped: false,
                            completion_ready: true,
                        });
                    }
                    return report;
                }
                let scan_dirs = if recursive_subtree_repair {
                    entry
                        .repair_cursor
                        .as_ref()
                        .map(|cursor| vec![cursor.dir.clone()])
                        .unwrap_or_else(|| dirs.clone())
                } else {
                    dirs.clone()
                };
                let mut had_failed_dir = false;
                for dir in &scan_dirs {
                    if should_skip_dirty_dir(dir, ignore_prefixes, &self.exclude_dirs) {
                        continue;
                    }
                    match std::fs::symlink_metadata(dir) {
                        Ok(meta) if meta.is_dir() => {
                            let allow_manifest_skip = entry.reason.is_cold_scan()
                                && !recursive_subtree_repair
                                && manifest_skip_dirs.contains(dir);
                            let discard_if_event_seq_advances = matches!(
                                entry.reason,
                                DirtyReason::PeriodicColdScan
                                    | DirtyReason::RotatingColdWindow { .. }
                                    | DirtyReason::StartupRepairDeferred
                                    | DirtyReason::FastScanBootstrapDir
                                    | DirtyReason::FastScanChangedDir
                            ) || entry
                                .requires_recursive_subtree_repair();
                            let (
                                outcome,
                                manifest_skipped,
                                dropped_stale_batch,
                                completion_ready,
                                scan_failed,
                                completed_recursive_outcome,
                            ) = if entry.reason.is_cold_scan() || recursive_subtree_repair {
                                let mut sliced = self.scan_dir_repair_slice_with_project_markers(
                                    dir,
                                    entry.repair_cursor.as_ref(),
                                    ignore_prefixes,
                                    project_markers,
                                    allow_manifest_skip,
                                    discard_if_event_seq_advances,
                                    recursive_subtree_repair,
                                );
                                let continuation_state = repair_continuation_cursor(
                                    &mut entry,
                                    &mut sliced,
                                    recursive_subtree_repair,
                                );
                                let continuation = continuation_state.cursor;
                                let mut completion_ready = sliced.completed
                                    && !sliced.dropped_stale_batch
                                    && !sliced.failed
                                    && continuation.is_none();
                                let mut scan_failed = sliced.failed;
                                if completion_ready && recursive_subtree_repair {
                                    let proof_root = entry.scope.dir_paths()[0].as_path();
                                    let proof_recorded = continuation_state
                                        .completion_stamps
                                        .as_deref()
                                        .is_some_and(|stamps| {
                                            self.record_complete_subtree_scan(
                                                proof_root,
                                                stamps,
                                                continuation_state.invalidation_epoch.expect(
                                                    "recursive completion tracks invalidation epoch",
                                                ),
                                            )
                                        });
                                    if !proof_recorded {
                                        completion_ready = false;
                                        scan_failed = true;
                                    }
                                }
                                if let Some(cursor) = continuation {
                                    let continuation_scope = if recursive_subtree_repair {
                                        entry.scope.dir_paths()[0].clone()
                                    } else {
                                        dir.clone()
                                    };
                                    self.enqueue_dirty_repair_slice(
                                        continuation_scope,
                                        &entry,
                                        cursor,
                                    );
                                }
                                if completion_ready && entry.reason.is_cold_scan() {
                                    self.mark_cold_sweep_completed();
                                }
                                (
                                    sliced.outcome,
                                    sliced.manifest_skipped,
                                    sliced.dropped_stale_batch,
                                    completion_ready,
                                    scan_failed,
                                    continuation_state.completed_outcome,
                                )
                            } else {
                                let scanned = self
                                    .scan_dirs_with_depth_and_project_markers_budgeted(
                                        &[dir],
                                        Some(1),
                                        10_000,
                                        project_markers,
                                        None,
                                        discard_if_event_seq_advances,
                                    );
                                (
                                    scanned.outcome,
                                    false,
                                    scanned.dropped_stale_batch,
                                    !scanned.dropped_stale_batch,
                                    false,
                                    None,
                                )
                            };
                            had_failed_dir |= scan_failed;
                            if dropped_stale_batch {
                                report.dropped_stale_batches =
                                    report.dropped_stale_batches.saturating_add(1);
                            }
                            report.dirs_scanned = report.dirs_scanned.saturating_add(1);
                            report.changed = report.changed.saturating_add(outcome.changed);
                            report.elapsed_ms =
                                report.elapsed_ms.saturating_add(outcome.elapsed_ms);
                            if !recursive_subtree_repair || completion_ready {
                                let published_dir = if recursive_subtree_repair {
                                    entry.scope.dir_paths()[0].clone()
                                } else {
                                    dir.clone()
                                };
                                let published_outcome = if recursive_subtree_repair {
                                    completed_recursive_outcome.expect(
                                        "completed recursive repair publishes accumulated outcome",
                                    )
                                } else {
                                    outcome
                                };
                                report.outcomes.push(DirtyScanOutcome {
                                    dir: published_dir,
                                    outcome: published_outcome,
                                    reason: entry.reason,
                                    manifest_skipped,
                                    completion_ready,
                                });
                            }
                        }
                        Ok(_) => {
                            had_failed_dir = true;
                        }
                        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                            // Scan work can race workload cleanup. Once a cold or
                            // fast-scan target is gone there is no scan result to
                            // publish, and retrying cannot restore that target.
                            if !matches!(
                                entry.reason,
                                DirtyReason::InotifyEvent
                                    | DirtyReason::FastScanBootstrapDir
                                    | DirtyReason::FastScanChangedDir
                                    | DirtyReason::PeriodicColdScan
                                    | DirtyReason::RotatingColdWindow { .. }
                            ) {
                                had_failed_dir = true;
                            }
                        }
                        Err(e) => {
                            tracing::debug!(
                                "dirty queue skipped unreadable dir {}: {}",
                                dir.display(),
                                e
                            );
                            had_failed_dir = true;
                        }
                    }
                }
                report.failed = had_failed_dir;
                if recursive_subtree_repair && (had_failed_dir || report.dropped_stale_batches > 0)
                {
                    report.failed = true;
                }
            }
        }

        report
    }

    fn record_complete_subtree_scan(
        &self,
        root: &Path,
        stamps: &[DirectoryStamp],
        expected_invalidation_epoch: u64,
    ) -> bool {
        let _event_boundary = self.snapshot_event_gate.lock();
        if self.delta_buffer.lock().invalidation_epoch() != expected_invalidation_epoch {
            return false;
        }
        if !recursive_completion_stable(stamps) {
            return false;
        }
        self.delta_buffer
            .lock()
            .note_complete_subtree_scan(root.as_os_str().as_encoded_bytes().to_vec());
        true
    }

    fn enqueue_dirty_repair_slice(
        &self,
        dir: PathBuf,
        source: &DirtyQueueEntry,
        cursor: DirtyRepairCursor,
    ) {
        {
            let mut queue = self.dirty_queue.lock();
            queue.enqueue_repair_slice(
                DirtyScope::dirs(now_ns(), vec![dir]),
                source,
                now_ns(),
                cursor,
            );
        }
        self.dirty_notify.notify_one();
    }

    pub(crate) fn fast_sync(
        &self,
        scope: DirtyScope,
        ignore_prefixes: &[PathBuf],
    ) -> FastSyncReport {
        use std::collections::HashSet;

        self.observe_clock_boundary();

        let mut report = FastSyncReport::default();
        let io_governor = self.io_governor.as_ref();

        // 1) 计算需要对齐的目录集合
        let mut dirs: Vec<PathBuf> = match scope {
            DirtyScope::All { cutoff_ns } => {
                let cutoff_ns = self.clock_cutoff_for_dirty(cutoff_ns);
                collect_dirs_changed_since(
                    &self.roots,
                    ignore_prefixes,
                    &self.exclude_dirs,
                    cutoff_ns,
                    io_governor,
                )
            }
            DirtyScope::Dirs { dirs, cutoff_ns } => {
                let root_set: HashSet<_> = self.roots.iter().cloned().collect();
                let (root_dirs, leaf_dirs): (Vec<_>, Vec<_>) =
                    dirs.into_iter().partition(|d| root_set.contains(d));

                let effective_cutoff_ns = self
                    .clock_cutoff_for_dirty(cutoff_ns)
                    .saturating_sub(10_000_000_000);
                let mut out = if !root_dirs.is_empty() {
                    collect_dirs_changed_since(
                        &root_dirs,
                        ignore_prefixes,
                        &self.exclude_dirs,
                        effective_cutoff_ns,
                        io_governor,
                    )
                } else {
                    Vec::new()
                };
                out.extend(leaf_dirs);
                out.sort();
                out.dedup();
                out
            }
        };

        // 过滤：忽略 self-write 目录/不存在目录
        dirs.retain(|d| {
            if ignore_prefixes
                .iter()
                .any(|ig| !ig.as_os_str().is_empty() && d.starts_with(ig))
                || path_has_excluded_component(d, &self.exclude_dirs)
            {
                return false;
            }
            io_governor.before_io();
            std::fs::symlink_metadata(d)
                .map(|m| m.is_dir())
                .unwrap_or(false)
        });
        dirs.sort();
        dirs.dedup();

        if dirs.is_empty() {
            self.stats.record_fast_sync();
            return report;
        }

        // 2) 扫描目录：生成 upsert events。
        //
        // 量化权衡（2026-06-20 实测）：
        // - 旧实现：30 万 stat × 50-200μs = 15-60s
        // - 新实现：1 readdir + 30 万 HashSet 查找（纳秒级）= 1-100ms
        // 30 万文件目录的 HashSet<OsString> 峰值约 20-30MB，远小于 760MB 的索引 RSS。
        let mut upsert_events: Vec<EventRecord> = Vec::with_capacity(2048);
        let mut upsert_metas: Vec<FileMeta> = Vec::with_capacity(2048);
        let mut seq: u64 = 0;

        for dir in dirs.iter() {
            report.dirs_scanned += 1;
            let mut builder = ignore::WalkBuilder::new(dir);
            builder
                .max_depth(Some(1))
                .hidden(!self.include_hidden)
                .follow_links(false)
                .ignore(self.ignore_enabled)
                .git_ignore(self.ignore_enabled)
                .git_global(self.ignore_enabled)
                .git_exclude(self.ignore_enabled);
            let fs_policy = crate::fs_policy::FsPolicy::current_with_shared_config(
                self.shared_fs_policy_config(),
            );
            let root = dir.clone();
            let exclude_dirs = self.exclude_dirs.clone();
            let mount_policy_counters = self.mount_policy_counters();
            builder.filter_entry(move |entry| {
                (exclude_dirs.is_empty()
                    || !path_has_excluded_component(entry.path(), &exclude_dirs))
                    && fs_policy
                        .as_ref()
                        .map(|policy| {
                            policy
                                .check_path_counted(
                                    entry.path(),
                                    Some(root.as_path()),
                                    mount_policy_counters.as_ref(),
                                )
                                .is_allowed()
                        })
                        .unwrap_or(true)
            });

            for ent in builder.build() {
                let ent = match ent {
                    Ok(e) => e,
                    Err(err) => {
                        tracing::warn!(
                            "fast-sync walker skipped entry under {}: {}",
                            dir.display(),
                            err
                        );
                        continue;
                    }
                };
                let Some(ft) = ent.file_type() else {
                    continue;
                };
                if !ft.is_file() && !ft.is_dir() {
                    continue;
                }
                if ft.is_dir() && ent.path() == dir.as_path() {
                    continue;
                }

                let path = super::normalize_path(ent.path());
                io_governor.before_io();
                let meta = match ent.metadata() {
                    Ok(meta) => meta,
                    Err(err) => {
                        tracing::warn!("fast-sync metadata failed for {}: {}", path.display(), err);
                        continue;
                    }
                };
                let Some(file_key) = FileKey::from_path_and_metadata(&path, &meta) else {
                    continue;
                };
                let mtime = meta.modified().ok();
                let mtime_ns = mtime_to_ns(mtime);
                let kind = FileKind::from_metadata(&meta);
                if self.path_freshness(&path, file_key, mtime_ns, kind) == PathFreshness::Unchanged
                {
                    continue;
                }
                seq = seq.wrapping_add(1);
                upsert_metas.push(FileMeta {
                    file_key,
                    path: path.clone(),
                    size: meta.len(),
                    mtime,
                    ctime: meta.created().ok(),
                    atime: meta.accessed().ok(),
                    kind,
                });
                upsert_events.push(EventRecord {
                    seq,
                    timestamp: std::time::SystemTime::now(),
                    event_type: EventType::Modify,
                    id: FileIdentifier::Path(path),
                    path_hint: None,
                });
                report.upsert_events += 1;
            }

            if upsert_events.len() >= 2048 {
                self.apply_upserted_metas_inner(upsert_events.as_slice(), &mut upsert_metas, true);
                upsert_events.clear();
            }
        }
        if !upsert_events.is_empty() {
            self.apply_upserted_metas_inner(upsert_events.as_slice(), &mut upsert_metas, true);
            upsert_events.clear();
        }

        let dirty_dirs: HashSet<PathBuf> = dirs.into_iter().collect();

        // 3) 删除对齐：对齐"被标记 dirty 的目录"下的条目。按 parent dir 分组，
        //    每个 dirty 目录做一次 readdir 构建 HashSet<OsString>，与索引文件名做差集，
        //    避免逐文件 stat() 风暴。HashSet 在每个目录处理完后立即 drop，不跨目录累积。
        let mut delete_events: Vec<EventRecord> = Vec::new();

        let base = self.base.load_full();
        let to_delete = if base.file_count() > 0 {
            base.delete_alignment_with_parent_index(&dirty_dirs)
        } else if !self.rebuild_in_progress() {
            let mut l2_doc_id = 0u64;
            let mut candidates = Vec::new();
            self.l2.load_full().for_each_live_meta(|meta| {
                if meta
                    .path
                    .parent()
                    .is_some_and(|parent| dirty_dirs.contains(parent))
                {
                    candidates.push((l2_doc_id, meta.path));
                    l2_doc_id = l2_doc_id.saturating_add(1);
                }
            });
            candidates
        } else {
            Vec::new()
        };

        // v2: 按 parent dir 分组，readdir + HashSet 差集检测删除（见 `readdir_delete_alignment`）。
        let deleted_paths = readdir_delete_alignment(to_delete, Some(io_governor));
        for path in deleted_paths {
            seq = seq.wrapping_add(1);
            delete_events.push(EventRecord {
                seq,
                timestamp: std::time::SystemTime::now(),
                event_type: EventType::Delete,
                id: FileIdentifier::Path(path),
                path_hint: None,
            });
        }

        report.delete_events = delete_events.len();
        for chunk in delete_events.chunks(2048) {
            self.apply_events(chunk);
        }

        self.mark_clock_reconciled();
        self.stats.record_fast_sync();
        report
    }

    fn scan_dirs_with_depth(
        &self,
        dirs: &[&PathBuf],
        max_depth: Option<usize>,
        max_entries_per_dir: usize,
    ) -> ScanOutcome {
        self.scan_dirs_with_depth_and_project_markers(dirs, max_depth, max_entries_per_dir, &[])
    }

    fn scan_dirs_with_depth_and_project_markers(
        &self,
        dirs: &[&PathBuf],
        max_depth: Option<usize>,
        max_entries_per_dir: usize,
        project_markers: &[String],
    ) -> ScanOutcome {
        self.scan_dirs_with_depth_and_project_markers_budgeted(
            dirs,
            max_depth,
            max_entries_per_dir,
            project_markers,
            None,
            false,
        )
        .outcome
    }

    fn scan_dirs_with_depth_and_project_markers_budgeted(
        &self,
        dirs: &[&PathBuf],
        max_depth: Option<usize>,
        max_entries_per_dir: usize,
        project_markers: &[String],
        budget_ms: Option<u64>,
        discard_if_event_seq_advances: bool,
    ) -> BudgetedScanOutcome {
        let start = Instant::now();
        let scan_started_seq = self.event_seq.load(Ordering::Relaxed);
        let invalidations =
            discard_if_event_seq_advances.then(|| self.delta_buffer.lock().invalidation_snapshot());

        let mut upsert_events: Vec<EventRecord> = Vec::new();
        let mut upsert_metas: Vec<FileMeta> = Vec::new();
        let mut scanned: usize = 0;
        let mut changed: usize = 0;
        let mut budget_exhausted = false;
        let mut seq: u64 = 0;
        let io_governor = self.io_governor.as_ref();

        let mut project_roots = Vec::new();
        let hidden_markers_enabled = project_markers.iter().any(|marker| marker.starts_with('.'));

        for dir in dirs {
            let mut dir_count = 0;
            let mut builder = ignore::WalkBuilder::new(dir);
            if let Some(d) = max_depth {
                builder.max_depth(Some(d));
            }
            builder
                .hidden(!self.include_hidden && !hidden_markers_enabled)
                .follow_links(false)
                .ignore(self.ignore_enabled)
                .git_ignore(self.ignore_enabled)
                .git_global(self.ignore_enabled)
                .git_exclude(self.ignore_enabled);
            let fs_policy = crate::fs_policy::FsPolicy::current_with_shared_config(
                self.shared_fs_policy_config(),
            );
            let root = (*dir).clone();
            let exclude_dirs = self.exclude_dirs.clone();
            let mount_policy_counters = self.mount_policy_counters();
            let include_hidden = self.include_hidden;
            let project_markers_filter = project_markers.to_vec();
            builder.filter_entry(move |entry| {
                (exclude_dirs.is_empty()
                    || !path_has_excluded_component(entry.path(), &exclude_dirs))
                    && (include_hidden
                        || !hidden_markers_enabled
                        || !path_has_hidden_component_after_root(entry.path(), root.as_path())
                        || project_root_for_marker(entry.path(), &project_markers_filter).is_some())
                    && fs_policy
                        .as_ref()
                        .map(|policy| {
                            policy
                                .check_path_counted(
                                    entry.path(),
                                    Some(root.as_path()),
                                    mount_policy_counters.as_ref(),
                                )
                                .is_allowed()
                        })
                        .unwrap_or(true)
            });
            for ent in builder.build() {
                if budget_ms
                    .filter(|budget| *budget > 0)
                    .is_some_and(|budget| start.elapsed().as_millis() as u64 >= budget)
                {
                    budget_exhausted = true;
                    break;
                }
                let ent = match ent {
                    Ok(e) => e,
                    Err(err) => {
                        tracing::warn!(
                            "scan_dirs_immediate walker skipped entry under {}: {}",
                            dir.display(),
                            err
                        );
                        continue;
                    }
                };
                let Some(ft) = ent.file_type() else {
                    continue;
                };
                if !ft.is_file() && !ft.is_dir() {
                    continue;
                }
                if ft.is_dir() && ent.path() == dir.as_path() {
                    continue;
                }
                if dir_count >= max_entries_per_dir {
                    break;
                }
                dir_count += 1;

                let path = super::normalize_path(ent.path());
                if let Some(project_root) = project_root_for_marker(path.as_path(), project_markers)
                {
                    project_roots.push(project_root);
                }
                if !self.include_hidden && path_has_hidden_component_after_root(ent.path(), dir) {
                    continue;
                }
                io_governor.before_io();
                let meta = match ent.metadata() {
                    Ok(m) => m,
                    Err(err) => {
                        tracing::warn!(
                            "scan_dirs_immediate metadata failed for {}: {}",
                            path.display(),
                            err
                        );
                        continue;
                    }
                };
                let Some(file_key) = FileKey::from_path_and_metadata(&path, &meta) else {
                    continue;
                };
                let mtime = meta.modified().ok();
                let mtime_ns = mtime_to_ns(mtime);
                let kind = FileKind::from_metadata(&meta);
                scanned += 1;
                let freshness = invalidations.as_ref().map_or_else(
                    || self.path_freshness(&path, file_key, mtime_ns, kind),
                    |snapshot| {
                        self.path_freshness_from_snapshot(&path, file_key, mtime_ns, kind, snapshot)
                    },
                );
                if freshness == PathFreshness::Unchanged {
                    continue;
                }
                changed += 1;
                seq = seq.wrapping_add(1);
                upsert_metas.push(FileMeta {
                    file_key,
                    path: path.clone(),
                    size: meta.len(),
                    mtime,
                    ctime: meta.created().ok(),
                    atime: meta.accessed().ok(),
                    kind,
                });
                upsert_events.push(EventRecord {
                    seq,
                    timestamp: std::time::SystemTime::now(),
                    event_type: EventType::Modify,
                    id: FileIdentifier::Path(path),
                    path_hint: None,
                });
            }
            if budget_exhausted {
                break;
            }
        }

        let applied = if let Some(snapshot) = invalidations.as_ref() {
            self.apply_upserted_metas_if_scan_snapshot(
                upsert_events.as_slice(),
                &mut upsert_metas,
                true,
                scan_started_seq,
                snapshot.epoch(),
            )
        } else {
            if !upsert_events.is_empty() {
                self.apply_upserted_metas_inner(upsert_events.as_slice(), &mut upsert_metas, true);
            }
            Some(self.event_seq.load(Ordering::Relaxed))
        };
        let stale_low_priority_scan = applied.is_none();
        if stale_low_priority_scan {
            tracing::debug!(
                "discarded stale low-priority scan result after its scan snapshot advanced"
            );
            changed = 0;
        } else {
            self.update_directory_manifests_for_dirs(dirs, project_markers);
        }

        let elapsed_ms = start.elapsed().as_millis() as u64;
        project_roots.sort();
        project_roots.dedup();

        BudgetedScanOutcome {
            outcome: ScanOutcome {
                scanned,
                changed,
                elapsed_ms,
                project_roots,
            },
            budget_exhausted,
            dropped_stale_batch: stale_low_priority_scan,
        }
    }

    pub(super) fn scan_dir_repair_slice_with_project_markers(
        &self,
        dir: &PathBuf,
        cursor: Option<&DirtyRepairCursor>,
        ignore_prefixes: &[PathBuf],
        project_markers: &[String],
        allow_manifest_skip: bool,
        discard_if_event_seq_advances: bool,
        force_scan: bool,
    ) -> SlicedScanOutcome {
        let dir_start_stamp = cursor
            .and_then(DirtyRepairCursor::current_dir_start_stamp)
            .or_else(|| {
                std::fs::symlink_metadata(dir)
                    .ok()
                    .filter(|meta| meta.is_dir())
                    .and_then(|meta| directory_read_fingerprint(dir, &meta))
            });

        // summary 去重状态：首次扫描没有已存 manifest，任何 summary 都不可能
        // 命中；可靠 unbounded summary 已证明 mismatch 后，bounded summary 也只会
        // 重复遍历，直接进入实际 repair。stat 错误时仍保留 bounded 兜底。
        let mut try_unbounded_summary = false;
        let mut try_bounded_summary = allow_manifest_skip && cursor.is_none();

        // ===== 段 1：目录 mtime 预检（Phase 1 新增）=====
        // 对所有 PeriodicColdScan 目录生效，独立于 allow_manifest_skip，
        // 在 manifest skip（段 2）之前执行——廉价优先（1 stat vs ≤512 stat）。
        if cursor.is_none() && !force_scan {
            if self.clock_cutoff_trusted() {
                match std::fs::symlink_metadata(dir) {
                    Ok(dir_meta) => match dir_meta.modified() {
                        Ok(dir_modified) => {
                            let current_mtime_ns = mtime_to_ns(Some(dir_modified));
                            match self
                                .directory_manifests
                                .try_mtime_precheck(dir.as_path(), current_mtime_ns)
                            {
                                None => {
                                    // 首次扫描，无 manifest 记录
                                    self.directory_manifests
                                        .mtime_precheck_no_record
                                        .fetch_add(1, Ordering::Relaxed);
                                    try_bounded_summary = false;
                                }
                                Some(true) => {
                                    // 目录 mtime 未变，跳过整个扫描
                                    self.directory_manifests
                                        .mtime_precheck_hits
                                        .fetch_add(1, Ordering::Relaxed);
                                    return SlicedScanOutcome {
                                        outcome: ScanOutcome {
                                            elapsed_ms: 0,
                                            ..ScanOutcome::default()
                                        },
                                        manifest_skipped: true,
                                        completed: true,
                                        next_cursor: None,
                                        child_dirs: Vec::new(),
                                        dropped_stale_batch: false,
                                        failed: false,
                                        dir_start_stamp,
                                        invalidation_epoch: None,
                                    };
                                }
                                Some(false) => {
                                    // 目录 mtime 变了，继续走后续逻辑
                                    self.directory_manifests
                                        .mtime_precheck_misses
                                        .fetch_add(1, Ordering::Relaxed);
                                    try_unbounded_summary = true;
                                }
                            }
                        }
                        Err(_) => {
                            self.directory_manifests
                                .mtime_precheck_stat_errors
                                .fetch_add(1, Ordering::Relaxed);
                        }
                    },
                    Err(_) => {
                        self.directory_manifests
                            .mtime_precheck_stat_errors
                            .fetch_add(1, Ordering::Relaxed);
                    }
                }
            } else {
                // clock 不可信时不走预检，与 should_skip 行为一致
                self.directory_manifests
                    .mtime_precheck_untrusted_clock
                    .fetch_add(1, Ordering::Relaxed);
                if allow_manifest_skip {
                    self.directory_manifests.record_untrusted_clock_bypass();
                }
                try_bounded_summary = false;
            }
        }

        // ===== 段 1.5：unbounded summary 二次确认（Phase 3 新增）=====
        // 仅在 mtime 预检确认“已有 manifest 且目录 mtime 已变”时执行；首次
        // 扫描无可命中的记录，直接进入 repair，避免两次无效 summary 遍历。
        // 计算不受 512 条限制的 summary（WalkBuilder 过滤），与已存储的 manifest 比对。
        // 匹配则跳过（可能是 touch/atime 导致 mtime 变了但内容没变），不匹配才继续。
        // 对大目录（>512 条）尤其重要：段 2 的 bounded summary 对大目录永远 complete=false，
        // 而此处用 unbounded summary 可以覆盖大目录的 manifest 跳过。
        if try_unbounded_summary {
            if let Some(current_summary) =
                self.directory_manifest_summary_with_limit(dir, project_markers, None)
            {
                if self
                    .directory_manifests
                    .stored_matches_summary(dir.as_path(), &current_summary)
                {
                    self.directory_manifests
                        .unbounded_summary_hits
                        .fetch_add(1, Ordering::Relaxed);
                    return SlicedScanOutcome {
                        outcome: ScanOutcome {
                            elapsed_ms: 0,
                            ..ScanOutcome::default()
                        },
                        manifest_skipped: true,
                        completed: true,
                        next_cursor: None,
                        child_dirs: Vec::new(),
                        dropped_stale_batch: false,
                        failed: false,
                        dir_start_stamp,
                        invalidation_epoch: None,
                    };
                }
                self.directory_manifests
                    .unbounded_summary_misses
                    .fetch_add(1, Ordering::Relaxed);
                self.directory_manifests.record_changed_scan();
                try_bounded_summary = false;
            }
        }

        // ===== 段 2：现有 manifest skip（仅 L2/L3 PeriodicColdScan 生效）=====
        if try_bounded_summary {
            if let Some((summary, complete)) = self.directory_manifest_summary_bounded(
                dir,
                project_markers,
                REPAIR_SLICE_MAX_ENTRIES,
            ) {
                if complete {
                    let trusted = self.clock_cutoff_trusted();
                    if self
                        .directory_manifests
                        .should_skip(dir.as_path(), &summary, trusted)
                    {
                        self.directory_manifests.update(
                            dir.clone(),
                            summary,
                            self.event_seq.load(Ordering::Relaxed),
                        );
                        return SlicedScanOutcome {
                            outcome: ScanOutcome {
                                elapsed_ms: 0,
                                ..ScanOutcome::default()
                            },
                            manifest_skipped: true,
                            completed: true,
                            next_cursor: None,
                            child_dirs: Vec::new(),
                            dropped_stale_batch: false,
                            failed: false,
                            dir_start_stamp,
                            invalidation_epoch: None,
                        };
                    }
                }
            }
        }

        let start = Instant::now();
        let scan_started_seq = self.event_seq.load(Ordering::Relaxed);
        let invalidations =
            discard_if_event_seq_advances.then(|| self.delta_buffer.lock().invalidation_snapshot());
        let invalidation_epoch = invalidations.as_ref().map(|snapshot| {
            cursor
                .and_then(DirtyRepairCursor::scan_invalidation_epoch)
                .unwrap_or_else(|| snapshot.epoch())
        });
        if invalidations
            .as_ref()
            .zip(invalidation_epoch)
            .is_some_and(|(snapshot, expected)| snapshot.epoch() != expected)
        {
            return SlicedScanOutcome {
                outcome: ScanOutcome::default(),
                manifest_skipped: false,
                completed: false,
                next_cursor: None,
                child_dirs: Vec::new(),
                dropped_stale_batch: true,
                failed: false,
                dir_start_stamp,
                invalidation_epoch,
            };
        }
        let start_offset = cursor
            .filter(|cursor| cursor.dir == *dir)
            .map(|cursor| cursor.offset.max(0))
            .unwrap_or(0);
        let (mut upsert_events, mut upsert_metas, mut scanned_manifest, mut dir_children) =
            take_repair_slice_scratch();
        let slice = match read_dir_slice(
            dir,
            start_offset,
            REPAIR_SLICE_MAX_ENTRIES,
            Duration::from_millis(REPAIR_SLICE_MAX_MS),
            &mut dir_children,
        ) {
            Ok(slice) => slice,
            Err(err) => {
                tracing::debug!(
                    "repair slice skipped unreadable dir {}: {}",
                    dir.display(),
                    err
                );
                return_repair_slice_scratch(
                    upsert_events,
                    upsert_metas,
                    scanned_manifest,
                    dir_children,
                );
                return SlicedScanOutcome {
                    outcome: ScanOutcome::default(),
                    manifest_skipped: false,
                    completed: true,
                    next_cursor: None,
                    child_dirs: Vec::new(),
                    dropped_stale_batch: false,
                    failed: true,
                    dir_start_stamp,
                    invalidation_epoch,
                };
            }
        };

        upsert_events.reserve(dir_children.len());
        upsert_metas.reserve(dir_children.len());
        let mut project_roots = Vec::new();
        let mut child_dirs = Vec::new();
        let mut scanned = 0usize;
        let mut changed = 0usize;
        let mut metadata_failed = false;
        let mut seq = 0u64;
        let collect_scanned_manifest = start_offset == 0 && slice.completed;
        let hidden_markers_enabled = project_markers.iter().any(|marker| marker.starts_with('.'));
        // 每切片构建一次 FsPolicy 并复用计数器 Arc，避免每 entry 重复
        // 克隆配置 Vec/PathBuf 与 Arc。挂载表在单个最多 512 条目的切片
        // 处理期间保持快照一致。
        let slice_fs_policy = FsPolicy::current_with_shared_config(self.shared_fs_policy_config());
        let slice_mount_policy_counters = self.mount_policy_counters();

        for child in dir_children.active() {
            let path = super::normalize_path(child.path.as_path());
            if should_skip_dirty_dir(path.as_path(), ignore_prefixes, &self.exclude_dirs) {
                continue;
            }
            self.io_governor.before_io();
            let meta = match std::fs::symlink_metadata(&path) {
                Ok(meta) => meta,
                Err(err) => {
                    tracing::debug!(
                        "repair slice metadata failed for {}: {}",
                        path.display(),
                        err
                    );
                    metadata_failed = true;
                    continue;
                }
            };
            if !meta.is_file() && !meta.is_dir() {
                continue;
            }
            if !self.repair_slice_path_allowed(
                dir.as_path(),
                path.as_path(),
                &meta,
                project_markers,
                hidden_markers_enabled,
                slice_fs_policy.as_ref(),
                slice_mount_policy_counters.as_ref(),
            ) {
                continue;
            }
            if force_scan && meta.is_dir() {
                child_dirs.push(path.clone());
            }
            if let Some(project_root) = project_root_for_marker(path.as_path(), project_markers) {
                project_roots.push(project_root);
            }

            let Some(file_key) = FileKey::from_path_and_metadata(&path, &meta) else {
                continue;
            };
            let mtime = meta.modified().ok();
            let mtime_ns = mtime_to_ns(mtime);
            let kind = FileKind::from_metadata(&meta);
            if collect_scanned_manifest {
                scanned_manifest.push_child(path.as_path(), kind, mtime_ns);
            }
            scanned = scanned.saturating_add(1);
            let freshness = invalidations.as_ref().map_or_else(
                || self.path_freshness(&path, file_key, mtime_ns, kind),
                |snapshot| {
                    self.path_freshness_from_snapshot(&path, file_key, mtime_ns, kind, snapshot)
                },
            );
            if freshness == PathFreshness::Unchanged {
                continue;
            }
            changed = changed.saturating_add(1);
            seq = seq.wrapping_add(1);
            upsert_metas.push(FileMeta {
                file_key,
                path: path.clone(),
                size: meta.len(),
                mtime,
                ctime: meta.created().ok(),
                atime: meta.accessed().ok(),
                kind,
            });
            upsert_events.push(EventRecord {
                seq,
                timestamp: std::time::SystemTime::now(),
                event_type: EventType::Modify,
                id: FileIdentifier::Path(path),
                path_hint: None,
            });
        }

        #[cfg(test)]
        run_repair_slice_pre_apply_test_hook(dir);

        let mut dropped_stale_batch = false;
        let mut alignment_started_seq = None;
        if metadata_failed {
            tracing::debug!("discarded incomplete repair slice after metadata failure");
            changed = 0;
        } else if discard_if_event_seq_advances {
            alignment_started_seq = self.apply_upserted_metas_if_scan_snapshot(
                upsert_events.as_slice(),
                &mut upsert_metas,
                true,
                scan_started_seq,
                invalidation_epoch.expect("low-priority scan tracks invalidation epoch"),
            );
            if alignment_started_seq.is_none() {
                dropped_stale_batch = true;
                changed = 0;
            }
        } else {
            if !upsert_events.is_empty() {
                self.apply_upserted_metas_inner(upsert_events.as_slice(), &mut upsert_metas, true);
            }
            alignment_started_seq = Some(self.event_seq.load(Ordering::Relaxed));
        }
        let completed = slice.completed;
        if completed && !dropped_stale_batch && !metadata_failed {
            let (deleted, dropped_stale) = self.align_missing_indexed_direct_children(
                dir,
                alignment_started_seq.expect("trusted repair slice apply sequence"),
            );
            changed = changed.saturating_add(deleted);
            dropped_stale_batch |= dropped_stale;
        }
        if completed && !dropped_stale_batch && !metadata_failed {
            let summary = if collect_scanned_manifest {
                Some(scanned_manifest.finish())
            } else {
                self.directory_manifest_summary_bounded(
                    dir,
                    project_markers,
                    REPAIR_SLICE_MAX_ENTRIES,
                )
                .and_then(|(summary, complete)| complete.then_some(summary))
            };
            if let Some(summary) = summary {
                self.directory_manifests.update(
                    dir.clone(),
                    summary,
                    self.event_seq.load(Ordering::Relaxed),
                );
            }
            // Phase 1 新增：记录目录 mtime（仅非 stale 路径，仅 last slice）
            // stale scan 的中间状态不应被记录，否则会延长陈旧窗口。
            if let Ok(dir_meta) = std::fs::symlink_metadata(dir) {
                if let Ok(dir_modified) = dir_meta.modified() {
                    self.directory_manifests
                        .record_dir_mtime(dir.as_path(), mtime_to_ns(Some(dir_modified)));
                }
            }
        }

        return_repair_slice_scratch(upsert_events, upsert_metas, scanned_manifest, dir_children);

        project_roots.sort();
        project_roots.dedup();
        SlicedScanOutcome {
            outcome: ScanOutcome {
                scanned,
                changed,
                elapsed_ms: start.elapsed().as_millis() as u64,
                project_roots,
            },
            manifest_skipped: false,
            completed,
            next_cursor: if completed {
                None
            } else {
                let offset = slice.next_offset.unwrap_or_else(|| {
                    start_offset.saturating_add(REPAIR_SLICE_MAX_ENTRIES as i64)
                });
                let cursor = DirtyRepairCursor::new(dir.clone(), offset);
                Some(if let Some(epoch) = invalidation_epoch {
                    cursor.with_scan_invalidation_epoch(epoch)
                } else {
                    cursor
                })
            },
            child_dirs,
            dropped_stale_batch,
            failed: metadata_failed,
            dir_start_stamp,
            invalidation_epoch,
        }
    }

    /// Reconcile missing direct children after a complete trusted directory scan.
    ///
    /// The sliced DIR cookie is intentionally not reused as deletion evidence. A
    /// fresh, error-free direct-child `read_dir` snapshot is required. Base/L2
    /// direct-child candidates and deep live Delta paths are projected
    /// to the first child below `dir`, so one subtree delete can invalidate all
    /// stale descendants. The apply boundary rechecks the directory fingerprint
    /// while holding the same gate as watcher events, so unrelated event traffic
    /// cannot starve a stable directory reconciliation.
    pub(super) fn align_missing_indexed_direct_children(
        &self,
        dir: &Path,
        scan_started_seq: u64,
    ) -> (usize, bool) {
        if self.path_is_frozen(dir) {
            return (0, true);
        }

        let mut scratch = take_alignment_scratch();
        let result = self.align_missing_indexed_direct_children_with_scratch(
            dir,
            scan_started_seq,
            &mut scratch,
        );
        return_alignment_scratch(scratch);
        result
    }

    fn align_missing_indexed_direct_children_with_scratch(
        &self,
        dir: &Path,
        _scan_started_seq: u64,
        scratch: &mut AlignmentScratch,
    ) -> (usize, bool) {
        scratch.current_names.reset();
        scratch.indexed_children.clear();
        scratch.missing_children.clear();
        scratch.delete_events.clear();

        self.io_governor.before_io();
        let before_meta = match std::fs::symlink_metadata(dir) {
            Ok(meta) if meta.is_dir() => meta,
            _ => return (0, true),
        };
        let Some(before_fingerprint) = directory_read_fingerprint(dir, &before_meta) else {
            return (0, true);
        };

        self.io_governor.before_io();
        if let Err(err) = read_dir_names(dir, &mut scratch.current_names) {
            tracing::debug!(
                "negative alignment abandoned incomplete readdir for {}: {}",
                dir.display(),
                err
            );
            return (0, true);
        }

        self.base
            .load_full()
            .append_delete_alignment_for_dir(dir, &mut scratch.indexed_children);
        self.l2
            .load_full()
            .append_delete_alignment_for_dir(dir, &mut scratch.indexed_children);
        for path in scratch.indexed_children.drain(..) {
            push_missing_direct_child(
                dir,
                path,
                &scratch.current_names,
                &mut scratch.missing_children,
            );
        }
        {
            let db = self.delta_buffer.lock();
            scratch.indexed_children.extend(
                db.live_records()
                    .filter_map(EventRecord::best_path)
                    .filter_map(|path| first_direct_child(dir, path)),
            );
        }
        for child in scratch.indexed_children.drain(..) {
            push_missing_direct_child(
                dir,
                child,
                &scratch.current_names,
                &mut scratch.missing_children,
            );
        }
        scratch.missing_children.sort();
        scratch.missing_children.dedup();
        if scratch.missing_children.is_empty() {
            return (0, false);
        }

        scratch
            .delete_events
            .extend(
                scratch
                    .missing_children
                    .drain(..)
                    .enumerate()
                    .map(|(index, path)| EventRecord {
                        seq: index as u64 + 1,
                        timestamp: std::time::SystemTime::now(),
                        event_type: EventType::Delete,
                        id: FileIdentifier::Path(path),
                        path_hint: None,
                    }),
            );

        self.io_governor.before_io();
        let _snapshot_boundary = self.snapshot_event_gate.lock();
        let after_fingerprint = std::fs::symlink_metadata(dir)
            .ok()
            .filter(|meta| meta.is_dir())
            .and_then(|meta| directory_read_fingerprint(dir, &meta));
        if after_fingerprint != Some(before_fingerprint) {
            tracing::debug!(
                "negative alignment abandoned changed directory for {}",
                dir.display()
            );
            return (0, true);
        }
        let mut freeze_gate = self.recovery_quarantine.freeze_gate.lock();
        if scratch
            .delete_events
            .iter()
            .any(|event| freeze_gate.should_block_event(event))
        {
            for event in &scratch.delete_events {
                if freeze_gate.should_block_event(event) {
                    freeze_gate.note_blocked();
                }
            }
            return (0, true);
        }
        drop(freeze_gate);

        let Some(batch) = self.begin_apply_batch(scratch.delete_events.as_slice(), true, None)
        else {
            return (0, false);
        };
        batch.l2.apply_events(scratch.delete_events.as_slice());
        self.event_seq
            .fetch_add(batch.event_count as u64, Ordering::Relaxed);
        self.stats.record_events_applied(batch.event_count as u64);
        (batch.event_count, false)
    }

    fn repair_slice_path_allowed(
        &self,
        root: &Path,
        path: &Path,
        meta: &std::fs::Metadata,
        project_markers: &[String],
        hidden_markers_enabled: bool,
        fs_policy: Option<&FsPolicy>,
        mount_policy_counters: &SharedMountPolicyCounters,
    ) -> bool {
        if path_has_excluded_component(path, &self.exclude_dirs) {
            return false;
        }
        if !self.follow_symlinks && meta.file_type().is_symlink() {
            return false;
        }
        if !self.include_hidden
            && path_has_hidden_component_after_root(path, root)
            && !(hidden_markers_enabled && project_root_for_marker(path, project_markers).is_some())
        {
            return false;
        }

        // FsPolicy 由调用方按切片/summary 构建一次，共享计数器 Arc 获取一次
        // 并复用；配置克隆若发生在每 entry 判定内，会形成分配与缺页放大。
        fs_policy
            .map(|policy| {
                policy
                    .check_path_counted(path, Some(root), mount_policy_counters)
                    .is_allowed()
            })
            .unwrap_or(true)
            && (meta.is_file() || meta.is_dir())
    }

    pub fn scan_dirs_immediate_outcome_with_project_markers(
        &self,
        dirs: &[PathBuf],
        project_markers: &[String],
    ) -> ScanOutcome {
        let dirs: Vec<&PathBuf> = dirs.iter().take(10).collect();
        self.scan_dirs_immediate_reconciled_with_project_markers(&dirs, project_markers)
            .outcome
    }

    fn scan_dirs_immediate_reconciled_with_project_markers(
        &self,
        dirs: &[&PathBuf],
        project_markers: &[String],
    ) -> ImmediateScanReconcileOutcome {
        // Negative reconciliation is intentionally restricted to explicit shallow
        // immediate scans. Startup and generic deep scans remain upsert-only.
        let started = Instant::now();
        let mut outcome =
            self.scan_dirs_with_depth_and_project_markers(dirs, Some(1), 10_000, project_markers);
        let mut deleted = 0usize;
        let mut stable = true;

        for dir in dirs {
            let alignment_started_seq = self.event_seq.load(Ordering::Relaxed);
            let (dir_deleted, dropped_stale) =
                self.align_missing_indexed_direct_children(dir.as_path(), alignment_started_seq);
            deleted = deleted.saturating_add(dir_deleted);
            stable &= !dropped_stale;
        }
        outcome.changed = outcome.changed.saturating_add(deleted);
        outcome.elapsed_ms = started.elapsed().as_millis() as u64;
        ImmediateScanReconcileOutcome {
            outcome,
            deleted,
            stable,
        }
    }

    fn directory_manifest_summary(
        &self,
        dir: &Path,
        project_markers: &[String],
    ) -> Option<DirectoryManifestSummary> {
        self.directory_manifest_summary_with_limit(dir, project_markers, None)
    }

    /// 计算目录 manifest summary，可选择限制条目数（Phase 3）。
    ///
    /// - `max_entries=None`：使用 WalkBuilder（ignore-filtered）路径，不限制条目数。
    ///   用于 mtime 预检失败后的二次确认（目录 mtime 变了但内容可能没变）。
    /// - `max_entries=Some(n)`：使用 read_dir + repair_slice_path_allowed 路径，
    ///   限制 n 条。返回 `Some` 仅当条目数 ≤ n（complete），否则 `None`（incomplete）。
    fn directory_manifest_summary_with_limit(
        &self,
        dir: &Path,
        project_markers: &[String],
        max_entries: Option<usize>,
    ) -> Option<DirectoryManifestSummary> {
        match max_entries {
            None => {
                let hidden_markers_enabled =
                    project_markers.iter().any(|marker| marker.starts_with('.'));
                let mut builder = ignore::WalkBuilder::new(dir);
                builder
                    .max_depth(Some(1))
                    .hidden(!self.include_hidden && !hidden_markers_enabled)
                    .follow_links(false)
                    .ignore(self.ignore_enabled)
                    .git_ignore(self.ignore_enabled)
                    .git_global(self.ignore_enabled)
                    .git_exclude(self.ignore_enabled);
                let fs_policy =
                    FsPolicy::current_with_shared_config(self.shared_fs_policy_config());
                let root = dir.to_path_buf();
                let exclude_dirs = self.exclude_dirs.clone();
                let mount_policy_counters = self.mount_policy_counters();
                let include_hidden = self.include_hidden;
                let project_markers_filter = project_markers.to_vec();
                builder.filter_entry(move |entry| {
                    (exclude_dirs.is_empty()
                        || !path_has_excluded_component(entry.path(), &exclude_dirs))
                        && (include_hidden
                            || !hidden_markers_enabled
                            || !path_has_hidden_component_after_root(entry.path(), root.as_path())
                            || project_root_for_marker(entry.path(), &project_markers_filter)
                                .is_some())
                        && fs_policy
                            .as_ref()
                            .map(|policy| {
                                policy
                                    .check_path_counted(
                                        entry.path(),
                                        Some(root.as_path()),
                                        mount_policy_counters.as_ref(),
                                    )
                                    .is_allowed()
                            })
                            .unwrap_or(true)
                });

                let mut manifest = DirectoryManifestBuilder::default();
                for ent in builder.build() {
                    let ent = match ent {
                        Ok(e) => e,
                        Err(err) => {
                            tracing::debug!(
                                "directory manifest skipped entry under {}: {}",
                                dir.display(),
                                err
                            );
                            continue;
                        }
                    };
                    let path = ent.path();
                    if path == dir {
                        continue;
                    }
                    let Some(ft) = ent.file_type() else {
                        continue;
                    };
                    if !ft.is_file() && !ft.is_dir() {
                        continue;
                    }
                    self.io_governor.before_io();
                    let meta = match ent.metadata() {
                        Ok(meta) => meta,
                        Err(err) => {
                            tracing::debug!(
                                "directory manifest metadata failed for {}: {}",
                                path.display(),
                                err
                            );
                            continue;
                        }
                    };
                    manifest.push_child(
                        path,
                        FileKind::from_metadata(&meta),
                        mtime_to_ns(meta.modified().ok()),
                    );
                }

                Some(manifest.finish())
            }
            Some(n) => {
                // read_dir + repair_slice_path_allowed 路径（bounded）。
                // 复用 directory_manifest_summary_bounded，仅在 complete 时返回 Some。
                self.directory_manifest_summary_bounded(dir, project_markers, n)
                    .and_then(|(summary, complete)| if complete { Some(summary) } else { None })
            }
        }
    }

    fn directory_manifest_summary_bounded(
        &self,
        dir: &Path,
        project_markers: &[String],
        max_entries: usize,
    ) -> Option<(DirectoryManifestSummary, bool)> {
        let hidden_markers_enabled = project_markers.iter().any(|marker| marker.starts_with('.'));
        let bounded_fs_policy =
            FsPolicy::current_with_shared_config(self.shared_fs_policy_config());
        let bounded_mount_policy_counters = self.mount_policy_counters();
        let mut manifest = DirectoryManifestBuilder::default();
        let mut seen = 0usize;
        let rd = match std::fs::read_dir(dir) {
            Ok(rd) => rd,
            Err(err) => {
                tracing::debug!(
                    "directory manifest bounded skipped unreadable dir {}: {}",
                    dir.display(),
                    err
                );
                return None;
            }
        };

        for child in rd {
            let child = match child {
                Ok(child) => child,
                Err(err) => {
                    tracing::debug!(
                        "directory manifest bounded skipped entry under {}: {}",
                        dir.display(),
                        err
                    );
                    continue;
                }
            };
            if seen >= max_entries {
                return Some((manifest.finish(), false));
            }
            let path = child.path();
            self.io_governor.before_io();
            let meta = match child.metadata() {
                Ok(meta) => meta,
                Err(err) => {
                    tracing::debug!(
                        "directory manifest bounded metadata failed for {}: {}",
                        path.display(),
                        err
                    );
                    continue;
                }
            };
            if !self.repair_slice_path_allowed(
                dir,
                path.as_path(),
                &meta,
                project_markers,
                hidden_markers_enabled,
                bounded_fs_policy.as_ref(),
                bounded_mount_policy_counters.as_ref(),
            ) {
                continue;
            }
            manifest.push_child(
                path.as_path(),
                FileKind::from_metadata(&meta),
                mtime_to_ns(meta.modified().ok()),
            );
            seen = seen.saturating_add(1);
        }

        Some((manifest.finish(), true))
    }

    fn update_directory_manifests_for_dirs(&self, dirs: &[&PathBuf], project_markers: &[String]) {
        let generation = self.event_seq.load(Ordering::Relaxed);
        for dir in dirs {
            if let Some(summary) = self.directory_manifest_summary(dir, project_markers) {
                self.directory_manifests
                    .update((*dir).clone(), summary, generation);
            }
        }
    }

    pub fn path_freshness(
        &self,
        path: &std::path::Path,
        file_key: FileKey,
        mtime_ns: i64,
        kind: FileKind,
    ) -> PathFreshness {
        if self
            .delta_buffer
            .lock()
            .invalidation_covering_path(path)
            .is_some()
        {
            return PathFreshness::Changed;
        }
        self.path_freshness_from_indexes(path, file_key, mtime_ns, kind)
    }

    fn path_freshness_from_snapshot(
        &self,
        path: &std::path::Path,
        file_key: FileKey,
        mtime_ns: i64,
        kind: FileKind,
        invalidations: &SubtreeInvalidationSnapshot,
    ) -> PathFreshness {
        if invalidations.covers(path) {
            return PathFreshness::Changed;
        }
        self.path_freshness_from_indexes(path, file_key, mtime_ns, kind)
    }

    fn path_freshness_from_indexes(
        &self,
        path: &std::path::Path,
        file_key: FileKey,
        mtime_ns: i64,
        kind: FileKind,
    ) -> PathFreshness {
        match self
            .l2
            .load_full()
            .path_freshness(path, file_key, mtime_ns, kind)
        {
            PathFreshness::Missing => self
                .base
                .load_full()
                .path_freshness(path, file_key, mtime_ns, kind),
            known => known,
        }
    }

    /// 即时扫描指定目录并更新索引（同步执行，不走 debounce/channel）。
    ///
    /// 限制：最多 10 个目录，每目录最多 10000 条目。
    /// 返回 (scanned_files, elapsed_ms)。
    pub fn scan_dirs_immediate(&self, dirs: &[PathBuf]) -> (usize, u64) {
        let (outcome, _, _) = self.scan_dirs_immediate_reconcile_outcome(dirs);
        (outcome.scanned, outcome.elapsed_ms)
    }

    pub fn scan_dirs_immediate_outcome(&self, dirs: &[PathBuf]) -> ScanOutcome {
        self.scan_dirs_immediate_reconcile_outcome(dirs).0
    }

    /// Explicit shallow scan result used by the manual `/scan` endpoint.
    pub(crate) fn scan_dirs_immediate_reconcile_outcome(
        &self,
        dirs: &[PathBuf],
    ) -> (ScanOutcome, usize, bool) {
        let dirs: Vec<&PathBuf> = dirs.iter().take(10).collect();
        let reconciled = self.scan_dirs_immediate_reconciled_with_project_markers(&dirs, &[]);
        (reconciled.outcome, reconciled.deleted, reconciled.stable)
    }

    /// 深度即时扫描指定目录并更新索引（递归，不走 debounce/channel）。
    ///
    /// 限制：最多 10 个目录，每目录最多 50000 条目。
    /// 返回 (scanned_files, elapsed_ms)。
    pub fn scan_dirs_immediate_deep(&self, dirs: &[PathBuf]) -> (usize, u64) {
        let dirs: Vec<&PathBuf> = dirs.iter().take(10).collect();
        let outcome = self.scan_dirs_with_depth(&dirs, None, 50_000);
        (outcome.scanned, outcome.elapsed_ms)
    }

    pub fn startup_repair_if_needed(
        &self,
        enabled: bool,
        mode: &str,
        max_dirs: usize,
        budget_ms: u64,
        force_rebuild_ratio: f32,
    ) -> StartupRepairStats {
        let report = self.recovery_status().report;
        let should_run = enabled
            && match mode {
                "never" => false,
                "always" => true,
                "dirty-only" => report.startup_scan_required,
                other => {
                    tracing::warn!("unknown startup_repair_mode={}, using dirty-only", other);
                    report.startup_scan_required
                }
            };

        if !should_run {
            let stats = StartupRepairStats::default();
            self.set_startup_repair_stats(stats.clone());
            return stats;
        }

        let roots = self
            .roots
            .iter()
            .take(max_dirs.max(1))
            .cloned()
            .collect::<Vec<_>>();
        let dirs = roots.iter().collect::<Vec<_>>();
        let budget = (budget_ms > 0).then_some(budget_ms);
        let budgeted = self.scan_dirs_with_depth_and_project_markers_budgeted(
            &dirs,
            None,
            50_000,
            &[],
            budget,
            false,
        );
        let outcome = budgeted.outcome;
        let delete_count = self.align_missing_base_paths_for_roots(&roots);
        let changed_ratio = if outcome.scanned == 0 {
            0.0
        } else {
            outcome.changed as f32 / outcome.scanned as f32
        };
        let changed = outcome.changed.saturating_add(delete_count);
        let empty_index = self.file_count() == 0 && !report.soft_repair_needed;
        let force_ratio_exceeded = changed_ratio > force_rebuild_ratio;
        let (escalated, escalation_reason) = if report.hard_rebuild_needed {
            (true, "hard_rebuild_evidence")
        } else if empty_index {
            (true, "empty_index")
        } else if force_ratio_exceeded {
            (true, "force_rebuild_ratio")
        } else {
            (false, "")
        };
        let stats = StartupRepairStats {
            ran: true,
            escalated,
            scanned: outcome.scanned,
            changed,
            elapsed_ms: outcome.elapsed_ms,
            budget_ms,
            budget_exhausted: budgeted.budget_exhausted,
            escalation_reason: escalation_reason.to_string(),
        };
        self.set_startup_repair_stats(stats.clone());
        stats
    }

    fn align_missing_base_paths_for_roots(&self, roots: &[PathBuf]) -> usize {
        let base = self.base.load_full();
        let mut delete_events = Vec::new();
        let mut seq = 0u64;
        base.for_each_live_meta(|meta| {
            if !roots.iter().any(|root| meta.path.starts_with(root)) {
                return;
            }
            self.io_governor.before_io();
            match std::fs::symlink_metadata(&meta.path) {
                Ok(_) => return,
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
                Err(_) => return,
            }
            seq = seq.wrapping_add(1);
            delete_events.push(EventRecord {
                seq,
                timestamp: std::time::SystemTime::now(),
                event_type: EventType::Delete,
                id: FileIdentifier::Path(meta.path),
                path_hint: None,
            });
        });

        let count = delete_events.len();
        for chunk in delete_events.chunks(2048) {
            self.apply_events(chunk);
        }
        count
    }
}

fn first_direct_child(root: &Path, path: &Path) -> Option<PathBuf> {
    let relative = path.strip_prefix(root).ok()?;
    let std::path::Component::Normal(name) = relative.components().next()? else {
        return None;
    };
    Some(root.join(name))
}

fn push_missing_direct_child(
    dir: &Path,
    path: PathBuf,
    current_names: &DirectoryNameBuffer,
    missing_children: &mut Vec<PathBuf>,
) {
    if path.parent() != Some(dir) {
        return;
    }
    let Some(name) = path.file_name() else {
        return;
    };
    if !current_names.contains(name) {
        missing_children.push(super::normalize_path(path.as_path()));
    }
}

#[cfg(unix)]
fn directory_read_fingerprint(
    path: &Path,
    meta: &std::fs::Metadata,
) -> Option<DirectoryFingerprint> {
    use std::os::unix::fs::MetadataExt;

    Some(DirectoryFingerprint {
        file_key: FileKey::from_path_and_metadata(path, meta)?,
        mtime_ns: i128::from(meta.mtime())
            .saturating_mul(1_000_000_000)
            .saturating_add(i128::from(meta.mtime_nsec())),
        ctime_ns: i128::from(meta.ctime())
            .saturating_mul(1_000_000_000)
            .saturating_add(i128::from(meta.ctime_nsec())),
        nlink: meta.nlink(),
    })
}

#[cfg(all(test, unix))]
mod reusable_directory_buffer_tests {
    use super::*;
    use std::os::unix::ffi::{OsStrExt, OsStringExt};

    #[test]
    fn linux_name_buffer_preserves_non_utf8_and_resets_active_slots() {
        let root = std::env::temp_dir().join(format!(
            "fd-rdd-name-buffer-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        let large = root.join("large");
        let small = root.join("small");
        std::fs::create_dir_all(&large).unwrap();
        std::fs::create_dir_all(&small).unwrap();
        let raw_name = std::ffi::OsString::from_vec(b"keep-\xff.txt".to_vec());
        std::fs::write(large.join(&raw_name), b"x").unwrap();
        for index in 0..64 {
            std::fs::write(large.join(format!("entry-{index:02}.txt")), b"x").unwrap();
        }
        std::fs::write(small.join("only.txt"), b"x").unwrap();

        let mut names = DirectoryNameBuffer::default();
        read_dir_names(&large, &mut names).unwrap();
        assert!(names.contains(std::ffi::OsStr::from_bytes(b"keep-\xff.txt")));
        assert!(names.slot_count() >= 65);

        read_dir_names(&small, &mut names).unwrap();
        assert!(names.contains(std::ffi::OsStr::new("only.txt")));
        assert!(!names.contains(std::ffi::OsStr::from_bytes(b"keep-\xff.txt")));

        let _ = std::fs::remove_dir_all(&root);
    }
}

#[cfg(not(unix))]
fn directory_read_fingerprint(
    path: &Path,
    meta: &std::fs::Metadata,
) -> Option<DirectoryFingerprint> {
    let mtime_ns = meta
        .modified()
        .ok()
        .and_then(|time| time.duration_since(UNIX_EPOCH).ok())
        .map(|duration| duration.as_nanos().min(i128::MAX as u128) as i128)
        .unwrap_or(0);
    Some(DirectoryFingerprint {
        file_key: FileKey::from_path_and_metadata(path, meta)?,
        mtime_ns,
        ctime_ns: mtime_ns,
        nlink: 0,
    })
}
