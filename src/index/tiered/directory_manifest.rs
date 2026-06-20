use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

use parking_lot::Mutex;
use xxhash_rust::xxh3::Xxh3;

use crate::core::FileKind;

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub(crate) struct DirectoryManifest {
    pub child_count: u64,
    pub names_hash: u64,
    pub child_mtime_hash: u64,
    pub min_mtime_ns: i64,
    pub max_mtime_ns: i64,
    pub last_scan_generation: u64,
    /// 目录自身的 mtime（纳秒），0 表示未记录。
    /// 由 Phase 1 mtime 预检使用：当目录 mtime 未变时跳过整个周期扫描。
    pub dir_mtime_ns: i64,
}

impl DirectoryManifest {
    fn matches_summary(&self, summary: &DirectoryManifestSummary) -> bool {
        self.child_count == summary.child_count
            && self.names_hash == summary.names_hash
            && self.child_mtime_hash == summary.child_mtime_hash
            && self.min_mtime_ns == summary.min_mtime_ns
            && self.max_mtime_ns == summary.max_mtime_ns
    }
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub(crate) struct DirectoryManifestSummary {
    pub child_count: u64,
    pub names_hash: u64,
    pub child_mtime_hash: u64,
    pub min_mtime_ns: i64,
    pub max_mtime_ns: i64,
}

impl DirectoryManifestSummary {
    pub fn into_manifest(self, last_scan_generation: u64) -> DirectoryManifest {
        DirectoryManifest {
            child_count: self.child_count,
            names_hash: self.names_hash,
            child_mtime_hash: self.child_mtime_hash,
            min_mtime_ns: self.min_mtime_ns,
            max_mtime_ns: self.max_mtime_ns,
            last_scan_generation,
            dir_mtime_ns: 0,
        }
    }
}

#[derive(Clone, Debug)]
struct DirectoryManifestChild {
    name: Vec<u8>,
    kind: FileKind,
    mtime_ns: i64,
}

#[derive(Debug, Default)]
pub(crate) struct DirectoryManifestBuilder {
    children: Vec<DirectoryManifestChild>,
}

impl DirectoryManifestBuilder {
    pub fn push_child(&mut self, path: &Path, kind: FileKind, mtime_ns: i64) {
        let Some(name) = path.file_name() else {
            return;
        };
        self.children.push(DirectoryManifestChild {
            name: name.as_encoded_bytes().to_vec(),
            kind,
            mtime_ns,
        });
    }

    pub fn finish(mut self) -> DirectoryManifestSummary {
        if self.children.is_empty() {
            return DirectoryManifestSummary {
                min_mtime_ns: -1,
                max_mtime_ns: -1,
                ..DirectoryManifestSummary::default()
            };
        }

        self.children
            .sort_by(|a, b| a.name.cmp(&b.name).then(a.mtime_ns.cmp(&b.mtime_ns)));

        let mut names = Xxh3::new();
        let mut mtimes = Xxh3::new();
        let mut min_mtime_ns = i64::MAX;
        let mut max_mtime_ns = i64::MIN;

        for child in &self.children {
            names.update(&(child.name.len() as u64).to_le_bytes());
            names.update(&child.name);
            names.update(&[kind_tag(child.kind)]);

            mtimes.update(&(child.name.len() as u64).to_le_bytes());
            mtimes.update(&child.name);
            mtimes.update(&child.mtime_ns.to_le_bytes());
            mtimes.update(&[kind_tag(child.kind)]);

            min_mtime_ns = min_mtime_ns.min(child.mtime_ns);
            max_mtime_ns = max_mtime_ns.max(child.mtime_ns);
        }

        DirectoryManifestSummary {
            child_count: self.children.len() as u64,
            names_hash: names.digest(),
            child_mtime_hash: mtimes.digest(),
            min_mtime_ns,
            max_mtime_ns,
        }
    }
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct DirectoryManifestReport {
    pub dirs: usize,
    pub skipped_scans: u64,
    pub changed_scans: u64,
    pub untrusted_clock_bypass: u64,
    /// Phase 1：mtime 预检命中（目录 mtime 未变，跳过扫描）。
    pub mtime_precheck_hits: u64,
    /// Phase 1：mtime 预检未命中（目录 mtime 已变，继续扫描）。
    pub mtime_precheck_misses: u64,
    /// Phase 1：mtime 预检无记录（首次扫描，无 manifest）。
    pub mtime_precheck_no_record: u64,
    /// Phase 1：mtime 预检 stat 错误（目录不可读或无 modified 时间）。
    pub mtime_precheck_stat_errors: u64,
    /// Phase 1：clock 不可信时 mtime 预检被禁用的次数。
    pub mtime_precheck_untrusted_clock: u64,
    /// Phase 3：unbounded summary 二次确认命中（目录 mtime 变了但内容没变，跳过扫描）。
    pub unbounded_summary_hits: u64,
    /// Phase 3：unbounded summary 二次确认未命中（内容确实变了或无存储记录，继续扫描）。
    pub unbounded_summary_misses: u64,
}

#[derive(Debug, Default)]
pub(crate) struct DirectoryManifestStore {
    manifests: Mutex<HashMap<PathBuf, DirectoryManifest>>,
    skipped_scans: AtomicU64,
    changed_scans: AtomicU64,
    untrusted_clock_bypass: AtomicU64,
    /// Phase 1 mtime 预检指标（pub(crate) 以便 sync.rs 直接递增）。
    pub(crate) mtime_precheck_hits: AtomicU64,
    pub(crate) mtime_precheck_misses: AtomicU64,
    pub(crate) mtime_precheck_no_record: AtomicU64,
    pub(crate) mtime_precheck_stat_errors: AtomicU64,
    pub(crate) mtime_precheck_untrusted_clock: AtomicU64,
    /// Phase 3 unbounded summary 二次确认指标（pub(crate) 以便 sync.rs 直接递增）。
    pub(crate) unbounded_summary_hits: AtomicU64,
    pub(crate) unbounded_summary_misses: AtomicU64,
}

impl DirectoryManifestStore {
    pub fn update(&self, path: PathBuf, summary: DirectoryManifestSummary, generation: u64) {
        let mut manifests = self.manifests.lock();
        // 保留已有的 dir_mtime_ns：summary → manifest 转换不携带 mtime，
        // 但 manifest skip 路径调用 update 后不应丢失之前记录的目录 mtime。
        let preserved_mtime_ns = manifests.get(&path).map(|m| m.dir_mtime_ns).unwrap_or(0);
        let mut manifest = summary.into_manifest(generation);
        manifest.dir_mtime_ns = preserved_mtime_ns;
        manifests.insert(path, manifest);
    }

    pub fn should_skip(
        &self,
        path: &Path,
        summary: &DirectoryManifestSummary,
        cutoff_trusted: bool,
    ) -> bool {
        if !cutoff_trusted {
            self.untrusted_clock_bypass.fetch_add(1, Ordering::Relaxed);
            return false;
        }

        let unchanged = self
            .manifests
            .lock()
            .get(path)
            .map(|manifest| manifest.matches_summary(summary))
            .unwrap_or(false);
        if unchanged {
            self.skipped_scans.fetch_add(1, Ordering::Relaxed);
        } else {
            self.changed_scans.fetch_add(1, Ordering::Relaxed);
        }
        unchanged
    }

    /// 尝试基于目录 mtime 做预检（Phase 1）。
    ///
    /// 返回值语义：
    /// - `None` = 无 manifest 记录（首次扫描），需要扫描
    /// - `Some(true)` = mtime 匹配，可跳过
    /// - `Some(false)` = mtime 不匹配，需要扫描
    pub(crate) fn try_mtime_precheck(&self, path: &Path, current_mtime_ns: i64) -> Option<bool> {
        self.manifests
            .lock()
            .get(path)
            .map(|m| m.dir_mtime_ns != 0 && m.dir_mtime_ns == current_mtime_ns)
    }

    /// 记录目录 mtime，在扫描完成后调用（Phase 1）。
    pub(crate) fn record_dir_mtime(&self, path: &Path, mtime_ns: i64) {
        let mut manifests = self.manifests.lock();
        manifests
            .entry(path.to_path_buf())
            .or_default()
            .dir_mtime_ns = mtime_ns;
    }

    /// 失效指定目录的 mtime 预检记录（Phase 1）。
    /// 由 lazy validation 在发现文件 mtime 已变时调用，
    /// 避免周期扫描错误跳过该目录。
    pub(crate) fn invalidate_mtime_precheck(&self, path: &Path) {
        if let Some(m) = self.manifests.lock().get_mut(path) {
            m.dir_mtime_ns = 0; // 0 = 未记录，下次扫描必然不命中
        }
    }

    /// 检查已存储的 manifest summary 是否与给定 summary 匹配（Phase 3）。
    /// 由 unbounded summary 二次确认调用：目录 mtime 变了但内容可能没变时，
    /// 比对当前 unbounded summary 与已存储的 summary。
    pub(crate) fn stored_matches_summary(
        &self,
        path: &Path,
        summary: &DirectoryManifestSummary,
    ) -> bool {
        self.manifests
            .lock()
            .get(path)
            .map(|manifest| manifest.matches_summary(summary))
            .unwrap_or(false)
    }

    pub fn report(&self) -> DirectoryManifestReport {
        DirectoryManifestReport {
            dirs: self.manifests.lock().len(),
            skipped_scans: self.skipped_scans.load(Ordering::Relaxed),
            changed_scans: self.changed_scans.load(Ordering::Relaxed),
            untrusted_clock_bypass: self.untrusted_clock_bypass.load(Ordering::Relaxed),
            mtime_precheck_hits: self.mtime_precheck_hits.load(Ordering::Relaxed),
            mtime_precheck_misses: self.mtime_precheck_misses.load(Ordering::Relaxed),
            mtime_precheck_no_record: self.mtime_precheck_no_record.load(Ordering::Relaxed),
            mtime_precheck_stat_errors: self.mtime_precheck_stat_errors.load(Ordering::Relaxed),
            mtime_precheck_untrusted_clock: self
                .mtime_precheck_untrusted_clock
                .load(Ordering::Relaxed),
            unbounded_summary_hits: self.unbounded_summary_hits.load(Ordering::Relaxed),
            unbounded_summary_misses: self.unbounded_summary_misses.load(Ordering::Relaxed),
        }
    }
}

fn kind_tag(kind: FileKind) -> u8 {
    if kind.is_directory() {
        1
    } else {
        0
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::index::l2_partition::mtime_to_ns;
    use std::fs;

    /// 辅助：获取目录的 mtime 纳秒值。
    fn dir_mtime_ns(path: &Path) -> i64 {
        let meta = fs::symlink_metadata(path).unwrap();
        mtime_to_ns(meta.modified().ok())
    }

    /// 辅助：等待足够时间以确保目录 mtime 在下次文件操作后发生变化。
    fn sleep_for_mtime() {
        std::thread::sleep(std::time::Duration::from_millis(100));
    }

    /// 辅助：创建唯一的临时目录。
    fn unique_tmp_dir(label: &str) -> PathBuf {
        let dir = std::env::temp_dir().join(format!(
            "fd-rdd-mtime-precheck-{}-{}-{}",
            label,
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        fs::create_dir_all(&dir).unwrap();
        dir
    }

    #[test]
    fn first_scan_not_skipped() {
        // 无 manifest 记录时 try_mtime_precheck 返回 None
        let store = DirectoryManifestStore::default();
        let dir = unique_tmp_dir("first-scan");
        let mtime = dir_mtime_ns(&dir);
        assert_eq!(store.try_mtime_precheck(&dir, mtime), None);
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn dir_mtime_unchanged_skips_scan() {
        // 记录 mtime 后，mtime 不变时 try_mtime_precheck 返回 Some(true)
        let store = DirectoryManifestStore::default();
        let dir = unique_tmp_dir("unchanged");
        fs::write(dir.join("file.txt"), b"hello").unwrap();
        let mtime = dir_mtime_ns(&dir);
        store.record_dir_mtime(&dir, mtime);
        assert_eq!(store.try_mtime_precheck(&dir, mtime), Some(true));
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn dir_mtime_changed_triggers_scan() {
        // 记录 mtime 后，mtime 变化时 try_mtime_precheck 返回 Some(false)
        let store = DirectoryManifestStore::default();
        let dir = unique_tmp_dir("changed");
        fs::write(dir.join("file.txt"), b"hello").unwrap();
        let mtime = dir_mtime_ns(&dir);
        store.record_dir_mtime(&dir, mtime);
        sleep_for_mtime();
        // 创建新文件改变目录 mtime
        fs::write(dir.join("new.txt"), b"new").unwrap();
        let new_mtime = dir_mtime_ns(&dir);
        assert_ne!(mtime, new_mtime);
        assert_eq!(store.try_mtime_precheck(&dir, new_mtime), Some(false));
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn file_create_changes_dir_mtime() {
        let store = DirectoryManifestStore::default();
        let dir = unique_tmp_dir("create");
        fs::write(dir.join("a.txt"), b"a").unwrap();
        let mtime = dir_mtime_ns(&dir);
        store.record_dir_mtime(&dir, mtime);
        sleep_for_mtime();
        // 创建文件改变目录 mtime
        fs::write(dir.join("b.txt"), b"b").unwrap();
        let new_mtime = dir_mtime_ns(&dir);
        assert_eq!(store.try_mtime_precheck(&dir, new_mtime), Some(false));
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn file_delete_changes_dir_mtime() {
        let store = DirectoryManifestStore::default();
        let dir = unique_tmp_dir("delete");
        fs::write(dir.join("a.txt"), b"a").unwrap();
        fs::write(dir.join("b.txt"), b"b").unwrap();
        let mtime = dir_mtime_ns(&dir);
        store.record_dir_mtime(&dir, mtime);
        sleep_for_mtime();
        // 删除文件改变目录 mtime
        fs::remove_file(dir.join("b.txt")).unwrap();
        let new_mtime = dir_mtime_ns(&dir);
        assert_eq!(store.try_mtime_precheck(&dir, new_mtime), Some(false));
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn file_modify_preserves_dir_mtime() {
        let store = DirectoryManifestStore::default();
        let dir = unique_tmp_dir("modify");
        fs::write(dir.join("file.txt"), b"original").unwrap();
        let mtime = dir_mtime_ns(&dir);
        store.record_dir_mtime(&dir, mtime);
        // 修改文件内容（不增删），目录 mtime 不变
        fs::write(dir.join("file.txt"), b"modified content").unwrap();
        let new_mtime = dir_mtime_ns(&dir);
        assert_eq!(
            mtime, new_mtime,
            "directory mtime should not change on file content modify"
        );
        assert_eq!(store.try_mtime_precheck(&dir, new_mtime), Some(true));
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn mtime_precheck_after_snapshot_restore() {
        // 快照恢复后（新 store），mtime 记录为空（默认 0），try_mtime_precheck 返回 None
        let store = DirectoryManifestStore::default();
        let dir = unique_tmp_dir("restore");
        fs::write(dir.join("file.txt"), b"data").unwrap();
        let mtime = dir_mtime_ns(&dir);
        store.record_dir_mtime(&dir, mtime);
        assert_eq!(store.try_mtime_precheck(&dir, mtime), Some(true));

        // 模拟快照恢复：新 store（DirectoryManifestStore 不持久化，恢复后为空）
        let restored_store = DirectoryManifestStore::default();
        assert_eq!(
            restored_store.try_mtime_precheck(&dir, mtime),
            None,
            "after snapshot restore, no mtime record should exist"
        );
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn mtime_precheck_after_lazy_validation_invalidation() {
        // 记录 mtime 后，invalidate 使预检失效
        let store = DirectoryManifestStore::default();
        let dir = unique_tmp_dir("invalidate");
        fs::write(dir.join("file.txt"), b"data").unwrap();
        let mtime = dir_mtime_ns(&dir);
        store.record_dir_mtime(&dir, mtime);
        assert_eq!(store.try_mtime_precheck(&dir, mtime), Some(true));

        // lazy validation 联动：失效 mtime 预检
        store.invalidate_mtime_precheck(&dir);
        // 失效后 dir_mtime_ns=0，try_mtime_precheck 返回 Some(false)（不跳过）
        assert_eq!(
            store.try_mtime_precheck(&dir, mtime),
            Some(false),
            "after invalidation, mtime precheck should not skip"
        );
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn invalidate_nonexistent_dir_is_noop() {
        // 对不存在的 manifest 记录调用 invalidate 不应 panic
        let store = DirectoryManifestStore::default();
        let dir = unique_tmp_dir("noop");
        store.invalidate_mtime_precheck(&dir);
        assert_eq!(store.try_mtime_precheck(&dir, 12345), None);
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn update_preserves_dir_mtime_ns() {
        // update（manifest skip 路径）不应丢失已记录的 dir_mtime_ns
        let store = DirectoryManifestStore::default();
        let dir = unique_tmp_dir("preserve");
        fs::write(dir.join("file.txt"), b"data").unwrap();
        let mtime = dir_mtime_ns(&dir);
        store.record_dir_mtime(&dir, mtime);

        // 模拟 manifest skip 路径调用 update
        let summary = DirectoryManifestSummary {
            child_count: 1,
            ..Default::default()
        };
        store.update(dir.clone(), summary, 42);
        // dir_mtime_ns 应被保留
        assert_eq!(
            store.try_mtime_precheck(&dir, mtime),
            Some(true),
            "update should preserve dir_mtime_ns"
        );
        let _ = fs::remove_dir_all(&dir);
    }
}
