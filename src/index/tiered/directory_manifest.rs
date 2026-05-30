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
}

#[derive(Debug, Default)]
pub(crate) struct DirectoryManifestStore {
    manifests: Mutex<HashMap<PathBuf, DirectoryManifest>>,
    skipped_scans: AtomicU64,
    changed_scans: AtomicU64,
    untrusted_clock_bypass: AtomicU64,
}

impl DirectoryManifestStore {
    pub fn update(&self, path: PathBuf, summary: DirectoryManifestSummary, generation: u64) {
        self.manifests
            .lock()
            .insert(path, summary.into_manifest(generation));
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

    pub fn report(&self) -> DirectoryManifestReport {
        DirectoryManifestReport {
            dirs: self.manifests.lock().len(),
            skipped_scans: self.skipped_scans.load(Ordering::Relaxed),
            changed_scans: self.changed_scans.load(Ordering::Relaxed),
            untrusted_clock_bypass: self.untrusted_clock_bypass.load(Ordering::Relaxed),
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
