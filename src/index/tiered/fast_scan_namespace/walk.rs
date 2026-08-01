use super::*;

impl TieredIndex {
    #[cfg(unix)]
    pub(super) fn walk_fast_scan_namespace(
        &self,
        dir: &Path,
        indexed: &HashMap<PathBuf, FileMeta>,
    ) -> Option<NamespaceChanges> {
        let mut changes = NamespaceChanges::default();
        for entry in self.fast_scan_namespace_walker(dir).build() {
            let entry = match entry {
                Ok(entry) => entry,
                Err(err) => {
                    tracing::debug!(
                        "fast-scan namespace walk failed under {}: {}",
                        dir.display(),
                        err
                    );
                    return None;
                }
            };
            match self.process_fast_scan_namespace_entry(dir, entry, indexed, &mut changes)? {
                NamespaceEntryChange::Ignore => {}
                NamespaceEntryChange::Upsert { meta, recursive } => {
                    let path = meta.path.clone();
                    let seq = changes.upsert_events.len() as u64 + 1;
                    changes.upsert_metas.push(meta);
                    changes.upsert_events.push(modify_event(seq, path.clone()));
                    if recursive {
                        changes.recursive_repair_dirs.push(path);
                    }
                }
                NamespaceEntryChange::NonIndexableDelete(path) => {
                    let seq = changes.non_indexable_deletes.len() as u64 + 1;
                    changes.non_indexable_deletes.push(delete_event(seq, path));
                }
            }
        }
        Some(changes)
    }

    #[cfg(unix)]
    pub(super) fn read_fast_scan_namespace_names(&self, dir: &Path) -> Option<HashSet<OsString>> {
        self.io_governor.before_io();
        let entries = std::fs::read_dir(dir)
            .map_err(|err| {
                tracing::debug!(
                    "fast-scan namespace cannot enumerate {}: {}",
                    dir.display(),
                    err
                );
            })
            .ok()?;
        let mut names = HashSet::new();
        for entry in entries {
            match entry {
                Ok(entry) => {
                    names.insert(entry.file_name());
                }
                Err(err) => {
                    tracing::debug!(
                        "fast-scan namespace incomplete enumeration under {}: {}",
                        dir.display(),
                        err
                    );
                    return None;
                }
            }
        }
        Some(names)
    }

    #[cfg(unix)]
    fn fast_scan_namespace_walker(&self, dir: &Path) -> ignore::WalkBuilder {
        let mut builder = ignore::WalkBuilder::new(dir);
        builder
            .max_depth(Some(1))
            .hidden(!self.include_hidden)
            .follow_links(false)
            .ignore(self.ignore_enabled)
            .git_ignore(self.ignore_enabled)
            .git_global(self.ignore_enabled)
            .git_exclude(self.ignore_enabled);
        let fs_policy = crate::fs_policy::FsPolicy::current_with_config(self.fs_policy_config());
        let root = dir.to_path_buf();
        let exclude_dirs = self.exclude_dirs.clone();
        let counters = self.mount_policy_counters();
        builder.filter_entry(move |entry| {
            (exclude_dirs.is_empty() || !path_has_excluded_component(entry.path(), &exclude_dirs))
                && fs_policy
                    .as_ref()
                    .map(|policy| {
                        policy
                            .check_path_counted(
                                entry.path(),
                                Some(root.as_path()),
                                counters.as_ref(),
                            )
                            .is_allowed()
                    })
                    .unwrap_or(true)
        });
        builder
    }

    #[cfg(unix)]
    fn process_fast_scan_namespace_entry(
        &self,
        dir: &Path,
        entry: ignore::DirEntry,
        indexed: &HashMap<PathBuf, FileMeta>,
        changes: &mut NamespaceChanges,
    ) -> Option<NamespaceEntryChange> {
        let file_type = entry.file_type()?;
        if file_type.is_dir() && entry.path() == dir {
            return Some(NamespaceEntryChange::Ignore);
        }
        let path = super::super::normalize_path(entry.path());
        if !file_type.is_file() && !file_type.is_dir() {
            return Some(if indexed.contains_key(path.as_path()) {
                NamespaceEntryChange::NonIndexableDelete(path)
            } else {
                NamespaceEntryChange::Ignore
            });
        }
        self.io_governor.before_io();
        changes.metadata_stats = changes.metadata_stats.saturating_add(1);
        let meta = entry
            .metadata()
            .map_err(|err| {
                tracing::debug!(
                    "fast-scan namespace metadata failed for {}: {}",
                    path.display(),
                    err
                );
            })
            .ok()?;
        let kind = FileKind::from_metadata(&meta);
        let mtime = meta.modified().ok();
        if indexed
            .get(path.as_path())
            .is_some_and(|old| unchanged_without_generation(old, &meta, mtime, kind))
        {
            changes.generation_lookups_avoided =
                changes.generation_lookups_avoided.saturating_add(1);
            return Some(NamespaceEntryChange::Ignore);
        }
        let file_key = FileKey::from_path_and_metadata(&path, &meta)?;
        if self.path_freshness(&path, file_key, mtime_to_ns(mtime), kind)
            == PathFreshness::Unchanged
        {
            return Some(NamespaceEntryChange::Ignore);
        }
        Some(NamespaceEntryChange::Upsert {
            recursive: kind.is_directory(),
            meta: file_meta(path, file_key, meta, mtime, kind),
        })
    }
}

#[cfg(unix)]
fn unchanged_without_generation(
    old: &FileMeta,
    meta: &std::fs::Metadata,
    mtime: Option<std::time::SystemTime>,
    kind: FileKind,
) -> bool {
    use std::os::unix::fs::MetadataExt;

    old.file_key.generation == 0
        && old.file_key.dev == meta.dev()
        && old.file_key.ino == meta.ino()
        && old.mtime == mtime
        && old.kind == kind
}

#[cfg(unix)]
fn file_meta(
    path: PathBuf,
    file_key: FileKey,
    meta: std::fs::Metadata,
    mtime: Option<std::time::SystemTime>,
    kind: FileKind,
) -> FileMeta {
    FileMeta {
        file_key,
        path,
        size: meta.len(),
        mtime,
        ctime: meta.created().ok(),
        atime: meta.accessed().ok(),
        kind,
    }
}

#[cfg(unix)]
fn modify_event(seq: u64, path: PathBuf) -> EventRecord {
    EventRecord {
        seq,
        timestamp: std::time::SystemTime::now(),
        event_type: EventType::Modify,
        id: FileIdentifier::Path(path),
        path_hint: None,
    }
}
