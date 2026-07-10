use std::borrow::Cow;
use std::path::Path;

use crate::core::{EventRecord, EventType, FileIdentifier, FileKey, FileKind, FileMeta};

use super::helpers::{mtime_to_ns, ResolvedFsMeta};
use super::PersistentIndex;

impl PersistentIndex {
    /// 批量应用事件
    pub fn apply_events(&self, events: &[EventRecord]) {
        for ev in events {
            self.apply_event_ref(ev);
        }
    }

    /// 批量应用事件（drain 版本）：
    /// - 消费 `Vec<EventRecord>`，避免 `Create/Modify/Rename` 在这里再次 `to_path_buf()` 造成的额外分配。
    /// - 用于 EventPipeline / fast-sync 这类"事件量很大、且不需要保留 EventRecord"的路径。
    pub fn apply_events_drain(&self, events: &mut Vec<EventRecord>) {
        for ev in events.drain(..) {
            self.apply_event_owned(ev);
        }
    }

    pub fn apply_file_metas(&self, metas: &[FileMeta]) {
        for meta in metas.iter().cloned() {
            self.upsert(meta);
        }
    }

    pub fn apply_file_metas_drain(&self, metas: &mut Vec<FileMeta>) {
        for meta in metas.drain(..) {
            self.upsert(meta);
        }
    }

    fn resolve_path_meta(path: &Path) -> Option<ResolvedFsMeta> {
        let meta = std::fs::metadata(path).ok()?;
        Some(ResolvedFsMeta {
            file_key: FileKey::from_path_and_metadata(path, &meta)?,
            mtime: meta.modified().ok(),
            kind: FileKind::from_metadata(&meta),
        })
    }

    fn existing_path_for_file_key(&self, fk: FileKey) -> Option<std::path::PathBuf> {
        let docid = self.filekey_representative(fk)?;
        self.path_buf_for_docid(docid)
    }

    fn apply_event_ref(&self, ev: &EventRecord) {
        match &ev.event_type {
            EventType::Create | EventType::Modify => {
                self.handle_create_or_modify(
                    ev.best_path().map(Cow::Borrowed),
                    ev.id.as_file_key(),
                );
            }
            EventType::Delete => {
                self.handle_delete(ev.best_path(), ev.id.as_file_key());
            }
            EventType::Rename {
                from,
                from_path_hint,
            } => {
                self.handle_rename(
                    from_path_hint.as_deref().or_else(|| from.as_path()),
                    from.as_file_key(),
                    ev.best_path().map(Cow::Borrowed),
                );
            }
        }
    }

    fn apply_event_owned(&self, ev: EventRecord) {
        let EventRecord {
            event_type,
            id,
            path_hint,
            ..
        } = ev;

        match event_type {
            EventType::Create | EventType::Modify => match (path_hint, id) {
                (Some(path), _) => self.handle_create_or_modify(Some(Cow::Owned(path)), None),
                (None, FileIdentifier::Path(path)) => {
                    self.handle_create_or_modify(Some(Cow::Owned(path)), None)
                }
                (None, FileIdentifier::Fid { dev, ino }) => self.handle_create_or_modify(
                    None,
                    Some(FileKey {
                        dev,
                        ino,
                        generation: 0,
                    }),
                ),
            },
            EventType::Delete => match (path_hint, id) {
                (_, FileIdentifier::Fid { dev, ino }) => {
                    self.handle_delete(
                        None,
                        Some(FileKey {
                            dev,
                            ino,
                            generation: 0,
                        }),
                    );
                }
                (Some(path), _) => {
                    self.handle_delete(Some(path.as_path()), None);
                }
                (None, FileIdentifier::Path(path)) => {
                    self.handle_delete(Some(path.as_path()), None);
                }
            },
            EventType::Rename {
                from,
                from_path_hint,
            } => {
                let to_path = match (path_hint, id) {
                    (Some(path), _) => Some(Cow::Owned(path)),
                    (None, FileIdentifier::Path(path)) => Some(Cow::Owned(path)),
                    (None, FileIdentifier::Fid { .. }) => None,
                };

                self.handle_rename(
                    from_path_hint.as_deref().or_else(|| from.as_path()),
                    from.as_file_key(),
                    to_path,
                );
            }
        }
    }

    fn handle_create_or_modify(&self, path: Option<Cow<'_, Path>>, fid: Option<FileKey>) {
        if let Some(path) = path {
            let Some(meta) = Self::resolve_path_meta(path.as_ref()) else {
                return;
            };
            self.upsert(FileMeta {
                file_key: meta.file_key,
                path: path.into_owned(),
                size: 0,
                mtime: meta.mtime,
                ctime: None,
                atime: None,
                kind: meta.kind,
            });
        }

        let Some(fk) = fid else {
            return;
        };
        let Some(path) = self.existing_path_for_file_key(fk) else {
            return;
        };
        let Some(meta) = Self::resolve_path_meta(&path) else {
            return;
        };
        if meta.file_key == fk {
            self.upsert(FileMeta {
                file_key: fk,
                path,
                size: 0,
                mtime: meta.mtime,
                ctime: None,
                atime: None,
                kind: meta.kind,
            });
        }
    }

    fn handle_delete(&self, path: Option<&Path>, fid: Option<FileKey>) {
        if let Some(fk) = fid {
            self.mark_deleted(fk);
        } else if let Some(path) = path {
            self.mark_deleted_by_path(path);
        }
    }

    fn handle_rename(
        &self,
        from_best_path: Option<&Path>,
        from_fid: Option<FileKey>,
        to_path: Option<Cow<'_, Path>>,
    ) {
        let to_path = to_path.map(|p| Cow::Owned(crate::index::tiered::normalize_path(p.as_ref())));
        let from_best_path = from_best_path.map(crate::index::tiered::normalize_path);
        let to_meta = to_path.as_deref().and_then(Self::resolve_path_meta);
        let fallback_meta = if to_meta.is_none() {
            from_best_path.as_deref().and_then(Self::resolve_path_meta)
        } else {
            None
        };

        let docid_opt = from_best_path
            .as_deref()
            .and_then(|p| self.lookup_docid_by_path(p))
            .or_else(|| from_fid.and_then(|fk| self.filekey_representative(fk)));

        if let Some(docid) = docid_opt {
            if let Some(old_path) = self.path_buf_for_docid(docid) {
                self.remove_trigrams(docid, &old_path);
                self.remove_path_hash(docid, &old_path);
            } else if let Some(ref path) = from_best_path {
                self.remove_trigrams(docid, path);
                self.remove_path_hash(docid, path);
            }

            if let Some(ref to_path) = to_path {
                let to_path_owned = to_path.clone();
                let mtime_ns = if let Some(meta) = to_meta {
                    mtime_to_ns(meta.mtime)
                } else {
                    self.entry_mtime(docid).unwrap_or(-1)
                };
                self.insert_trigrams(docid, &to_path_owned);
                self.insert_path_hash(docid, &to_path_owned);
                let abs_path_bytes = to_path_owned.as_os_str().as_encoded_bytes().to_vec();
                let kind = to_meta.map(|meta| meta.kind).unwrap_or(FileKind::File);
                self.update_entry_path(docid, &abs_path_bytes, mtime_ns, kind);
            } else if let Some(meta) = fallback_meta {
                self.update_entry_metadata(docid, mtime_to_ns(meta.mtime), meta.kind);
                if let Some(old_path) = self.path_buf_for_docid(docid) {
                    self.insert_trigrams(docid, &old_path);
                    self.insert_path_hash(docid, &old_path);
                }
            } else {
                // 没有新路径时恢复旧路径倒排，避免仅凭 FID rename 事件造成误删。
                if let Some(old_path) = self.path_buf_for_docid(docid) {
                    self.insert_trigrams(docid, &old_path);
                    self.insert_path_hash(docid, &old_path);
                }
            }

            if (self.entries.read().get(docid as usize)).is_some() {
                self.tombstones.write().remove(docid);
                let file_key = self
                    .entries
                    .read()
                    .get(docid as usize)
                    .map(|entry| entry.file_key());
                if let Some(file_key) = file_key {
                    self.replace_filekey_representative(file_key, docid);
                }
                self.dirty.store(true, std::sync::atomic::Ordering::Release);
            }
            return;
        }

        self.handle_delete(from_best_path.as_deref(), from_fid);
        if let (Some(to_path), Some(meta)) = (to_path, to_meta) {
            self.upsert(FileMeta {
                file_key: meta.file_key,
                path: to_path.into_owned(),
                size: 0,
                mtime: meta.mtime,
                ctime: None,
                atime: None,
                kind: meta.kind,
            });
        }
    }
}
