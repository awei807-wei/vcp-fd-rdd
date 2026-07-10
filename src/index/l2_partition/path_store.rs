use std::error::Error;
use std::fmt;
use std::mem::size_of;
use std::path::PathBuf;

use crate::util::pathbuf_from_encoded_vec;

use super::DocId;

/// A compact reference into [`PathStore`]'s contiguous byte arena.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct PathRef {
    pub offset: u32,
    pub len: u32,
}

/// Errors returned while mutating a [`PathStore`].
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum PathStoreError {
    /// The requested append would exceed the addressable `u32` arena range.
    ArenaTooLarge {
        current_len: usize,
        additional_len: usize,
    },
    /// The supplied document ID does not exist in the store.
    InvalidDocId(DocId),
    /// The store cannot assign another 32-bit document identifier.
    TooManyPaths(usize),
}

impl fmt::Display for PathStoreError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::ArenaTooLarge {
                current_len,
                additional_len,
            } => write!(
                f,
                "path arena exceeds u32 address space: {current_len} + {additional_len} bytes"
            ),
            Self::InvalidDocId(doc_id) => write!(f, "invalid path store DocId: {doc_id}"),
            Self::TooManyPaths(count) => write!(f, "path store exceeds u32 DocId range: {count}"),
        }
    }
}

impl Error for PathStoreError {}

/// Runtime absolute-path storage backed by one contiguous byte arena.
///
/// Each `DocId` indexes one [`PathRef`]. Updating a path appends its new bytes
/// and replaces only the reference, so readers never observe partially
/// overwritten path data. Superseded arena slots are reclaimed by rebuilding
/// or clearing the store.
#[derive(Clone, Debug, Default)]
pub struct PathStore {
    arena: Vec<u8>,
    refs: Vec<PathRef>,
}

impl PathStore {
    /// Creates an empty path store.
    pub fn new() -> Self {
        Self::default()
    }

    /// Creates an empty store with reserved document and arena capacity.
    pub fn with_capacity(path_capacity: usize, byte_capacity: usize) -> Self {
        Self {
            arena: Vec::with_capacity(byte_capacity),
            refs: Vec::with_capacity(path_capacity),
        }
    }

    /// Appends encoded path bytes and returns their assigned `DocId`.
    pub fn push(&mut self, bytes: &[u8]) -> Result<DocId, PathStoreError> {
        let path_ref = self.append_bytes(bytes)?;
        let doc_id = DocId::try_from(self.refs.len())
            .map_err(|_| PathStoreError::TooManyPaths(self.refs.len()))?;
        self.refs.push(path_ref);
        Ok(doc_id)
    }

    /// Returns the encoded path bytes for `doc_id`.
    pub fn get_bytes(&self, doc_id: DocId) -> Option<&[u8]> {
        let path_ref = *self.refs.get(usize::try_from(doc_id).ok()?)?;
        let start = path_ref.offset as usize;
        let end = start.checked_add(path_ref.len as usize)?;
        self.arena.get(start..end)
    }

    /// Returns an owned platform path for `doc_id`.
    pub fn get_path_buf(&self, doc_id: DocId) -> Option<PathBuf> {
        Some(pathbuf_from_encoded_vec(self.get_bytes(doc_id)?.to_vec()))
    }

    /// Replaces a document's path reference after appending the new bytes.
    ///
    /// The old arena slot remains allocated until the store is rebuilt or
    /// cleared.
    pub fn update(&mut self, doc_id: DocId, bytes: &[u8]) -> Result<(), PathStoreError> {
        let index = usize::try_from(doc_id).map_err(|_| PathStoreError::InvalidDocId(doc_id))?;
        if index >= self.refs.len() {
            return Err(PathStoreError::InvalidDocId(doc_id));
        }
        let new_ref = self.append_bytes(bytes)?;
        self.refs[index] = new_ref;
        Ok(())
    }

    /// Removes all paths while retaining allocated capacity for reuse.
    pub fn clear(&mut self) {
        self.arena.clear();
        self.refs.clear();
    }

    /// Returns the number of indexed paths.
    pub fn len(&self) -> usize {
        self.refs.len()
    }

    /// Returns the byte capacity of the contiguous arena.
    pub fn arena_capacity(&self) -> usize {
        self.arena.capacity()
    }

    /// Returns the heap bytes currently reserved by the arena and references.
    pub fn allocated_bytes(&self) -> usize {
        self.arena
            .capacity()
            .saturating_add(self.refs.capacity().saturating_mul(size_of::<PathRef>()))
    }

    fn append_bytes(&mut self, bytes: &[u8]) -> Result<PathRef, PathStoreError> {
        let current_len = self.arena.len();
        let new_len =
            current_len
                .checked_add(bytes.len())
                .ok_or(PathStoreError::ArenaTooLarge {
                    current_len,
                    additional_len: bytes.len(),
                })?;
        if new_len > u32::MAX as usize || bytes.len() > u32::MAX as usize {
            return Err(PathStoreError::ArenaTooLarge {
                current_len,
                additional_len: bytes.len(),
            });
        }

        let path_ref = PathRef {
            offset: current_len as u32,
            len: bytes.len() as u32,
        };
        self.arena.extend_from_slice(bytes);
        Ok(path_ref)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn stores_paths_contiguously_and_assigns_doc_ids() {
        let mut store = PathStore::new();
        store.push(b"/tmp/alpha").expect("path should fit");
        store.push(b"/tmp/beta").expect("path should fit");

        assert_eq!(store.len(), 2);
        assert_eq!(store.refs[0], PathRef { offset: 0, len: 10 });
        assert_eq!(store.refs[1], PathRef { offset: 10, len: 9 });
        assert_eq!(store.get_bytes(0), Some(b"/tmp/alpha".as_slice()));
        assert_eq!(store.get_path_buf(1), Some(PathBuf::from("/tmp/beta")));
    }

    #[test]
    fn supports_paths_longer_than_u16() {
        let path = vec![b'x'; usize::from(u16::MAX) + 1];
        let mut store = PathStore::new();
        let doc_id = store.push(&path).expect("u32 length should accept path");

        assert_eq!(store.refs[doc_id as usize].len, u32::from(u16::MAX) + 1);
        assert_eq!(store.get_bytes(doc_id), Some(path.as_slice()));
    }

    #[test]
    fn update_appends_and_points_to_new_path() {
        let mut store = PathStore::new();
        let doc_id = store.push(b"/old").expect("initial path should fit");
        let old_arena_len = store.arena.len();

        store
            .update(doc_id, b"/new/path")
            .expect("update should fit");

        assert_eq!(&store.arena[..old_arena_len], b"/old");
        assert_eq!(store.arena.len(), old_arena_len + b"/new/path".len());
        assert_eq!(store.get_bytes(doc_id), Some(b"/new/path".as_slice()));
    }

    #[test]
    fn clear_keeps_capacity_and_resets_doc_ids() {
        let mut store = PathStore::with_capacity(4, 64);
        store.push(b"/tmp/value").expect("path should fit");
        let ref_capacity = store.refs.capacity();
        let allocated = store.allocated_bytes();

        store.clear();

        assert_eq!(store.len(), 0);
        assert_eq!(store.refs.capacity(), ref_capacity);
        assert_eq!(store.allocated_bytes(), allocated);
        assert_eq!(store.get_bytes(0), None);
        assert_eq!(store.push(b"/again").expect("path should fit"), 0);
    }
}
