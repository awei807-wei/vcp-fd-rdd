use std::collections::HashMap;
use std::hash::{Hash, Hasher};
use std::mem::size_of;

use crate::index::parent_index::PathTable as ParentIndexPathTable;

type PathHashFn = fn(&[u8]) -> u64;

fn hash_path(bytes: &[u8]) -> u64 {
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    bytes.hash(&mut hasher);
    hasher.finish()
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) struct PathRef {
    offset: u32,
    len: u32,
}

impl PathRef {
    fn range(self) -> Option<std::ops::Range<usize>> {
        let start = self.offset as usize;
        let end = start.checked_add(self.len as usize)?;
        Some(start..end)
    }
}

#[derive(Clone, Debug)]
enum OneOrManyPathId {
    One(u32),
    Many(u32),
}

#[derive(Clone, Debug, Default)]
struct CollisionBuckets {
    groups: Vec<Vec<u32>>,
}

impl CollisionBuckets {
    fn ids<'a>(&'a self, bucket: &'a OneOrManyPathId) -> Option<&'a [u32]> {
        match bucket {
            OneOrManyPathId::One(id) => Some(std::slice::from_ref(id)),
            OneOrManyPathId::Many(group) => self.groups.get(*group as usize).map(Vec::as_slice),
        }
    }

    fn push(&mut self, bucket: &mut OneOrManyPathId, id: u32) {
        match bucket {
            OneOrManyPathId::One(existing) => {
                let group = u32::try_from(self.groups.len())
                    .expect("path hash collision groups exceed u32 space");
                self.groups.push(vec![*existing, id]);
                *bucket = OneOrManyPathId::Many(group);
            }
            OneOrManyPathId::Many(group) => self.groups[*group as usize].push(id),
        }
    }

    fn allocated_bytes(&self) -> usize {
        self.groups.capacity() * size_of::<Vec<u32>>()
            + self
                .groups
                .iter()
                .map(|ids| ids.capacity() * size_of::<u32>())
                .sum::<usize>()
    }

    fn shrink_to_fit(&mut self) {
        for ids in &mut self.groups {
            ids.shrink_to_fit();
        }
        self.groups.shrink_to_fit();
    }
}

/// 构建 ParentIndex 时使用的临时路径表。
///
/// 所有路径字节只在 `arena` 中连续保存一次；哈希表仅保存紧凑 path id。
/// 哈希冲突始终通过原始字节比较消歧，不能造成错误命中。
#[derive(Clone, Debug)]
pub(super) struct CompactPathTable {
    arena: Vec<u8>,
    paths: Vec<PathRef>,
    hash_to_ids: HashMap<u64, OneOrManyPathId>,
    collision_ids: CollisionBuckets,
    dir_bits: Vec<u64>,
    hash_fn: PathHashFn,
}

impl Default for CompactPathTable {
    fn default() -> Self {
        Self::new()
    }
}

impl CompactPathTable {
    pub(super) fn new() -> Self {
        Self {
            arena: Vec::new(),
            paths: Vec::new(),
            hash_to_ids: HashMap::new(),
            collision_ids: CollisionBuckets::default(),
            dir_bits: Vec::new(),
            hash_fn: hash_path,
        }
    }

    #[cfg(test)]
    fn with_hash_fn(hash_fn: PathHashFn) -> Self {
        Self {
            hash_fn,
            ..Self::new()
        }
    }

    /// 返回路径的稳定构建期 ID；重复路径不会再次复制到 arena。
    pub(super) fn intern(&mut self, path_bytes: &[u8], is_dir: bool) -> u32 {
        let hash = (self.hash_fn)(path_bytes);
        if let Some(id) = self.lookup_hashed(hash, path_bytes) {
            if is_dir {
                self.mark_dir(id);
            }
            return id;
        }

        let id = u32::try_from(self.paths.len()).expect("path table exceeds u32 path id space");
        let offset =
            u32::try_from(self.arena.len()).expect("path table arena exceeds 4 GiB offset space");
        let len = u32::try_from(path_bytes.len()).expect("single path exceeds 4 GiB");
        let new_len = self
            .arena
            .len()
            .checked_add(path_bytes.len())
            .expect("path table arena size overflow");
        assert!(
            new_len <= u32::MAX as usize,
            "path table arena exceeds 4 GiB"
        );
        self.arena.extend_from_slice(path_bytes);
        self.paths.push(PathRef { offset, len });

        match self.hash_to_ids.get_mut(&hash) {
            Some(ids) => self.collision_ids.push(ids, id),
            None => {
                self.hash_to_ids.insert(hash, OneOrManyPathId::One(id));
            }
        }
        if is_dir {
            self.mark_dir(id);
        }
        id
    }

    pub(super) fn lookup(&self, path_bytes: &[u8]) -> Option<u32> {
        self.lookup_hashed((self.hash_fn)(path_bytes), path_bytes)
    }

    pub(super) fn path_bytes(&self, id: u32) -> Option<&[u8]> {
        let path_ref = *self.paths.get(id as usize)?;
        self.arena.get(path_ref.range()?)
    }

    pub(super) fn len(&self) -> usize {
        self.paths.len()
    }

    #[cfg(test)]
    pub(super) fn allocated_bytes(&self) -> usize {
        let buckets =
            self.hash_to_ids.capacity() * (size_of::<u64>() + size_of::<OneOrManyPathId>() + 1);
        size_of::<Self>()
            + self.arena.capacity()
            + self.paths.capacity() * size_of::<PathRef>()
            + self.dir_bits.capacity() * size_of::<u64>()
            + buckets
            + self.collision_ids.allocated_bytes()
    }

    fn lookup_hashed(&self, hash: u64, path_bytes: &[u8]) -> Option<u32> {
        let ids = self.collision_ids.ids(self.hash_to_ids.get(&hash)?)?;
        ids.iter().copied().find(|&id| {
            self.path_bytes(id)
                .is_some_and(|stored| stored == path_bytes)
        })
    }

    fn mark_dir(&mut self, id: u32) {
        let word = id as usize / 64;
        if self.dir_bits.len() <= word {
            self.dir_bits.resize(word + 1, 0);
        }
        self.dir_bits[word] |= 1_u64 << (id % 64);
    }

    fn is_dir_id(&self, id: u32) -> bool {
        self.dir_bits
            .get(id as usize / 64)
            .is_some_and(|word| word & (1_u64 << (id % 64)) != 0)
    }
}

impl ParentIndexPathTable for CompactPathTable {
    fn parent_idx(&self, path_idx: u32) -> Option<u32> {
        let path = self.path_bytes(path_idx)?;
        if path == b"/" {
            return None;
        }
        let parent_len = match path.iter().rposition(|&byte| byte == b'/') {
            Some(0) => 1,
            Some(position) => position,
            None => return None,
        };
        self.lookup(&path[..parent_len])
    }

    fn is_dir(&self, path_idx: u32) -> bool {
        self.is_dir_id(path_idx)
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct DirectoryPathRef {
    original_id: u32,
    path: PathRef,
}

/// ParentIndex 构建完成后的常驻目录反查表。
///
/// 此结构只保留目录路径。`lookup` 返回 `CompactPathTable` 分配的原始 path id，
/// 因而可以直接查询构建期生成、以该 ID 为键的 ParentIndex，无需重映射。
#[derive(Clone, Debug)]
pub(super) struct ParentPathLookup {
    arena: Vec<u8>,
    directories: Vec<DirectoryPathRef>,
    hash_to_slots: HashMap<u64, OneOrManyPathId>,
    collision_slots: CollisionBuckets,
    hash_fn: PathHashFn,
}

impl ParentPathLookup {
    pub(super) fn from_table(table: &CompactPathTable) -> Self {
        let dir_count = table
            .dir_bits
            .iter()
            .map(|word| word.count_ones() as usize)
            .sum();
        let mut lookup = Self {
            arena: Vec::new(),
            directories: Vec::with_capacity(dir_count),
            hash_to_slots: HashMap::with_capacity(dir_count),
            collision_slots: CollisionBuckets::default(),
            hash_fn: table.hash_fn,
        };

        for original_id in 0..table.len() {
            let original_id =
                u32::try_from(original_id).expect("CompactPathTable contains an invalid path id");
            if !table.is_dir_id(original_id) {
                continue;
            }
            let path = table
                .path_bytes(original_id)
                .expect("CompactPathTable contains an invalid PathRef");
            lookup.push_directory(original_id, path);
        }
        lookup.shrink_to_fit();
        lookup
    }

    pub(super) fn lookup(&self, path_bytes: &[u8]) -> Option<u32> {
        let hash = (self.hash_fn)(path_bytes);
        self.collision_slots
            .ids(self.hash_to_slots.get(&hash)?)?
            .iter()
            .filter_map(|&slot| self.directories.get(slot as usize))
            .find(|entry| {
                entry
                    .path
                    .range()
                    .and_then(|range| self.arena.get(range))
                    .is_some_and(|stored| stored == path_bytes)
            })
            .map(|entry| entry.original_id)
    }

    #[cfg(test)]
    pub(super) fn len(&self) -> usize {
        self.directories.len()
    }

    pub(super) fn allocated_bytes(&self) -> usize {
        let buckets =
            self.hash_to_slots.capacity() * (size_of::<u64>() + size_of::<OneOrManyPathId>() + 1);
        size_of::<Self>()
            + self.arena.capacity()
            + self.directories.capacity() * size_of::<DirectoryPathRef>()
            + buckets
            + self.collision_slots.allocated_bytes()
    }

    fn push_directory(&mut self, original_id: u32, path_bytes: &[u8]) {
        let slot = u32::try_from(self.directories.len())
            .expect("parent path lookup exceeds u32 directory slots");
        let offset =
            u32::try_from(self.arena.len()).expect("parent path arena exceeds 4 GiB offset space");
        let len = u32::try_from(path_bytes.len()).expect("single path exceeds 4 GiB");
        let new_len = self
            .arena
            .len()
            .checked_add(path_bytes.len())
            .expect("parent path arena size overflow");
        assert!(
            new_len <= u32::MAX as usize,
            "parent path arena exceeds 4 GiB"
        );
        self.arena.extend_from_slice(path_bytes);
        self.directories.push(DirectoryPathRef {
            original_id,
            path: PathRef { offset, len },
        });

        let hash = (self.hash_fn)(path_bytes);
        match self.hash_to_slots.get_mut(&hash) {
            Some(slots) => self.collision_slots.push(slots, slot),
            None => {
                self.hash_to_slots.insert(hash, OneOrManyPathId::One(slot));
            }
        }
    }

    fn shrink_to_fit(&mut self) {
        self.arena.shrink_to_fit();
        self.directories.shrink_to_fit();
        self.collision_slots.shrink_to_fit();
        self.hash_to_slots.shrink_to_fit();
    }
}

#[cfg(test)]
#[path = "parent_path_tests.rs"]
mod tests;
