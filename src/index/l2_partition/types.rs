use std::collections::{HashMap, HashSet};

use crate::index::parent_index::PathTable as PathTableTrait;

use super::DocId;

#[derive(Clone, Debug)]
pub(super) enum OneOrManyDocId {
    One(DocId),
    Many(Vec<DocId>),
}

impl OneOrManyDocId {
    pub(super) fn iter(&self) -> impl Iterator<Item = &DocId> {
        match self {
            OneOrManyDocId::One(id) => std::slice::from_ref(id).iter(),
            OneOrManyDocId::Many(v) => v.iter(),
        }
    }

    pub(super) fn insert(&mut self, id: DocId) {
        match self {
            OneOrManyDocId::One(existing) => {
                if *existing == id {
                    return;
                }
                *self = OneOrManyDocId::Many(vec![*existing, id]);
            }
            OneOrManyDocId::Many(v) => {
                if !v.contains(&id) {
                    v.push(id);
                }
            }
        }
    }

    /// 返回 true 表示变为空，需要从 map 移除
    pub(super) fn remove(&mut self, id: DocId) -> bool {
        match self {
            OneOrManyDocId::One(existing) => *existing == id,
            OneOrManyDocId::Many(v) => {
                v.retain(|x| *x != id);
                if v.len() == 1 {
                    let only = v[0];
                    *self = OneOrManyDocId::One(only);
                    false
                } else {
                    v.is_empty()
                }
            }
        }
    }

    pub(super) fn len(&self) -> usize {
        match self {
            OneOrManyDocId::One(_) => 1,
            OneOrManyDocId::Many(v) => v.len(),
        }
    }
}

#[derive(Clone, Debug, Default)]
pub(super) struct RebuildPathTable {
    path_to_id: HashMap<Vec<u8>, u32>,
    pub(super) id_to_path: Vec<Vec<u8>>,
    dirs: HashSet<u32>,
}

impl RebuildPathTable {
    pub(super) fn new() -> Self {
        Self::default()
    }

    pub(super) fn intern(&mut self, path_bytes: Vec<u8>, is_dir: bool) -> u32 {
        if let Some(&id) = self.path_to_id.get(&path_bytes) {
            if is_dir {
                self.dirs.insert(id);
            }
            id
        } else {
            let id = self.id_to_path.len() as u32;
            self.path_to_id.insert(path_bytes.clone(), id);
            self.id_to_path.push(path_bytes);
            if is_dir {
                self.dirs.insert(id);
            }
            id
        }
    }

    pub(super) fn lookup(&self, path_bytes: &[u8]) -> Option<u32> {
        self.path_to_id.get(path_bytes).copied()
    }
}

impl PathTableTrait for RebuildPathTable {
    fn parent_idx(&self, path_idx: u32) -> Option<u32> {
        let path = self.id_to_path.get(path_idx as usize)?;
        if path.as_slice() == b"/" {
            return None;
        }
        let parent_len = match path.iter().rposition(|&b| b == b'/') {
            Some(0) => 1,
            Some(pos) => pos,
            None => return None,
        };
        let parent_bytes = &path[..parent_len];
        self.path_to_id.get(parent_bytes).copied()
    }

    fn is_dir(&self, path_idx: u32) -> bool {
        self.dirs.contains(&path_idx)
    }
}
