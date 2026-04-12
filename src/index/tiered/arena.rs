use std::hash::{Hash, Hasher};
use std::sync::Arc;

fn hash_bytes64(bytes: &[u8]) -> u64 {
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    bytes.hash(&mut hasher);
    hasher.finish()
}

#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub(super) struct Span {
    off: u32,
    len: u32,
}

impl Span {
    fn range(self) -> std::ops::Range<usize> {
        let off = self.off as usize;
        let len = self.len as usize;
        off..(off + len)
    }
}

#[derive(Clone, Debug)]
pub(super) enum OneOrManySpan {
    One(Span),
    Many(Vec<Span>),
}

impl OneOrManySpan {
    fn iter(&self) -> impl Iterator<Item = &Span> {
        match self {
            OneOrManySpan::One(s) => std::slice::from_ref(s).iter(),
            OneOrManySpan::Many(v) => v.iter(),
        }
    }

    fn push(&mut self, s: Span) {
        match self {
            OneOrManySpan::One(existing) => {
                let old = *existing;
                *self = OneOrManySpan::Many(vec![old, s]);
            }
            OneOrManySpan::Many(v) => v.push(s),
        }
    }
}

/// 一次 flush 周期内的"路径集合"：
/// - 以 hash 为索引（正确性靠 byte-compare，不依赖 hash 不碰撞）
/// - 路径字节统一落在 arena（append-only），避免每条路径一个独立堆分配
#[derive(Clone, Debug, Default)]
pub(crate) struct PathArenaSet {
    arena: Vec<u8>,
    map: std::collections::HashMap<u64, OneOrManySpan>,
    paths_len: usize,
    active_bytes: u64,
}

impl PathArenaSet {
    const ARENA_KEEP_BYTES: usize = 256 * 1024;
    const MAP_KEEP_CAP: usize = 1024;

    pub(crate) fn len_paths(&self) -> usize {
        self.paths_len
    }

    pub(crate) fn active_bytes(&self) -> u64 {
        self.active_bytes
    }

    pub(crate) fn arena_len(&self) -> usize {
        self.arena.len()
    }

    pub(crate) fn arena_cap(&self) -> usize {
        self.arena.capacity()
    }

    pub(crate) fn map_len(&self) -> usize {
        self.map.len()
    }

    pub(crate) fn map_cap(&self) -> usize {
        self.map.capacity()
    }

    fn bytes_at(&self, span: Span) -> Option<&[u8]> {
        let r = span.range();
        self.arena.get(r)
    }

    pub(crate) fn contains(&self, bytes: &[u8]) -> bool {
        let h = hash_bytes64(bytes);
        let Some(v) = self.map.get(&h) else {
            return false;
        };
        v.iter()
            .filter_map(|s| self.bytes_at(*s))
            .any(|b| b == bytes)
    }

    /// 返回 true 表示本次为"新路径"插入（用于统计）
    pub(crate) fn insert(&mut self, bytes: &[u8]) -> bool {
        let h = hash_bytes64(bytes);
        if let Some(v) = self.map.get(&h) {
            if v.iter()
                .filter_map(|s| self.bytes_at(*s))
                .any(|b| b == bytes)
            {
                return false;
            }
        }

        let off: u32 = match self.arena.len().try_into() {
            Ok(v) => v,
            Err(_) => {
                // 极端：arena 超过 4GiB。为避免溢出导致错误索引，直接清空本轮 overlay。
                // 这会丢失"跨段屏蔽集合"，但能阻止进程继续无界增长；后续 flush 会重建磁盘真相。
                tracing::warn!("Overlay arena exceeded 4GiB, clearing overlay to avoid overflow");
                self.clear();
                0
            }
        };
        let len: u32 = bytes.len().try_into().unwrap_or(u32::MAX);
        self.arena.extend_from_slice(bytes);
        let span = Span { off, len };

        match self.map.get_mut(&h) {
            Some(v) => v.push(span),
            None => {
                self.map.insert(h, OneOrManySpan::One(span));
            }
        }

        self.paths_len += 1;
        self.active_bytes += bytes.len() as u64;
        true
    }

    /// 返回 true 表示存在并移除
    pub(crate) fn remove(&mut self, bytes: &[u8]) -> bool {
        let h = hash_bytes64(bytes);
        let arena: &[u8] = &self.arena;

        let span_eq = |s: Span, bytes: &[u8]| -> bool {
            let r = s.range();
            arena.get(r).is_some_and(|b| b == bytes)
        };

        let mut removed = false;
        let mut drop_key = false;
        {
            let Some(v) = self.map.get_mut(&h) else {
                return false;
            };
            match v {
                OneOrManySpan::One(s) => {
                    if span_eq(*s, bytes) {
                        removed = true;
                        drop_key = true;
                    }
                }
                OneOrManySpan::Many(vs) => {
                    if let Some(i) = vs.iter().position(|s| span_eq(*s, bytes)) {
                        vs.swap_remove(i);
                        removed = true;
                    }
                    if removed {
                        if vs.is_empty() {
                            drop_key = true;
                        } else if vs.len() == 1 {
                            let only = vs[0];
                            *v = OneOrManySpan::One(only);
                        }
                    }
                }
            }
        }

        if !removed {
            return false;
        }
        if drop_key {
            self.map.remove(&h);
        }

        self.paths_len = self.paths_len.saturating_sub(1);
        self.active_bytes = self.active_bytes.saturating_sub(bytes.len() as u64);
        true
    }

    pub(crate) fn for_each_bytes(&self, mut f: impl FnMut(&[u8])) {
        for v in self.map.values() {
            for s in v.iter() {
                if let Some(b) = self.bytes_at(*s) {
                    f(b);
                }
            }
        }
    }

    pub(crate) fn clear(&mut self) {
        self.map.clear();
        self.arena.clear();
        self.paths_len = 0;
        self.active_bytes = 0;
    }

    /// flush/rebuild 后按阈值回收容量，避免历史高水位长期常驻。
    pub(crate) fn maybe_shrink_after_clear(&mut self) {
        if self.arena.capacity() > Self::ARENA_KEEP_BYTES * 2 {
            self.arena.shrink_to(Self::ARENA_KEEP_BYTES);
        }
        if self.map.capacity() > Self::MAP_KEEP_CAP * 2 {
            self.map.shrink_to(Self::MAP_KEEP_CAP);
        }
    }

    /// 估算 overlay 堆占用（粗估、偏保守）：arena + HashMap 桶 + collision Vec 容量。
    pub(crate) fn estimated_bytes(&self) -> u64 {
        use std::mem::size_of;

        // HashMap 真实实现细节与装载因子会影响开销；这里取"桶数组 + 控制字节"的保守估算。
        // 该值用于解释 RSS，目标是"不低估"，而不是字节级精确。
        let bucket = size_of::<(u64, OneOrManySpan)>() as u64;
        let ctrl = 16u64; // 经验保守常数：控制字节/对齐等摊销
        let map_bytes = self.map.capacity() as u64 * (bucket + ctrl);

        let mut many_bytes = 0u64;
        for v in self.map.values() {
            if let OneOrManySpan::Many(vs) = v {
                many_bytes += vs.capacity() as u64 * size_of::<Span>() as u64;
            }
        }

        self.arena.capacity() as u64 + map_bytes + many_bytes
    }
}

pub(super) fn path_arena_set_from_paths(paths: Vec<Vec<u8>>) -> PathArenaSet {
    let mut set = PathArenaSet::default();

    for path in paths {
        let _ = set.insert(&path);
    }
    set
}

pub(super) fn deleted_paths_stats(paths: &PathArenaSet) -> (usize, u64, u64) {
    (
        paths.len_paths(),
        paths.active_bytes(),
        paths.estimated_bytes(),
    )
}

pub(super) fn path_deleted_by_any(path_bytes: &[u8], deleted_sets: &[Arc<PathArenaSet>]) -> bool {
    deleted_sets.iter().any(|paths| paths.contains(path_bytes))
}
