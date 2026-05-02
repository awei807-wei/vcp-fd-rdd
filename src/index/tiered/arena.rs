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

    pub(crate) fn clear(&mut self) {
        self.map.clear();
        self.arena.clear();
        self.paths_len = 0;
        self.active_bytes = 0;
    }
}

pub(super) fn path_deleted_by_any(path_bytes: &[u8], deleted_sets: &[Arc<PathArenaSet>]) -> bool {
    deleted_sets.iter().any(|paths| paths.contains(path_bytes))
}
