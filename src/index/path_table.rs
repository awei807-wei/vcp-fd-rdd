//! 排序路径表 + front-encoding 差量压缩（参见 `重构方案包/causal-chain-report.md` §8.3.1）。
//!
//! 借鉴 plocate / locate 的思路：
//! 路径按字典序排序后相邻条目共享长前缀，第 i 条只存
//!   - `shared_len`：与第 i-1 条的共享前缀长度（varint）
//!   - `suffix_len`：新字节数（varint）
//!   - `suffix`：[u8; suffix_len]
//!
//! 每隔 `anchor_interval` 条存一个完整锚点（`shared_len = 0`），保证 O(1) 跳到锚点 +
//! 最多 (anchor_interval - 1) 步差量重放即可解出任意 idx 的完整字节。
//!
//! 本模块在 2A 脚手架中是**纯内存版**（`Vec<u8>` 基底）；§8.4 的 mmap v7 接入会在 2C
//! 阶段把 `data` 替换为 `Mmap` 引用，API 保持兼容。
//!
//! 设计权衡
//! - 锚点间隔 256：worst-case 解析开销 = 255 * (varint 解码 + memcpy)，
//!   plocate 实测在 4M 文件下可控；过大会拖慢随机访问，过小会让锚点表本身变大。
//! - varint 用 LEB128：我们工程上路径长度普遍 < 16K，1-2 字节即可。

/// 默认锚点间隔（条目数）。
pub const DEFAULT_ANCHOR_INTERVAL: u16 = 256;

/// 路径数据后端。
///
/// - `Owned(Vec<u8>)`：可读可写。builder 路径用这个；运行期 delta 不影响。
/// - `Mmap { arc, range }`：只读，借用一段 mmap 文件区间。8M 文件冷启动后 ~100MB
///   路径字节常驻 mmap 而不是 RSS——参见 `重构方案包/causal-chain-report.md` §8.4
///   第四阶段的"100-180MB RSS"承诺。Arc<Mmap> 持锁保活，PathTable drop 后引用归零
///   即可释放。
///
/// **不变量**：Mmap 变体不允许 [`PathTable::push`]——push 路径会 panic。这种表只
/// 通过 [`PathTable::from_mmap_parts`] 构造，给 v7 mmap 加载用。
#[derive(Debug, Clone)]
enum PathBytes {
    Owned(Vec<u8>),
    Mmap {
        _arc: std::sync::Arc<memmap2::Mmap>,
        range: std::ops::Range<usize>,
    },
}

impl Default for PathBytes {
    fn default() -> Self {
        Self::Owned(Vec::new())
    }
}

impl PathBytes {
    fn as_bytes(&self) -> &[u8] {
        match self {
            Self::Owned(v) => v.as_slice(),
            Self::Mmap { _arc, range } => &_arc.as_ref()[range.clone()],
        }
    }

    fn len(&self) -> usize {
        match self {
            Self::Owned(v) => v.len(),
            Self::Mmap { range, .. } => range.end - range.start,
        }
    }

    fn as_owned_mut(&mut self) -> &mut Vec<u8> {
        match self {
            Self::Owned(v) => v,
            Self::Mmap { .. } => {
                panic!("PathTable: cannot mutate mmap-backed data—construct via PathTable::new for builder")
            }
        }
    }
}

/// 排序路径表（builder 与 reader 一体）。
///
/// 内存布局
/// - `data`: 差量编码后的所有 entries 字节。可由 [`PathBytes::Owned`] 或
///   [`PathBytes::Mmap`] 后端持有；reader API 对两种来源透明。
/// - `anchors[i]` = 第 (i * anchor_interval) 条的字节起点（绝对 offset）。
/// - `count`: 条目总数。
#[derive(Debug, Clone, Default)]
pub struct PathTable {
    data: PathBytes,
    anchors: Vec<u32>,
    count: u32,
    anchor_interval: u16,
    /// 最后一条已写入的完整路径，用于 builder 阶段的 front-encoding。
    /// 在 reader-only 状态下值为空，不影响 `resolve`。
    last_full: Vec<u8>,
}

impl PathTable {
    /// 创建空表（默认锚点间隔 256）。
    pub fn new() -> Self {
        Self::with_anchor_interval(DEFAULT_ANCHOR_INTERVAL)
    }

    /// 自定义锚点间隔（必须 ≥ 1）。
    pub fn with_anchor_interval(anchor_interval: u16) -> Self {
        Self {
            data: PathBytes::Owned(Vec::new()),
            anchors: Vec::new(),
            count: 0,
            anchor_interval: anchor_interval.max(1),
            last_full: Vec::new(),
        }
    }

    /// 从已**字典序排序**的路径列表批量构建。
    /// 输入顺序错乱时仅会得到次优压缩——不会破坏 `resolve` 的正确性，因为 builder
    /// 仅与上一条做差量，但二分查找 [`find_prefix_range`] 会因此返回错误结果。
    pub fn build_sorted<I, P>(paths: I) -> Self
    where
        I: IntoIterator<Item = P>,
        P: AsRef<[u8]>,
    {
        let mut table = Self::new();
        for p in paths {
            table.push(p.as_ref());
        }
        table
    }

    /// 追加一条路径（必须 ≥ 上一条；本函数不检查）。
    /// 返回新条目的 idx。
    ///
    /// **panic**：若 `self.data` 是 mmap 后端（`PathTable::from_mmap_parts` 构造）则
    /// panic——mmap 表是只读的，调用方应该走 [`PathTable::new`] 那条 owned 路径。
    pub fn push(&mut self, path: &[u8]) -> u32 {
        let idx = self.count;
        let interval = self.anchor_interval as u32;

        // 锚点：第 0 条强制是锚点；每 interval 条放一个锚点。
        let is_anchor = idx.is_multiple_of(interval);
        let shared_len = if is_anchor {
            0
        } else {
            common_prefix_len(&self.last_full, path)
        };

        let data = self.data.as_owned_mut();
        if is_anchor {
            self.anchors.push(data.len() as u32);
        }

        let suffix = &path[shared_len..];
        write_varint(data, shared_len as u64);
        write_varint(data, suffix.len() as u64);
        data.extend_from_slice(suffix);

        // 更新 builder 的 last_full：先截到 shared_len，再追加 suffix。
        self.last_full.truncate(shared_len);
        self.last_full.extend_from_slice(suffix);

        self.count = self.count.saturating_add(1);
        idx
    }

    /// 路径数量。
    pub fn len(&self) -> u32 {
        self.count
    }

    pub fn is_empty(&self) -> bool {
        self.count == 0
    }

    pub fn anchor_interval(&self) -> u16 {
        self.anchor_interval
    }

    /// 编码字节大小。
    pub fn data_len(&self) -> usize {
        self.data.len()
    }

    /// 锚点表大小（每个锚点 4 字节 offset）。
    pub fn anchors_bytes(&self) -> usize {
        self.anchors.len() * 4
    }

    /// 是否 mmap 后端——给 memory_stats 用，区分 RSS 占用类型。
    pub fn is_mmap_backed(&self) -> bool {
        matches!(self.data, PathBytes::Mmap { .. })
    }

    /// v7 持久化用：直接拿到差量编码后的字节流。
    pub fn raw_data(&self) -> &[u8] {
        self.data.as_bytes()
    }

    /// v7 持久化用：直接拿到锚点表（绝对 offset 列表）。
    pub fn raw_anchors(&self) -> &[u32] {
        &self.anchors
    }

    /// v7 加载用：从原始 parts 反构造一个**只读**的 PathTable（owned 后端）。
    /// 仅做轻量校验：anchor_interval ≥ 1、anchors 数量 = ceil(count/anchor_interval)。
    /// 不重新走差量解码做完整性验证（那是 [`PathTable::resolve`] 在调用时按需做的）。
    /// `last_full` 留空——只读模式不会再 [`PathTable::push`]，留空不影响 `resolve`。
    pub fn from_parts(
        data: Vec<u8>,
        anchors: Vec<u32>,
        count: u32,
        anchor_interval: u16,
    ) -> Result<Self, &'static str> {
        Self::from_parts_inner(PathBytes::Owned(data), anchors, count, anchor_interval)
    }

    /// v7 mmap 加载用：data 区间借用一段 mmap，PathTable 仅持 [`std::sync::Arc`]
    /// 引用保活，不复制字节。
    ///
    /// `data_range` 必须在 `arc.as_ref()` 范围内。
    pub fn from_mmap_parts(
        arc: std::sync::Arc<memmap2::Mmap>,
        data_range: std::ops::Range<usize>,
        anchors: Vec<u32>,
        count: u32,
        anchor_interval: u16,
    ) -> Result<Self, &'static str> {
        if data_range.start > data_range.end {
            return Err("invalid data_range (start > end)");
        }
        if data_range.end > arc.as_ref().len() {
            return Err("data_range exceeds mmap length");
        }
        Self::from_parts_inner(
            PathBytes::Mmap {
                _arc: arc,
                range: data_range,
            },
            anchors,
            count,
            anchor_interval,
        )
    }

    fn from_parts_inner(
        data: PathBytes,
        anchors: Vec<u32>,
        count: u32,
        anchor_interval: u16,
    ) -> Result<Self, &'static str> {
        if anchor_interval == 0 {
            return Err("anchor_interval must be >= 1");
        }
        let interval = anchor_interval as u32;
        let expected_anchors = if count == 0 {
            0
        } else {
            count.div_ceil(interval) as usize
        };
        if anchors.len() != expected_anchors {
            return Err("anchors length mismatch");
        }
        let data_len = data.len();
        for &a in &anchors {
            if (a as usize) > data_len {
                return Err("anchor offset out of data range");
            }
        }
        Ok(Self {
            data,
            anchors,
            count,
            anchor_interval,
            last_full: Vec::new(),
        })
    }

    /// 顺序遍历整张表：对每条 (idx, path_bytes) 调一次 `f`。
    ///
    /// 与逐条 [`Self::resolve`] 比较：
    /// - 总开销 = O(data.len())：每个差量条目仅一次 varint 解码 + 一次 memcpy。
    /// - 逐条 resolve 总开销 = O(N × anchor_interval)：每次都要从最近锚点回放。
    ///
    /// 8M 条 / 默认 anchor_interval=256 时省 ≈ 20×。
    ///
    /// 内部复用一个 buffer（不分配每条 path）；`f` 拿到的 `&[u8]` 在下一次回调
    /// 前有效，跨调用使用必须自行 `.to_vec()`。idx 单调从 0 到 count-1。
    pub fn for_each_path<F: FnMut(u32, &[u8])>(&self, mut f: F) {
        let data = self.data.as_bytes();
        let mut current: Vec<u8> = Vec::new();
        let mut cursor = 0usize;
        for idx in 0..self.count {
            let Some((shared_len, n1)) = read_varint(&data[cursor..]) else {
                return;
            };
            cursor += n1;
            let Some((suffix_len, n2)) = read_varint(&data[cursor..]) else {
                return;
            };
            cursor += n2;
            let suffix_end = match cursor.checked_add(suffix_len as usize) {
                Some(e) if e <= data.len() => e,
                _ => return,
            };
            current.truncate(shared_len as usize);
            current.extend_from_slice(&data[cursor..suffix_end]);
            cursor = suffix_end;
            f(idx, &current);
        }
    }

    /// 解出第 idx 条完整路径字节。`idx ≥ len()` 时返回 None。
    ///
    /// 复杂度：O(idx mod anchor_interval)。
    pub fn resolve(&self, idx: u32) -> Option<Vec<u8>> {
        if idx >= self.count {
            return None;
        }

        let data = self.data.as_bytes();
        let interval = self.anchor_interval as u32;
        let anchor_idx = idx / interval;
        let anchor_pos = self.anchors.get(anchor_idx as usize).copied()? as usize;

        let mut cursor = anchor_pos;
        let mut current: Vec<u8> = Vec::new();

        let target_off_in_anchor = idx % interval;
        for _ in 0..=target_off_in_anchor {
            let (shared_len, n1) = read_varint(&data[cursor..])?;
            cursor += n1;
            let (suffix_len, n2) = read_varint(&data[cursor..])?;
            cursor += n2;
            let suffix_end = cursor.checked_add(suffix_len as usize)?;
            if suffix_end > data.len() {
                return None;
            }
            current.truncate(shared_len as usize);
            current.extend_from_slice(&data[cursor..suffix_end]);
            cursor = suffix_end;
        }

        Some(current)
    }

    /// 精确路径反查：返回 `path_idx` 等于 `path` 字节流的条目下标；不存在返回 None。
    ///
    /// 设计目的：替代 `path_hash_to_id` 这张运行时常驻的 `HashMap<u64, DocId>`。
    /// PathTable 已按字典序排序，二分查找直接复用 [`Self::lower_bound`]，省去
    /// 8M 文件场景下 ≈160MB 的 HashMap 开销。
    ///
    /// 复杂度：O(log(N) * anchor_interval / 2)；在 8M 文件 + interval=256 下
    /// 单次查询 ≈3000 个 varint 解码 + memcmp，几微秒级。
    pub fn find_path_idx(&self, path: &[u8]) -> Option<u32> {
        let idx = self.lower_bound(path);
        if idx >= self.count {
            return None;
        }
        let candidate = self.resolve(idx)?;
        if candidate.as_slice() == path {
            Some(idx)
        } else {
            None
        }
    }

    /// 二分定位 prefix 起始/终止 idx，返回 `[lo, hi)` 半开区间。
    ///
    /// `lo` = 第一个 ≥ prefix 的 idx；`hi` = 第一个 > prefix 的 idx（不含通配后缀）。
    /// 当没有任何条目以 prefix 开头时返回 `(some_idx, some_idx)`（空区间）。
    ///
    /// 复杂度：O(log(N/anchor_interval) * anchor_interval)。
    pub fn find_prefix_range(&self, prefix: &[u8]) -> (u32, u32) {
        let lo = self.lower_bound(prefix);
        let hi = self.upper_bound_with_prefix(prefix);
        (lo, hi)
    }

    /// 返回第一个 ≥ key 的 idx（不存在则返回 len）。
    fn lower_bound(&self, key: &[u8]) -> u32 {
        // 在锚点上二分；然后在锚点内线性扫。
        let mut lo = 0u32;
        let mut hi = self.count;
        while lo < hi {
            let mid = lo + (hi - lo) / 2;
            let mid_path = match self.resolve(mid) {
                Some(p) => p,
                None => return self.count,
            };
            if mid_path.as_slice() < key {
                lo = mid + 1;
            } else {
                hi = mid;
            }
        }
        lo
    }

    /// 返回第一个使 `path > prefix-block` 的 idx；语义上等价于
    /// "lower_bound(prefix 的字典序后继)"。
    fn upper_bound_with_prefix(&self, prefix: &[u8]) -> u32 {
        // prefix 后缀的字典序后继：在 prefix 末尾累加，遇到 0xFF 进位则上溯。
        let mut next = prefix.to_vec();
        while let Some(last) = next.last_mut() {
            if *last == 0xFF {
                next.pop();
            } else {
                *last += 1;
                return self.lower_bound(&next);
            }
        }
        // prefix 是 0xFF... 全是 0xFF，没有字典序后继 → 返回末尾。
        self.count
    }
}

fn common_prefix_len(a: &[u8], b: &[u8]) -> usize {
    let n = a.len().min(b.len());
    let mut i = 0usize;
    while i < n && a[i] == b[i] {
        i += 1;
    }
    i
}

fn write_varint(out: &mut Vec<u8>, mut v: u64) {
    while v >= 0x80 {
        out.push(((v & 0x7F) as u8) | 0x80);
        v >>= 7;
    }
    out.push(v as u8);
}

fn read_varint(buf: &[u8]) -> Option<(u64, usize)> {
    let mut v: u64 = 0;
    let mut shift = 0u32;
    for (i, &b) in buf.iter().enumerate() {
        if shift >= 64 {
            return None;
        }
        v |= u64::from(b & 0x7F) << shift;
        if b & 0x80 == 0 {
            return Some((v, i + 1));
        }
        shift += 7;
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;

    fn build(paths: &[&str]) -> PathTable {
        PathTable::build_sorted(paths.iter().map(|s| s.as_bytes()))
    }

    fn build_with_interval(paths: &[&str], interval: u16) -> PathTable {
        let mut t = PathTable::with_anchor_interval(interval);
        for p in paths {
            t.push(p.as_bytes());
        }
        t
    }

    #[test]
    fn empty_table() {
        let t = PathTable::new();
        assert_eq!(t.len(), 0);
        assert!(t.is_empty());
        assert!(t.resolve(0).is_none());
    }

    #[test]
    fn varint_roundtrip() {
        for v in [
            0u64,
            1,
            127,
            128,
            255,
            256,
            16_383,
            16_384,
            1_000_000,
            u64::MAX,
        ] {
            let mut buf = Vec::new();
            write_varint(&mut buf, v);
            let (got, n) = read_varint(&buf).unwrap();
            assert_eq!(got, v);
            assert_eq!(n, buf.len());
        }
    }

    #[test]
    fn common_prefix_len_basic() {
        assert_eq!(common_prefix_len(b"abc", b"abd"), 2);
        assert_eq!(common_prefix_len(b"abc", b"abc"), 3);
        assert_eq!(common_prefix_len(b"", b"abc"), 0);
        assert_eq!(common_prefix_len(b"abc", b"xyz"), 0);
        assert_eq!(common_prefix_len(b"abc", b"abcdef"), 3);
    }

    #[test]
    fn resolve_returns_inserted_paths() {
        let paths = ["/a", "/b", "/c"];
        let t = build(&paths);
        for (i, p) in paths.iter().enumerate() {
            assert_eq!(t.resolve(i as u32).unwrap(), p.as_bytes());
        }
        assert!(t.resolve(3).is_none());
    }

    #[test]
    fn front_encoding_compresses_shared_prefixes() {
        let paths = [
            "/home/shiyi/.bashrc",
            "/home/shiyi/.config/",
            "/home/shiyi/.config/fd-rdd/",
            "/home/shiyi/.config/fd-rdd/index.db",
            "/home/shiyi/.config/fd-rdd/wal.bin",
        ];
        let t = build(&paths);

        for (i, p) in paths.iter().enumerate() {
            assert_eq!(t.resolve(i as u32).unwrap(), p.as_bytes(), "idx={i}");
        }

        // 对比"全量绝对路径相加"应该明显更小。
        let raw: usize = paths.iter().map(|p| p.len()).sum();
        // varint(0) + varint(len) + bytes 也有开销，但深目录共享前缀后净节省。
        assert!(
            t.data_len() < raw,
            "expected compression: data_len={} raw={}",
            t.data_len(),
            raw
        );
    }

    #[test]
    fn for_each_path_yields_same_as_resolve() {
        let mut paths: Vec<String> = (0..500).map(|i| format!("/pkg/{:04}.txt", i)).collect();
        paths.sort();
        let t = build_with_interval(&paths.iter().map(|s| s.as_str()).collect::<Vec<_>>(), 16);

        let mut collected: Vec<(u32, Vec<u8>)> = Vec::with_capacity(paths.len());
        t.for_each_path(|idx, bytes| collected.push((idx, bytes.to_vec())));

        assert_eq!(collected.len(), paths.len());
        for (i, (idx, bytes)) in collected.iter().enumerate() {
            assert_eq!(*idx, i as u32);
            assert_eq!(bytes.as_slice(), paths[i].as_bytes());
            assert_eq!(t.resolve(*idx).unwrap(), *bytes);
        }
    }

    #[test]
    fn for_each_path_empty_table_no_callbacks() {
        let t = PathTable::new();
        let mut count = 0;
        t.for_each_path(|_, _| count += 1);
        assert_eq!(count, 0);
    }

    #[test]
    fn anchor_interval_bounds_resolve_walk() {
        // 短的锚点间隔保证大表也能解出。
        let mut paths: Vec<String> = (0..1000).map(|i| format!("/pkg/{:04}.txt", i)).collect();
        paths.sort();
        let t = build_with_interval(&paths.iter().map(|s| s.as_str()).collect::<Vec<_>>(), 16);

        for (i, p) in paths.iter().enumerate() {
            assert_eq!(t.resolve(i as u32).unwrap(), p.as_bytes(), "idx={i}");
        }

        // 锚点条数 = ceil(1000/16) = 63
        assert_eq!(t.anchors.len(), 1000_usize.div_ceil(16));
    }

    #[test]
    fn find_prefix_range_finds_subtree() {
        let mut paths: Vec<String> = vec![
            "/a/x.txt".into(),
            "/b/q.txt".into(),
            "/b/r.txt".into(),
            "/b/s/sub.txt".into(),
            "/c/foo.txt".into(),
        ];
        paths.sort();
        let t = build(&paths.iter().map(|s| s.as_str()).collect::<Vec<_>>());

        let (lo, hi) = t.find_prefix_range(b"/b/");
        let in_range: Vec<Vec<u8>> = (lo..hi).map(|i| t.resolve(i).unwrap()).collect();
        assert_eq!(in_range.len(), 3);
        for p in &in_range {
            assert!(p.starts_with(b"/b/"), "got: {}", String::from_utf8_lossy(p));
        }
    }

    #[test]
    fn find_prefix_range_handles_no_match() {
        let paths = ["/a/x", "/b/x"];
        let t = build(&paths);
        let (lo, hi) = t.find_prefix_range(b"/zz/");
        assert_eq!(lo, hi, "non-existent prefix should return empty range");
    }

    #[test]
    fn find_prefix_range_handles_empty_prefix() {
        let paths = ["/a", "/b", "/c"];
        let t = build(&paths);
        let (lo, hi) = t.find_prefix_range(b"");
        assert_eq!(lo, 0);
        assert_eq!(hi, 3);
    }

    #[test]
    fn find_prefix_range_handles_high_byte_terminator() {
        // 即使 prefix 末字节是 0xFF，find 也不能 panic。
        let mut t = PathTable::new();
        t.push(&[0xFF, 0xFF]);
        let (lo, hi) = t.find_prefix_range(&[0xFF]);
        assert_eq!(lo, 0);
        assert_eq!(hi, 1);
    }

    #[test]
    fn build_sorted_iter_works() {
        let t = PathTable::build_sorted(vec![b"/a".to_vec(), b"/b".to_vec()]);
        assert_eq!(t.len(), 2);
        assert_eq!(t.resolve(1).unwrap(), b"/b");
    }

    #[test]
    fn many_long_paths_resolve_correctly() {
        // Stress: 5000 long paths with shared prefixes + interval 64.
        let prefix = "/very/long/shared/prefix/that/is/much/larger/than/short/keys/";
        let mut paths: Vec<String> = (0..5000)
            .map(|i| format!("{}{:08}.dat", prefix, i))
            .collect();
        paths.sort();
        let t = build_with_interval(&paths.iter().map(|s| s.as_str()).collect::<Vec<_>>(), 64);

        for (i, p) in paths.iter().enumerate() {
            assert_eq!(t.resolve(i as u32).unwrap(), p.as_bytes(), "idx={i}");
        }

        // Compression sanity check: shared prefix saved a lot.
        let raw: usize = paths.iter().map(|p| p.len()).sum();
        assert!(
            t.data_len() < raw / 2,
            "expected at least 2x compression, got data_len={} raw={}",
            t.data_len(),
            raw
        );
    }

    #[test]
    fn from_parts_round_trips_resolve_and_prefix() {
        let paths = [
            "/home/a/x.txt",
            "/home/a/y.txt",
            "/home/b/log.txt",
            "/var/cache/foo",
        ];
        let original = build_with_interval(&paths, 2);

        // 模拟 v7 持久化：拿 raw_data + raw_anchors，反构造一份只读 PathTable。
        let restored = PathTable::from_parts(
            original.raw_data().to_vec(),
            original.raw_anchors().to_vec(),
            original.len(),
            original.anchor_interval(),
        )
        .unwrap();

        for i in 0..original.len() {
            assert_eq!(restored.resolve(i), original.resolve(i));
        }
        // prefix 查询也要保留。
        let (lo, hi) = restored.find_prefix_range(b"/home/");
        assert_eq!((lo, hi), original.find_prefix_range(b"/home/"));
        assert!(hi > lo);
    }

    #[test]
    fn from_parts_rejects_wrong_anchor_count() {
        let original = build_with_interval(&["/a", "/b", "/c"], 2);
        // 锚点应有 ceil(3/2) = 2 条；故意传 1 条，应失败。
        let err = PathTable::from_parts(
            original.raw_data().to_vec(),
            vec![original.raw_anchors()[0]],
            original.len(),
            original.anchor_interval(),
        )
        .unwrap_err();
        assert!(err.contains("anchors length"));
    }

    #[test]
    fn from_parts_rejects_zero_anchor_interval() {
        let err = PathTable::from_parts(Vec::new(), Vec::new(), 0, 0).unwrap_err();
        assert!(err.contains("anchor_interval"));
    }

    #[test]
    fn find_path_idx_returns_correct_index() {
        let paths = ["/a/x.txt", "/a/y.txt", "/b/log.txt", "/c/zzz"];
        let t = build_with_interval(&paths, 2);
        for (i, p) in paths.iter().enumerate() {
            assert_eq!(t.find_path_idx(p.as_bytes()), Some(i as u32));
        }
        assert_eq!(t.find_path_idx(b"/a/missing.txt"), None);
        assert_eq!(t.find_path_idx(b"/zzz/last_after_all"), None);
        assert_eq!(t.find_path_idx(b""), None);
    }

    #[test]
    fn find_path_idx_handles_anchor_boundary_collisions() {
        // 同一锚块内多条 + 跨锚块边界各一条：保证 lower_bound 落点正确。
        let paths: Vec<String> = (0..50).map(|i| format!("/p/{i:04}.dat")).collect();
        let t = build_with_interval(
            &paths.iter().map(|s| s.as_str()).collect::<Vec<_>>(),
            8,
        );
        for (i, p) in paths.iter().enumerate() {
            assert_eq!(t.find_path_idx(p.as_bytes()), Some(i as u32));
        }
        // 不存在的 prefix 不应误命中。
        assert_eq!(t.find_path_idx(b"/p/0050.dat"), None);
        assert_eq!(t.find_path_idx(b"/p/0000"), None); // shorter than any path
    }
}
