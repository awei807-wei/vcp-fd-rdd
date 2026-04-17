use crate::core::FileMeta;
use crate::query::dsl::{CmpOp, DateRange, EntryKind, SizeFilter};

#[derive(Debug, Clone)]
pub(crate) enum Filter {
    ExtAny(Vec<Vec<u8>>),
    Size(SizeFilter),
    DateModified(DateRange),
    DateCreated(DateRange),
    DateAccessed(DateRange),
    Parent(String),
    Depth(CmpOp, usize),
    NameLen(CmpOp, usize),
    EntryType(EntryKind),
    Content(String),
}

impl Filter {
    pub(crate) fn matches(&self, meta: &FileMeta) -> bool {
        match self {
            Filter::ExtAny(exts) => {
                let Some(ext) = meta.path.extension() else {
                    return false;
                };
                let ext = ext.to_string_lossy();
                let ext_lc = ascii_lower_bytes(ext.as_bytes());
                exts.contains(&ext_lc)
            }
            Filter::Size(sf) => apply_cmp(sf.op, meta.size, sf.bytes),
            Filter::DateModified(dr) => {
                let Some(t) = meta.mtime else {
                    return false;
                };
                t >= dr.start && t < dr.end
            }
            Filter::DateCreated(dr) => {
                let Some(t) = meta.ctime else {
                    return false;
                };
                t >= dr.start && t < dr.end
            }
            Filter::DateAccessed(dr) => {
                let Some(t) = meta.atime else {
                    return false;
                };
                t >= dr.start && t < dr.end
            }
            Filter::Parent(parent) => meta
                .path
                .parent()
                .map(|p| p.to_string_lossy().eq_ignore_ascii_case(parent))
                .unwrap_or(false),
            Filter::Depth(op, n) => {
                let s = meta.path.to_string_lossy();
                let depth = s.matches('/').count() + s.matches('\\').count();
                apply_cmp(*op, depth as u64, *n as u64)
            }
            Filter::NameLen(op, n) => {
                let len = meta.path.file_name().map(|f| f.len()).unwrap_or(0);
                apply_cmp(*op, len as u64, *n as u64)
            }
            Filter::EntryType(kind) => match kind {
                EntryKind::File => true,
                EntryKind::Folder => false,
            },
            Filter::Content(keyword) => {
                tracing::warn!(
                    "Content filter is not yet implemented (keyword: {}). Returning false.",
                    keyword
                );
                false
            }
        }
    }
}

pub(crate) fn apply_cmp(op: CmpOp, lhs: u64, rhs: u64) -> bool {
    match op {
        CmpOp::Lt => lhs < rhs,
        CmpOp::Le => lhs <= rhs,
        CmpOp::Eq => lhs == rhs,
        CmpOp::Ge => lhs >= rhs,
        CmpOp::Gt => lhs > rhs,
    }
}

pub(crate) fn ascii_lower_bytes(bytes: &[u8]) -> Vec<u8> {
    bytes.iter().map(|b| b.to_ascii_lowercase()).collect()
}

pub(crate) fn normalize_ext_list(exts: &[String]) -> Vec<Vec<u8>> {
    exts.iter()
        .map(|s| s.trim().trim_start_matches('.'))
        .filter(|s| !s.is_empty())
        .map(|s| ascii_lower_bytes(s.as_bytes()))
        .collect()
}

pub(crate) fn doc_exts() -> Vec<Vec<u8>> {
    normalize_ext_list(&[
        "txt".into(),
        "md".into(),
        "markdown".into(),
        "pdf".into(),
        "doc".into(),
        "docx".into(),
        "xls".into(),
        "xlsx".into(),
        "ppt".into(),
        "pptx".into(),
        "rtf".into(),
        "odt".into(),
        "ods".into(),
        "odp".into(),
        "epub".into(),
        "csv".into(),
    ])
}

pub(crate) fn pic_exts() -> Vec<Vec<u8>> {
    normalize_ext_list(&[
        "jpg".into(),
        "jpeg".into(),
        "png".into(),
        "gif".into(),
        "bmp".into(),
        "webp".into(),
        "tif".into(),
        "tiff".into(),
        "heic".into(),
        "heif".into(),
        "svg".into(),
        "ico".into(),
    ])
}

pub(crate) fn video_exts() -> Vec<Vec<u8>> {
    normalize_ext_list(&[
        "mp4".into(),
        "mkv".into(),
        "mov".into(),
        "avi".into(),
        "webm".into(),
        "flv".into(),
        "m4v".into(),
        "mpg".into(),
        "mpeg".into(),
        "wmv".into(),
        "3gp".into(),
        "ts".into(),
    ])
}
