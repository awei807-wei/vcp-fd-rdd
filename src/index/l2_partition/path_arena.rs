use std::path::{Path, PathBuf};
use std::sync::Arc;

use crate::util::pathbuf_from_encoded_vec;

/// Path blob arena：所有路径的连续字节存储
#[derive(Clone, Debug, Default)]
pub struct PathArena {
    pub data: Arc<Vec<u8>>,
}

impl serde::Serialize for PathArena {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        self.data.as_ref().serialize(serializer)
    }
}

impl<'de> serde::Deserialize<'de> for PathArena {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let data = Vec::<u8>::deserialize(deserializer)?;
        Ok(PathArena {
            data: Arc::new(data),
        })
    }
}

impl PathArena {
    pub fn new() -> Self {
        Self {
            data: Arc::new(Vec::new()),
        }
    }

    pub fn push_bytes(&mut self, bytes: &[u8]) -> Option<(u32, u16)> {
        let len: u16 = match bytes.len().try_into() {
            Ok(len) => len,
            Err(_) => {
                tracing::warn!(
                    "Skipping indexed path longer than {} bytes: {} bytes",
                    u16::MAX,
                    bytes.len()
                );
                return None;
            }
        };
        let data = Arc::make_mut(&mut self.data);
        let off: u32 = data.len().try_into().ok()?;
        data.extend_from_slice(bytes);
        Some((off, len))
    }

    pub fn push_path(&mut self, path: &Path) -> Option<(u32, u16)> {
        let bytes = path.as_os_str().as_encoded_bytes();
        self.push_bytes(bytes)
    }

    pub fn get_bytes(&self, off: u32, len: u16) -> Option<&[u8]> {
        let start: usize = off as usize;
        let end: usize = start.checked_add(len as usize)?;
        self.data.get(start..end)
    }

    pub fn get_path_buf(&self, off: u32, len: u16) -> Option<PathBuf> {
        let bytes = self.get_bytes(off, len)?.to_vec();
        Some(pathbuf_from_encoded_vec(bytes))
    }
}
