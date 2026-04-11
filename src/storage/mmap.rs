use memmap2::MmapMut;
use std::fs::OpenOptions;
use std::path::Path;

pub struct MmapStorage;

impl MmapStorage {
    pub fn open_mut<P: AsRef<Path>>(path: P, size: u64) -> anyhow::Result<MmapMut> {
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(path)?;
        file.set_len(size)?;
        let mmap = unsafe { MmapMut::map_mut(&file)? };
        Ok(mmap)
    }
}

impl crate::storage::traits::MmapOpen for MmapStorage {
    fn open_mut(&self, path: &std::path::Path, size: u64) -> anyhow::Result<memmap2::MmapMut> {
        MmapStorage::open_mut(path, size)
    }
}
