#[cfg(feature = "mimalloc")]
#[global_allocator]
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;

pub mod core;
pub mod event;
pub mod index;
pub mod query;
pub mod stats;
pub mod storage;
