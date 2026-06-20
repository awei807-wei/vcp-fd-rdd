// Core module: project-wide fundamental types and abstractions.
//
// Architecture note: Despite the name `rdd.rs` (borrowed from Spark RDD),
// this module is NOT an over-engineered abstraction — it defines the core data
// types (FileKey, FileMeta, FileKind, EventRecord, FileIdentifier) used
// throughout the entire project. The RDD/Partition/BuildRDD/FsScanRDD traits
// are only used by `l3_cold::IndexBuilder` for cold segment scanning, which is
// a legitimate use of the abstraction. No simplification needed at this time.

pub mod adaptive;
pub mod event_types;
pub mod partition;
pub mod rdd;

pub use adaptive::{AdaptiveScheduler, ExecutionStrategy, Task};
pub use event_types::{EventRecord, EventType, FileIdentifier};
pub use rdd::{BuildRDD, FileKey, FileKeyEntry, FileKind, FileMeta, FsScanRDD, Partition};
