// Core module: project-wide fundamental types and abstractions.
//
// Architecture note: Despite the name `rdd.rs` (borrowed from Spark RDD),
// this module is NOT an over-engineered abstraction — it defines the core data
// types (FileKey, FileMeta, FileKind, EventRecord, FileIdentifier) used
// throughout the entire project. The RDD/Partition/BuildRDD/FsScanRDD traits
// are only used by `l3_cold::IndexBuilder` for cold segment scanning, which is
// a legitimate use of the abstraction. No simplification needed at this time.
//
// `dag.rs` exists on disk but is NOT declared as a module here — it appears to
// be dead code from an earlier architecture experiment. Consider deleting it.

pub mod adaptive;
pub mod lineage;
pub mod partition;
pub mod rdd;

pub use adaptive::{AdaptiveScheduler, ExecutionStrategy, Task};
pub use lineage::{EventRecord, EventType, FileIdentifier};
pub use rdd::{
    BuildLineage, BuildRDD, FileKey, FileKeyEntry, FileKind, FileMeta, FsScanRDD, Partition,
};
