//! Local SQLite persistence for Zepp data.
//!
//! The store deliberately accepts provider-shaped JSON and explicit source
//! metadata. This preserves high-resolution payloads without making the
//! future provider-independent health API depend on Zepp's schema.

mod sqlite;
mod sync;

pub use sqlite::{
    CoverageRange, RawRecord, SampleRecord, Store, StoreError, StoredRawRecord, StoredSample,
};
pub use sync::{SyncDecision, SyncPolicy};
