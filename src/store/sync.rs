use super::{Store, StoreError};

/// Controls when a previously covered range should be fetched again.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct SyncPolicy {
    /// If set, coverage older than this many milliseconds is refreshed.
    /// `None` keeps historical coverage indefinitely.
    pub recent_refresh_ms: Option<i64>,
}

impl SyncPolicy {
    pub const fn historical() -> Self {
        Self {
            recent_refresh_ms: None,
        }
    }

    pub const fn refresh_recently(age_ms: i64) -> Self {
        Self {
            recent_refresh_ms: Some(age_ms),
        }
    }

    pub fn decide(
        self,
        store: &Store,
        metric: &str,
        from_ms: i64,
        to_ms: i64,
        now_ms: i64,
    ) -> Result<SyncDecision, StoreError> {
        if store.coverage_covers(metric, from_ms, to_ms, now_ms, self.recent_refresh_ms)? {
            Ok(SyncDecision::Skip)
        } else {
            Ok(SyncDecision::Fetch)
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SyncDecision {
    Fetch,
    Skip,
}
