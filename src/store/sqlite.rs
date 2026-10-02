use rusqlite::{Connection, OptionalExtension, params};
use serde::Serialize;
use serde_json::Value;
use thiserror::Error;

const SCHEMA: &str = r#"
CREATE TABLE IF NOT EXISTS schema_migrations (
    version INTEGER PRIMARY KEY,
    applied_at_ms INTEGER NOT NULL
);
CREATE TABLE IF NOT EXISTS raw_records (
    metric TEXT NOT NULL,
    record_key TEXT NOT NULL,
    observed_at_ms INTEGER,
    source_endpoint TEXT NOT NULL,
    source_device TEXT,
    source_timezone TEXT,
    payload_json TEXT NOT NULL,
    fetched_at_ms INTEGER NOT NULL,
    PRIMARY KEY (metric, record_key)
);
CREATE TABLE IF NOT EXISTS samples (
    metric TEXT NOT NULL,
    timestamp_ms INTEGER NOT NULL,
    source_record_key TEXT,
    value_json TEXT NOT NULL,
    PRIMARY KEY (metric, timestamp_ms)
);
CREATE TABLE IF NOT EXISTS coverage (
    metric TEXT NOT NULL,
    from_ms INTEGER NOT NULL,
    to_ms INTEGER NOT NULL,
    source_endpoint TEXT NOT NULL,
    synced_at_ms INTEGER NOT NULL,
    PRIMARY KEY (metric, from_ms, to_ms)
);
CREATE INDEX IF NOT EXISTS idx_raw_records_metric_observed
    ON raw_records(metric, observed_at_ms);
CREATE INDEX IF NOT EXISTS idx_samples_metric_timestamp
    ON samples(metric, timestamp_ms);
CREATE INDEX IF NOT EXISTS idx_coverage_metric_range
    ON coverage(metric, from_ms, to_ms);
"#;

#[derive(Debug, Error)]
pub enum StoreError {
    #[error("SQLite error: {0}")]
    Sqlite(#[from] rusqlite::Error),
    #[error("JSON serialization error: {0}")]
    Json(#[from] serde_json::Error),
    #[error("invalid coverage range: from_ms must be <= to_ms")]
    InvalidCoverageRange,
}

#[derive(Debug, Clone)]
pub struct RawRecord {
    pub metric: String,
    pub record_key: String,
    pub observed_at_ms: Option<i64>,
    pub source_endpoint: String,
    pub source_device: Option<String>,
    pub source_timezone: Option<String>,
    pub payload: Value,
    pub fetched_at_ms: i64,
}

#[derive(Debug, Clone, Serialize)]
pub struct SampleRecord {
    pub metric: String,
    pub timestamp_ms: i64,
    pub source_record_key: Option<String>,
    pub value: Value,
}

#[derive(Debug, Clone)]
pub struct CoverageRange {
    pub metric: String,
    pub from_ms: i64,
    pub to_ms: i64,
    pub source_endpoint: String,
    pub synced_at_ms: i64,
}

#[derive(Debug)]
pub struct Store {
    connection: Connection,
}

impl Store {
    pub fn open(path: impl AsRef<std::path::Path>) -> Result<Self, StoreError> {
        let connection = Connection::open(path)?;
        let store = Self { connection };
        store.migrate()?;
        Ok(store)
    }

    pub fn open_in_memory() -> Result<Self, StoreError> {
        let connection = Connection::open_in_memory()?;
        let store = Self { connection };
        store.migrate()?;
        Ok(store)
    }

    pub fn migrate(&self) -> Result<(), StoreError> {
        self.connection.execute_batch(SCHEMA)?;
        self.connection.execute(
            "INSERT OR IGNORE INTO schema_migrations(version, applied_at_ms) VALUES (1, ?1)",
            [now_ms()],
        )?;
        Ok(())
    }

    pub fn put_raw_record(&self, record: &RawRecord) -> Result<(), StoreError> {
        let payload = serde_json::to_string(&record.payload)?;
        self.connection.execute(
            "INSERT INTO raw_records
             (metric, record_key, observed_at_ms, source_endpoint, source_device,
              source_timezone, payload_json, fetched_at_ms)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)
             ON CONFLICT(metric, record_key) DO UPDATE SET
               observed_at_ms=excluded.observed_at_ms,
               source_endpoint=excluded.source_endpoint,
               source_device=excluded.source_device,
               source_timezone=excluded.source_timezone,
               payload_json=excluded.payload_json,
               fetched_at_ms=excluded.fetched_at_ms",
            params![
                record.metric,
                record.record_key,
                record.observed_at_ms,
                record.source_endpoint,
                record.source_device,
                record.source_timezone,
                payload,
                record.fetched_at_ms,
            ],
        )?;
        Ok(())
    }

    pub fn put_sample(&self, sample: &SampleRecord) -> Result<(), StoreError> {
        let value = serde_json::to_string(&sample.value)?;
        self.connection.execute(
            "INSERT INTO samples(metric, timestamp_ms, source_record_key, value_json)
             VALUES (?1, ?2, ?3, ?4)
             ON CONFLICT(metric, timestamp_ms) DO UPDATE SET
               source_record_key=excluded.source_record_key,
               value_json=excluded.value_json",
            params![
                sample.metric,
                sample.timestamp_ms,
                sample.source_record_key,
                value
            ],
        )?;
        Ok(())
    }

    pub fn mark_coverage(&self, range: &CoverageRange) -> Result<(), StoreError> {
        if range.from_ms > range.to_ms {
            return Err(StoreError::InvalidCoverageRange);
        }
        self.connection.execute(
            "INSERT INTO coverage(metric, from_ms, to_ms, source_endpoint, synced_at_ms)
             VALUES (?1, ?2, ?3, ?4, ?5)
             ON CONFLICT(metric, from_ms, to_ms) DO UPDATE SET
               source_endpoint=excluded.source_endpoint,
               synced_at_ms=excluded.synced_at_ms",
            params![
                range.metric,
                range.from_ms,
                range.to_ms,
                range.source_endpoint,
                range.synced_at_ms,
            ],
        )?;
        Ok(())
    }

    /// Returns true only when an existing range fully covers the request.
    /// `refresh_after_ms` makes recent ranges expire deliberately.
    pub fn coverage_covers(
        &self,
        metric: &str,
        from_ms: i64,
        to_ms: i64,
        now_ms: i64,
        refresh_after_ms: Option<i64>,
    ) -> Result<bool, StoreError> {
        if from_ms > to_ms {
            return Err(StoreError::InvalidCoverageRange);
        }
        let minimum_sync = refresh_after_ms.map(|age| now_ms.saturating_sub(age));
        let found: Option<i64> = self
            .connection
            .query_row(
                "SELECT synced_at_ms FROM coverage
                 WHERE metric=?1 AND from_ms<=?2 AND to_ms>=?3
                   AND (?4 IS NULL OR synced_at_ms>=?4)
                 ORDER BY (to_ms-from_ms) ASC LIMIT 1",
                params![metric, from_ms, to_ms, minimum_sync],
                |row| row.get(0),
            )
            .optional()?;
        Ok(found.is_some())
    }

    pub fn count_samples(&self, metric: &str) -> Result<i64, StoreError> {
        Ok(self.connection.query_row(
            "SELECT COUNT(*) FROM samples WHERE metric=?1",
            [metric],
            |row| row.get(0),
        )?)
    }

    pub fn count_raw_records(&self, metric: &str) -> Result<i64, StoreError> {
        Ok(self.connection.query_row(
            "SELECT COUNT(*) FROM raw_records WHERE metric=?1",
            [metric],
            |row| row.get(0),
        )?)
    }
}

fn now_ms() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("system clock must be after Unix epoch")
        .as_millis() as i64
}
