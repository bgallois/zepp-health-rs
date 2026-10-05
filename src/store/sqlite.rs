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
pub struct StoredSample {
    pub timestamp_ms: i64,
    pub source_record_key: Option<String>,
    pub value: Value,
}

#[derive(Debug, Clone)]
pub struct StoredRawRecord {
    pub record_key: String,
    pub observed_at_ms: Option<i64>,
    pub source_endpoint: String,
    pub source_device: Option<String>,
    pub source_timezone: Option<String>,
    pub payload: Value,
    pub fetched_at_ms: i64,
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

    #[cfg(feature = "intervals")]
    pub fn migrate_intervals(&self) -> Result<(), StoreError> {
        self.connection.execute_batch(
            "CREATE TABLE IF NOT EXISTS intervals_activities (
                activity_id TEXT PRIMARY KEY,
                start_ms INTEGER NOT NULL,
                end_ms INTEGER NOT NULL,
                sport_type TEXT,
                summary_json TEXT NOT NULL,
                fetched_at_ms INTEGER NOT NULL
            );
            CREATE INDEX IF NOT EXISTS idx_intervals_activities_start
                ON intervals_activities(start_ms);
            CREATE TABLE IF NOT EXISTS intervals_wellness (
                record_date TEXT PRIMARY KEY,
                payload_json TEXT NOT NULL,
                fetched_at_ms INTEGER NOT NULL
            );
            CREATE TABLE IF NOT EXISTS intervals_activity_streams (
                activity_id TEXT NOT NULL,
                stream_type TEXT NOT NULL,
                stream_json TEXT NOT NULL,
                fetched_at_ms INTEGER NOT NULL,
                PRIMARY KEY(activity_id, stream_type)
            );",
        )?;
        Ok(())
    }

    #[cfg(feature = "intervals")]
    pub fn put_intervals_wellness(
        &self,
        record_date: &str,
        payload: &Value,
        fetched_at_ms: i64,
    ) -> Result<(), StoreError> {
        self.migrate_intervals()?;
        self.connection.execute(
            "INSERT INTO intervals_wellness(record_date,payload_json,fetched_at_ms)
             VALUES (?1,?2,?3)
             ON CONFLICT(record_date) DO UPDATE SET payload_json=excluded.payload_json,
             fetched_at_ms=excluded.fetched_at_ms",
            params![record_date, serde_json::to_string(payload)?, fetched_at_ms],
        )?;
        Ok(())
    }

    #[cfg(feature = "intervals")]
    pub fn intervals_wellness_payloads(
        &self,
        from_date: &str,
        to_date: &str,
    ) -> Result<Vec<Value>, StoreError> {
        self.migrate_intervals()?;
        let mut stmt = self.connection.prepare(
            "SELECT payload_json FROM intervals_wellness
             WHERE record_date>=?1 AND record_date<=?2 ORDER BY record_date",
        )?;
        let rows = stmt.query_map(params![from_date, to_date], |row| {
            let raw: String = row.get(0)?;
            serde_json::from_str(&raw).map_err(|e| {
                rusqlite::Error::FromSqlConversionFailure(
                    0,
                    rusqlite::types::Type::Text,
                    Box::new(e),
                )
            })
        })?;
        Ok(rows.collect::<Result<Vec<_>, _>>()?)
    }

    #[cfg(feature = "intervals")]
    pub fn put_intervals_activity(
        &self,
        activity_id: &str,
        start_ms: i64,
        end_ms: i64,
        sport_type: Option<&str>,
        summary: &Value,
        fetched_at_ms: i64,
    ) -> Result<(), StoreError> {
        self.migrate_intervals()?;
        self.connection.execute(
            "INSERT INTO intervals_activities(activity_id,start_ms,end_ms,sport_type,summary_json,fetched_at_ms)
             VALUES (?1,?2,?3,?4,?5,?6)
             ON CONFLICT(activity_id) DO UPDATE SET start_ms=excluded.start_ms,end_ms=excluded.end_ms,
             sport_type=excluded.sport_type,summary_json=excluded.summary_json,fetched_at_ms=excluded.fetched_at_ms",
            params![activity_id, start_ms, end_ms, sport_type, serde_json::to_string(summary)?, fetched_at_ms],
        )?;
        Ok(())
    }

    #[cfg(feature = "intervals")]
    pub fn replace_intervals_streams(
        &self,
        activity_id: &str,
        streams: &[(String, Value)],
        fetched_at_ms: i64,
    ) -> Result<(), StoreError> {
        self.migrate_intervals()?;
        let tx = self.connection.unchecked_transaction()?;
        tx.execute(
            "DELETE FROM intervals_activity_streams WHERE activity_id=?1",
            [activity_id],
        )?;
        for (stream_type, value) in streams {
            tx.execute(
                "INSERT INTO intervals_activity_streams(activity_id,stream_type,stream_json,fetched_at_ms) VALUES (?1,?2,?3,?4)",
                params![activity_id, stream_type, serde_json::to_string(value)?, fetched_at_ms],
            )?;
        }
        tx.commit()?;
        Ok(())
    }

    #[cfg(feature = "intervals")]
    pub fn intervals_activity_payloads(
        &self,
        from_ms: i64,
        to_ms: i64,
    ) -> Result<Vec<Value>, StoreError> {
        self.migrate_intervals()?;
        let mut stmt = self.connection.prepare("SELECT summary_json FROM intervals_activities WHERE start_ms<=?2 AND end_ms>=?1 ORDER BY start_ms")?;
        let rows = stmt.query_map(params![from_ms, to_ms], |row| {
            let raw: String = row.get(0)?;
            serde_json::from_str(&raw).map_err(|e| {
                rusqlite::Error::FromSqlConversionFailure(
                    0,
                    rusqlite::types::Type::Text,
                    Box::new(e),
                )
            })
        })?;
        Ok(rows.collect::<Result<Vec<_>, _>>()?)
    }

    #[cfg(feature = "intervals")]
    pub fn intervals_stream_payloads(
        &self,
        activity_id: &str,
    ) -> Result<Vec<(String, Value)>, StoreError> {
        self.migrate_intervals()?;
        let mut stmt = self.connection.prepare("SELECT stream_type,stream_json FROM intervals_activity_streams WHERE activity_id=?1 ORDER BY stream_type")?;
        let rows = stmt.query_map([activity_id], |row| {
            let kind: String = row.get(0)?;
            let raw: String = row.get(1)?;
            let value = serde_json::from_str(&raw).map_err(|e| {
                rusqlite::Error::FromSqlConversionFailure(
                    1,
                    rusqlite::types::Type::Text,
                    Box::new(e),
                )
            })?;
            Ok((kind, value))
        })?;
        Ok(rows.collect::<Result<Vec<_>, _>>()?)
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

    pub fn raw_records(
        &self,
        metric: &str,
        from_ms: i64,
        to_ms: i64,
    ) -> Result<Vec<StoredRawRecord>, StoreError> {
        let mut statement = self.connection.prepare(
            "SELECT record_key, observed_at_ms, source_endpoint, source_device,
                    source_timezone, payload_json, fetched_at_ms
             FROM raw_records
             WHERE metric=?1 AND (observed_at_ms IS NULL OR
                    (observed_at_ms>=?2 AND observed_at_ms<=?3))
             ORDER BY observed_at_ms, record_key",
        )?;
        let rows = statement.query_map(params![metric, from_ms, to_ms], |row| {
            let payload_json: String = row.get(5)?;
            let payload = serde_json::from_str(&payload_json).map_err(|error| {
                rusqlite::Error::FromSqlConversionFailure(
                    5,
                    rusqlite::types::Type::Text,
                    Box::new(error),
                )
            })?;
            Ok(StoredRawRecord {
                record_key: row.get(0)?,
                observed_at_ms: row.get(1)?,
                source_endpoint: row.get(2)?,
                source_device: row.get(3)?,
                source_timezone: row.get(4)?,
                payload,
                fetched_at_ms: row.get(6)?,
            })
        })?;
        Ok(rows.collect::<Result<Vec<_>, _>>()?)
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

    pub fn samples(
        &self,
        metric: &str,
        from_ms: i64,
        to_ms: i64,
    ) -> Result<Vec<StoredSample>, StoreError> {
        let mut statement = self.connection.prepare(
            "SELECT timestamp_ms, source_record_key, value_json
             FROM samples WHERE metric=?1 AND timestamp_ms>=?2 AND timestamp_ms<=?3
             ORDER BY timestamp_ms",
        )?;
        let rows = statement.query_map(params![metric, from_ms, to_ms], |row| {
            let value_json: String = row.get(2)?;
            let value = serde_json::from_str(&value_json).map_err(|error| {
                rusqlite::Error::FromSqlConversionFailure(
                    2,
                    rusqlite::types::Type::Text,
                    Box::new(error),
                )
            })?;
            Ok(StoredSample {
                timestamp_ms: row.get(0)?,
                source_record_key: row.get(1)?,
                value,
            })
        })?;
        Ok(rows.collect::<Result<Vec<_>, _>>()?)
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
