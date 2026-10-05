//! Optional Intervals provider used by the stable health/MCP layer.
//!
//! This module uses an Intervals.icu personal API key through HTTP Basic auth.
//! OAuth refresh and write operations are outside the scope of this crate.

use std::{
    path::{Path, PathBuf},
    time::{SystemTime, UNIX_EPOCH},
};

use reqwest::{Client, StatusCode};
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};
use thiserror::Error;

use crate::health::{
    CacheMode, DataProvenance, ExternalWellnessRecord, ExternalWellnessSource, HealthError,
    TimeRange,
};
use crate::store::{Store, StoreError};

const API_ROOT: &str = "https://intervals.icu/api/v1";
const DEFAULT_STREAMS: &[&str] = &[
    "time",
    "heartrate",
    "watts",
    "cadence",
    "distance",
    "altitude",
    "velocity_smooth",
    "temp",
];

#[derive(Debug, Error)]
pub enum IntervalsError {
    #[error("INTERVALS_TOKEN is not configured")]
    MissingToken,
    #[error("Intervals request failed: {0}")]
    Transport(#[from] reqwest::Error),
    #[error("Intervals returned HTTP status {0}")]
    HttpStatus(StatusCode),
    #[error("Intervals JSON error: {0}")]
    Json(#[from] serde_json::Error),
    #[error("Intervals storage error: {0}")]
    Store(#[from] StoreError),
    #[error("invalid Intervals data: {0}")]
    InvalidData(String),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ActivitySummary {
    pub source: String,
    pub source_id: String,
    pub start_ms: i64,
    pub end_ms: i64,
    pub sport_type: Option<String>,
    pub fields: Map<String, Value>,
    pub related_zepp_ids: Vec<String>,
    pub match_heuristic: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ActivityStream {
    pub source: String,
    pub activity_id: String,
    pub stream_type: String,
    pub stream: Value,
}

#[derive(Debug)]
pub struct IntervalsHealthClient {
    http: Client,
    token: String,
    store: Store,
    #[allow(dead_code)]
    db_path: PathBuf,
}

impl IntervalsHealthClient {
    pub fn from_env(path: impl AsRef<Path>) -> Result<Option<Self>, IntervalsError> {
        let Some(token) = std::env::var_os("INTERVALS_TOKEN") else {
            return Ok(None);
        };
        let token = token.to_string_lossy().trim().to_owned();
        if token.is_empty() {
            return Ok(None);
        }
        let db_path = path.as_ref().to_path_buf();
        let store = Store::open(&db_path)?;
        store.migrate_intervals()?;
        Ok(Some(Self {
            http: Client::new(),
            token,
            store,
            db_path,
        }))
    }

    pub async fn activities(
        &self,
        from_ms: i64,
        to_ms: i64,
        cache_mode: &str,
    ) -> Result<Vec<ActivitySummary>, IntervalsError> {
        if from_ms > to_ms {
            return Err(IntervalsError::InvalidData(
                "activity range start_ms must be <= end_ms".into(),
            ));
        }
        if cache_mode != "refresh" {
            let cached = self.store.intervals_activity_payloads(from_ms, to_ms)?;
            if !cached.is_empty() || cache_mode == "cache_only" {
                return cached.into_iter().map(parse_activity).collect();
            }
        }
        if cache_mode == "cache_only" {
            return Ok(Vec::new());
        }
        let mut all = Vec::new();
        let response = self
            .http
            .get(format!("{API_ROOT}/athlete/0/activities"))
            .basic_auth("API_KEY", Some(&self.token))
            .query(&[
                ("oldest", date_from_ms(from_ms)),
                ("newest", date_from_ms(to_ms)),
                ("limit", "1000".to_owned()),
            ])
            .send()
            .await?;
        let response = response.error_for_status().map_err(|e| {
            e.status()
                .map(IntervalsError::HttpStatus)
                .unwrap_or_else(|| IntervalsError::Transport(e))
        })?;
        let values: Vec<Value> = response.json().await?;
        if !values.is_empty() {
            let fetched_at = now_ms();
            for value in values {
                let parsed = parse_activity(value.clone())?;
                self.store.put_intervals_activity(
                    &parsed.source_id,
                    parsed.start_ms,
                    parsed.end_ms,
                    parsed.sport_type.as_deref(),
                    &value,
                    fetched_at,
                )?;
                all.push(parsed);
            }
        }
        all.sort_by_key(|activity| activity.start_ms);
        Ok(all)
    }

    pub async fn activity_streams(
        &self,
        activity_id: &str,
        stream_types: Option<&[String]>,
        cache_mode: &str,
    ) -> Result<Vec<ActivityStream>, IntervalsError> {
        if cache_mode != "refresh" {
            let cached = self.store.intervals_stream_payloads(activity_id)?;
            if !cached.is_empty() || cache_mode == "cache_only" {
                return Ok(cached
                    .into_iter()
                    .map(|(kind, stream)| ActivityStream {
                        source: "intervals".into(),
                        activity_id: activity_id.into(),
                        stream_type: kind,
                        stream,
                    })
                    .collect());
            }
        }
        if cache_mode == "cache_only" {
            return Ok(Vec::new());
        }
        let keys = stream_types
            .map(|items| items.join(","))
            .unwrap_or_else(|| DEFAULT_STREAMS.join(","));
        let response = self
            .http
            .get(format!("{API_ROOT}/activity/{activity_id}/streams.json"))
            .basic_auth("API_KEY", Some(&self.token))
            .query(&[("types", keys)])
            .send()
            .await?;
        let response = response.error_for_status().map_err(|e| {
            e.status()
                .map(IntervalsError::HttpStatus)
                .unwrap_or_else(|| IntervalsError::Transport(e))
        })?;
        let payload: Value = response.json().await?;
        let pairs = stream_pairs(payload)?;
        self.store
            .replace_intervals_streams(activity_id, &pairs, now_ms())?;
        Ok(pairs
            .into_iter()
            .map(|(kind, stream)| ActivityStream {
                source: "intervals".into(),
                activity_id: activity_id.into(),
                stream_type: kind,
                stream,
            })
            .collect())
    }

    pub async fn wellness(
        &self,
        metric: &str,
        range: TimeRange,
        mode: CacheMode,
    ) -> Result<Vec<ExternalWellnessRecord>, IntervalsError> {
        let from_date = date_from_ms(range.from_ms);
        let to_date = date_from_ms(range.to_ms);
        let field = wellness_field(metric);
        if mode != CacheMode::Refresh {
            let cached = self
                .store
                .intervals_wellness_payloads(&from_date, &to_date)?;
            if !cached.is_empty() || mode == CacheMode::CacheOnly {
                return wellness_records(metric, field, cached);
            }
        }
        if mode == CacheMode::CacheOnly {
            return Ok(Vec::new());
        }
        let response = self
            .http
            .get(format!("{API_ROOT}/athlete/0/wellness"))
            .basic_auth("API_KEY", Some(&self.token))
            .query(&[("oldest", from_date.clone()), ("newest", to_date.clone())])
            .send()
            .await?;
        let response = response.error_for_status().map_err(|e| {
            e.status()
                .map(IntervalsError::HttpStatus)
                .unwrap_or_else(|| IntervalsError::Transport(e))
        })?;
        let values: Vec<Value> = response.json().await?;
        let fetched_at = now_ms();
        for value in &values {
            let date = value.get("id").and_then(Value::as_str).ok_or_else(|| {
                IntervalsError::InvalidData("wellness record has no id date".into())
            })?;
            self.store.put_intervals_wellness(date, value, fetched_at)?;
        }
        wellness_records(metric, field, values)
    }
}

impl ExternalWellnessSource for IntervalsHealthClient {
    fn fetch_wellness<'a>(
        &'a self,
        metric: &'a str,
        range: TimeRange,
        mode: CacheMode,
    ) -> std::pin::Pin<
        Box<
            dyn std::future::Future<Output = Result<Vec<ExternalWellnessRecord>, HealthError>> + 'a,
        >,
    > {
        Box::pin(async move {
            self.wellness(metric, range, mode)
                .await
                .map_err(|error| HealthError::External(error.to_string()))
        })
    }
}

fn wellness_field(metric: &str) -> Option<&str> {
    match metric {
        "resting_heart_rate" => Some("restingHR"),
        "sleep_hrv" => Some("hrv"),
        "weight" => Some("weight"),
        "sleep_score" => Some("sleepScore"),
        "sleep_duration" | "sleep" => Some("sleepSecs"),
        "avg_sleeping_hr" => Some("avgSleepingHR"),
        "sleep_quality" => Some("sleepQuality"),
        "soreness" => Some("soreness"),
        "fatigue" => Some("fatigue"),
        "stress" => Some("stress"),
        "mood" => Some("mood"),
        "motivation" => Some("motivation"),
        _ => None,
    }
}

fn wellness_records(
    metric: &str,
    field: Option<&str>,
    values: Vec<Value>,
) -> Result<Vec<ExternalWellnessRecord>, IntervalsError> {
    let Some(field) = field else {
        return Ok(Vec::new());
    };
    values
        .into_iter()
        .filter_map(|value| {
            let object = value.as_object()?.clone();
            let date = object.get("id")?.as_str()?.to_owned();
            let value = object.get(field)?.clone();
            if value.is_null() {
                return None;
            }
            Some(Ok(ExternalWellnessRecord {
                metric: metric.to_owned(),
                record_date: date.clone(),
                timestamp_ms: None,
                value: Some(value),
                fields: object,
                provenance: DataProvenance {
                    provider: "intervals".into(),
                    external: true,
                    device_id: None,
                    device_label: Some("Intervals.icu".into()),
                    record_date: Some(date),
                },
            }))
        })
        .collect()
}

fn parse_activity(value: Value) -> Result<ActivitySummary, IntervalsError> {
    let object = value
        .as_object()
        .ok_or_else(|| IntervalsError::InvalidData("activity is not an object".into()))?;
    let source_id = object
        .get("id")
        .and_then(value_string)
        .ok_or_else(|| IntervalsError::InvalidData("activity has no id".into()))?;
    let start = object
        .get("start_date_local")
        .or_else(|| object.get("start_date"))
        .and_then(Value::as_str)
        .ok_or_else(|| IntervalsError::InvalidData("activity has no start date".into()))?;
    let start_ms = parse_rfc3339_ms(start)?;
    let elapsed = object
        .get("elapsed_time")
        .or_else(|| object.get("moving_time"))
        .and_then(Value::as_i64)
        .unwrap_or(0)
        .max(0);
    let sport_type = object
        .get("sport_type")
        .or_else(|| object.get("type"))
        .and_then(Value::as_str)
        .map(str::to_owned);
    Ok(ActivitySummary {
        source: "intervals".into(),
        source_id,
        start_ms,
        end_ms: start_ms + elapsed * 1000,
        sport_type,
        fields: object.clone(),
        related_zepp_ids: Vec::new(),
        match_heuristic: false,
    })
}

fn stream_pairs(payload: Value) -> Result<Vec<(String, Value)>, IntervalsError> {
    if let Some(object) = payload.as_object() {
        return Ok(object
            .iter()
            .map(|(key, value)| (key.clone(), value.clone()))
            .collect());
    }
    let Some(array) = payload.as_array() else {
        return Err(IntervalsError::InvalidData(
            "stream response is neither object nor array".into(),
        ));
    };
    array
        .iter()
        .filter_map(|value| {
            let object = value.as_object()?;
            let kind = object.get("type").and_then(Value::as_str)?.to_owned();
            Some((kind, value.clone()))
        })
        .collect::<Vec<_>>()
        .pipe(Ok)
}

fn value_string(value: &Value) -> Option<String> {
    value
        .as_str()
        .map(str::to_owned)
        .or_else(|| value.as_i64().map(|v| v.to_string()))
}

fn parse_rfc3339_ms(value: &str) -> Result<i64, IntervalsError> {
    let bytes = value.as_bytes();
    if bytes.len() < 19 {
        return Err(IntervalsError::InvalidData(format!(
            "invalid start_date: {value}"
        )));
    }
    let year = parse_digits(&bytes[0..4])? as i64;
    let month = parse_digits(&bytes[5..7])? as i64;
    let day = parse_digits(&bytes[8..10])? as i64;
    let hour = parse_digits(&bytes[11..13])? as i64;
    let minute = parse_digits(&bytes[14..16])? as i64;
    let second = parse_digits(&bytes[17..19])? as i64;
    let mut offset = 0i64;
    let mut fraction_len = 0usize;
    if bytes.get(19) == Some(&b'.') {
        while bytes
            .get(20 + fraction_len)
            .is_some_and(|b| b.is_ascii_digit())
        {
            fraction_len += 1;
        }
    }
    let tz_index = if bytes.get(19) == Some(&b'.') {
        20 + fraction_len
    } else {
        19
    };
    if let Some(&tz) = bytes.get(tz_index)
        && (tz == b'+' || tz == b'-')
    {
        let sign = if tz == b'+' { 1 } else { -1 };
        let tz_hour = parse_digits(
            bytes
                .get(tz_index + 1..tz_index + 3)
                .ok_or_else(|| IntervalsError::InvalidData("invalid timezone".into()))?,
        )? as i64;
        let tz_minute = parse_digits(
            bytes
                .get(tz_index + 4..tz_index + 6)
                .ok_or_else(|| IntervalsError::InvalidData("invalid timezone".into()))?,
        )? as i64;
        offset = sign * (tz_hour * 3600 + tz_minute * 60);
    }
    let days = days_from_civil(year, month, day);
    Ok((days * 86_400 + hour * 3600 + minute * 60 + second - offset) * 1000)
}

fn parse_digits(bytes: &[u8]) -> Result<u32, IntervalsError> {
    if bytes.is_empty() || bytes.iter().any(|b| !b.is_ascii_digit()) {
        return Err(IntervalsError::InvalidData(
            "invalid timestamp digits".into(),
        ));
    }
    Ok(bytes
        .iter()
        .fold(0u32, |value, byte| value * 10 + u32::from(byte - b'0')))
}

fn days_from_civil(year: i64, month: i64, day: i64) -> i64 {
    let year = year - i64::from(month <= 2);
    let era = (if year >= 0 { year } else { year - 399 }) / 400;
    let yoe = year - era * 400;
    let month_prime = month + if month > 2 { -3 } else { 9 };
    let doy = (153 * month_prime + 2) / 5 + day - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    era * 146097 + doe - 719468
}

fn date_from_ms(timestamp_ms: i64) -> String {
    let days = timestamp_ms.div_euclid(86_400_000);
    let z = days + 719_468;
    let era = (if z >= 0 { z } else { z - 146_096 }) / 146_097;
    let doe = z - era * 146_097;
    let yoe = (doe - doe / 1_460 + doe / 36_524 - doe / 146_096) / 365;
    let year = yoe + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let day = doy - (153 * mp + 2) / 5 + 1;
    let month = mp + if mp < 10 { 3 } else { -9 };
    let year = year + i64::from(month <= 2);
    format!("{year:04}-{month:02}-{day:02}")
}

fn now_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as i64
}

trait Pipe: Sized {
    fn pipe<T>(self, function: impl FnOnce(Self) -> T) -> T {
        function(self)
    }
}
impl<T> Pipe for T {}

#[cfg(test)]
mod tests {
    use super::{
        days_from_civil, parse_rfc3339_ms, stream_pairs, wellness_field, wellness_records,
    };
    use serde_json::json;

    #[test]
    fn parses_utc_timestamp() {
        assert_eq!(
            parse_rfc3339_ms("2026-10-02T04:31:00Z").unwrap(),
            1790915460000
        );
    }
    #[test]
    fn parses_date_epoch() {
        assert_eq!(days_from_civil(1970, 1, 1), 0);
    }
    #[test]
    fn accepts_keyed_streams() {
        assert_eq!(
            stream_pairs(json!({"watts":{"data":[1]}})).unwrap().len(),
            1
        );
    }

    #[test]
    fn wellness_records_preserve_external_provenance_and_fields() {
        let records = wellness_records(
            "resting_heart_rate",
            wellness_field("resting_heart_rate"),
            vec![json!({"id":"2026-10-02","restingHR":48,"sleepScore":78,"unknown":true})],
        )
        .unwrap();
        assert_eq!(records.len(), 1);
        assert_eq!(records[0].record_date, "2026-10-02");
        assert_eq!(records[0].value, Some(json!(48)));
        assert!(records[0].provenance.external);
        assert_eq!(records[0].fields.get("unknown"), Some(&json!(true)));
        assert!(records[0].timestamp_ms.is_none());
    }
}
