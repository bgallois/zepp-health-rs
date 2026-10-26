//! Optional Garmin Connect Personal API wellness source.
//!
//! Garmin's Personal API is pull-only and exposes documented HRV, sleep,
//! daily-stress, and daily-heart-rate resources. Payloads are retained as JSON
//! because Garmin can add fields without requiring a breaking change here.

use std::{
    path::{Path, PathBuf},
    time::{SystemTime, UNIX_EPOCH},
};

use reqwest::{Client, StatusCode};
use serde_json::Value;
use thiserror::Error;

use crate::{
    health::{
        CacheMode, DataProvenance, ExternalWellnessQuery, ExternalWellnessRecord,
        ExternalWellnessSource, HealthError, TimeRange,
    },
    store::{Store, StoreError},
};

const TOKEN_URL: &str = "https://personal.connectapi.garmin.com/personalapitoken-service/personal-api/personal-api-key/token";
// Garmin's Personal API uses this host for both token and data routes. The
// documented Personal API paths do not include the web `/gc-api` prefix.
const API_ROOT: &str = "https://personal.connectapi.garmin.com";

#[derive(Debug, Error)]
pub enum GarminError {
    #[error("GARMIN_API_KEY is not configured")]
    MissingKey,
    #[error("Garmin request failed: {0}")]
    Transport(#[from] reqwest::Error),
    #[error("Garmin returned HTTP status {status} for {url}; response body: {body}")]
    HttpStatus {
        status: StatusCode,
        url: String,
        body: String,
    },
    #[error("Garmin JSON error: {0}")]
    Json(#[from] serde_json::Error),
    #[error(
        "Garmin response decode failed for {url}: status={status}, content_type={content_type:?}, content_encoding={content_encoding:?}, body_bytes={body_bytes}, cause={cause}"
    )]
    ResponseDecode {
        url: String,
        status: StatusCode,
        content_type: Option<String>,
        content_encoding: Option<String>,
        body_bytes: usize,
        cause: String,
    },
    #[error("Garmin storage error: {0}")]
    Store(#[from] StoreError),
    #[error("invalid Garmin data: {0}")]
    InvalidData(String),
}

#[derive(Debug)]
pub struct GarminHealthClient {
    http: Client,
    api_key: String,
    api_root: String,
    store: Store,
    #[allow(dead_code)]
    db_path: PathBuf,
}

impl GarminHealthClient {
    pub fn from_env(path: impl AsRef<Path>) -> Result<Option<Self>, GarminError> {
        let Some(key) = std::env::var_os("GARMIN_API_KEY") else {
            return Ok(None);
        };
        let key = key.to_string_lossy().trim().to_owned();
        if key.is_empty() {
            return Ok(None);
        }
        let db_path = path.as_ref().to_path_buf();
        let store = Store::open(&db_path)?;
        store.migrate_garmin()?;
        Ok(Some(Self {
            http: Client::new(),
            api_key: key,
            api_root: std::env::var("GARMIN_API_BASE_URL").unwrap_or_else(|_| API_ROOT.into()),
            store,
            db_path,
        }))
    }

    async fn token(&self) -> Result<String, GarminError> {
        let response = self
            .http
            .post(TOKEN_URL)
            .header("Authorization", format!("ApiKey {}", self.api_key))
            .send()
            .await?;
        let response = checked_response(response, TOKEN_URL).await?;
        let bytes = response.bytes().await?;
        if let Ok(value) = serde_json::from_slice::<Value>(&bytes)
            && let Some(token) = value
                .get("access_token")
                .or_else(|| value.get("accessToken"))
                .and_then(Value::as_str)
        {
            return Ok(token.to_owned());
        }
        let token = String::from_utf8_lossy(&bytes).trim().to_owned();
        if token.is_empty() {
            return Err(GarminError::InvalidData(
                "token response has no access token".into(),
            ));
        }
        Ok(token)
    }

    pub async fn wellness(
        &self,
        metric: &str,
        range: TimeRange,
        mode: CacheMode,
    ) -> Result<Vec<ExternalWellnessRecord>, GarminError> {
        self.wellness_query(metric, ExternalWellnessQuery::new(range, mode))
            .await
    }

    pub async fn wellness_query(
        &self,
        metric: &str,
        query: ExternalWellnessQuery,
    ) -> Result<Vec<ExternalWellnessRecord>, GarminError> {
        let (from, to) = query_dates(&query)?;
        let mode = query.mode;
        if mode != CacheMode::Refresh {
            let cached = self.store.garmin_wellness_payloads(metric, &from, &to)?;
            if !cached.is_empty() || mode == CacheMode::CacheOnly {
                return records(metric, cached);
            }
        }
        if mode == CacheMode::CacheOnly {
            return Ok(Vec::new());
        }
        let token = self.token().await?;
        let path = match metric {
            "sleep_hrv" | "hrv_rmssd" => {
                if from == to {
                    format!("{}/hrv-service/hrv/{from}", self.api_root)
                } else {
                    format!("{}/hrv-service/hrv/daily/{from}/{to}", self.api_root)
                }
            }
            "sleep" | "sleep_duration" | "sleep_score" => format!(
                "{}/sleep-service/sleep/dailySleepSummariesByDate",
                self.api_root
            ),
            "stress" => format!(
                "{}/usersummary-service/stats/stress/daily/{from}/{to}",
                self.api_root
            ),
            "daily_steps" => format!(
                "{}/usersummary-service/stats/steps/daily/{from}/{to}",
                self.api_root
            ),
            "daily_summary" | "daily_calories" | "floors_ascended" | "floors_descended"
            | "body_battery" | "hydration" | "daily_spo2" => {
                format!("{}/usersummary-service/v2/dailysummaries", self.api_root)
            }
            "weight" => format!("{}/weight-service/weight/all/{from}/{to}", self.api_root),
            "heart_rate_detail" | "resting_heart_rate" => {
                format!("{}/wellness-service/wellness/dailyHeartRate", self.api_root)
            }
            _ => return Ok(Vec::new()),
        };
        let items = if matches!(
            metric,
            "sleep"
                | "sleep_duration"
                | "sleep_score"
                | "daily_summary"
                | "daily_calories"
                | "floors_ascended"
                | "floors_descended"
                | "body_battery"
                | "hydration"
                | "daily_spo2"
        ) {
            let response = self
                .http
                .get(&path)
                .bearer_auth(&token)
                .header("Accept", "application/json")
                .query(&[("startDate", from.as_str()), ("endDate", to.as_str())])
                .send()
                .await?;
            let response = checked_response(response, &path).await?;
            flatten_records(decode_json(response, &path).await?)
        } else if matches!(metric, "heart_rate_detail" | "resting_heart_rate") {
            // Garmin exposes daily heart-rate detail as a date-keyed endpoint,
            // unlike its other range endpoints. The same payload contains the
            // provider-computed resting heart rate. Fetch one civil date at a
            // time and retain the complete response so summary and detailed
            // queries share Garmin's source value without recomputing RHR.
            let mut date = from.clone();
            let mut values = Vec::new();
            loop {
                let response = self
                    .http
                    .get(&path)
                    .bearer_auth(&token)
                    .header("Accept", "application/json")
                    .query(&[("date", date.as_str())])
                    .send()
                    .await?;
                let response = checked_response(response, &path).await?;
                let payload = decode_json(response, &path).await?;
                values.extend(flatten_daily_heart_rate(payload, &date));
                if date == to {
                    break;
                }
                date = next_date(&date)
                    .ok_or_else(|| GarminError::InvalidData("invalid Garmin date range".into()))?;
            }
            values
        } else {
            let response = self
                .http
                .get(&path)
                .bearer_auth(token)
                .header("Accept", "application/json")
                .send()
                .await?;
            let response = checked_response(response, &path).await?;
            flatten_records(decode_json(response, &path).await?)
        };
        let now = now_ms();
        for (date, value) in items.iter().filter_map(|(d, v)| d.clone().map(|d| (d, v))) {
            self.store.put_garmin_wellness(metric, &date, value, now)?;
        }
        records(metric, items.into_iter().map(|(_, v)| v).collect())
    }
}

impl ExternalWellnessSource for GarminHealthClient {
    fn provider_name(&self) -> &'static str {
        "garmin"
    }

    fn fetch_wellness<'a>(
        &'a self,
        metric: &'a str,
        query: ExternalWellnessQuery,
    ) -> std::pin::Pin<
        Box<
            dyn std::future::Future<Output = Result<Vec<ExternalWellnessRecord>, HealthError>> + 'a,
        >,
    > {
        Box::pin(async move {
            self.wellness_query(metric, query)
                .await
                .map_err(|e| HealthError::External(e.to_string()))
        })
    }
}

fn query_dates(query: &ExternalWellnessQuery) -> Result<(String, String), GarminError> {
    match (&query.from_date, &query.to_date) {
        (Some(from), Some(to)) if next_date(from).is_some() && next_date(to).is_some() => {
            if from > to {
                return Err(GarminError::InvalidData(
                    "Garmin from_date is after to_date".into(),
                ));
            }
            Ok((from.clone(), to.clone()))
        }
        (None, None) => Ok((
            date_from_ms(query.range.from_ms),
            date_from_ms(query.range.to_ms),
        )),
        (Some(_), Some(_)) => Err(GarminError::InvalidData(
            "invalid Garmin YYYY-MM-DD date".into(),
        )),
        _ => Err(GarminError::InvalidData(
            "Garmin from_date and to_date must be provided together".into(),
        )),
    }
}

fn records(metric: &str, values: Vec<Value>) -> Result<Vec<ExternalWellnessRecord>, GarminError> {
    Ok(values
        .into_iter()
        .filter_map(|value| {
            let object = value.as_object()?.clone();
            let date = object
                .get("calendarDate")
                .or_else(|| object.get("date"))
                .or_else(|| object.get("summaryDate"))?
                .as_str()?
                .to_owned();
            let scalar = match metric {
                "sleep_score" => object
                    .get("sleepScore")
                    .or_else(|| object.get("score"))
                    .cloned(),
                "sleep_duration" => object
                    .get("sleepSecs")
                    .or_else(|| object.get("sleepDuration"))
                    .cloned(),
                "sleep_hrv" | "hrv_rmssd" => object
                    .get("hrv")
                    .or_else(|| object.get("hrvMs"))
                    .or_else(|| object.get("rmssd"))
                    .or_else(|| object.get("lastNightAvg"))
                    .or_else(|| {
                        object.get("hrvSummary").and_then(|summary| {
                            summary.get("lastNightAvg").or_else(|| summary.get("hrvMs"))
                        })
                    })
                    .cloned(),
                "stress" => object
                    .get("averageStressLevel")
                    .or_else(|| object.get("avgStressLevel"))
                    .or_else(|| object.get("stressLevel"))
                    .cloned(),
                "resting_heart_rate" => object.get("restingHeartRate").cloned(),
                "heart_rate_detail" => object
                    .get("heartRateValues")
                    .or_else(|| object.get("heartRateSamples"))
                    .or_else(|| object.get("heartRate"))
                    .or_else(|| object.get("samples"))
                    .cloned(),
                "daily_steps" => object.get("totalSteps").cloned(),
                "daily_calories" => nested_value(&object, &["calories", "burnedActive"]),
                "floors_ascended" => {
                    nested_value(&object, &["movement", "floorsAscended", "value"])
                }
                "floors_descended" => {
                    nested_value(&object, &["movement", "floorsDescended", "value"])
                }
                "body_battery" => nested_value(&object, &["bodyBattery", "latestValue"]),
                "hydration" => nested_value(&object, &["hydration", "consumedInFractionalMl"]),
                "daily_spo2" => nested_value(&object, &["pulseOx", "latestValue"]),
                "weight" => object.get("weight").cloned(),
                "daily_summary" => Some(Value::Object(object.clone())),
                _ => None,
            }?;
            Some(ExternalWellnessRecord {
                metric: metric.into(),
                record_date: date.clone(),
                timestamp_ms: None,
                value: Some(scalar),
                fields: object,
                provenance: DataProvenance {
                    provider: "garmin".into(),
                    external: true,
                    device_id: None,
                    device_label: Some("Garmin Connect".into()),
                    record_date: Some(date),
                },
            })
        })
        .collect())
}

fn nested_value(object: &serde_json::Map<String, Value>, path: &[&str]) -> Option<Value> {
    let mut value = object.get(*path.first()?)?;
    for key in &path[1..] {
        value = value.get(*key)?;
    }
    Some(value.clone())
}

async fn decode_json(response: reqwest::Response, url: &str) -> Result<Value, GarminError> {
    let status = response.status();
    let content_type = response
        .headers()
        .get(reqwest::header::CONTENT_TYPE)
        .and_then(|value| value.to_str().ok())
        .map(ToOwned::to_owned);
    let content_encoding = response
        .headers()
        .get(reqwest::header::CONTENT_ENCODING)
        .and_then(|value| value.to_str().ok())
        .map(ToOwned::to_owned);
    let bytes = response.bytes().await?;
    serde_json::from_slice(&bytes).map_err(|cause| GarminError::ResponseDecode {
        url: url.to_owned(),
        status,
        content_type,
        content_encoding,
        body_bytes: bytes.len(),
        cause: cause.to_string(),
    })
}

async fn checked_response(
    response: reqwest::Response,
    url: &str,
) -> Result<reqwest::Response, GarminError> {
    if response.status().is_success() {
        return Ok(response);
    }
    let status = response.status();
    let bytes = response.bytes().await?;
    let body = String::from_utf8_lossy(&bytes)
        .chars()
        .filter(|character| !character.is_control() || matches!(character, '\n' | '\t'))
        .take(1000)
        .collect::<String>();
    Err(GarminError::HttpStatus {
        status,
        url: url.to_owned(),
        body: if body.is_empty() {
            format!("<empty response body; {} bytes>", bytes.len())
        } else {
            body
        },
    })
}

fn flatten_records(value: Value) -> Vec<(Option<String>, Value)> {
    match value {
        Value::Array(items) => items.into_iter().flat_map(flatten_records).collect(),
        Value::Object(mut object) => {
            let date = object
                .get("calendarDate")
                .or_else(|| object.get("date"))
                .or_else(|| object.get("summaryDate"))
                .and_then(Value::as_str)
                .map(ToOwned::to_owned);
            if date.is_some() {
                vec![(date, Value::Object(object))]
            } else {
                let children = object.values_mut().map(std::mem::take).collect::<Vec<_>>();
                children.into_iter().flat_map(flatten_records).collect()
            }
        }
        _ => Vec::new(),
    }
}

fn flatten_daily_heart_rate(value: Value, date: &str) -> Vec<(Option<String>, Value)> {
    match value {
        Value::Object(mut object) => {
            if !object
                .keys()
                .any(|key| matches!(key.as_str(), "calendarDate" | "date" | "summaryDate"))
            {
                object.insert("calendarDate".into(), Value::String(date.into()));
            }
            flatten_records(Value::Object(object))
        }
        other => flatten_records(other),
    }
}

fn next_date(date: &str) -> Option<String> {
    let bytes = date.as_bytes();
    if bytes.len() != 10 || bytes[4] != b'-' || bytes[7] != b'-' {
        return None;
    }
    let year = date[0..4].parse::<i32>().ok()?;
    let month = date[5..7].parse::<u32>().ok()?;
    let day = date[8..10].parse::<u32>().ok()?;
    if !(1..=12).contains(&month) || day == 0 || day > days_in_month(year, month) {
        return None;
    }
    if day < days_in_month(year, month) {
        Some(format!("{year:04}-{month:02}-{:02}", day + 1))
    } else if month < 12 {
        Some(format!("{year:04}-{:02}-01", month + 1))
    } else {
        Some(format!("{:04}-01-01", year + 1))
    }
}

fn days_in_month(year: i32, month: u32) -> u32 {
    match month {
        2 if year % 4 == 0 && (year % 100 != 0 || year % 400 == 0) => 29,
        2 => 28,
        4 | 6 | 9 | 11 => 30,
        _ => 31,
    }
}

fn date_from_ms(ms: i64) -> String {
    chrono_like_date(ms)
}
fn chrono_like_date(ms: i64) -> String {
    // Civil-date conversion without introducing another dependency.
    let days = ms.div_euclid(86_400_000) + 719_468;
    let era = (if days >= 0 { days } else { days - 146_096 }) / 146_097;
    let doe = days - era * 146_097;
    let yoe = (doe - doe / 1_460 + doe / 36_524 - doe / 146_096) / 365;
    let y = yoe + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = doy - (153 * mp + 2) / 5 + 1;
    let m = mp + if mp < 10 { 3 } else { -9 };
    let y = y + if m <= 2 { 1 } else { 0 };
    format!("{y:04}-{m:02}-{d:02}")
}
fn now_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or_default()
}

#[cfg(test)]
mod tests {
    use super::{flatten_daily_heart_rate, flatten_records, query_dates, records};
    use crate::health::{CacheMode, ExternalWellnessQuery, TimeRange};
    use serde_json::json;

    #[test]
    fn explicit_civil_dates_override_utc_dates_from_milliseconds() {
        let query = ExternalWellnessQuery::new(
            TimeRange {
                // 2026-10-27 23:00 UTC is 2026-10-28 00:00 in Paris.
                from_ms: 1_793_142_000_000,
                to_ms: 1_793_228_399_000,
            },
            CacheMode::Refresh,
        )
        .with_dates("2026-10-28".into(), "2026-10-28".into());

        assert_eq!(
            query_dates(&query).expect("valid dates"),
            ("2026-10-28".into(), "2026-10-28".into())
        );
    }

    #[test]
    fn decodes_daily_hrv_record_with_provenance() {
        let payload = json!([{"calendarDate":"2026-10-05","hrvSummary":{"hrvMs":42.5}}]);
        let values = flatten_records(payload)
            .into_iter()
            .map(|(_, value)| value)
            .collect();
        let decoded = records("sleep_hrv", values).expect("decode");
        assert_eq!(decoded.len(), 1);
        assert_eq!(decoded[0].record_date, "2026-10-05");
        assert_eq!(decoded[0].provenance.provider, "garmin");
        assert!(decoded[0].provenance.external);
    }

    #[test]
    fn skips_nested_objects_without_a_calendar_date() {
        let payload = json!({"sleep":{"score":80},"calendarDate":"2026-10-05"});
        assert_eq!(flatten_records(payload).len(), 1);
    }

    #[test]
    fn decodes_daily_heart_rate_samples_and_injects_requested_date() {
        let payload = json!({"heartRateValues":[[0,61],[60000,62]]});
        let values = flatten_daily_heart_rate(payload, "2026-10-05")
            .into_iter()
            .map(|(_, value)| value)
            .collect();
        let decoded = records("heart_rate_detail", values).expect("decode");
        assert_eq!(decoded.len(), 1);
        assert_eq!(decoded[0].record_date, "2026-10-05");
        assert_eq!(decoded[0].value, Some(json!([[0, 61], [60000, 62]])));
        assert_eq!(decoded[0].provenance.provider, "garmin");
    }

    #[test]
    fn decodes_provider_computed_resting_heart_rate() {
        let payload = json!({
            "restingHeartRate": 48,
            "heartRateValues": [[0, 61], [60000, 62]]
        });
        let values = flatten_daily_heart_rate(payload, "2026-10-05")
            .into_iter()
            .map(|(_, value)| value)
            .collect();
        let decoded = records("resting_heart_rate", values).expect("decode");

        assert_eq!(decoded.len(), 1);
        assert_eq!(decoded[0].record_date, "2026-10-05");
        assert_eq!(decoded[0].value, Some(json!(48)));
        assert_eq!(
            decoded[0].fields["heartRateValues"],
            json!([[0, 61], [60000, 62]])
        );
        assert_eq!(decoded[0].provenance.provider, "garmin");
        assert!(decoded[0].provenance.external);
    }

    #[test]
    fn decodes_documented_daily_summary_metrics() {
        let payload = json!({
            "calendarDate": "2026-10-05",
            "movement": {
                "steps": {"value": 12345, "goal": 10000, "distanceInMeters": 8765},
                "floorsAscended": {"value": 14, "goal": 10},
                "floorsDescended": {"value": 11}
            },
            "calories": {"burnedActive": 640, "burnedResting": 1700, "burnedTotal": 2340},
            "bodyBattery": {"latestValue": 72, "minValue": 25, "maxValue": 88},
            "hydration": {"consumedInFractionalMl": 2250.5, "goalInMl": 2500},
            "pulseOx": {"latestValue": 97, "minValue": 92}
        });

        let cases = [
            ("daily_calories", json!(640)),
            ("floors_ascended", json!(14)),
            ("floors_descended", json!(11)),
            ("body_battery", json!(72)),
            ("hydration", json!(2250.5)),
            ("daily_spo2", json!(97)),
        ];
        for (metric, expected) in cases {
            let decoded = records(metric, vec![payload.clone()]).expect("decode");
            assert_eq!(decoded.len(), 1, "metric {metric}");
            assert_eq!(decoded[0].value, Some(expected), "metric {metric}");
            assert_eq!(decoded[0].provenance.provider, "garmin");
        }

        let summary = records("daily_summary", vec![payload.clone()]).expect("decode");
        assert_eq!(summary[0].value, Some(payload));
    }

    #[test]
    fn decodes_dedicated_steps_and_weight_records() {
        let steps = records(
            "daily_steps",
            vec![json!({
                "calendarDate": "2026-10-05",
                "totalSteps": 12345,
                "totalDistance": 8765,
                "stepGoal": 10000
            })],
        )
        .expect("steps");
        assert_eq!(steps[0].value, Some(json!(12345)));

        let weight = records(
            "weight",
            vec![json!({
                "calendarDate": "2026-10-05",
                "weight": 72500,
                "bmi": 22.4,
                "bodyFat": 14.2
            })],
        )
        .expect("weight");
        assert_eq!(weight[0].value, Some(json!(72500)));
        assert_eq!(weight[0].fields["bodyFat"], json!(14.2));
    }

    #[test]
    fn missing_metric_value_does_not_claim_provider_coverage() {
        let payload = json!({
            "calendarDate": "2026-10-05",
            "movement": {"steps": {"value": 12345}}
        });

        assert!(
            records("body_battery", vec![payload])
                .expect("decode")
                .is_empty()
        );
    }
}
