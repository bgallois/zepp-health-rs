//! Minimal MCP JSON-RPC frontend over the stable [`crate::health`] API.
//!
//! The server intentionally knows nothing about Zepp endpoints or transport.

use serde_json::{Value, json};
use thiserror::Error;

#[cfg(feature = "intervals")]
use crate::api::ApiError;
use crate::health::{
    BandSource, CacheMode, DailyHealthRecord, EventRecord, EventSource, HealthClient, HealthError,
    HrvPoint, HrvSource, SportSource, SportStatisticRecord, TimeRange, WeightRecord, WeightSource,
};
#[cfg(feature = "intervals")]
use crate::intervals::{ActivityStream, ActivitySummary, IntervalsError, IntervalsHealthClient};

#[derive(Debug, Error)]
pub enum McpError {
    #[error("invalid MCP request: {0}")]
    InvalidRequest(String),
    #[error("health query failed: {0}")]
    Health(#[from] HealthError),
    #[error("JSON error: {0}")]
    Json(#[from] serde_json::Error),
    #[error("CSV export failed: {0}")]
    Export(#[from] std::io::Error),
    #[cfg(feature = "intervals")]
    #[error("Intervals query failed: {0}")]
    Intervals(#[from] IntervalsError),
}

pub struct McpServer<S> {
    health: HealthClient<S>,
    #[cfg(feature = "intervals")]
    intervals: Option<IntervalsHealthClient>,
}

impl<S> McpServer<S> {
    pub fn new(health: HealthClient<S>) -> Self {
        Self {
            health,
            #[cfg(feature = "intervals")]
            intervals: None,
        }
    }

    #[cfg(feature = "intervals")]
    pub fn new_with_intervals(
        health: HealthClient<S>,
        intervals: Option<IntervalsHealthClient>,
    ) -> Self {
        Self { health, intervals }
    }
}

impl<S> McpServer<S>
where
    S: HrvSource + EventSource + BandSource + SportSource + WeightSource,
{
    pub async fn handle(&self, request: Value) -> Result<Option<Value>, McpError> {
        let id = request.get("id").cloned();
        let method = request
            .get("method")
            .and_then(Value::as_str)
            .ok_or_else(|| McpError::InvalidRequest("missing method".into()))?;
        let result = match method {
            "initialize" => json!({
                "protocolVersion": "2024-11-05",
                "capabilities": {"tools": {"listChanged": false}},
                "serverInfo": {"name": "zepp-health-rs", "version": env!("CARGO_PKG_VERSION")},
                "instructions": "Use get_summary first for named health metrics such as resting heart rate, sleep HRV, HRV score, sleep score, daily steps, daily calories, sport load, and VO2 max. Summary results may merge Zepp and Intervals.icu records to fill date gaps. Always inspect each record's provenance.provider and provenance.external fields and report provider coverage when relevant; provider=zepp is device data, provider=intervals with external=true is external wellness data. Treat external records as supplemental and potentially not directly comparable to Zepp values because they may come from another device, algorithm, timezone, or measurement context. Do not silently combine providers into a single average, trend, baseline, or causal conclusion; keep provider-specific statistics separate and clearly label any comparison as approximate. Use get_timeseries only when the user asks for detailed samples, a custom aggregation, or analysis that cannot be answered by a provider summary. Do not derive a named summary from raw samples when Zepp provides an authoritative processed value."
            }),
            "notifications/initialized" => return Ok(None),
            "tools/list" => json!({"tools": tools()}),
            "tools/call" => match self.call_tool(request.get("params")).await {
                Ok(result) => result,
                Err(error) => tool_error(error.to_string()),
            },
            _ => return Ok(id.map(|id| error_response(id, -32601, "method not found"))),
        };
        Ok(id.map(|id| json!({"jsonrpc":"2.0", "id":id, "result":result})))
    }

    async fn call_tool(&self, params: Option<&Value>) -> Result<Value, McpError> {
        let params = params.and_then(Value::as_object).ok_or_else(|| {
            McpError::InvalidRequest("tools/call params must be an object".into())
        })?;
        let name = params
            .get("name")
            .and_then(Value::as_str)
            .ok_or_else(|| McpError::InvalidRequest("tools/call requires a tool name".into()))?;
        let args = params.get("arguments").unwrap_or(&Value::Null);
        let result = match name {
            "get_timeseries" => serde_json::to_value(self.get_timeseries(args).await?)?,
            "get_summary" => self.get_summary(args).await?,
            "get_events" => serde_json::to_value(self.get_events(args).await?)?,
            "get_band_data" => serde_json::to_value(self.get_band_data(args).await?)?,
            "get_sport_statistics" => serde_json::to_value(self.get_sport_statistics(args).await?)?,
            #[cfg(feature = "intervals")]
            "get_activities" => serde_json::to_value(self.get_activities(args).await?)?,
            "export_csv" => serde_json::to_value(self.export_csv(args).await?)?,
            _ => {
                return Ok(
                    json!({"isError":true,"content":[{"type":"text","text":"unknown tool"}]}),
                );
            }
        };
        Ok(
            json!({"isError":false,"structuredContent":result,"content":[{"type":"text","text":result.to_string()}]}),
        )
    }

    async fn get_hrv(&self, args: &Value) -> Result<Vec<HrvPoint>, McpError> {
        let (range, mode, now_ms, refresh) = query_args(args)?;
        Ok(self.health.hrv(range, mode, now_ms, refresh).await?)
    }

    async fn get_timeseries(&self, args: &Value) -> Result<Value, McpError> {
        let metric = args
            .get("metric")
            .and_then(Value::as_str)
            .ok_or_else(|| McpError::InvalidRequest("get_timeseries requires metric".into()))?;
        if metric == "hrv" || metric == "hrv_rmssd" {
            return Ok(serde_json::to_value(self.get_hrv(args).await?)?);
        }
        if matches!(
            metric,
            "heart_rate_detail" | "activity" | "steps" | "calories"
        ) {
            return Ok(serde_json::to_value(self.get_band_data(args).await?)?);
        }
        if metric == "sleep" {
            return Ok(serde_json::to_value(self.get_sleep(args).await?)?);
        }
        if matches!(metric, "sport_load" | "vo2_max") {
            let mut sport_args = args.clone();
            sport_args
                .as_object_mut()
                .expect("timeseries arguments must be an object")
                .insert(
                    "metric".into(),
                    Value::String(if metric == "sport_load" {
                        "SPORT_LOAD".into()
                    } else {
                        "VO2_MAX".into()
                    }),
                );
            return Ok(serde_json::to_value(
                self.get_sport_statistics(&sport_args).await?,
            )?);
        }
        if metric == "weight" {
            return Ok(serde_json::to_value(self.get_weight(args).await?)?);
        }
        #[cfg(feature = "intervals")]
        if metric == "activity_stream" {
            return Ok(serde_json::to_value(self.get_activity_stream(args).await?)?);
        }
        let (event_type, default_subtype) = match metric {
            "readiness" | "resting_heart_rate" => ("readiness", Some("watch_score")),
            "respiratory_rate" => ("RespiratoryRate", Some("real_data")),
            "charge" => ("Charge", Some("real_data")),
            "spo2" => ("blood_oxygen", Some("click")),
            "exertion" | "training_load" => ("exertion", Some("algo_result")),
            "daily_health" => ("DailyHealth", Some("summary")),
            "stress" => ("all_day_stress", None),
            _ => {
                return Err(McpError::InvalidRequest(format!(
                    "unsupported metric '{metric}'; use get_events for an explicit event selector"
                )));
            }
        };
        let mut event_args = args.clone();
        let object = event_args.as_object_mut().ok_or_else(|| {
            McpError::InvalidRequest("get_timeseries arguments must be an object".into())
        })?;
        object.insert("event_type".into(), Value::String(event_type.into()));
        if !object.contains_key("sub_type")
            && let Some(subtype) = default_subtype
        {
            object.insert("sub_type".into(), Value::String(subtype.into()));
        }
        Ok(serde_json::to_value(self.get_events(&event_args).await?)?)
    }

    async fn get_summary(&self, args: &Value) -> Result<Value, McpError> {
        let metric = args
            .get("metric")
            .and_then(Value::as_str)
            .ok_or_else(|| McpError::InvalidRequest("get_summary requires metric".into()))?;
        let mut forwarded = args.clone();
        let object = forwarded.as_object_mut().ok_or_else(|| {
            McpError::InvalidRequest("get_summary arguments must be an object".into())
        })?;
        match metric {
            "resting_heart_rate" => {
                object.insert("event_type".into(), Value::String("readiness".into()));
                object.insert("sub_type".into(), Value::String("watch_score".into()));
                let events = self.zepp_summary_value(self.get_events(&forwarded).await)?;
                let events: Vec<EventRecord> = serde_json::from_value(events)?;
                let records = events
                    .into_iter()
                    .filter_map(|event| {
                        let value = event.value.clone()?;
                        let rhr = value.get("sleepRHR")?.clone();
                        Some(json!({
                            "metric": "resting_heart_rate",
                            "timestamp_ms": event.timestamp_ms,
                            "record_date": event.timestamp_ms.and_then(date_from_timestamp_ms_opt),
                            "value": rhr,
                            "fields": value,
                        }))
                    })
                    .collect::<Vec<_>>();
                self.summary_with_fallback(metric, forwarded.clone(), Value::Array(records))
                    .await
            }
            "sleep_hrv" | "hrv_score" | "skin_temperature" => {
                object.insert("event_type".into(), Value::String("readiness".into()));
                object.insert("sub_type".into(), Value::String("watch_score".into()));
                let events = self.zepp_summary_value(self.get_events(&forwarded).await)?;
                self.summary_with_fallback(
                    metric,
                    forwarded.clone(),
                    normalize_summary_records(metric, events),
                )
                .await
            }
            "sleep_score" | "daily_steps" | "daily_calories" | "daily_summary" => {
                let band = self.zepp_summary_value(self.get_band_data(&forwarded).await)?;
                self.summary_with_fallback(
                    metric,
                    forwarded.clone(),
                    normalize_summary_records(metric, band),
                )
                .await
            }
            "sleep" => {
                self.summary_with_fallback(
                    metric,
                    forwarded.clone(),
                    serde_json::to_value(self.get_sleep(&forwarded).await?)?,
                )
                .await
            }
            "sleep_duration" => {
                let band = self.zepp_summary_value(self.get_band_data(&forwarded).await)?;
                let zepp_sleep = serde_json::from_value::<Vec<DailyHealthRecord>>(band)?
                    .into_iter()
                    .filter_map(|record| {
                        let sleep = record.sleep?;
                        let duration = match (sleep.start_s, sleep.end_s) {
                            (Some(start), Some(end)) if end >= start => end - start,
                            _ => return None,
                        };
                        Some(json!({
                            "metric": "sleep_duration",
                            "record_date": record.date,
                            "value": duration,
                        }))
                    })
                    .collect::<Vec<_>>();
                self.summary_with_fallback(metric, forwarded.clone(), Value::Array(zepp_sleep))
                    .await
            }
            "sleep_awake" | "sleep_light" | "sleep_deep" | "sleep_rem" => {
                self.summary_with_fallback(metric, forwarded.clone(), Value::Array(Vec::new()))
                    .await
            }
            "sport_load" | "vo2_max" => {
                let statistic_metric = if metric == "sport_load" {
                    "SPORT_LOAD"
                } else {
                    "VO2_MAX"
                };
                object.insert("metric".into(), Value::String(statistic_metric.into()));
                self.summary_with_fallback(
                    metric,
                    forwarded.clone(),
                    self.zepp_summary_value(self.get_sport_statistics(&forwarded).await)?,
                )
                .await
            }
            "weight" => {
                self.summary_with_fallback(
                    metric,
                    forwarded.clone(),
                    self.zepp_summary_value(self.get_weight(&forwarded).await)?,
                )
                .await
            }
            _ => Err(McpError::InvalidRequest(format!(
                "unsupported summary metric '{metric}'"
            ))),
        }
    }

    async fn summary_with_fallback(
        &self,
        metric: &str,
        args: Value,
        zepp: Value,
    ) -> Result<Value, McpError> {
        #[cfg(not(feature = "intervals"))]
        let _ = (metric, &args);
        let value = annotate_zepp_provenance(zepp);
        #[cfg(feature = "intervals")]
        {
            let (range, mode, _, _) = query_args(&args)?;
            let external = self.health.external_wellness(metric, range, mode).await?;
            if !external.is_empty() {
                let covered: std::collections::HashSet<String> = value
                    .as_array()
                    .into_iter()
                    .flatten()
                    .filter_map(json_record_date)
                    .collect();
                let mut merged = value.as_array().cloned().unwrap_or_default();
                merged.extend(
                    external
                        .into_iter()
                        .filter(|record| !covered.contains(&record.record_date))
                        .map(serde_json::to_value)
                        .collect::<Result<Vec<_>, _>>()?,
                );
                return Ok(Value::Array(merged));
            }
        }
        Ok(value)
    }

    fn zepp_summary_value<T: serde::Serialize>(
        &self,
        result: Result<T, McpError>,
    ) -> Result<Value, McpError> {
        match result {
            Ok(value) => Ok(serde_json::to_value(value)?),
            #[cfg(feature = "intervals")]
            Err(McpError::Health(HealthError::Provider(ApiError::MissingToken))) => {
                Ok(Value::Array(Vec::new()))
            }
            Err(error) => Err(error),
        }
    }

    async fn get_events(&self, args: &Value) -> Result<Vec<EventRecord>, McpError> {
        let event_type = args
            .get("event_type")
            .and_then(Value::as_str)
            .ok_or_else(|| McpError::InvalidRequest("get_events requires event_type".into()))?;
        let subtype = args.get("sub_type").and_then(Value::as_str);
        let (range, mode, now_ms, refresh) = query_args(args)?;
        Ok(self
            .health
            .events(event_type, subtype, range, mode, now_ms, refresh)
            .await?)
    }

    async fn get_band_data(&self, args: &Value) -> Result<Vec<DailyHealthRecord>, McpError> {
        let from_date = args
            .get("from_date")
            .and_then(Value::as_str)
            .ok_or_else(|| McpError::InvalidRequest("get_band_data requires from_date".into()))?;
        let to_date = args
            .get("to_date")
            .and_then(Value::as_str)
            .ok_or_else(|| McpError::InvalidRequest("get_band_data requires to_date".into()))?;
        let (range, mode, now_ms, refresh) = query_args(args)?;
        Ok(self
            .health
            .band_data(from_date, to_date, range, mode, now_ms, refresh)
            .await?)
    }

    async fn get_sleep(&self, args: &Value) -> Result<Vec<Value>, McpError> {
        Ok(self
            .get_band_data(args)
            .await?
            .into_iter()
            .map(|record| json!({"date": record.date, "sleep": record.sleep}))
            .collect())
    }

    async fn get_sport_statistics(
        &self,
        args: &Value,
    ) -> Result<Vec<SportStatisticRecord>, McpError> {
        let metric = args.get("metric").and_then(Value::as_str).ok_or_else(|| {
            McpError::InvalidRequest("get_sport_statistics requires metric".into())
        })?;
        let from_date = args
            .get("from_date")
            .and_then(Value::as_str)
            .ok_or_else(|| {
                McpError::InvalidRequest("get_sport_statistics requires from_date".into())
            })?;
        let to_date = args.get("to_date").and_then(Value::as_str).ok_or_else(|| {
            McpError::InvalidRequest("get_sport_statistics requires to_date".into())
        })?;
        let (range, mode, now_ms, refresh) = query_args(args)?;
        Ok(self
            .health
            .sport_statistics(metric, from_date, to_date, range, mode, now_ms, refresh)
            .await?)
    }

    async fn get_weight(&self, args: &Value) -> Result<Vec<WeightRecord>, McpError> {
        let (range, mode, now_ms, refresh) = query_args(args)?;
        Ok(self.health.weight(range, mode, now_ms, refresh).await?)
    }

    #[cfg(feature = "intervals")]
    async fn get_activities(&self, args: &Value) -> Result<Vec<ActivitySummary>, McpError> {
        let from_ms = args
            .get("start_ms")
            .and_then(Value::as_i64)
            .ok_or_else(|| McpError::InvalidRequest("get_activities requires start_ms".into()))?;
        let to_ms = args
            .get("end_ms")
            .and_then(Value::as_i64)
            .ok_or_else(|| McpError::InvalidRequest("get_activities requires end_ms".into()))?;
        let source = args.get("source").and_then(Value::as_str).unwrap_or("all");
        if !matches!(source, "all" | "intervals" | "zepp") {
            return Err(McpError::InvalidRequest(
                "source must be all, intervals, or zepp".into(),
            ));
        }
        if source == "zepp" {
            return Err(McpError::InvalidRequest("Zepp workout summaries are not exposed by the stable API yet; use get_band_data for daily activity records".into()));
        }
        let Some(intervals) = self.intervals.as_ref() else {
            if source == "intervals" {
                return Err(McpError::InvalidRequest(
                    "Intervals support is compiled in but INTERVALS_TOKEN is not configured".into(),
                ));
            }
            return Ok(Vec::new());
        };
        let mode = args
            .get("cache_mode")
            .and_then(Value::as_str)
            .unwrap_or("cache_first");
        Ok(intervals.activities(from_ms, to_ms, mode).await?)
    }

    #[cfg(feature = "intervals")]
    async fn get_activity_stream(&self, args: &Value) -> Result<Vec<ActivityStream>, McpError> {
        let activity_id = args
            .get("activity_id")
            .and_then(|value| {
                value
                    .as_str()
                    .map(str::to_owned)
                    .or_else(|| value.as_i64().map(|id| id.to_string()))
            })
            .ok_or_else(|| {
                McpError::InvalidRequest("activity_stream requires activity_id".into())
            })?;
        let stream_types = args
            .get("stream_types")
            .and_then(Value::as_array)
            .map(|values| {
                values
                    .iter()
                    .filter_map(Value::as_str)
                    .map(str::to_owned)
                    .collect::<Vec<_>>()
            });
        let Some(intervals) = self.intervals.as_ref() else {
            return Err(McpError::InvalidRequest(
                "Intervals support is compiled in but INTERVALS_TOKEN is not configured".into(),
            ));
        };
        let mode = args
            .get("cache_mode")
            .and_then(Value::as_str)
            .unwrap_or("cache_first");
        Ok(intervals
            .activity_streams(&activity_id, stream_types.as_deref(), mode)
            .await?)
    }

    async fn export_csv(&self, args: &Value) -> Result<ExportResult, McpError> {
        let (metric, payload) =
            if let Some(event_type) = args.get("event_type").and_then(Value::as_str) {
                (
                    event_type,
                    serde_json::to_value(self.get_events(args).await?)?,
                )
            } else {
                let metric = args.get("metric").and_then(Value::as_str).ok_or_else(|| {
                    McpError::InvalidRequest("export_csv requires metric or event_type".into())
                })?;
                (metric, self.get_timeseries(args).await?)
            };
        let rows = csv_rows(metric, &payload);
        if rows.is_empty() {
            return Err(McpError::InvalidRequest(
                "the requested metric returned no records".into(),
            ));
        }
        let directory = std::env::var_os("ZEPP_EXPORT_DIR")
            .map(std::path::PathBuf::from)
            .unwrap_or_else(|| std::path::PathBuf::from("exports"));
        std::fs::create_dir_all(&directory)?;
        let filename = args
            .get("filename")
            .and_then(Value::as_str)
            .unwrap_or(metric);
        let filename = safe_csv_filename(filename)?;
        let path = directory.join(filename);
        let row_count = rows.len();
        let headers = csv_headers(&rows);
        let mut writer = std::fs::File::create(&path)?;
        write_csv_record(&mut writer, &headers)?;
        for row in rows {
            let values = headers
                .iter()
                .map(|header| row.get(header).map(String::as_str).unwrap_or(""))
                .collect::<Vec<_>>();
            write_csv_record(&mut writer, &values)?;
        }
        std::io::Write::flush(&mut writer)?;
        Ok(ExportResult {
            path: path.to_string_lossy().into_owned(),
            metric: metric.to_owned(),
            rows: row_count,
        })
    }
}

fn annotate_zepp_provenance(value: Value) -> Value {
    let Value::Array(records) = value else {
        return value;
    };
    Value::Array(
        records
            .into_iter()
            .map(|mut record| {
                #[cfg(feature = "intervals")]
                let record_date = json_record_date(&record);
                #[cfg(not(feature = "intervals"))]
                let record_date: Option<String> = None;
                if let Some(object) = record.as_object_mut() {
                    let device_id = object
                        .get("value")
                        .and_then(Value::as_object)
                        .and_then(|value| value.get("deviceId"))
                        .cloned();
                    if !object.contains_key("provenance") {
                        object.insert(
                            "provenance".into(),
                            json!({
                                "provider": "zepp",
                                "external": false,
                                "device_id": device_id,
                                "device_label": Value::Null,
                                "record_date": record_date,
                            }),
                        );
                    }
                }
                record
            })
            .collect(),
    )
}

fn normalize_summary_records(metric: &str, value: Value) -> Value {
    let Some(records) = value.as_array() else {
        return value;
    };
    let field = match metric {
        "sleep_hrv" => "sleepHRV",
        "hrv_score" => "hrvScore",
        "skin_temperature" => "skinTempCalibrated",
        _ => "",
    };
    if !field.is_empty() {
        return Value::Array(records.iter().filter_map(|record| {
            let object = record.as_object()?;
            let nested = object.get("value")?.as_object()?;
            let value = nested.get(field)?.clone();
            let timestamp_ms = object.get("timestamp_ms").cloned().unwrap_or(Value::Null);
            Some(json!({"metric": metric, "timestamp_ms": timestamp_ms,
                "record_date": object.get("timestamp_ms").and_then(Value::as_i64).map(date_from_timestamp_ms),
                "value": value, "fields": nested}))
        }).collect());
    }
    if matches!(metric, "sleep_score" | "daily_steps" | "daily_calories") {
        return Value::Array(records.iter().filter_map(|record| {
            let object = record.as_object()?;
            let date = object.get("date")?.as_str()?;
            let value = match metric {
                "sleep_score" => object.get("sleep")?.get("score")?.clone(),
                "daily_steps" => object.get("steps")?.clone(),
                "daily_calories" => object.get("calories")?.clone(),
                _ => return None,
            };
            Some(json!({"metric": metric, "record_date": date, "value": value, "fields": object}))
        }).collect());
    }
    value
}

#[derive(Debug, serde::Serialize)]
struct ExportResult {
    path: String,
    metric: String,
    rows: usize,
}

fn safe_csv_filename(filename: &str) -> Result<String, McpError> {
    let mut name = filename.trim().replace(['/', '\\'], "_");
    if name.is_empty() {
        return Err(McpError::InvalidRequest("filename cannot be empty".into()));
    }
    if !name.ends_with(".csv") {
        name.push_str(".csv");
    }
    if name == ".csv" || name == "..csv" {
        return Err(McpError::InvalidRequest("invalid filename".into()));
    }
    Ok(name)
}

type CsvRow = std::collections::BTreeMap<String, String>;

fn csv_rows(metric: &str, payload: &Value) -> Vec<CsvRow> {
    let records = payload.as_array().cloned().unwrap_or_default();
    if metric == "sleep" {
        return records.iter().flat_map(sleep_csv_rows).collect();
    }
    records.iter().map(flatten_csv_record).collect()
}

fn flatten_csv_record(value: &Value) -> CsvRow {
    let mut row = CsvRow::new();
    if let Some(object) = value.as_object() {
        for (key, value) in object {
            row.insert(key.clone(), csv_value(value));
        }
    } else {
        row.insert("value".into(), csv_value(value));
    }
    row
}

fn sleep_csv_rows(value: &Value) -> Vec<CsvRow> {
    let Some(object) = value.as_object() else {
        return vec![flatten_csv_record(value)];
    };
    let mut base = CsvRow::new();
    if let Some(date) = object.get("date") {
        base.insert("date".into(), csv_value(date));
    }
    if let Some(sleep) = object.get("sleep").and_then(Value::as_object) {
        for (key, value) in sleep {
            if key != "stages" {
                base.insert(format!("sleep_{key}"), csv_value(value));
            }
        }
        if let Some(stages) = sleep.get("stages").and_then(Value::as_array) {
            return stages
                .iter()
                .map(|stage| {
                    let mut row = base.clone();
                    if let Some(stage) = stage.as_object() {
                        for (key, value) in stage {
                            row.insert(format!("stage_{key}"), csv_value(value));
                        }
                    }
                    row
                })
                .collect();
        }
    }
    vec![base]
}

fn csv_value(value: &Value) -> String {
    match value {
        Value::Null => String::new(),
        Value::String(value) => value.clone(),
        Value::Bool(value) => value.to_string(),
        Value::Number(value) => value.to_string(),
        Value::Array(_) | Value::Object(_) => value.to_string(),
    }
}

fn csv_headers(rows: &[CsvRow]) -> Vec<String> {
    rows.iter()
        .flat_map(|row| row.keys().cloned())
        .collect::<std::collections::BTreeSet<_>>()
        .into_iter()
        .collect()
}

fn write_csv_record<W: std::io::Write>(
    writer: &mut W,
    fields: &[impl AsRef<str>],
) -> Result<(), McpError> {
    for (index, field) in fields.iter().enumerate() {
        if index > 0 {
            writer.write_all(b",")?;
        }
        let escaped = field.as_ref().replace('"', "\"\"");
        if escaped.contains([',', '"', '\n', '\r']) {
            write!(writer, "\"{escaped}\"")?;
        } else {
            writer.write_all(escaped.as_bytes())?;
        }
    }
    writer.write_all(b"\n")?;
    Ok(())
}

fn query_args(args: &Value) -> Result<(TimeRange, CacheMode, i64, Option<i64>), McpError> {
    let from_ms = args
        .get("start_ms")
        .and_then(Value::as_i64)
        .ok_or_else(|| McpError::InvalidRequest("start_ms is required".into()))?;
    let to_ms = args
        .get("end_ms")
        .and_then(Value::as_i64)
        .ok_or_else(|| McpError::InvalidRequest("end_ms is required".into()))?;
    let mode = match args
        .get("cache_mode")
        .and_then(Value::as_str)
        .unwrap_or("cache_first")
    {
        "cache_only" => CacheMode::CacheOnly,
        "cache_first" => CacheMode::CacheFirst,
        "refresh" => CacheMode::Refresh,
        value => {
            return Err(McpError::InvalidRequest(format!(
                "invalid cache_mode: {value}"
            )));
        }
    };
    let now_ms = args
        .get("now_ms")
        .and_then(Value::as_i64)
        .unwrap_or_else(now_ms);
    let refresh = args.get("recent_refresh_ms").and_then(Value::as_i64);
    Ok((TimeRange::new(from_ms, to_ms)?, mode, now_ms, refresh))
}

#[cfg(feature = "intervals")]
fn json_record_date(value: &Value) -> Option<String> {
    let object = value.as_object()?;
    if let Some(date) = object
        .get("date")
        .or_else(|| object.get("record_date"))
        .or_else(|| object.get("fields").and_then(|fields| fields.get("date")))
        .or_else(|| object.get("fields").and_then(|fields| fields.get("id")))
        .and_then(Value::as_str)
    {
        return Some(date.to_owned());
    }
    let timestamp = object
        .get("timestamp_ms")
        .and_then(Value::as_i64)
        .or_else(|| {
            object
                .get("value")
                .and_then(Value::as_object)
                .and_then(|nested| nested.get("timestamp"))
                .and_then(Value::as_i64)
        })?;
    Some(date_from_timestamp_ms(timestamp))
}

fn date_from_timestamp_ms(timestamp_ms: i64) -> String {
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

fn date_from_timestamp_ms_opt(timestamp_ms: i64) -> Option<String> {
    Some(date_from_timestamp_ms(timestamp_ms))
}

#[allow(unused_mut)]
fn tools() -> Value {
    let mut event_properties = query_properties()
        .as_object()
        .cloned()
        .expect("query properties must be an object");
    event_properties.insert("event_type".into(), json!({"type":"string"}));
    event_properties.insert("sub_type".into(), json!({"type":"string"}));
    let mut tools = json!([
        {"name":"get_timeseries","description":"Get a health time series for a selected metric. Use metric=hrv, readiness, respiratory_rate, charge, spo2, exertion, daily_health, or stress.","inputSchema":{"type":"object","required":["metric","start_ms","end_ms"],"properties":{
            "metric":{"type":"string","description":"hrv, resting_heart_rate, heart_rate_detail (intraday samples), sleep, activity, steps, calories, weight, readiness, respiratory_rate, charge, spo2, exertion, daily_health, stress, sport_load, vo2_max, or activity_stream (Intervals build)"},
            "start_ms":{"type":"integer"},"end_ms":{"type":"integer"},
            "activity_id":{"type":["string","integer"],"description":"Required for metric=activity_stream"},
            "stream_types":{"type":"array","items":{"type":"string"}},
            "from_date":{"type":"string","description":"Required for daily band or sport metrics (YYYY-MM-DD)"},"to_date":{"type":"string","description":"Required for daily band or sport metrics (YYYY-MM-DD)"},
            "cache_mode":{"type":"string","enum":["cache_only","cache_first","refresh"]},
            "now_ms":{"type":"integer"},"recent_refresh_ms":{"type":"integer"}
        }}},
        {"name":"get_summary","description":"Get an authoritative provider summary first. Use this before get_timeseries for named metrics including resting_heart_rate, sleep_hrv, sleep_duration, sleep_score, sleep_quality, sleep_awake, sleep_light, sleep_deep, sleep_rem, hrv_score, skin_temperature, daily_steps, daily_calories, daily_summary, sport_load, or vo2_max. Results may merge Zepp and Intervals.icu records to fill date gaps; inspect provenance and keep external values separate when comparing.","inputSchema":{"type":"object","required":["metric","start_ms","end_ms"],"properties":{
            "metric":{"type":"string","description":"Named summary metric; prefer this tool for processed wellness values and sleep duration/components."},"start_ms":{"type":"integer"},"end_ms":{"type":"integer"},
            "from_date":{"type":"string"},"to_date":{"type":"string"},
            "cache_mode":{"type":"string","enum":["cache_only","cache_first","refresh"]},
            "now_ms":{"type":"integer"},"recent_refresh_ms":{"type":"integer"}
        }}},
        {"name":"get_events","description":"Get a structured Zepp event stream for a time range. Use this for readiness, Charge, PAI, SpO2, exertion, DailyHealth, respiratory rate, stress, and other event selectors.","inputSchema":{"type":"object","required":["event_type","start_ms","end_ms"],"properties":event_properties}},
        {"name":"get_band_data","description":"Get detailed daily band records including minute heart rate, sleep, steps, calories, distance, activity segments, and preserved raw activity data.","inputSchema":{"type":"object","required":["from_date","to_date","start_ms","end_ms"],"properties":{
            "from_date":{"type":"string","description":"Inclusive YYYY-MM-DD date"},
            "to_date":{"type":"string","description":"Inclusive YYYY-MM-DD date"},
            "start_ms":{"type":"integer"},"end_ms":{"type":"integer"},
            "cache_mode":{"type":"string","enum":["cache_only","cache_first","refresh"]},
            "now_ms":{"type":"integer"},"recent_refresh_ms":{"type":"integer"}
        }}},
        {"name":"get_sport_statistics","description":"Get raw structured sport statistics such as SPORT_LOAD or VO2_MAX.","inputSchema":{"type":"object","required":["metric","from_date","to_date","start_ms","end_ms"],"properties":{
            "metric":{"type":"string"},"from_date":{"type":"string"},"to_date":{"type":"string"},
            "start_ms":{"type":"integer"},"end_ms":{"type":"integer"},
            "cache_mode":{"type":"string","enum":["cache_only","cache_first","refresh"]},
            "now_ms":{"type":"integer"},"recent_refresh_ms":{"type":"integer"}
        }}},
        {"name":"export_csv","description":"Export any supported time-series metric or explicit event stream to a CSV file. Nested values are JSON columns; sleep stages are emitted as one row per stage. The file is written below ZEPP_EXPORT_DIR (default: ./exports).","inputSchema":{"type":"object","required":["start_ms","end_ms"],"properties":{
            "metric":{"type":"string","description":"Any get_timeseries metric, such as hrv, sleep, heart_rate_detail, readiness, charge, sport_load, or weight"},
            "event_type":{"type":"string","description":"Optional explicit event stream selector; use with sub_type instead of metric"},
            "sub_type":{"type":"string"},
            "start_ms":{"type":"integer"},"end_ms":{"type":"integer"},
            "from_date":{"type":"string"},"to_date":{"type":"string"},
            "filename":{"type":"string","description":"Optional filename; path components are removed and .csv is added"},
            "cache_mode":{"type":"string","enum":["cache_only","cache_first","refresh"]},
            "now_ms":{"type":"integer"},"recent_refresh_ms":{"type":"integer"}
        }}}
    ]);
    #[cfg(feature = "intervals")]
    if let Some(items) = tools.as_array_mut() {
        items.push(json!({"name":"get_activities","description":"Get activity summaries in a time range. Intervals summaries are preferred when available; use source=intervals or source=all. Mean power and other provider fields are preserved.","inputSchema":{"type":"object","required":["start_ms","end_ms"],"properties":{"start_ms":{"type":"integer"},"end_ms":{"type":"integer"},"source":{"type":"string","enum":["all","intervals","zepp"]},"cache_mode":{"type":"string","enum":["cache_only","cache_first","refresh"]}}}}));
    }
    tools
}

fn query_properties() -> Value {
    json!({
        "start_ms":{"type":"integer"}, "end_ms":{"type":"integer"},
        "cache_mode":{"type":"string","enum":["cache_only","cache_first","refresh"]},
        "now_ms":{"type":"integer"}, "recent_refresh_ms":{"type":"integer"}
    })
}

fn error_response(id: Value, code: i64, message: &str) -> Value {
    json!({"jsonrpc":"2.0","id":id,"error":{"code":code,"message":message}})
}

fn tool_error(message: String) -> Value {
    json!({
        "isError": true,
        "content": [{"type": "text", "text": message}]
    })
}

fn now_ms() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis() as i64
}

/// Serve newline-delimited JSON-RPC requests on stdin and responses on stdout.
pub async fn run_stdio<S>(server: McpServer<S>) -> Result<(), McpError>
where
    S: HrvSource + EventSource + BandSource + SportSource + WeightSource,
{
    use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
    let stdin = tokio::io::stdin();
    let mut lines = BufReader::new(stdin).lines();
    let mut stdout = tokio::io::stdout();
    while let Some(line) = lines
        .next_line()
        .await
        .map_err(|error| McpError::InvalidRequest(error.to_string()))?
    {
        if line.trim().is_empty() {
            continue;
        }
        if let Some(response) = server.handle(serde_json::from_str(&line)?).await? {
            stdout
                .write_all(response.to_string().as_bytes())
                .await
                .map_err(|error| McpError::InvalidRequest(error.to_string()))?;
            stdout
                .write_all(b"\n")
                .await
                .map_err(|error| McpError::InvalidRequest(error.to_string()))?;
            stdout
                .flush()
                .await
                .map_err(|error| McpError::InvalidRequest(error.to_string()))?;
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::{csv_rows, safe_csv_filename};
    use serde_json::json;

    #[test]
    fn sleep_csv_expands_each_stage_to_a_row() {
        let rows = csv_rows(
            "sleep",
            &json!([{
                "date": "2026-10-02",
                "sleep": {
                    "score": 78,
                    "stages": [
                        {"start_minute": 1, "end_minute": 2, "mode": 4},
                        {"start_minute": 3, "end_minute": 4, "mode": 5}
                    ]
                }
            }]),
        );
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0]["date"], "2026-10-02");
        assert_eq!(rows[1]["stage_mode"], "5");
    }

    #[test]
    fn export_filename_cannot_escape_export_directory() {
        assert_eq!(safe_csv_filename("../sleep").unwrap(), ".._sleep.csv");
    }
}
