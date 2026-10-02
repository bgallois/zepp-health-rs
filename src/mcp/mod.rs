//! Minimal MCP JSON-RPC frontend over the stable [`crate::health`] API.
//!
//! The server intentionally knows nothing about Zepp endpoints or transport.

use serde_json::{Value, json};
use thiserror::Error;

use crate::health::{
    BandSource, CacheMode, DailyHealthRecord, EventRecord, EventSource, HealthClient, HealthError,
    HrvPoint, HrvSource, SportSource, SportStatisticRecord, TimeRange,
};

#[derive(Debug, Error)]
pub enum McpError {
    #[error("invalid MCP request: {0}")]
    InvalidRequest(String),
    #[error("health query failed: {0}")]
    Health(#[from] HealthError),
    #[error("JSON error: {0}")]
    Json(#[from] serde_json::Error),
}

pub struct McpServer<S> {
    health: HealthClient<S>,
}

impl<S> McpServer<S> {
    pub fn new(health: HealthClient<S>) -> Self {
        Self { health }
    }
}

impl<S> McpServer<S>
where
    S: HrvSource + EventSource + BandSource + SportSource,
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
                "serverInfo": {"name": "zepp-health-rs", "version": env!("CARGO_PKG_VERSION")}
            }),
            "notifications/initialized" => return Ok(None),
            "tools/list" => json!({"tools": tools()}),
            "tools/call" => self.call_tool(request.get("params")).await?,
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
            "get_events" => serde_json::to_value(self.get_events(args).await?)?,
            "get_band_data" => serde_json::to_value(self.get_band_data(args).await?)?,
            "get_sport_statistics" => serde_json::to_value(self.get_sport_statistics(args).await?)?,
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
            "heart_rate" | "sleep" | "activity" | "steps" | "calories"
        ) {
            return Ok(serde_json::to_value(self.get_band_data(args).await?)?);
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
        let (event_type, default_subtype) = match metric {
            "readiness" => ("readiness", Some("watch_score")),
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

fn tools() -> Value {
    let mut event_properties = query_properties()
        .as_object()
        .cloned()
        .expect("query properties must be an object");
    event_properties.insert("event_type".into(), json!({"type":"string"}));
    event_properties.insert("sub_type".into(), json!({"type":"string"}));
    json!([
        {"name":"get_timeseries","description":"Get a health time series for a selected metric. Use metric=hrv, readiness, respiratory_rate, charge, spo2, exertion, daily_health, or stress.","inputSchema":{"type":"object","required":["metric","start_ms","end_ms"],"properties":{
            "metric":{"type":"string","description":"hrv, heart_rate, sleep, activity, steps, calories, readiness, respiratory_rate, charge, spo2, exertion, daily_health, stress, sport_load, or vo2_max"},
            "start_ms":{"type":"integer"},"end_ms":{"type":"integer"},
            "from_date":{"type":"string","description":"Required for daily band or sport metrics (YYYY-MM-DD)"},"to_date":{"type":"string","description":"Required for daily band or sport metrics (YYYY-MM-DD)"},
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
        }}}
    ])
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

fn now_ms() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis() as i64
}

/// Serve newline-delimited JSON-RPC requests on stdin and responses on stdout.
pub async fn run_stdio<S>(server: McpServer<S>) -> Result<(), McpError>
where
    S: HrvSource + EventSource + BandSource + SportSource,
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
