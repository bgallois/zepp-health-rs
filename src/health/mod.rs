//! Provider-independent health queries over the local store.

use std::{future::Future, pin::Pin};

use serde_json::json;
use thiserror::Error;

use crate::{
    api::{
        ActivityStage as ApiActivityStage, ApiError, BandDataRecord, EventItem, EventRequest,
        ReadinessSample as ApiReadinessSample, RespiratoryRateSample as ApiRespiratoryRateSample,
        SleepStage as ApiSleepStage, ZeppApiClient,
    },
    store::{CoverageRange, SampleRecord, Store, StoreError},
};

pub const HRV_METRIC: &str = "hrv_rmssd";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TimeRange {
    pub from_ms: i64,
    pub to_ms: i64,
}

impl TimeRange {
    pub fn new(from_ms: i64, to_ms: i64) -> Result<Self, HealthError> {
        if from_ms > to_ms {
            return Err(HealthError::InvalidRange);
        }
        Ok(Self { from_ms, to_ms })
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CacheMode {
    CacheOnly,
    CacheFirst,
    Refresh,
}

#[derive(Debug, Clone, PartialEq)]
pub struct HrvPoint {
    pub timestamp_ms: i64,
    pub rmssd_ms: f64,
}

#[derive(Debug, Clone, PartialEq)]
pub struct DailyHealthRecord {
    pub date: String,
    pub heart_rate_bpm: Vec<Option<u8>>,
    pub steps: Option<u64>,
    pub distance_m: Option<u64>,
    pub calories: Option<u64>,
    pub running_calories: Option<u64>,
    pub running_distance_m: Option<u64>,
    pub activity_segments: Vec<ActivitySegment>,
    pub sleep: Option<SleepRecord>,
    pub raw_activity_base64: Option<String>,
    pub device_id: Option<String>,
    pub record_id: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ActivitySegment {
    pub start_minute: u32,
    pub end_minute: u32,
    pub mode: u16,
    pub steps: Option<u64>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SleepRecord {
    pub start_s: Option<i64>,
    pub end_s: Option<i64>,
    pub resting_hr_bpm: Option<u16>,
    pub score: Option<u16>,
    pub stages: Vec<SleepStage>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SleepStage {
    pub start_minute: u32,
    pub end_minute: u32,
    pub mode: u16,
}

#[derive(Debug, Clone, PartialEq)]
pub struct ReadinessPoint {
    pub timestamp_ms: i64,
    pub sleep_hrv_rmssd_ms: f64,
    pub hrv_score_percent: f64,
    pub sleep_rhr_bpm: f64,
    pub skin_temp_calibrated_centi_delta: Option<i16>,
    pub skin_temp_score: Option<u8>,
    pub skin_temp_baseline_centi_delta: Option<i16>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RespiratoryPoint {
    pub minute_of_day: u16,
    pub breaths_per_minute: Option<u8>,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, PartialEq)]
pub struct EventRecord {
    pub event_type: Option<String>,
    pub sub_type: Option<String>,
    pub timestamp_ms: Option<i64>,
    pub value: Option<serde_json::Value>,
    pub data: Option<String>,
    pub extra: Option<String>,
}

pub trait EventSource: Send + Sync {
    fn fetch_events<'a>(
        &'a self,
        event_type: &'a str,
        sub_type: Option<&'a str>,
        range: TimeRange,
    ) -> Pin<Box<dyn Future<Output = Result<Vec<EventRecord>, ApiError>> + Send + 'a>>;
}

pub trait BandSource: Send + Sync {
    fn fetch_band<'a>(
        &'a self,
        from_date: &'a str,
        to_date: &'a str,
    ) -> Pin<Box<dyn Future<Output = Result<Vec<BandDataRecord>, ApiError>> + Send + 'a>>;
}

impl BandSource for ZeppApiClient {
    fn fetch_band<'a>(
        &'a self,
        from_date: &'a str,
        to_date: &'a str,
    ) -> Pin<Box<dyn Future<Output = Result<Vec<BandDataRecord>, ApiError>> + Send + 'a>> {
        Box::pin(async move {
            self.fetch_band_data(crate::api::BandDataRequest { from_date, to_date })
                .await
        })
    }
}

impl EventSource for ZeppApiClient {
    fn fetch_events<'a>(
        &'a self,
        event_type: &'a str,
        sub_type: Option<&'a str>,
        range: TimeRange,
    ) -> Pin<Box<dyn Future<Output = Result<Vec<EventRecord>, ApiError>> + Send + 'a>> {
        Box::pin(async move {
            let events = self
                .fetch_events_v2(EventRequest {
                    event_type,
                    sub_type,
                    from_ms: range.from_ms,
                    to_ms: range.to_ms,
                    limit: Some(2000),
                })
                .await?;
            Ok(events.iter().map(expose_event).collect())
        })
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct SportStatisticRecord {
    pub fields: serde_json::Map<String, serde_json::Value>,
}

pub fn decode_daily_health(record: &BandDataRecord) -> Result<DailyHealthRecord, HealthError> {
    let summary = record.decode_summary()?;
    let heart_rate_bpm = record.decode_heart_rate()?.unwrap_or_default();
    let (steps, distance_m, calories, running_calories, running_distance_m, activity_segments) =
        summary
            .stp
            .map_or((None, None, None, None, None, Vec::new()), |steps| {
                (
                    steps.ttl,
                    steps.dis,
                    steps.cal,
                    steps.run_cal,
                    steps.run_dist,
                    steps.stage.into_iter().map(ActivitySegment::from).collect(),
                )
            });
    let sleep = summary.slp.map(SleepRecord::from);
    Ok(DailyHealthRecord {
        date: record.date_time.clone(),
        heart_rate_bpm,
        steps,
        distance_m,
        calories,
        running_calories,
        running_distance_m,
        activity_segments,
        sleep,
        raw_activity_base64: record.data.clone(),
        device_id: record.device_id.clone(),
        record_id: record.uuid.clone(),
    })
}

pub fn decode_readiness(event: &EventItem) -> Result<Option<ReadinessPoint>, HealthError> {
    Ok(event
        .decode_readiness_watch_score()?
        .map(ReadinessPoint::from))
}

pub fn decode_respiratory_rate(
    event: &EventItem,
) -> Result<Option<Vec<RespiratoryPoint>>, HealthError> {
    Ok(event
        .decode_respiratory_rate()?
        .map(|points| points.into_iter().map(RespiratoryPoint::from).collect()))
}

pub fn expose_event(event: &EventItem) -> EventRecord {
    EventRecord {
        event_type: event.event_type.clone(),
        sub_type: event.sub_type.clone(),
        timestamp_ms: event.timestamp,
        value: event.value.clone(),
        data: event.data.clone(),
        extra: event.extra.clone(),
    }
}

pub fn expose_sport_statistic(statistic: &crate::api::SportStatistic) -> SportStatisticRecord {
    SportStatisticRecord {
        fields: statistic.fields.clone(),
    }
}

impl From<ApiActivityStage> for ActivitySegment {
    fn from(value: ApiActivityStage) -> Self {
        Self {
            start_minute: value.start,
            end_minute: value.stop,
            mode: value.mode,
            steps: value.step,
        }
    }
}

impl From<ApiSleepStage> for SleepStage {
    fn from(value: ApiSleepStage) -> Self {
        Self {
            start_minute: value.start,
            end_minute: value.stop,
            mode: value.mode,
        }
    }
}

impl From<crate::api::SleepSummary> for SleepRecord {
    fn from(value: crate::api::SleepSummary) -> Self {
        Self {
            start_s: value.st,
            end_s: value.ed,
            resting_hr_bpm: value.rhr,
            score: value.ss,
            stages: value.stage.into_iter().map(SleepStage::from).collect(),
        }
    }
}

impl From<ApiReadinessSample> for ReadinessPoint {
    fn from(value: ApiReadinessSample) -> Self {
        Self {
            timestamp_ms: value.timestamp_ms,
            sleep_hrv_rmssd_ms: value.sleep_hrv_rmssd_ms,
            hrv_score_percent: value.hrv_score_percent,
            sleep_rhr_bpm: value.sleep_rhr_bpm,
            skin_temp_calibrated_centi_delta: value.skin_temp_calibrated_centi_delta,
            skin_temp_score: value.skin_temp_score,
            skin_temp_baseline_centi_delta: value.skin_temp_baseline_centi_delta,
        }
    }
}

impl From<ApiRespiratoryRateSample> for RespiratoryPoint {
    fn from(value: ApiRespiratoryRateSample) -> Self {
        Self {
            minute_of_day: value.minute_of_day,
            breaths_per_minute: value.breaths_per_minute,
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct FetchedHrvPoint {
    pub timestamp_ms: i64,
    pub rmssd_ms: f64,
    pub source_record_key: Option<String>,
}

#[derive(Debug, Error)]
pub enum HealthError {
    #[error("invalid time range")]
    InvalidRange,
    #[error("requested health data is not available in the local cache")]
    CacheMiss,
    #[error("store error: {0}")]
    Store(#[from] StoreError),
    #[error("provider error: {0}")]
    Provider(#[from] ApiError),
    #[error("JSON error: {0}")]
    Json(#[from] serde_json::Error),
    #[error("cached sample has invalid value: {0}")]
    InvalidCachedValue(String),
}

pub trait HrvSource: Send + Sync {
    fn fetch_hrv<'a>(
        &'a self,
        range: TimeRange,
    ) -> Pin<Box<dyn Future<Output = Result<Vec<FetchedHrvPoint>, ApiError>> + Send + 'a>>;
}

impl HrvSource for ZeppApiClient {
    fn fetch_hrv<'a>(
        &'a self,
        range: TimeRange,
    ) -> Pin<Box<dyn Future<Output = Result<Vec<FetchedHrvPoint>, ApiError>> + Send + 'a>> {
        Box::pin(async move {
            let events = self
                .fetch_events_v2(EventRequest {
                    event_type: "HRVRMSSD",
                    sub_type: Some("real_data"),
                    from_ms: range.from_ms,
                    to_ms: range.to_ms,
                    limit: Some(200),
                })
                .await?;
            let mut points = Vec::new();
            for event in events {
                let Some(value) = event.value.as_ref() else {
                    continue;
                };
                let Some(start_ms) = value.get("startTime").and_then(|v| v.as_i64()) else {
                    continue;
                };
                let key = event.timestamp.map(|timestamp| timestamp.to_string());
                if let Some(samples) = event.decode_hrv_samples()? {
                    points.extend(samples.into_iter().map(|sample| FetchedHrvPoint {
                        timestamp_ms: start_ms + sample.offset_ms,
                        rmssd_ms: sample.rmssd_ms,
                        source_record_key: key.clone(),
                    }));
                }
            }
            Ok(points)
        })
    }
}

pub struct HealthClient<S> {
    store: Store,
    source: S,
}

impl<S: HrvSource> HealthClient<S> {
    pub fn new(store: Store, source: S) -> Self {
        Self { store, source }
    }

    pub async fn hrv(
        &self,
        range: TimeRange,
        mode: CacheMode,
        now_ms: i64,
        recent_refresh_ms: Option<i64>,
    ) -> Result<Vec<HrvPoint>, HealthError> {
        let covered = self.store.coverage_covers(
            HRV_METRIC,
            range.from_ms,
            range.to_ms,
            now_ms,
            recent_refresh_ms,
        )?;
        if covered && mode != CacheMode::Refresh {
            return self.read_hrv(range);
        }
        if mode == CacheMode::CacheOnly {
            return Err(HealthError::CacheMiss);
        }
        let fetched = self.source.fetch_hrv(range).await?;
        for point in &fetched {
            self.store.put_sample(&SampleRecord {
                metric: HRV_METRIC.to_owned(),
                timestamp_ms: point.timestamp_ms,
                source_record_key: point.source_record_key.clone(),
                value: json!({"rmssd_ms": point.rmssd_ms}),
            })?;
        }
        self.store.mark_coverage(&CoverageRange {
            metric: HRV_METRIC.to_owned(),
            from_ms: range.from_ms,
            to_ms: range.to_ms,
            source_endpoint: "zepp/hrv".to_owned(),
            synced_at_ms: now_ms,
        })?;
        self.read_hrv(range)
    }

    fn read_hrv(&self, range: TimeRange) -> Result<Vec<HrvPoint>, HealthError> {
        self.store
            .samples(HRV_METRIC, range.from_ms, range.to_ms)?
            .into_iter()
            .map(|sample| {
                let rmssd_ms = sample
                    .value
                    .get("rmssd_ms")
                    .and_then(|value| value.as_f64())
                    .ok_or_else(|| HealthError::InvalidCachedValue("rmssd_ms".to_owned()))?;
                Ok(HrvPoint {
                    timestamp_ms: sample.timestamp_ms,
                    rmssd_ms,
                })
            })
            .collect()
    }
}

impl<S: EventSource> HealthClient<S> {
    /// Fetch any verified or exploratory Zepp event stream with the same
    /// cache-first/refresh semantics as typed health queries.
    pub async fn events(
        &self,
        event_type: &str,
        sub_type: Option<&str>,
        range: TimeRange,
        mode: CacheMode,
        now_ms: i64,
        recent_refresh_ms: Option<i64>,
    ) -> Result<Vec<EventRecord>, HealthError> {
        let metric = event_metric(event_type, sub_type);
        let covered = self.store.coverage_covers(
            &metric,
            range.from_ms,
            range.to_ms,
            now_ms,
            recent_refresh_ms,
        )?;
        if covered && mode != CacheMode::Refresh {
            return self.read_events(&metric, range);
        }
        if mode == CacheMode::CacheOnly {
            return Err(HealthError::CacheMiss);
        }
        let fetched = self
            .source
            .fetch_events(event_type, sub_type, range)
            .await?;
        for (index, event) in fetched.iter().enumerate() {
            let observed_at_ms = event.timestamp_ms;
            let record_key = event
                .timestamp_ms
                .map(|timestamp| timestamp.to_string())
                .unwrap_or_else(|| format!("{now_ms}-{index}"));
            self.store.put_raw_record(&crate::store::RawRecord {
                metric: metric.clone(),
                record_key,
                observed_at_ms,
                source_endpoint: format!("zepp/events/{event_type}"),
                source_device: None,
                source_timezone: None,
                payload: serde_json::to_value(event)?,
                fetched_at_ms: now_ms,
            })?;
        }
        self.store.mark_coverage(&CoverageRange {
            metric,
            from_ms: range.from_ms,
            to_ms: range.to_ms,
            source_endpoint: format!("zepp/events/{event_type}"),
            synced_at_ms: now_ms,
        })?;
        self.read_events(&event_metric(event_type, sub_type), range)
    }

    fn read_events(&self, metric: &str, range: TimeRange) -> Result<Vec<EventRecord>, HealthError> {
        self.store
            .raw_records(metric, range.from_ms, range.to_ms)?
            .into_iter()
            .map(|record| serde_json::from_value(record.payload).map_err(HealthError::from))
            .collect()
    }
}

impl<S: BandSource> HealthClient<S> {
    /// Synchronize detailed daily band records and return stable daily health
    /// records. `coverage` is the Unix-millisecond range represented by the
    /// requested date strings and is used only for cache bookkeeping.
    pub async fn band_data(
        &self,
        from_date: &str,
        to_date: &str,
        coverage: TimeRange,
        mode: CacheMode,
        now_ms: i64,
        recent_refresh_ms: Option<i64>,
    ) -> Result<Vec<DailyHealthRecord>, HealthError> {
        const METRIC: &str = "band_data";
        let covered = self.store.coverage_covers(
            METRIC,
            coverage.from_ms,
            coverage.to_ms,
            now_ms,
            recent_refresh_ms,
        )?;
        if covered && mode != CacheMode::Refresh {
            return self.read_band_data(coverage);
        }
        if mode == CacheMode::CacheOnly {
            return Err(HealthError::CacheMiss);
        }
        let fetched = self.source.fetch_band(from_date, to_date).await?;
        for (index, record) in fetched.iter().enumerate() {
            let record_key = record
                .uuid
                .clone()
                .unwrap_or_else(|| format!("{}-{index}", record.date_time));
            self.store.put_raw_record(&crate::store::RawRecord {
                metric: METRIC.to_owned(),
                record_key,
                observed_at_ms: None,
                source_endpoint: crate::api::BAND_DATA_PATH.to_owned(),
                source_device: record.device_id.clone(),
                source_timezone: None,
                payload: serde_json::to_value(record)?,
                fetched_at_ms: now_ms,
            })?;
        }
        self.store.mark_coverage(&CoverageRange {
            metric: METRIC.to_owned(),
            from_ms: coverage.from_ms,
            to_ms: coverage.to_ms,
            source_endpoint: crate::api::BAND_DATA_PATH.to_owned(),
            synced_at_ms: now_ms,
        })?;
        self.read_band_data(coverage)
    }

    fn read_band_data(&self, coverage: TimeRange) -> Result<Vec<DailyHealthRecord>, HealthError> {
        self.store
            .raw_records("band_data", coverage.from_ms, coverage.to_ms)?
            .into_iter()
            .map(|record| {
                let band: BandDataRecord = serde_json::from_value(record.payload)?;
                decode_daily_health(&band)
            })
            .collect()
    }
}

fn event_metric(event_type: &str, sub_type: Option<&str>) -> String {
    match sub_type {
        Some(sub_type) => format!("event:{event_type}:{sub_type}"),
        None => format!("event:{event_type}"),
    }
}
