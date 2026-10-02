use base64::{Engine as _, engine::general_purpose::STANDARD};
use serde::{Deserialize, Serialize};
use serde_json::Value;

#[derive(Debug, Clone, Deserialize, Serialize)]
/// One daily record returned by `band_data`.
pub struct BandDataRecord {
    /// Zepp account identifier, when returned.
    pub uid: Option<String>,
    /// Zepp record type, when returned.
    pub data_type: Option<i64>,
    /// Zepp's date assignment for this record (`YYYY-MM-DD`).
    pub date_time: String,
    /// Device source identifier, when returned.
    pub source: Option<i64>,
    /// Base64-encoded JSON summary.
    pub summary: String,
    /// Base64-encoded minute-indexed heart-rate bytes.
    pub data_hr: Option<String>,
    /// Base64-encoded activity payload. Its binary layout is not decoded yet.
    pub data: Option<String>,
    /// Device identifier, when returned. Treat as sensitive account data.
    pub device_id: Option<String>,
    /// Zepp record UUID, when returned.
    pub uuid: Option<String>,
}

#[derive(Debug, Clone, Deserialize, Serialize, Default)]
/// Decoded fields from a daily `summary` payload.
pub struct BandSummary {
    pub stp: Option<StepSummary>,
    pub slp: Option<SleepSummary>,
    pub tz: Option<String>,
}

#[derive(Debug, Clone, Deserialize, Serialize, Default)]
/// Daily step, distance, calorie, and activity-segment summary.
pub struct StepSummary {
    pub ttl: Option<u64>,
    pub dis: Option<u64>,
    pub cal: Option<u64>,
    pub run_cal: Option<u64>,
    pub run_dist: Option<u64>,
    pub stage: Vec<ActivityStage>,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
/// Activity segment represented by Zepp minute-of-day offsets.
pub struct ActivityStage {
    pub start: u32,
    pub stop: u32,
    pub mode: u16,
    pub step: Option<u64>,
}

#[derive(Debug, Clone, Deserialize, Serialize, Default)]
/// Sleep summary and sleep-stage intervals.
pub struct SleepSummary {
    pub st: Option<i64>,
    pub ed: Option<i64>,
    pub rhr: Option<u16>,
    pub ss: Option<u16>,
    pub stage: Vec<SleepStage>,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
/// Sleep stage represented by Zepp minute-of-day offsets.
pub struct SleepStage {
    pub start: u32,
    pub stop: u32,
    pub mode: u16,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
/// One item from either the v1 or v2 events API.
pub struct EventItem {
    #[serde(rename = "userId")]
    pub user_id: Option<String>,
    #[serde(rename = "eventType")]
    pub event_type: Option<String>,
    #[serde(rename = "subType")]
    pub sub_type: Option<String>,
    pub timestamp: Option<i64>,
    pub value: Option<Value>,
    pub data: Option<String>,
    /// Some streams, including HRV, place their payload in this JSON string.
    pub extra: Option<String>,
    #[serde(rename = "avgStress")]
    pub avg_stress: Option<String>,
    #[serde(rename = "maxStress")]
    pub max_stress: Option<String>,
    #[serde(rename = "minStress")]
    pub min_stress: Option<String>,
}

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq)]
/// One verified `HRVRMSSD / real_data` sample.
pub struct HrvSample {
    /// Millisecond offset supplied by Zepp in the stream (`s`). Its absolute
    /// date is determined by the enclosing event/range and is not inferred here.
    #[serde(rename = "s")]
    pub offset_ms: i64,
    /// RMSSD in milliseconds, as reported by the Helio Strap stream (`hrv`).
    #[serde(rename = "hrv")]
    pub rmssd_ms: f64,
}

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq)]
/// Processed nightly metrics from `readiness / watch_score`.
pub struct ReadinessSample {
    /// Event timestamp in Unix milliseconds.
    pub timestamp_ms: i64,
    /// Zepp's processed nocturnal RMSSD value in milliseconds.
    pub sleep_hrv_rmssd_ms: f64,
    /// Zepp's HRV score, expressed as a percentage-like score.
    pub hrv_score_percent: f64,
    /// Sleep resting heart rate in beats per minute.
    pub sleep_rhr_bpm: f64,
    /// Calibrated skin-temperature delta, reported by Zepp in hundredths of a
    /// degree Celsius according to observed community payloads. This is not
    /// an absolute body temperature.
    pub skin_temp_calibrated_centi_delta: Option<i16>,
    /// Zepp's skin-temperature quality/score value.
    pub skin_temp_score: Option<u8>,
    /// Personal baseline delta in hundredths of a degree Celsius, when present.
    pub skin_temp_baseline_centi_delta: Option<i16>,
}

#[derive(Debug, Clone, PartialEq)]
/// One decoded minute of the observed respiratory-rate stream.
pub struct RespiratoryRateSample {
    /// Minute offset from the event day's midnight.
    pub minute_of_day: u16,
    /// Respiratory rate in breaths per minute. `None` means Zepp supplied a
    /// zero/missing byte for that minute.
    pub breaths_per_minute: Option<u8>,
}

impl EventItem {
    /// Decode the JSON-string HRV payload from `extra` (or `data` fallback).
    pub fn decode_hrv_samples(&self) -> Result<Option<Vec<HrvSample>>, crate::api::ApiError> {
        if let Some(value) = &self.value
            && let Some(samples) = value.get("samples")
        {
            return Ok(Some(serde_json::from_value(samples.clone())?));
        }
        let Some(raw) = self.extra.as_deref().or(self.data.as_deref()) else {
            return Ok(None);
        };
        Ok(Some(serde_json::from_str(raw)?))
    }

    /// Decode processed nightly metrics from a readiness event.
    pub fn decode_readiness_watch_score(
        &self,
    ) -> Result<Option<ReadinessSample>, crate::api::ApiError> {
        let Some(timestamp_ms) = self.timestamp else {
            return Err(crate::api::ApiError::InvalidData(
                "readiness event has no timestamp".to_owned(),
            ));
        };
        let Some(value) = &self.value else {
            return Ok(None);
        };
        #[derive(Deserialize)]
        struct RawReadiness {
            #[serde(rename = "sleepHRV")]
            sleep_hrv: f64,
            #[serde(rename = "hrvScore")]
            hrv_score: f64,
            #[serde(rename = "sleepRHR")]
            sleep_rhr: f64,
            #[serde(rename = "skinTempCalibrated")]
            skin_temp_calibrated: Option<i16>,
            #[serde(rename = "skinTempScore")]
            skin_temp_score: Option<u8>,
            #[serde(rename = "skinTempBaseLine")]
            skin_temp_baseline: Option<i16>,
        }
        let raw: RawReadiness = serde_json::from_value(value.clone())?;
        Ok(Some(ReadinessSample {
            timestamp_ms,
            sleep_hrv_rmssd_ms: raw.sleep_hrv,
            hrv_score_percent: raw.hrv_score,
            sleep_rhr_bpm: raw.sleep_rhr,
            skin_temp_calibrated_centi_delta: raw.skin_temp_calibrated,
            skin_temp_score: raw.skin_temp_score,
            skin_temp_baseline_centi_delta: raw.skin_temp_baseline,
        }))
    }

    /// Decode the base64 minute series in a `RespiratoryRate / real_data`
    /// event. The stream is observed as one byte per minute; timezone periods
    /// remain available in the raw event value and are not applied here.
    pub fn decode_respiratory_rate(
        &self,
    ) -> Result<Option<Vec<RespiratoryRateSample>>, crate::api::ApiError> {
        let Some(value) = &self.value else {
            return Ok(None);
        };
        let Some(encoded) = value.get("measurements").and_then(|v| v.as_str()) else {
            return Ok(None);
        };
        let bytes = STANDARD.decode(encoded)?;
        Ok(Some(
            bytes
                .into_iter()
                .enumerate()
                .map(|(minute, value)| RespiratoryRateSample {
                    minute_of_day: minute as u16,
                    breaths_per_minute: (value != 0).then_some(value),
                })
                .collect(),
        ))
    }
}

#[derive(Debug, Clone, Deserialize, Serialize)]
/// A sport-statistics item. Fields vary by metric, so the raw object is kept.
pub struct SportStatistic {
    #[serde(flatten)]
    pub fields: serde_json::Map<String, Value>,
}

impl BandDataRecord {
    /// Decode the base64 JSON summary into typed fields.
    pub fn decode_summary(&self) -> Result<BandSummary, crate::api::ApiError> {
        let bytes = STANDARD.decode(&self.summary)?;
        Ok(serde_json::from_slice(&bytes)?)
    }

    /// Decode the minute-indexed HR bytes, retaining Zepp's missing-value
    /// sentinels as `None`.  The API reference reports 1,440 bytes for a full
    /// day; this method deliberately does not reject shorter records.
    pub fn decode_heart_rate(&self) -> Result<Option<Vec<Option<u8>>>, crate::api::ApiError> {
        let Some(raw) = &self.data_hr else {
            return Ok(None);
        };
        let bytes = STANDARD.decode(raw)?;
        Ok(Some(
            bytes
                .into_iter()
                .map(|value| (1..=253).contains(&value).then_some(value))
                .collect(),
        ))
    }
}
