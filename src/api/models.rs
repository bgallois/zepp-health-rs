use base64::{Engine as _, engine::general_purpose::STANDARD};
use serde::{Deserialize, Serialize};

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
