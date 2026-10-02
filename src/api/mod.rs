//! Zepp Cloud transport and decoding types.
//!
//! This module models currently supported, unofficial Zepp endpoints,
//! including `band_data` and weight records. It does not perform login or token refresh. Supply an app token
//! obtained outside this crate and keep it out of logs and source control.

mod client;
mod error;
mod models;

pub use client::{BandDataRequest, EventRequest, ZeppApiClient};
pub use error::ApiError;
pub use models::{
    ActivityStage, BandDataRecord, BandSummary, EventItem, HrvSample, ReadinessSample,
    RespiratoryRateSample, SleepStage, SleepSummary, SportStatistic, StepSummary,
};

/// The daily band-data endpoint implemented by this crate.
pub const BAND_DATA_PATH: &str = "/v1/data/band_data.json";
