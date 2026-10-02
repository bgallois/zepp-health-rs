//! Zepp Cloud transport and decoding types.
//!
//! This module models the currently supported, unofficial `band_data`
//! endpoint. It does not perform login or token refresh. Supply an app token
//! obtained outside this crate and keep it out of logs and source control.

mod client;
mod error;
mod models;

pub use client::{BandDataRequest, ZeppApiClient};
pub use error::ApiError;
pub use models::{
    ActivityStage, BandDataRecord, BandSummary, SleepStage, SleepSummary, StepSummary,
};

/// The endpoint currently implemented by this crate.
pub const BAND_DATA_PATH: &str = "/v1/data/band_data.json";
