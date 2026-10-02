use reqwest::header::{HeaderMap, HeaderValue};
use serde::Deserialize;

use super::{ApiError, BAND_DATA_PATH, BandDataRecord};

#[derive(Debug, Clone)]
/// A read-only client for the regional Zepp Cloud API.
///
/// `host` must be the regional API origin, for example
/// `https://api-mifit-us2.zepp.com`. The client sends the supplied token as
/// the `apptoken` header and never logs or exposes it.
pub struct ZeppApiClient {
    http: reqwest::Client,
    host: String,
    token: String,
    /// Numeric Zepp account identifier used by the band-data endpoint.
    pub user_id: String,
}

#[derive(Debug, Clone, Copy)]
/// Inclusive date strings passed to Zepp's band-data request.
///
/// Dates must use `YYYY-MM-DD`. Zepp's exact range-boundary and timezone
/// behavior remains endpoint behavior rather than a guarantee of this type.
pub struct BandDataRequest<'a> {
    /// First date requested.
    pub from_date: &'a str,
    /// Last date requested.
    pub to_date: &'a str,
}

#[derive(Debug, Deserialize)]
struct BandDataEnvelope {
    code: i64,
    #[serde(default)]
    message: String,
    #[serde(default)]
    data: Vec<BandDataRecord>,
}

impl ZeppApiClient {
    /// Construct a client from an API origin, token, and account ID.
    pub fn new(
        host: impl Into<String>,
        token: impl Into<String>,
        user_id: impl Into<String>,
    ) -> Result<Self, ApiError> {
        let host = host.into().trim_end_matches('/').to_owned();
        if !(host.starts_with("https://") || host.starts_with("http://")) {
            return Err(ApiError::InvalidHost);
        }
        let token = token.into();
        if token.is_empty() {
            return Err(ApiError::MissingToken);
        }
        Ok(Self {
            http: reqwest::Client::new(),
            host,
            token,
            user_id: user_id.into(),
        })
    }

    pub fn from_env() -> Result<Self, ApiError> {
        let token = std::env::var("ZEPP_TOKEN").map_err(|_| ApiError::MissingToken)?;
        let host = std::env::var("ZEPP_BASE_URL")
            .unwrap_or_else(|_| "https://api-mifit-us2.zepp.com".to_owned());
        let user_id = std::env::var("ZEPP_USER_ID").unwrap_or_default();
        Self::new(host, token, user_id)
    }

    /// Retrieve Zepp's detailed daily health records for a date range.
    ///
    /// The returned records preserve Zepp's base64 fields. Call
    /// [`BandDataRecord::decode_summary`] and
    /// [`BandDataRecord::decode_heart_rate`] explicitly to decode the fields
    /// that are understood by this crate.
    pub async fn fetch_band_data(
        &self,
        request: BandDataRequest<'_>,
    ) -> Result<Vec<BandDataRecord>, ApiError> {
        let mut headers = HeaderMap::new();
        headers.insert(
            "apptoken",
            HeaderValue::try_from(&self.token).map_err(|_| ApiError::MissingToken)?,
        );
        headers.insert("appPlatform", HeaderValue::from_static("web"));
        headers.insert("appname", HeaderValue::from_static("com.xiaomi.hm.health"));
        let response = self
            .http
            .get(format!("{}{}", self.host, BAND_DATA_PATH))
            .headers(headers)
            .query(&[
                ("query_type", "detail"),
                ("device_type", "android_phone"),
                ("userid", &self.user_id),
                ("from_date", request.from_date),
                ("to_date", request.to_date),
            ])
            .send()
            .await?;
        let status = response.status();
        if !status.is_success() {
            return Err(ApiError::HttpStatus(status));
        }
        let envelope: BandDataEnvelope = response.json().await?;
        if envelope.code != 1 {
            return Err(ApiError::Response {
                code: envelope.code,
                message: envelope.message,
            });
        }
        Ok(envelope.data)
    }
}
