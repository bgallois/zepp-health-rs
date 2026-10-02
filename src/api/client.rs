use reqwest::header::{HeaderMap, HeaderValue};
use serde::Deserialize;

use super::{ApiError, BAND_DATA_PATH, BandDataRecord};

#[derive(Debug, Clone)]
/// A read-only client for the regional Zepp Cloud API.
///
/// `host` may be a regional hostname or origin, for example
/// `api-mifit-us2.zepp.com` or `https://api-mifit-us2.zepp.com`. The client sends the supplied token as
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

#[derive(Debug, Clone, Copy)]
/// Time range and event selector for a Zepp events request.
pub struct EventRequest<'a> {
    /// Event category, such as `all_day_stress`, `exertion`, or `phn`.
    pub event_type: &'a str,
    /// Optional event subtype. v1 stress requests may omit it.
    pub sub_type: Option<&'a str>,
    /// Start Unix timestamp in milliseconds.
    pub from_ms: i64,
    /// End Unix timestamp in milliseconds.
    pub to_ms: i64,
    /// Server-side result cap, when supported.
    pub limit: Option<u32>,
}

#[derive(Debug, Deserialize)]
struct BandDataEnvelope {
    code: i64,
    #[serde(default)]
    message: String,
    #[serde(default)]
    data: Vec<BandDataRecord>,
}

#[derive(Debug, Deserialize)]
struct EventsEnvelope {
    #[serde(default)]
    items: Vec<super::EventItem>,
}

#[derive(Debug, Deserialize)]
struct SportStatisticsEnvelope {
    #[serde(default)]
    items: Vec<super::SportStatistic>,
}

impl ZeppApiClient {
    /// Construct a client from an API origin, token, and account ID.
    pub fn new(
        host: impl Into<String>,
        token: impl Into<String>,
        user_id: impl Into<String>,
    ) -> Result<Self, ApiError> {
        let supplied_host = host.into();
        let host = if supplied_host.starts_with("https://") || supplied_host.starts_with("http://")
        {
            supplied_host
        } else if !supplied_host.is_empty() {
            format!("https://{supplied_host}")
        } else {
            return Err(ApiError::InvalidHost);
        };
        let host = host.trim_end_matches('/').to_owned();
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
        let host = std::env::var("ZEPP_HOST")
            .or_else(|_| std::env::var("ZEPP_BASE_URL"))
            .unwrap_or_else(|_| "https://api-mifit-us2.zepp.com".to_owned());
        let user_id = std::env::var("ZEPP_USER_ID").unwrap_or_default();
        Self::new(host, token, user_id)
    }

    /// Construct a client for MCP startup when credentials will be supplied
    /// later. Requests made with this client will be rejected by Zepp until a
    /// real `ZEPP_TOKEN` is configured; this exists so an MCP client can finish
    /// its protocol handshake and report the configuration problem as a tool
    /// error instead of seeing the server process exit.
    pub fn unconfigured_from_env() -> Result<Self, ApiError> {
        let host = std::env::var("ZEPP_HOST")
            .or_else(|_| std::env::var("ZEPP_BASE_URL"))
            .unwrap_or_else(|_| "https://api-mifit-us2.zepp.com".to_owned());
        let user_id = std::env::var("ZEPP_USER_ID").unwrap_or_default();
        let supplied_host = host.clone();
        let host = if supplied_host.starts_with("https://") || supplied_host.starts_with("http://")
        {
            supplied_host
        } else if !supplied_host.is_empty() {
            format!("https://{supplied_host}")
        } else {
            return Err(ApiError::InvalidHost);
        };
        Ok(Self {
            http: reqwest::Client::new(),
            host: host.trim_end_matches('/').to_owned(),
            token: String::new(),
            user_id,
        })
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
        self.ensure_token()?;
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

    /// Fetch an event stream from `/v2/users/me/events`.
    ///
    /// Candidate streams can be tried by passing the names observed in the
    /// inventory, such as `HRV`, `blood_oxygen`, or `Charge`; empty results
    /// are valid responses and do not prove that a metric is unsupported.
    pub async fn fetch_events_v2(
        &self,
        request: EventRequest<'_>,
    ) -> Result<Vec<super::EventItem>, ApiError> {
        self.fetch_events("/v2/users/me/events", request).await
    }

    /// Fetch an event stream from the legacy `/users/{id}/events` endpoint.
    pub async fn fetch_events_v1(
        &self,
        request: EventRequest<'_>,
    ) -> Result<Vec<super::EventItem>, ApiError> {
        self.fetch_events(&format!("/users/{}/events", self.user_id), request)
            .await
    }

    async fn fetch_events(
        &self,
        path: &str,
        request: EventRequest<'_>,
    ) -> Result<Vec<super::EventItem>, ApiError> {
        let mut query = vec![
            ("eventType", request.event_type.to_owned()),
            ("from", request.from_ms.to_string()),
            ("to", request.to_ms.to_string()),
        ];
        if let Some(sub_type) = request.sub_type {
            query.push(("subType", sub_type.to_owned()));
        }
        if let Some(limit) = request.limit {
            query.push(("limit", limit.to_string()));
        }
        let response = self.request(path)?.query(&query).send().await?;
        let status = response.status();
        if !status.is_success() {
            return Err(ApiError::HttpStatus(status));
        }
        Ok(response.json::<EventsEnvelope>().await?.items)
    }

    /// Fetch daily sport statistics, such as `SPORT_LOAD` or `VO2_MAX`.
    pub async fn fetch_sport_statistics(
        &self,
        metric: &str,
        start_day: &str,
        end_day: &str,
        limit: Option<u32>,
        reverse: bool,
    ) -> Result<Vec<super::SportStatistic>, ApiError> {
        let path = format!(
            "/v2/watch/users/{}/WatchSportStatistics/{}",
            self.user_id, metric
        );
        let mut query = vec![
            ("startDay", start_day.to_owned()),
            ("endDay", end_day.to_owned()),
            ("isReverse", reverse.to_string()),
        ];
        if let Some(limit) = limit {
            query.push(("limit", limit.to_string()));
        }
        let response = self.request(&path)?.query(&query).send().await?;
        let status = response.status();
        if !status.is_success() {
            return Err(ApiError::HttpStatus(status));
        }
        Ok(response.json::<SportStatisticsEnvelope>().await?.items)
    }

    /// Fetch a candidate or newly discovered endpoint as raw JSON.
    ///
    /// This is intentionally available for metrics whose event names or
    /// response schemas are still changing (for example HRV, SpO2,
    /// temperature, respiratory rate, or body-battery streams). The caller
    /// supplies the exact path and query parameters observed for its account.
    pub async fn fetch_raw_json(
        &self,
        path: &str,
        query: &[(String, String)],
    ) -> Result<serde_json::Value, ApiError> {
        let response = self.request(path)?.query(query).send().await?;
        let status = response.status();
        if !status.is_success() {
            return Err(ApiError::HttpStatus(status));
        }
        Ok(response.json().await?)
    }

    /// Fetch the observed workout history endpoint.
    ///
    /// The response is raw JSON because the history schema and sport path
    /// variants are not stable across accounts.
    pub async fn fetch_workout_history(
        &self,
        sport: &str,
        from_date: &str,
        to_date: &str,
    ) -> Result<serde_json::Value, ApiError> {
        self.fetch_raw_json(
            &format!("/v1/sport/{sport}/history.json"),
            &[
                ("userid".to_owned(), self.user_id.clone()),
                ("from".to_owned(), from_date.to_owned()),
                ("to".to_owned(), to_date.to_owned()),
            ],
        )
        .await
    }

    /// Fetch an observed workout detail endpoint with caller-supplied query
    /// parameters, preserving the response as raw JSON.
    pub async fn fetch_workout_detail(
        &self,
        sport: &str,
        query: &[(String, String)],
    ) -> Result<serde_json::Value, ApiError> {
        self.fetch_raw_json(&format!("/v1/sport/{sport}/detail.json"), query)
            .await
    }

    /// Fetch the observed per-second-heart-rate file manifest endpoint.
    pub async fn fetch_second_heart_rate_events(
        &self,
        query: &[(String, String)],
    ) -> Result<serde_json::Value, ApiError> {
        self.fetch_raw_json("/users/me/fileInfo/events", query)
            .await
    }

    fn request(&self, path: &str) -> Result<reqwest::RequestBuilder, ApiError> {
        self.ensure_token()?;
        let mut headers = reqwest::header::HeaderMap::new();
        headers.insert(
            "apptoken",
            HeaderValue::try_from(&self.token).map_err(|_| ApiError::MissingToken)?,
        );
        headers.insert("appPlatform", HeaderValue::from_static("web"));
        headers.insert("appname", HeaderValue::from_static("com.xiaomi.hm.health"));
        Ok(self
            .http
            .get(format!("{}{}", self.host, path))
            .headers(headers))
    }

    fn ensure_token(&self) -> Result<(), ApiError> {
        if self.token.is_empty() {
            Err(ApiError::MissingToken)
        } else {
            Ok(())
        }
    }
}
