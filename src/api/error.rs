use thiserror::Error;

#[derive(Debug, Error)]
/// Errors produced by configuration, transport, or Zepp response handling.
pub enum ApiError {
    #[error("missing Zepp API token")]
    MissingToken,
    #[error("Zepp API host must be a hostname or absolute http(s) URL")]
    InvalidHost,
    #[error("Zepp request failed: {0}")]
    Transport(#[from] reqwest::Error),
    #[error("Zepp API returned HTTP status {0}")]
    HttpStatus(reqwest::StatusCode),
    #[error("Zepp API returned error code {code}: {message}")]
    Response { code: i64, message: String },
    #[error("could not decode Zepp response: {0}")]
    Decode(#[from] serde_json::Error),
    #[error("could not decode Zepp base64 field: {0}")]
    Base64(#[from] base64::DecodeError),
    #[error("Zepp response contained an invalid record: {0}")]
    InvalidData(String),
}
