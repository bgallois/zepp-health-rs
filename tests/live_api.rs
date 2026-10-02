//! Explicitly ignored live smoke test. It never runs in the normal suite.

#[tokio::test]
#[ignore = "requires ZEPP_TOKEN, ZEPP_USER_ID, and live Zepp access"]
async fn live_band_data_smoke_test() -> Result<(), zepp_health_rs::api::ApiError> {
    let client = zepp_health_rs::api::ZeppApiClient::from_env()?;
    let records = client
        .fetch_band_data(zepp_health_rs::api::BandDataRequest {
            from_date: "2026-01-01",
            to_date: "2026-01-01",
        })
        .await?;
    if records.iter().any(|record| record.date_time.len() < 10) {
        return Err(zepp_health_rs::api::ApiError::InvalidData(
            "a band-data record has an invalid date_time".to_owned(),
        ));
    }
    Ok(())
}
