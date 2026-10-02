//! Explicit probes for currently uncertain event names.
//!
//! Run only with an explicit token. An empty `items` response is accepted by
//! design; a non-success HTTP response identifies a candidate that is not
//! usable with the current account/region.

use zepp_health_rs::api::{ApiError, EventRequest, ZeppApiClient};

#[tokio::test]
#[ignore = "requires ZEPP_TOKEN and live Zepp access"]
async fn live_candidate_event_streams() -> Result<(), ApiError> {
    let client = ZeppApiClient::from_env()?;
    let candidates = [
        ("HRVRMSSD", Some("real_data")),
        ("readiness", Some("watch_score")),
        ("RespiratoryRate", Some("real_data")),
        ("Charge", Some("real_data")),
    ];

    for (event_type, sub_type) in candidates {
        client
            .fetch_events_v2(EventRequest {
                event_type,
                sub_type,
                from_ms: 1_704_067_200_000,
                to_ms: 1_704_153_600_000,
                limit: Some(200),
            })
            .await?;
    }

    client
        .fetch_events_v1(EventRequest {
            event_type: "blood_oxygen",
            sub_type: Some("click"),
            from_ms: 1_704_067_200_000,
            to_ms: 1_704_153_600_000,
            limit: Some(2000),
        })
        .await?;
    Ok(())
}
