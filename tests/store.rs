use serde_json::json;
use zepp_health_rs::store::{
    CoverageRange, RawRecord, SampleRecord, Store, StoreError, SyncDecision, SyncPolicy,
};

#[test]
fn migrations_are_idempotent_and_records_are_upserted() {
    let store = Store::open_in_memory().unwrap();
    store.migrate().unwrap();
    let record = RawRecord {
        metric: "hrv".into(),
        record_key: "day-1".into(),
        observed_at_ms: Some(1_000),
        source_endpoint: "/v2/users/me/events".into(),
        source_device: Some("strap".into()),
        source_timezone: Some("Europe/Paris".into()),
        payload: json!({"samples": [{"s": 0, "hrv": 55}]}),
        fetched_at_ms: 2_000,
    };
    store.put_raw_record(&record).unwrap();
    store
        .put_raw_record(&RawRecord {
            payload: json!({"updated": true}),
            ..record
        })
        .unwrap();
    assert_eq!(store.count_raw_records("hrv").unwrap(), 1);

    let sample = SampleRecord {
        metric: "hrv".into(),
        timestamp_ms: 1_000,
        source_record_key: Some("day-1".into()),
        value: json!({"rmssd_ms": 55}),
    };
    store.put_sample(&sample).unwrap();
    store
        .put_sample(&SampleRecord {
            value: json!({"rmssd_ms": 56}),
            ..sample
        })
        .unwrap();
    assert_eq!(store.count_samples("hrv").unwrap(), 1);
}

#[test]
fn coverage_prevents_repeated_historical_sync_and_refreshes_recent_data() {
    let store = Store::open_in_memory().unwrap();
    store
        .mark_coverage(&CoverageRange {
            metric: "hrv".into(),
            from_ms: 0,
            to_ms: 10_000,
            source_endpoint: "hrv".into(),
            synced_at_ms: 90_000,
        })
        .unwrap();
    assert!(
        store
            .coverage_covers("hrv", 1_000, 9_000, 100_000, None)
            .unwrap()
    );
    assert_eq!(
        SyncPolicy::historical()
            .decide(&store, "hrv", 1_000, 9_000, 100_000)
            .unwrap(),
        SyncDecision::Skip
    );
    assert!(
        !store
            .coverage_covers("hrv", 1_000, 9_000, 100_000, Some(5_000))
            .unwrap()
    );
    assert!(
        !store
            .coverage_covers("hrv", 0, 20_000, 100_000, None)
            .unwrap()
    );
}

#[test]
fn invalid_coverage_is_rejected() {
    let store = Store::open_in_memory().unwrap();
    let error = store
        .mark_coverage(&CoverageRange {
            metric: "hrv".into(),
            from_ms: 2,
            to_ms: 1,
            source_endpoint: "hrv".into(),
            synced_at_ms: 0,
        })
        .unwrap_err();
    assert!(matches!(error, StoreError::InvalidCoverageRange));
}
