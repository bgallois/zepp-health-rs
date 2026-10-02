use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};

use zepp_health_rs::api::ApiError;
use zepp_health_rs::health::{
    CacheMode, EventRecord, EventSource, FetchedHrvPoint, HealthClient, HrvSource, TimeRange,
};
use zepp_health_rs::store::Store;

#[derive(Clone)]
struct MockSource {
    calls: Arc<AtomicUsize>,
}

impl HrvSource for MockSource {
    fn fetch_hrv<'a>(
        &'a self,
        _range: TimeRange,
    ) -> std::pin::Pin<
        Box<dyn std::future::Future<Output = Result<Vec<FetchedHrvPoint>, ApiError>> + Send + 'a>,
    > {
        self.calls.fetch_add(1, Ordering::SeqCst);
        Box::pin(async {
            Ok(vec![FetchedHrvPoint {
                timestamp_ms: 1_000,
                rmssd_ms: 55.0,
                source_record_key: Some("mock".into()),
            }])
        })
    }
}

impl EventSource for MockSource {
    fn fetch_events<'a>(
        &'a self,
        event_type: &'a str,
        sub_type: Option<&'a str>,
        _range: TimeRange,
    ) -> std::pin::Pin<
        Box<dyn std::future::Future<Output = Result<Vec<EventRecord>, ApiError>> + Send + 'a>,
    > {
        self.calls.fetch_add(1, Ordering::SeqCst);
        let event = EventRecord {
            event_type: Some(event_type.to_owned()),
            sub_type: sub_type.map(str::to_owned),
            timestamp_ms: Some(1_000),
            value: Some(serde_json::json!({"value": 42})),
            data: None,
            extra: None,
        };
        Box::pin(async move { Ok(vec![event]) })
    }
}

#[tokio::test]
async fn cache_modes_prevent_repeated_historical_fetches() {
    let calls = Arc::new(AtomicUsize::new(0));
    let client = HealthClient::new(
        Store::open_in_memory().unwrap(),
        MockSource {
            calls: calls.clone(),
        },
    );
    let range = TimeRange::new(0, 2_000).unwrap();
    let first = client
        .hrv(range, CacheMode::CacheFirst, 10_000, None)
        .await
        .unwrap();
    let cached = client
        .hrv(range, CacheMode::CacheOnly, 10_000, None)
        .await
        .unwrap();
    assert_eq!(first, cached);
    assert_eq!(calls.load(Ordering::SeqCst), 1);

    let refreshed = client
        .hrv(range, CacheMode::Refresh, 20_000, None)
        .await
        .unwrap();
    assert_eq!(refreshed[0].rmssd_ms, 55.0);
    assert_eq!(calls.load(Ordering::SeqCst), 2);
}

#[tokio::test]
async fn cache_only_reports_a_miss() {
    let client = HealthClient::new(
        Store::open_in_memory().unwrap(),
        MockSource {
            calls: Arc::new(AtomicUsize::new(0)),
        },
    );
    let error = client
        .hrv(
            TimeRange::new(0, 2_000).unwrap(),
            CacheMode::CacheOnly,
            10_000,
            None,
        )
        .await
        .unwrap_err();
    assert!(matches!(
        error,
        zepp_health_rs::health::HealthError::CacheMiss
    ));
}

#[tokio::test]
async fn generic_event_stream_uses_the_same_cache_policy() {
    let calls = Arc::new(AtomicUsize::new(0));
    let client = HealthClient::new(
        Store::open_in_memory().unwrap(),
        MockSource {
            calls: calls.clone(),
        },
    );
    let range = TimeRange::new(0, 2_000).unwrap();
    let first = client
        .events(
            "Charge",
            Some("real_data"),
            range,
            CacheMode::CacheFirst,
            10_000,
            None,
        )
        .await
        .unwrap();
    let second = client
        .events(
            "Charge",
            Some("real_data"),
            range,
            CacheMode::CacheOnly,
            10_000,
            None,
        )
        .await
        .unwrap();
    assert_eq!(first, second);
    assert_eq!(calls.load(Ordering::SeqCst), 1);
}
