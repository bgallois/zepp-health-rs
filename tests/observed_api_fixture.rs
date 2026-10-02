use serde_json::Value;
use zepp_health_rs::api::{EventItem, SportStatistic};

#[test]
fn stress_event_fixture_preserves_nested_json() {
    let root: Value = serde_json::from_str(include_str!("fixtures/events.json")).unwrap();
    let item: EventItem = serde_json::from_value(root["items"][0].clone()).unwrap();
    assert_eq!(item.event_type.as_deref(), Some("all_day_stress"));
    let readings: Vec<Value> = serde_json::from_str(item.data.as_deref().unwrap()).unwrap();
    assert_eq!(readings[0]["value"], 47);
}

#[test]
fn sport_statistics_fixture_preserves_zepp_field_names() {
    let root: Value = serde_json::from_str(include_str!("fixtures/sport_statistics.json")).unwrap();
    let item: SportStatistic = serde_json::from_value(root["items"][0].clone()).unwrap();
    assert_eq!(item.fields["currnetDayTrainLoad"], 56);
}

#[test]
fn hrv_rmssd_fixture_decodes_milliseconds() {
    let item: EventItem = serde_json::from_str(include_str!("fixtures/hrv_event.json")).unwrap();
    let samples = item.decode_hrv_samples().unwrap().unwrap();
    assert_eq!(samples[0].offset_ms, 9_000_000);
    assert_eq!(samples[0].rmssd_ms, 48.0);
    assert_eq!(samples[1].rmssd_ms, 56.0);
}

#[test]
fn readiness_watch_score_fixture_decodes_nightly_metrics() {
    let item: EventItem =
        serde_json::from_str(include_str!("fixtures/readiness_watch_score.json")).unwrap();
    let readiness = item.decode_readiness_watch_score().unwrap().unwrap();
    assert_eq!(readiness.timestamp_ms, 1_790_822_520_000);
    assert_eq!(readiness.sleep_hrv_rmssd_ms, 49.0);
    assert_eq!(readiness.hrv_score_percent, 69.0);
    assert_eq!(readiness.sleep_rhr_bpm, 48.0);
    assert_eq!(readiness.skin_temp_calibrated_centi_delta, Some(9));
    assert_eq!(readiness.skin_temp_score, Some(98));
    assert_eq!(readiness.skin_temp_baseline_centi_delta, Some(-3));
}

#[test]
fn respiratory_rate_fixture_decodes_minute_bytes() {
    let item: EventItem =
        serde_json::from_str(include_str!("fixtures/respiratory_rate_event.json")).unwrap();
    let samples = item.decode_respiratory_rate().unwrap().unwrap();
    assert_eq!(samples.len(), 5);
    assert_eq!(samples[0].breaths_per_minute, Some(14));
    assert_eq!(samples[2].breaths_per_minute, Some(14));
    assert_eq!(samples[3].breaths_per_minute, None);
    assert_eq!(samples[4].breaths_per_minute, Some(15));
}
