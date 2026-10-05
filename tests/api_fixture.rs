use base64::{Engine as _, engine::general_purpose::STANDARD};
use zepp_health_rs::api::BandDataRecord;

#[test]
fn band_data_fixture_decodes_summary_and_minute_hr() {
    let envelope: serde_json::Value =
        serde_json::from_str(include_str!("fixtures/band_data_detail.json")).unwrap();
    let record: BandDataRecord = serde_json::from_value(envelope["data"][0].clone()).unwrap();

    let summary = record.decode_summary().unwrap();
    assert_eq!(summary.stp.unwrap().ttl, Some(1234));
    let sleep = summary.slp.unwrap();
    assert_eq!(sleep.rhr, Some(54));
    assert_eq!(sleep.stage[1].mode, 8);

    let heart_rate = record.decode_heart_rate().unwrap().unwrap();
    assert_eq!(
        heart_rate,
        vec![None, None, Some(61), Some(128), None, None, None, None]
    );
}

#[test]
fn invalid_summary_is_reported() {
    let record = BandDataRecord {
        uid: None,
        data_type: None,
        date_time: "fixture".into(),
        source: None,
        summary: "not-base64".into(),
        data_hr: None,
        data: None,
        device_id: None,
        uuid: None,
    };
    assert!(record.decode_summary().is_err());
}

#[test]
fn incomplete_sleep_stages_do_not_block_step_summary() {
    let payload = serde_json::json!({
        "stp": {"ttl": 42, "dis": 10, "stage": [{"start": 1, "stop": 2, "mode": 3}]},
        "slp": {"st": 100, "ed": 200, "stage": [
            {"start": 1, "stop": 2, "mode": 4},
            {"start": 3, "stop": 4}
        ]}
    });
    let record = BandDataRecord {
        uid: None,
        data_type: None,
        date_time: "2026-01-01".into(),
        source: None,
        summary: STANDARD.encode(serde_json::to_vec(&payload).unwrap()),
        data_hr: None,
        data: None,
        device_id: None,
        uuid: None,
    };
    let summary = record.decode_summary().unwrap();
    assert_eq!(summary.stp.unwrap().ttl, Some(42));
    assert_eq!(summary.slp.unwrap().stage.len(), 1);
}

#[test]
fn missing_stage_arrays_are_tolerated() {
    let payload = serde_json::json!({"stp": {"ttl": 7, "stage": null}, "slp": {"ss": 80, "stage": "invalid"}});
    let record = BandDataRecord {
        uid: None,
        data_type: None,
        date_time: "2026-01-01".into(),
        source: None,
        summary: STANDARD.encode(serde_json::to_vec(&payload).unwrap()),
        data_hr: None,
        data: None,
        device_id: None,
        uuid: None,
    };
    let summary = record.decode_summary().unwrap();
    assert!(summary.stp.unwrap().stage.is_empty());
    assert!(summary.slp.unwrap().stage.is_empty());
}
