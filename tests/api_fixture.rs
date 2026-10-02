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
