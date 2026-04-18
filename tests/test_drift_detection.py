from src.drift_detection import detect_entity_drift


def test_drift_detected():
    result = detect_entity_drift(
        current_match_rate=0.50,
        historical_avg=0.25,
        tolerance=0.10,
    )
    assert result["is_anomaly"] is True


def test_drift_not_detected():
    result = detect_entity_drift(
        current_match_rate=0.27,
        historical_avg=0.25,
        tolerance=0.10,
    )
    assert result["is_anomaly"] is False
