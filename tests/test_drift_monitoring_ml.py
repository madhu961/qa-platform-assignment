from src.drift_detection import detect_entity_drift


def test_prediction_rate_drift_detected():
    result = detect_entity_drift(
        current_match_rate=0.30,
        historical_avg=0.60,
        tolerance=0.10,
    )
    assert result["is_anomaly"] is True
