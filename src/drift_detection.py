def detect_entity_drift(current_match_rate: float, historical_avg: float, tolerance: float = 0.15) -> dict:
    lower = historical_avg - tolerance
    upper = historical_avg + tolerance
    is_anomaly = current_match_rate < lower or current_match_rate > upper

    return {
        "current_match_rate": current_match_rate,
        "historical_avg": historical_avg,
        "tolerance": tolerance,
        "is_anomaly": is_anomaly
    }
