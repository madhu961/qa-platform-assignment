import argparse
import json

import pandas as pd

from src.drift_detection import detect_entity_drift


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--predictions-parquet", required=True)
    parser.add_argument("--historical-avg", type=float, required=True)
    parser.add_argument("--tolerance", type=float, default=0.15)
    args = parser.parse_args()

    df = pd.read_parquet(args.predictions_parquet)

    if "prediction" not in df.columns:
        raise ValueError("prediction column not found in parquet")

    current_match_rate = float(df["prediction"].mean())

    result = detect_entity_drift(
        current_match_rate=current_match_rate,
        historical_avg=args.historical_avg,
        tolerance=args.tolerance
    )

    print(json.dumps(result, indent=2))


if __name__ == "__main__":
    main()
