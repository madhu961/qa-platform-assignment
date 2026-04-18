import argparse
import os

import joblib
import pandas as pd


FEATURE_COLUMNS = [
    "revenue",
    "profit",
    "num_top_suppliers",
    "num_main_customers",
    "num_activity_places",
]


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--input-parquet", required=True)
    parser.add_argument("--model-path", required=True)
    parser.add_argument("--output-parquet", required=True)
    args = parser.parse_args()

    df = pd.read_parquet(args.input_parquet)
    missing = [c for c in FEATURE_COLUMNS if c not in df.columns]
    if missing:
        raise ValueError(f"Missing required feature columns: {missing}")

    model = joblib.load(args.model_path)

    X = df[FEATURE_COLUMNS]
    df["prediction"] = model.predict(X)
    if hasattr(model, "predict_proba"):
        df["prediction_score"] = model.predict_proba(X)[:, 1]

    os.makedirs(os.path.dirname(args.output_parquet), exist_ok=True)
    df.to_parquet(args.output_parquet, index=False)

    print(f"Predictions written to: {args.output_parquet}")


if __name__ == "__main__":
    main()
