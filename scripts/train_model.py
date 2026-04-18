import argparse
import json
import os

import joblib
import pandas as pd
from sklearn.ensemble import RandomForestClassifier
from sklearn.metrics import classification_report, roc_auc_score
from sklearn.model_selection import train_test_split


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
    parser.add_argument("--model-output", required=True)
    parser.add_argument("--metrics-output", required=True)
    parser.add_argument("--test-size", type=float, default=0.2)
    parser.add_argument("--random-state", type=int, default=42)
    args = parser.parse_args()

    df = pd.read_parquet(args.input_parquet)

    missing = [c for c in FEATURE_COLUMNS + ["label"] if c not in df.columns]
    if missing:
        raise ValueError(f"Missing required columns: {missing}")

    X = df[FEATURE_COLUMNS]
    y = df["label"]

    X_train, X_test, y_train, y_test = train_test_split(
        X,
        y,
        test_size=args.test_size,
        random_state=args.random_state,
        stratify=y if y.nunique() > 1 else None,
    )

    model = RandomForestClassifier(
        n_estimators=200,
        max_depth=6,
        random_state=args.random_state
    )
    model.fit(X_train, y_train)

    preds = model.predict(X_test)
    proba = model.predict_proba(X_test)[:, 1] if y.nunique() > 1 else None

    report = classification_report(y_test, preds, output_dict=True)
    metrics = {
        "feature_columns": FEATURE_COLUMNS,
        "rows": int(len(df)),
        "train_rows": int(len(X_train)),
        "test_rows": int(len(X_test)),
        "classification_report": report,
    }

    if proba is not None:
        metrics["roc_auc"] = float(roc_auc_score(y_test, proba))

    os.makedirs(os.path.dirname(args.model_output), exist_ok=True)
    os.makedirs(os.path.dirname(args.metrics_output), exist_ok=True)

    joblib.dump(model, args.model_output)

    with open(args.metrics_output, "w") as f:
        json.dump(metrics, f, indent=2)

    print(f"Model saved to: {args.model_output}")
    print(f"Metrics saved to: {args.metrics_output}")


if __name__ == "__main__":
    main()
