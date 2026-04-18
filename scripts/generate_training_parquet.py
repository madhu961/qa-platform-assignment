import argparse
import os

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from src.feature_engineering import add_ml_features
from src.quality_checks import run_source2_checks


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--input-csv", required=True, help="Path to raw CSV input")
    parser.add_argument("--output-parquet", required=True, help="Output parquet path")
    parser.add_argument("--profit-threshold", type=float, default=1000.0)
    parser.add_argument("--min-rows", type=int, default=1)
    args = parser.parse_args()

    spark = (
        SparkSession.builder
        .appName("generate-training-parquet")
        .getOrCreate()
    )

    df = (
        spark.read
        .option("header", True)
        .option("inferSchema", True)
        .csv(args.input_csv)
    )

    # Minimal guardrails based on your existing quality checks
    run_source2_checks(df, min_rows=args.min_rows)

    # Ensure list-like text columns exist
    for col_name in ["top_suppliers", "main_customers", "activity_places"]:
        if col_name not in df.columns:
            df = df.withColumn(col_name, F.lit(""))

    if "revenue" not in df.columns:
        df = df.withColumn("revenue", F.lit(0.0))

    if "profit" not in df.columns:
        df = df.withColumn("profit", F.lit(0.0))

    featured = add_ml_features(df, profit_threshold=args.profit_threshold)

    selected = featured.select(
        "revenue",
        "profit",
        "num_top_suppliers",
        "num_main_customers",
        "num_activity_places",
        "label"
    )

    os.makedirs(os.path.dirname(args.output_parquet), exist_ok=True)

    (
        selected.write
        .mode("overwrite")
        .parquet(args.output_parquet)
    )

    print(f"Parquet written to: {args.output_parquet}")
    spark.stop()


if __name__ == "__main__":
    main()
