#!/usr/bin/env bash
set -e

export AWS_REGION=us-east-1
export S3_BUCKET=qa-platform-assignment-bucket
export SOURCE1_PATH=s3://qa-platform-assignment-bucket/input/source1/source1.csv
export SOURCE2_PATH=s3://qa-platform-assignment-bucket/input/source2/source2.csv
export ICEBERG_WAREHOUSE=s3://qa-platform-assignment-bucket/iceberg/warehouse/
export ICEBERG_DB=qa_assignment
export ICEBERG_TABLE=corporate_registry
export MLFLOW_TRACKING_URI=file:///tmp/mlruns
export BATCH_ID=$(date +%Y%m%d_%H%M%S)

python -m src.main_pipeline
