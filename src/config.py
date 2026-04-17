from dataclasses import dataclass
import os


@dataclass
class AppConfig:
    aws_region: str = os.getenv("AWS_REGION", "us-east-1")
    s3_bucket: str = os.getenv("S3_BUCKET", "qa-platform-assignment-bucket")
    source1_path: str = os.getenv("SOURCE1_PATH", "s3://qa-platform-assignment-bucket/input/source1/source1.csv")
    source2_path: str = os.getenv("SOURCE2_PATH", "s3://qa-platform-assignment-bucket/input/source2/source2.csv")
    iceberg_warehouse: str = os.getenv("ICEBERG_WAREHOUSE", "s3://qa-platform-assignment-bucket/iceberg/warehouse/")
    catalog_name: str = os.getenv("ICEBERG_CATALOG", "glue_catalog")
    database_name: str = os.getenv("ICEBERG_DB", "qa_assignment")
    table_name: str = os.getenv("ICEBERG_TABLE", "corporate_registry")
    mlflow_tracking_uri: str = os.getenv("MLFLOW_TRACKING_URI", "file:///tmp/mlruns")
    profit_threshold: float = float(os.getenv("PROFIT_THRESHOLD", "100000"))
    suspicious_revenue_drop_pct: float = float(os.getenv("SUSPICIOUS_REVENUE_DROP_PCT", "0.7"))
    min_expected_rows: int = int(os.getenv("MIN_EXPECTED_ROWS", "1000"))
    entity_match_threshold: float = float(os.getenv("ENTITY_MATCH_THRESHOLD", "85.0"))
    batch_id: str = os.getenv("BATCH_ID", "batch_001")
