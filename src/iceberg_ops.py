from pyspark.sql import SparkSession, DataFrame


def build_spark_session(app_name: str, warehouse: str) -> SparkSession:
    spark = (
        SparkSession.builder
        .appName(app_name)
        .config("spark.sql.catalog.glue_catalog", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.glue_catalog.warehouse", warehouse)
        .config("spark.sql.catalog.glue_catalog.catalog-impl", "org.apache.iceberg.aws.glue.GlueCatalog")
        .config("spark.sql.catalog.glue_catalog.io-impl", "org.apache.iceberg.aws.s3.S3FileIO")
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
        .getOrCreate()
    )
    return spark


def create_database_and_table(spark: SparkSession, database: str, table: str) -> None:
    spark.sql(f"CREATE DATABASE IF NOT EXISTS glue_catalog.{database}")

    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS glue_catalog.{database}.{table} (
            corporate_id STRING,
            canonical_name STRING,
            canonical_address STRING,
            activity_places STRING,
            top_suppliers STRING,
            main_customers STRING,
            revenue DOUBLE,
            profit DOUBLE,
            match_confidence DOUBLE,
            source1_present BOOLEAN,
            source2_present BOOLEAN,
            last_updated_ts TIMESTAMP,
            batch_id STRING
        )
        USING iceberg
    """)


def stage_temp_view(df: DataFrame, view_name: str = "staged_corporate_registry") -> None:
    df.createOrReplaceTempView(view_name)


def merge_into_corporate_registry(spark: SparkSession, database: str, table: str, staging_view: str) -> None:
    spark.sql(f"""
        MERGE INTO glue_catalog.{database}.{table} t
        USING {staging_view} s
        ON t.corporate_id = s.corporate_id
        WHEN MATCHED THEN UPDATE SET
            t.canonical_name = s.canonical_name,
            t.canonical_address = s.canonical_address,
            t.activity_places = s.activity_places,
            t.top_suppliers = s.top_suppliers,
            t.main_customers = s.main_customers,
            t.revenue = s.revenue,
            t.profit = s.profit,
            t.match_confidence = s.match_confidence,
            t.source1_present = s.source1_present,
            t.source2_present = s.source2_present,
            t.last_updated_ts = s.last_updated_ts,
            t.batch_id = s.batch_id
        WHEN NOT MATCHED THEN INSERT *
    """)


def read_corporate_registry(spark: SparkSession, database: str, table: str) -> DataFrame:
    return spark.table(f"glue_catalog.{database}.{table}")
