from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F


def read_source1(spark: SparkSession, path: str, batch_id: str) -> DataFrame:
    df = (
        spark.read.option("header", True).csv(path)
        .withColumn("source_name", F.lit("source1"))
        .withColumn("batch_id", F.lit(batch_id))
        .withColumn("ingestion_ts", F.current_timestamp())
    )
    return df


def read_source2(spark: SparkSession, path: str, batch_id: str) -> DataFrame:
    df = (
        spark.read.option("header", True).csv(path)
        .withColumn("source_name", F.lit("source2"))
        .withColumn("batch_id", F.lit(batch_id))
        .withColumn("ingestion_ts", F.current_timestamp())
        .withColumn("revenue", F.col("revenue").cast("double"))
        .withColumn("profit", F.col("profit").cast("double"))
    )
    return df
