from src.iceberg_ops import build_spark_session

WAREHOUSE = "s3://qa-platform-assignment/iceberg/warehouse/"

spark = build_spark_session("iceberg-smoke-test", WAREHOUSE)

spark.sql("CREATE DATABASE IF NOT EXISTS glue_catalog.qa_assignment")

df = spark.createDataFrame(
    [("c1", "Acme Corp", 1000000.0, 120000.0)],
    ["corporate_id", "canonical_name", "revenue", "profit"]
)

df.createOrReplaceTempView("tmp_registry")

spark.sql("""
CREATE TABLE IF NOT EXISTS glue_catalog.qa_assignment.corporate_registry_test
USING iceberg
TBLPROPERTIES ("format-version"="2")
AS
SELECT * FROM tmp_registry
""")

result = spark.read.format("iceberg").load("glue_catalog.qa_assignment.corporate_registry_test")
result.show(truncate=False)

spark.stop()
