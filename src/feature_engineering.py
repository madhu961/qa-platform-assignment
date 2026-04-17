from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def add_ml_features(df: DataFrame, profit_threshold: float) -> DataFrame:
    return (
        df.fillna({"revenue": 0.0, "profit": 0.0})
          .withColumn("num_top_suppliers", F.size(F.split(F.col("top_suppliers"), r"\|")))
          .withColumn("num_main_customers", F.size(F.split(F.col("main_customers"), r"\|")))
          .withColumn("num_activity_places", F.size(F.split(F.col("activity_places"), r"\|")))
          .withColumn("label", F.when(F.col("profit") > profit_threshold, 1).otherwise(0))
    )
