from pyspark.sql import DataFrame
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.classification import LogisticRegression
from pyspark.ml.evaluation import BinaryClassificationEvaluator


def train_profit_model(df: DataFrame):
    feature_cols = ["revenue", "num_top_suppliers", "num_main_customers", "num_activity_places"]

    assembler = VectorAssembler(inputCols=feature_cols, outputCol="features")
    assembled = assembler.transform(df).select("features", "label")

    train_df, test_df = assembled.randomSplit([0.8, 0.2], seed=42)

    model = LogisticRegression(featuresCol="features", labelCol="label")
    fitted = model.fit(train_df)

    predictions = fitted.transform(test_df)

    evaluator = BinaryClassificationEvaluator(labelCol="label")
    auc = evaluator.evaluate(predictions)

    metrics = {
        "auc": float(auc),
        "train_count": train_df.count(),
        "test_count": test_df.count()
    }
    return fitted, metrics
