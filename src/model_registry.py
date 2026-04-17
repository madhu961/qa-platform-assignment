import mlflow
import mlflow.spark


def log_model_run(tracking_uri: str, model, metrics: dict, params: dict) -> None:
    mlflow.set_tracking_uri(tracking_uri)
    mlflow.set_experiment("qa_platform_assignment")

    with mlflow.start_run():
        for k, v in params.items():
            mlflow.log_param(k, v)

        for k, v in metrics.items():
            mlflow.log_metric(k, v)

        mlflow.spark.log_model(model, artifact_path="model")
