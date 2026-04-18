from src.config import AppConfig
from src.ingest import read_source1, read_source2
from src.quality_checks import run_source1_checks, run_source2_checks
from src.entity_resolution import resolve_entities
from src.harmonize import harmonize_records
from src.iceberg_ops import (
    build_spark_session,
    create_database_and_table,
    stage_temp_view,
    write_staging_table,
    merge_into_corporate_registry,
    read_corporate_registry,
)
from src.reconcile import build_reconciliation_report
from src.feature_engineering import add_ml_features
from src.train_model import train_profit_model
from src.model_registry import log_model_run
from src.observability import configure_logger, log_event, timed_block
from src.drift_detection import detect_entity_drift


def main():
    config = AppConfig()
    logger = configure_logger()

    spark = build_spark_session("qa-platform-assignment", config.iceberg_warehouse)

    log_event(logger, "pipeline_start", {"batch_id": config.batch_id})

    source1_df = read_source1(spark, config.source1_path, config.batch_id)
    source2_df = read_source2(spark, config.source2_path, config.batch_id)

    run_source1_checks(source1_df, config.min_expected_rows)
    run_source2_checks(source2_df, config.min_expected_rows)

    with timed_block(logger, "entity_resolution"):
        resolved_df = resolve_entities(source1_df, source2_df, config.entity_match_threshold)

    harmonized_df = harmonize_records(resolved_df)

    harmonized_df = harmonized_df.select(
        "corporate_id",
        "canonical_name",
        "canonical_address",
        "activity_places",
        "top_suppliers",
        "main_customers",
        "revenue",
        "profit",
        "match_confidence",
        "source1_present",
        "source2_present",
        "last_updated_ts",
        "batch_id",
        "match_type"
        )

    create_database_and_table(spark, config.database_name, config.table_name)

    write_staging_table(
        harmonized_df,
        config.database_name,
        "corporate_registry_staging"
    )
    merge_into_corporate_registry(spark, config.database_name, config.table_name, "corporate_registry_staging")
    spark.sql(f"DROP TABLE IF EXISTS glue_catalog.{config.database_name}.corporate_registry_staging")

    final_df = read_corporate_registry(spark, config.database_name, config.table_name)

    source1_rows = source1_df.count()
    source2_rows = source2_df.count()
    harmonized_rows = harmonized_df.count()
    final_rows = final_df.count()
    exact_matches = final_df.filter(final_df.match_type == "exact_name").count()
    fuzzy_matches = final_df.filter(final_df.match_type == "fuzzy_name").count()
    source1_only = final_df.filter(final_df.match_type == "source1_only").count()
    source2_only = final_df.filter(final_df.match_type == "source2_only").count()
    recon = build_reconciliation_report(source1_rows, source2_rows, final_rows, exact_matches, fuzzy_matches, source1_only, source2_only)
    log_event(logger, "harmonized_counts", {"harmonized_rows": harmonized_rows})
    log_event(logger, "reconciliation", recon)

    current_match_rate = round(harmonized_rows / max(source1_rows + source2_rows, 1), 4)
    drift = detect_entity_drift(current_match_rate=current_match_rate, historical_avg=0.25)
    log_event(logger, "entity_drift", drift)

    feature_df = add_ml_features(final_df, config.profit_threshold)
    model, metrics = train_profit_model(feature_df)
    log_event(logger, "model_metrics", metrics)

    log_model_run(
        config.mlflow_tracking_uri,
        model,
        metrics,
        {
            "batch_id": config.batch_id,
            "profit_threshold": config.profit_threshold
        }
    )

    log_event(logger, "pipeline_complete", {"batch_id": config.batch_id})
    print("=" * 80)
    print("PIPELINE SUCCESS")
    print("Reconciliation:", recon)
    print("=" * 80)
    spark.stop()


if __name__ == "__main__":
    main()
