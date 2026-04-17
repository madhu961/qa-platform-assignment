from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, DoubleType
from pyspark.sql.window import Window
from rapidfuzz import fuzz

from src.utils import clean_corporate_name, normalize_address


def _name_clean_udf():
    return F.udf(clean_corporate_name, StringType())


def _address_clean_udf():
    return F.udf(normalize_address, StringType())


def _fuzzy_score_udf():
    def score(a: str, b: str) -> float:
        if not a or not b:
            return 0.0
        return float(fuzz.token_sort_ratio(a, b))
    return F.udf(score, DoubleType())


def prepare_source1(df: DataFrame) -> DataFrame:
    return (
        df.withColumn("clean_name", _name_clean_udf()(F.col("corporate_name_s1")))
          .withColumn("clean_address", _address_clean_udf()(F.col("address")))
          .withColumn("name_block", F.substring(F.col("clean_name"), 1, 6))
          .withColumn("row_id_s1", F.monotonically_increasing_id())
    )


def prepare_source2(df: DataFrame) -> DataFrame:
    return (
        df.withColumn("clean_name", _name_clean_udf()(F.col("corporate_name_s2")))
          .withColumn("name_block", F.substring(F.col("clean_name"), 1, 6))
          .withColumn("row_id_s2", F.monotonically_increasing_id())
    )


def exact_match(source1_df: DataFrame, source2_df: DataFrame) -> DataFrame:
    s1 = prepare_source1(source1_df).alias("s1")
    s2 = prepare_source2(source2_df).alias("s2")

    matched = (
        s1.join(
            s2,
            on=F.col("s1.clean_name") == F.col("s2.clean_name"),
            how="inner"
        )
        .withColumn("match_type", F.lit("exact_name"))
        .withColumn("match_confidence", F.lit(100.0))
    )
    return matched


def get_unmatched_source1(prepared_s1: DataFrame, exact_matches: DataFrame) -> DataFrame:
    matched_ids = exact_matches.select(F.col("row_id_s1")).distinct()
    return prepared_s1.join(matched_ids, on="row_id_s1", how="left_anti")


def get_unmatched_source2(prepared_s2: DataFrame, exact_matches: DataFrame) -> DataFrame:
    matched_ids = exact_matches.select(F.col("row_id_s2")).distinct()
    return prepared_s2.join(matched_ids, on="row_id_s2", how="left_anti")


def fuzzy_match(unmatched_s1: DataFrame, unmatched_s2: DataFrame, threshold: float) -> DataFrame:
    score_udf = _fuzzy_score_udf()

    candidate_pairs = (
        unmatched_s1.alias("s1")
        .join(
            unmatched_s2.alias("s2"),
            on=F.col("s1.name_block") == F.col("s2.name_block"),
            how="inner"
        )
        .withColumn("name_score", score_udf(F.col("s1.clean_name"), F.col("s2.clean_name")))
        .filter(F.col("name_score") >= threshold)
    )

    best_matches = (
        candidate_pairs
        .withColumn(
            "rank",
            F.row_number().over(
                Window.partitionBy("s1.row_id_s1").orderBy(F.desc("name_score"))
            )
        )
        .filter(F.col("rank") == 1)
        .drop("rank")
        .withColumn("match_type", F.lit("fuzzy_name"))
        .withColumn("match_confidence", F.col("name_score"))
    )
    return best_matches


def build_matched_records(exact_matches: DataFrame, fuzzy_matches: DataFrame) -> DataFrame:
    return exact_matches.unionByName(fuzzy_matches, allowMissingColumns=True)


def build_unmatched_source1_only(prepared_s1: DataFrame, matched_records: DataFrame) -> DataFrame:
    matched_ids = matched_records.select(F.col("row_id_s1")).distinct()
    s1_only = (
        prepared_s1.join(matched_ids, on="row_id_s1", how="left_anti")
        .withColumn("match_type", F.lit("source1_only"))
        .withColumn("match_confidence", F.lit(0.0))
    )
    return s1_only


def build_unmatched_source2_only(prepared_s2: DataFrame, matched_records: DataFrame) -> DataFrame:
    matched_ids = matched_records.select(F.col("row_id_s2")).distinct()
    s2_only = (
        prepared_s2.join(matched_ids, on="row_id_s2", how="left_anti")
        .withColumn("match_type", F.lit("source2_only"))
        .withColumn("match_confidence", F.lit(0.0))
    )
    return s2_only


def resolve_entities(source1_df: DataFrame, source2_df: DataFrame, threshold: float) -> DataFrame:
    prepared_s1 = prepare_source1(source1_df)
    prepared_s2 = prepare_source2(source2_df)

    exact_matches = exact_match(source1_df, source2_df)

    unmatched_s1 = get_unmatched_source1(prepared_s1, exact_matches)
    unmatched_s2 = get_unmatched_source2(prepared_s2, exact_matches)

    fuzzy_matches = fuzzy_match(unmatched_s1, unmatched_s2, threshold)

    matched_records = build_matched_records(exact_matches, fuzzy_matches)

    s1_only = build_unmatched_source1_only(prepared_s1, matched_records)
    s2_only = build_unmatched_source2_only(prepared_s2, matched_records)

    matched_harmonized = (
        matched_records
        .withColumn(
            "canonical_name",
            F.coalesce(F.col("corporate_name_s1"), F.col("corporate_name_s2"))
        )
        .withColumn(
            "corporate_id",
            F.sha2(F.coalesce(F.col("s1.clean_name"), F.col("s2.clean_name"), F.col("clean_name")), 256)
        )
    )

    s1_only_harmonized = (
        s1_only
        .withColumn("canonical_name", F.col("corporate_name_s1"))
        .withColumn("corporate_id", F.sha2(F.col("clean_name"), 256))
    )

    s2_only_harmonized = (
        s2_only
        .withColumn("canonical_name", F.col("corporate_name_s2"))
        .withColumn("corporate_id", F.sha2(F.col("clean_name"), 256))
    )

    final_df = (
        matched_harmonized
        .unionByName(s1_only_harmonized, allowMissingColumns=True)
        .unionByName(s2_only_harmonized, allowMissingColumns=True)
    )

    return final_df
