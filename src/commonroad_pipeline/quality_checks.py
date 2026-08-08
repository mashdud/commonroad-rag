"""
Task 3: post-pipeline data quality gate.

Cheap, explicit assertions on row counts and failure rates. This is what
turns "the notebook ran without a Python exception" into "the data is
actually trustworthy" — the gap that caused the original 61,464-row
silent failure to go unnoticed until someone manually counted rows.

Run as a Databricks Job task:
    python -m commonroad_pipeline.quality_checks
"""

import logging

from pyspark.sql import SparkSession

from commonroad_pipeline.config import load_config

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("quality_checks")


def run(spark: SparkSession) -> None:
    cfg = load_config()

    bronze_count = spark.table(cfg.bronze_table).count()
    distinct_files = spark.table(cfg.bronze_table).select("file_name").distinct().count()
    scenario_count = spark.table(cfg.silver_scenarios_table).count()
    failed_count = spark.table(cfg.silver_failed_table).count()

    logger.info(
        "bronze_rows=%d distinct_files=%d silver_scenarios=%d parse_failures=%d",
        bronze_count, distinct_files, scenario_count, failed_count,
    )

    assert bronze_count == distinct_files, (
        f"bronze row count ({bronze_count}) != distinct file_name count "
        f"({distinct_files}) — duplicate ingestion detected, check the "
        f"Auto Loader checkpoint."
    )

    assert scenario_count > 0, "silver.scenarios is empty after a non-empty bronze load."

    failure_rate = failed_count / max(bronze_count, 1)
    assert failure_rate < 0.05, (
        f"parse failure rate {failure_rate:.1%} exceeds 5% threshold "
        f"({failed_count}/{bronze_count})."
    )

    logger.info("Quality checks passed.")


def main() -> None:
    spark = SparkSession.builder.getOrCreate()
    run(spark)


if __name__ == "__main__":
    main()
