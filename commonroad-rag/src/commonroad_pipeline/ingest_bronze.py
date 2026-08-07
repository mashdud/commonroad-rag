"""
Task 1: raw -> bronze ingestion.

Uses Auto Loader (cloudFiles) with trigger(availableNow=True) so that:
  - each run only processes files not yet seen (checkpoint-tracked)
  - the job behaves like a batch job (runs once, exits) but never
    reprocesses or duplicates a file, which was the root cause of the
    'no element found' truncation errors from the manual notebook version.

Run as a Databricks Job task:
    python -m commonroad_pipeline.ingest_bronze
"""

import logging

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from commonroad_pipeline.config import load_config

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("ingest_bronze")


def build_bronze_stream(spark: SparkSession, cfg) -> DataFrame:
    return (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "text")
        .option("cloudFiles.schemaLocation", cfg.schema_location)
        .option("wholetext", True)
        .load(cfg.raw_path)
        .withColumn("source_path", F.col("_metadata.file_path"))
        .withColumn("ingestion_time", F.current_timestamp())
        .withColumn("file_name", F.regexp_extract("source_path", r"([^/]+$)", 1))
        .withColumn("scenario_id", F.regexp_extract("file_name", r"(.+)\.xml", 1))
        .withColumnRenamed("value", "xml_content")
    )


def run(spark: SparkSession) -> None:
    cfg = load_config()
    logger.info("Ingesting raw XML from %s into %s", cfg.raw_path, cfg.bronze_table)

    bronze_stream = build_bronze_stream(spark, cfg)

    query = (
        bronze_stream.writeStream
        .option("checkpointLocation", cfg.checkpoint_path)
        .trigger(availableNow=True)
        .toTable(cfg.bronze_table)
    )
    query.awaitTermination()

    count = spark.table(cfg.bronze_table).count()
    logger.info("Bronze table %s now has %d total rows", cfg.bronze_table, count)


def main() -> None:
    spark = SparkSession.builder.getOrCreate()
    run(spark)


if __name__ == "__main__":
    main()
