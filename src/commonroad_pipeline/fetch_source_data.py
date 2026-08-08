"""
Task 0: fetch the official CommonRoad scenario dataset and land it in
raw/, ahead of the bronze/silver pipeline.

Runs entirely on the Databricks cluster -- clones the dataset repo to
the driver's local disk (plain /tmp, NOT DBFS -- same reasoning as the
tempfile trick used in parse_silver.py: local disk isn't blocked by
DBFS_DISABLED), then copies every .xml file into the UC-governed ADLS
raw/ path via dbutils.fs.cp. Nothing ever touches a laptop.

Idempotent by design: dbutils.fs.cp skips a file if it already exists at
the destination with the same content path (Auto Loader on the bronze
side is what actually guards against reprocessing -- this task's job is
just "make sure every upstream scenario file has a copy in raw/").

Run as a Databricks Job task (must run on a classic/job cluster, not
plain serverless Python, since it needs dbutils -- see note in run()):
    python -m commonroad_pipeline.fetch_source_data
"""

import logging
import subprocess
from pathlib import Path

from pyspark.sql import SparkSession

from commonroad_pipeline.config import load_config

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("fetch_source_data")

DATASET_REPO_URL = "https://gitlab.lrz.de/tum-cps/commonroad-scenarios.git"
LOCAL_CLONE_PATH = "/tmp/commonroad-scenarios"


def clone_dataset(repo_url: str = DATASET_REPO_URL, local_path: str = LOCAL_CLONE_PATH) -> Path:
    """Shallow-clone the dataset repo to local driver disk. Re-clones fresh
    each run (rm -rf first) rather than trying to `git pull` an existing
    clone, since job clusters don't persist /tmp between runs anyway."""
    subprocess.run(["rm", "-rf", local_path], check=True)
    logger.info("Cloning %s -> %s", repo_url, local_path)
    result = subprocess.run(
        ["git", "clone", "--depth", "1", repo_url, local_path],
        capture_output=True, text=True,
    )
    if result.returncode != 0:
        raise RuntimeError(f"git clone failed: {result.stderr}")
    return Path(local_path)


def find_xml_files(local_path: Path) -> list:
    return sorted(local_path.rglob("*.xml"))


def copy_to_raw(dbutils, xml_files: list, raw_path: str) -> int:
    copied = 0
    for i, local_file in enumerate(xml_files):
        dest = f"{raw_path}/{local_file.name}"
        dbutils.fs.cp(f"file:{local_file}", dest)
        copied += 1
        if i % 200 == 0:
            logger.info("Copied %d/%d", i, len(xml_files))
    return copied


def run(spark: SparkSession, dbutils) -> None:
    cfg = load_config()

    local_path = clone_dataset()
    xml_files = find_xml_files(local_path)
    logger.info("Found %d XML scenario files in cloned dataset", len(xml_files))

    if not xml_files:
        raise RuntimeError("No .xml files found in cloned dataset -- clone may have failed silently.")

    copied = copy_to_raw(dbutils, xml_files, cfg.raw_path)
    logger.info("Copied %d files into %s", copied, cfg.raw_path)


def main() -> None:
    spark = SparkSession.builder.getOrCreate()
    # dbutils is injected into the REPL on notebooks/Repos automatically, but a
    # plain "Python script" job task needs it constructed explicitly:
    from pyspark.dbutils import DBUtils
    dbutils = DBUtils(spark)
    run(spark, dbutils)


if __name__ == "__main__":
    main()
