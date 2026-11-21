"""Glue job: raw Scania APS CSV -> curated Parquet + Glue Data Catalog table.

Input  (raw zone, read-only, never modified):
    s3://<raw-bucket>/scania_aps/split=train/aps_failure_training_set.csv
    s3://<raw-bucket>/scania_aps/split=test/aps_failure_test_set.csv

Output (curated zone, fully rebuilt on every run):
    s3://<curated-bucket>/scania_aps/event_date=YYYY-MM-DD/part-*.parquet
    Glue table <database>.<table>, queryable from Athena.

Note: reading_id, vehicle_id and event_date are SYNTHETIC. They were added by
scripts/prepare_dataset.py because the source dataset has no IDs or timestamps.
"""

import sys

from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

# Columns that are NOT sensors. "split" comes from the folder name split=train/.
ID_COLUMNS = ["reading_id", "vehicle_id", "event_date", "split"]
LABEL_COLUMN = "class"  # "pos" = failure in the air pressure system, "neg" = other


def sensor_columns(df: DataFrame) -> list[str]:
    """Every column that isn't an ID or the label: the 170 sensors (aa_000 ... eg_000)."""
    return [c for c in df.columns if c not in ID_COLUMNS + [LABEL_COLUMN]]


def check_raw_quality(raw: DataFrame) -> None:
    """Fail the job loudly instead of silently writing bad data to curated."""
    if raw.limit(1).count() == 0:
        raise ValueError("Raw input is empty. Check raw_path.")

    unknown_labels = raw.filter(~F.col(LABEL_COLUMN).isin("neg", "pos")).count()
    if unknown_labels:
        raise ValueError(f"{unknown_labels} rows have a class other than 'neg'/'pos'")

    # A value that isn't "na" but still fails to become a number means the
    # source format changed. cast() would turn it into null without any error,
    # so we count those cases ourselves (one pass over the data for all columns).
    sensors = sensor_columns(raw)
    bad_counts = raw.select(
        [
            F.sum(
                (F.col(c).isNotNull() & (F.col(c) != "na") & F.col(c).cast("double").isNull()).cast("int")
            ).alias(c)
            for c in sensors
        ]
    ).first().asDict()
    bad = {col: n for col, n in bad_counts.items() if n}
    if bad:
        raise ValueError(f"Non-numeric sensor values found (column: count): {bad}")


def transform(raw: DataFrame) -> DataFrame:
    """The actual cleaning: na -> null, strings -> numbers/dates, add is_failure."""
    sensors = sensor_columns(raw)
    return raw.select(
        "reading_id",
        "vehicle_id",
        F.to_date("event_date", "yyyy-MM-dd").alias("event_date"),
        "split",
        LABEL_COLUMN,
        (F.col(LABEL_COLUMN) == "pos").alias("is_failure"),
        *[
            F.when(F.col(c) == "na", None).otherwise(F.col(c)).cast("double").alias(c)
            for c in sensors
        ],
    )


def main() -> None:
    args = getResolvedOptions(sys.argv, ["JOB_NAME", "raw_path", "curated_path", "database", "table"])

    sc = SparkContext.getOrCreate()
    glue_context = GlueContext(sc)
    spark = glue_context.spark_session
    job = Job(glue_context)
    job.init(args["JOB_NAME"], args)

    # header=True: first line is column names. No inferSchema: every column is a
    # string, so "na" survives the read and we control every type conversion.
    raw = spark.read.option("header", True).csv(args["raw_path"])
    raw.cache()  # read twice (quality check + write); keep it in memory

    check_raw_quality(raw)
    curated = transform(raw)

    table = f"{args['database']}.{args['table']}"
    (
        curated.repartition("event_date")  # one file per date instead of many tiny ones
        .write.mode("overwrite")  # curated is rebuildable: replace it fully each run
        .format("parquet")
        .partitionBy("event_date")  # folders event_date=2026-03-14/ -> Athena skips dates you don't query
        .option("path", args["curated_path"])  # data goes to S3, not Glue's default warehouse
        .saveAsTable(table)  # also creates/updates the table in the Glue Data Catalog
    )

    rows = curated.count()
    failures = curated.filter("is_failure").count()
    print(f"Wrote {rows} rows ({failures} failures) to {table} at {args['curated_path']}")

    job.commit()


if __name__ == "__main__":
    main()
