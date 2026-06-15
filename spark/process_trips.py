"""
process_trips.py
================
PURPOSE:
    PySpark job that runs on Google Cloud Dataproc Serverless.

    Reads raw Citi Bike trip history CSV files from GCS,
    cleans and enriches them, then writes columnar Parquet files
    back to GCS — partitioned by year and month for efficient querying.

WHY SPARK INSTEAD OF PLAIN PYTHON?
    Citi Bike has 10+ years of trip history = hundreds of millions of rows.
    Pandas runs on one machine in RAM. Spark distributes the work across
    many machines automatically — Dataproc Serverless handles scaling for us.

WHY PARQUET?
    Parquet is a columnar format. When BigQuery runs:
        SELECT station_id, COUNT(*) FROM trips WHERE year=2024
    it reads ONLY the station_id and year columns, skipping everything else.
    This makes queries 5-10x cheaper and faster vs. CSV.

DATAPROC SERVERLESS:
    We submit this job using:
        gcloud dataproc batches submit pyspark spark/process_trips.py \
          --project=citibike-pipeline-499418 \
          --region=us-east1 \
          --deps-bucket=gs://citibike-pipeline-499418-spark-logs

    GCP spins up workers, runs the job, tears them down. No cluster management.

OUTPUT SCHEMA (what each Parquet row contains):
    trip_id, ride_id, rideable_type, started_at, ended_at,
    start_station_id, start_station_name, end_station_id, end_station_name,
    start_lat, start_lng, end_lat, end_lng, member_casual,
    trip_duration_minutes, start_hour, start_day_of_week, year, month
"""

import sys
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    DoubleType, IntegerType, StringType, StructField, StructType, TimestampType
)

# ── Configuration passed as command-line arguments ─────────────────────────────
# This lets Cloud Scheduler pass different date ranges without changing code.
INPUT_PATH  = sys.argv[1] if len(sys.argv) > 1 else "gs://citibike-pipeline-499418-data/raw/trip_history/*.csv.zip"
OUTPUT_PATH = sys.argv[2] if len(sys.argv) > 2 else "gs://citibike-pipeline-499418-data/processed/trips"


def create_spark_session() -> SparkSession:
    """
    Initialize SparkSession configured for GCS + BigQuery.
    On Dataproc, GCS connectors are pre-installed — no extra config needed.
    """
    return (
        SparkSession.builder
        .appName("CitiBike-TripHistory-Processing")
        # Optimize for large CSV reads
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
        .getOrCreate()
    )


def read_raw_trips(spark: SparkSession, input_path: str):
    """
    Read raw CSV files. Citi Bike changed their schema in Feb 2021
    (from 'tripduration' style to 'ride_id' style), so we handle both.

    We use inferSchema=False and provide our own schema for reliability —
    auto-inference can fail on mixed data types in dirty real-world CSVs.
    """
    # New schema (2021-present): 13 columns
    new_schema = StructType([
        StructField("ride_id",              StringType(),    True),
        StructField("rideable_type",        StringType(),    True),  # classic_bike / electric_bike
        StructField("started_at",           StringType(),    True),  # read as string, parse later
        StructField("ended_at",             StringType(),    True),
        StructField("start_station_name",   StringType(),    True),
        StructField("start_station_id",     StringType(),    True),
        StructField("end_station_name",     StringType(),    True),
        StructField("end_station_id",       StringType(),    True),
        StructField("start_lat",            DoubleType(),    True),
        StructField("start_lng",            DoubleType(),    True),
        StructField("end_lat",              DoubleType(),    True),
        StructField("end_lng",              DoubleType(),    True),
        StructField("member_casual",        StringType(),    True),  # "member" or "casual"
    ])

    df = (
        spark.read
        .option("header", "true")
        .option("inferSchema", "false")
        .option("mode", "PERMISSIVE")       # Don't crash on malformed rows
        .option("columnNameOfCorruptRecord", "_corrupt_record")
        .schema(new_schema)
        .csv(input_path)
    )

    print(f"[INFO] Raw rows read: {df.count():,}")
    return df


def clean_and_enrich(df):
    """
    Apply all transformations to produce the clean analytical dataset.

    Steps:
    1. Parse timestamps (Citi Bike uses mixed datetime formats across years)
    2. Filter out bad rows (nulls in key fields, invalid timestamps)
    3. Calculate trip duration
    4. Extract time-based features for our business question
    5. Add partition columns (year, month) for efficient BigQuery queries
    """

    # ── 1. Parse timestamps ────────────────────────────────────────────────
    # Citi Bike uses "2024-01-15 14:30:22" or "2024-01-15 14:30:22.897"
    # coalesce tries milliseconds first, falls back to seconds-only
    df = df.withColumn(
        "started_at_ts",
        F.coalesce(
            F.to_timestamp(F.col("started_at"), "yyyy-MM-dd HH:mm:ss.SSS"),
            F.to_timestamp(F.col("started_at"), "yyyy-MM-dd HH:mm:ss")
        )
    ).withColumn(
        "ended_at_ts",
        F.coalesce(
            F.to_timestamp(F.col("ended_at"), "yyyy-MM-dd HH:mm:ss.SSS"),
            F.to_timestamp(F.col("ended_at"), "yyyy-MM-dd HH:mm:ss")
        )
    )

    # ── 2. Filter bad rows ─────────────────────────────────────────────────
    df_clean = df.filter(
        F.col("started_at_ts").isNotNull() &
        F.col("ended_at_ts").isNotNull() &
        F.col("start_station_id").isNotNull() &
        F.col("end_station_id").isNotNull() &
        # Remove trips shorter than 1 minute (likely docking errors)
        (F.col("ended_at_ts") > F.col("started_at_ts")) &
        # Remove trips longer than 24 hours (likely stolen/lost bikes)
        (F.unix_timestamp("ended_at_ts") - F.unix_timestamp("started_at_ts") < 86400)
    )

    print(f"[INFO] Rows after cleaning: {df_clean.count():,}")

    # ── 3. Feature engineering ─────────────────────────────────────────────
    df_enriched = (
        df_clean
        # Trip duration in minutes (key metric for demand analysis)
        .withColumn(
            "trip_duration_minutes",
            F.round(
                (F.unix_timestamp("ended_at_ts") - F.unix_timestamp("started_at_ts")) / 60.0,
                2
            ).cast(DoubleType())
        )
        # Hour of day (0-23) — answers "when does supply gap peak?"
        .withColumn("start_hour", F.hour("started_at_ts").cast(IntegerType()))

        # Day of week (1=Sunday, 7=Saturday in Spark)
        .withColumn("start_day_of_week", F.dayofweek("started_at_ts").cast(IntegerType()))

        # Day name for readability in dashboards
        .withColumn(
            "start_day_name",
            F.date_format("started_at_ts", "EEEE")  # "Monday", "Tuesday", etc.
        )

        # Date only — useful for daily aggregations
        .withColumn("trip_date", F.to_date("started_at_ts"))

        # Partition columns — BigQuery uses these to skip entire partitions
        .withColumn("year",  F.year("started_at_ts").cast(IntegerType()))
        .withColumn("month", F.month("started_at_ts").cast(IntegerType()))
    )

    # ── 4. Select final columns (drop intermediates) ───────────────────────
    return df_enriched.select(
        "ride_id",
        "rideable_type",
        "started_at_ts",
        "ended_at_ts",
        "start_station_id",
        "start_station_name",
        "end_station_id",
        "end_station_name",
        "start_lat",
        "start_lng",
        "end_lat",
        "end_lng",
        "member_casual",
        "trip_duration_minutes",
        "start_hour",
        "start_day_of_week",
        "start_day_name",
        "trip_date",
        "year",
        "month"
    )


def write_parquet(df, output_path: str) -> None:
    """
    Write the cleaned DataFrame as Parquet to GCS.

    partitionBy("year", "month") creates a folder structure like:
        processed/trips/year=2024/month=1/part-00000.parquet
        processed/trips/year=2024/month=2/part-00000.parquet

    BigQuery's external table will auto-detect these partitions,
    so a query for 2024 data only reads year=2024/ folders.
    """
    (
        df.write
        .mode("overwrite")                    # Safe to re-run — replaces old output
        .partitionBy("year", "month")
        .parquet(output_path)
    )
    print(f"[INFO] Written to {output_path}")


def main():
    print("=" * 60)
    print("CitiBike Trip History — Spark Processing Job")
    print(f"  Input  : {INPUT_PATH}")
    print(f"  Output : {OUTPUT_PATH}")
    print("=" * 60)

    spark = create_spark_session()

    # Show Spark version and executor info for debugging
    print(f"[INFO] Spark version: {spark.version}")

    raw_df      = read_raw_trips(spark, INPUT_PATH)
    cleaned_df  = clean_and_enrich(raw_df)
    write_parquet(cleaned_df, OUTPUT_PATH)

    print("Job complete!")
    spark.stop()


if __name__ == "__main__":
    main()
