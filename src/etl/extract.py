import json
import os

import requests
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from common.lineage import api_dataset, bronze_dataset, emit_run_event, with_lineage
from common.quality import ensure_constraints, split_by_rules, validate_payload


MINIO_PASSWORD = os.getenv("MINIO_ROOT_PASSWORD", "minio1234")
MINIO_USER = os.getenv("MINIO_ROOT_USER", "minio12345")
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT_S3", "http://minio:9000")

GBFS_BASE_URL = os.getenv("GBFS_BASE_URL", "https://gbfs.urbansharing.com/rowermevo.pl")
BRONZE_PATH = os.getenv("BRONZE_PATH", "s3a://lakehouse/bronze/mevo")
QUARANTINE_PATH = os.getenv("QUARANTINE_PATH", "s3a://lakehouse/quarantine/mevo")
# Above this share of rejected records the feed is treated as broken and nothing is written to bronze.
MAX_REJECTED_RATIO = float(os.getenv("BRONZE_MAX_REJECTED_RATIO", "0.1"))

headers = {
    "Client-Identifier": "mevo-bikes-lakehouse"
}

# Feed name -> key inside the JSON "data" section that holds the list of records.
FEEDS = {
    "vehicle_types": "vehicle_types",
    "system_pricing_plans": "plans",
    "station_information": "stations",
    "station_status": "stations",
    "free_bike_status": "bikes",
}

# Rarely changing feeds: keep only the latest snapshot (history via Delta time travel).
STATIC_FEEDS = {"vehicle_types", "system_pricing_plans", "station_information"}


def create_http_session():
    # Retries immediately, then waits 2s, 4s, 8s, 16s; honours Retry-After on 429/503.
    retry = Retry(
        total=5,
        backoff_factor=1,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET"],
        respect_retry_after_header=True,
    )
    session = requests.Session()
    session.mount("https://", HTTPAdapter(max_retries=retry))
    return session


http = create_http_session()


def fetch_feed(feed_name):
    response = http.get(
        f"{GBFS_BASE_URL}/{feed_name}.json",
        headers=headers,
        timeout=30,
    )
    response.raise_for_status()
    return response.json()


def save_feed(spark, feed_name, records_key):
    payload = fetch_feed(feed_name)
    validate_payload(payload, feed_name, records_key)
    records = payload["data"][records_key]

    if not records:
        print(f"{feed_name}: no records, skipping")
        return

    # read.json infers nested objects as structs instead of maps.
    df = spark.read.json(
        spark.sparkContext.parallelize([json.dumps(record) for record in records])
    )

    df = df.withColumn("last_updated", F.timestamp_seconds(F.lit(payload["last_updated"])))

    valid, rejected = split_by_rules(df, feed_name)
    rejected_count = rejected.count()

    if rejected_count:
        (
            rejected
            .withColumn("rejected_at", F.current_timestamp())
            .coalesce(1)
            .write
            .format("delta")
            .mode("append")
            .option("mergeSchema", "true")
            .save(f"{QUARANTINE_PATH}/{feed_name}")
        )

    rejected_ratio = rejected_count / len(records)
    if rejected_ratio > MAX_REJECTED_RATIO:
        raise ValueError(
            f"{feed_name}: {rejected_count}/{len(records)} records rejected "
            f"({rejected_ratio:.1%} > {MAX_REJECTED_RATIO:.1%}), bronze not updated"
        )

    df = valid.coalesce(1)

    if feed_name in STATIC_FEEDS:
        writer = (
            df.write
            .format("delta")
            .mode("overwrite")
            .option("overwriteSchema", "true")
        )
    else:
        writer = (
            df
            .withColumn("year", F.year("last_updated"))
            .withColumn("month", F.month("last_updated"))
            .write
            .format("delta")
            .mode("append")
            .option("mergeSchema", "true")
            .partitionBy("year", "month")
        )

    path = f"{BRONZE_PATH}/{feed_name}"
    writer.save(path)
    ensure_constraints(spark, path, feed_name)

    # Spark only sees the Delta write, so the API -> bronze step is reported explicitly.
    emit_run_event(
        "COMPLETE",
        job_name=f"fetch_{feed_name}",
        inputs=[api_dataset(GBFS_BASE_URL, feed_name)],
        outputs=[bronze_dataset(feed_name)],
    )

    print(f"{feed_name}: saved {len(records) - rejected_count} records, quarantined {rejected_count}")


def extract_and_save():
    builder = (
        SparkSession.builder
        .appName("bronze-raw-ingestion")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.hadoop.fs.s3a.endpoint", MINIO_ENDPOINT)
        .config("spark.hadoop.fs.s3a.access.key", MINIO_USER)
        .config("spark.hadoop.fs.s3a.secret.key", MINIO_PASSWORD)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    )
    spark = with_lineage(builder).getOrCreate()
    try:
        for feed_name, records_key in FEEDS.items():
            save_feed(spark, feed_name, records_key)

    except Exception as e:

        print(f"Error during extraction: {e}")

        raise

    finally:

        spark.stop()

if __name__ == "__main__":

    extract_and_save()
