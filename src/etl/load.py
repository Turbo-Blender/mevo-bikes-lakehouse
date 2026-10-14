import os
from pathlib import Path

import clickhouse_connect
from pyspark.sql import SparkSession


MINIO_PASSWORD = os.getenv("MINIO_ROOT_PASSWORD", "minio1234")
MINIO_USER = os.getenv("MINIO_ROOT_USER", "minio12345")
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT_S3", "http://minio:9000")

CLICKHOUSE_HOST = os.getenv("CLICKHOUSE_HOST", "clickhouse")
CLICKHOUSE_PORT = int(os.getenv("CLICKHOUSE_PORT", "8123"))
CLICKHOUSE_USER = os.getenv("CLICKHOUSE_USER", "clickhouse")
CLICKHOUSE_PASSWORD = os.getenv("CLICKHOUSE_PASSWORD", "clickhouse")
CLICKHOUSE_DB = os.getenv("CLICKHOUSE_DB", "warehouse")

GOLD_PATH = "s3a://lakehouse/gold/mevo"
DDL_FILE = Path(__file__).resolve().parents[1] / "SQL" / "clickhouse" / "00_create_tables.sql"

GOLD_TABLES = [
    "dim_station",
    "dim_vehicle_type",
    "dim_date",
    "fact_station_status",
    "station_hourly_occupancy",
]

BATCH_SIZE = 50_000


def create_spark():
    return (
        SparkSession.builder
        .appName("gold-to-clickhouse")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.hadoop.fs.s3a.endpoint", MINIO_ENDPOINT)
        .config("spark.hadoop.fs.s3a.access.key", MINIO_USER)
        .config("spark.hadoop.fs.s3a.secret.key", MINIO_PASSWORD)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .getOrCreate()
    )


def create_client():
    admin = clickhouse_connect.get_client(
        host=CLICKHOUSE_HOST,
        port=CLICKHOUSE_PORT,
        username=CLICKHOUSE_USER,
        password=CLICKHOUSE_PASSWORD,
    )
    admin.command(f"CREATE DATABASE IF NOT EXISTS {CLICKHOUSE_DB}")
    admin.close()

    return clickhouse_connect.get_client(
        host=CLICKHOUSE_HOST,
        port=CLICKHOUSE_PORT,
        username=CLICKHOUSE_USER,
        password=CLICKHOUSE_PASSWORD,
        database=CLICKHOUSE_DB,
    )


def create_tables(client):
    statements = DDL_FILE.read_text(encoding="utf-8").split(";")
    for statement in statements:
        if statement.strip():
            client.command(statement)


def load_table(spark, client, table):
    df = spark.read.format("delta").load(f"{GOLD_PATH}/{table}")
    columns = df.columns

    # Gold is rebuilt from silver on every run, so the warehouse table is replaced as a whole.
    client.command(f"TRUNCATE TABLE {table}")

    total = 0
    batch = []
    for row in df.toLocalIterator():
        batch.append(tuple(row))
        if len(batch) >= BATCH_SIZE:
            client.insert(table, batch, column_names=columns)
            total += len(batch)
            batch = []

    if batch:
        client.insert(table, batch, column_names=columns)
        total += len(batch)

    print(f"{table}: loaded {total} rows into ClickHouse")


if __name__ == "__main__":
    spark = None
    client = None

    try:
        spark = create_spark()
        client = create_client()

        create_tables(client)

        for table in GOLD_TABLES:
            load_table(spark, client, table)

    except Exception as e:
        print(f"Error during loading: {e}")
        raise

    finally:
        if client is not None:
            client.close()
        if spark is not None:
            spark.stop()
