import json
import os
from datetime import date, datetime
from decimal import Decimal
from pathlib import Path

import requests
from pyspark.sql import SparkSession

from common.lineage import clickhouse_dataset, emit_run_event, gold_dataset


MINIO_PASSWORD = os.getenv("MINIO_ROOT_PASSWORD", "minio1234")
MINIO_USER = os.getenv("MINIO_ROOT_USER", "minio12345")
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT_S3", "http://minio:9000")

CLICKHOUSE_HOST = os.getenv("CLICKHOUSE_HOST", "clickhouse")
CLICKHOUSE_PORT = os.getenv("CLICKHOUSE_PORT", "8123")
CLICKHOUSE_USER = os.getenv("CLICKHOUSE_USER", "clickhouse")
CLICKHOUSE_PASSWORD = os.getenv("CLICKHOUSE_PASSWORD", "clickhouse")
CLICKHOUSE_DB = os.getenv("CLICKHOUSE_DB", "warehouse")

CLICKHOUSE_URL = f"http://{CLICKHOUSE_HOST}:{CLICKHOUSE_PORT}/"

GOLD_PATH = os.getenv("GOLD_PATH", "s3a://lakehouse/gold/mevo")
DDL_FILE = Path(__file__).resolve().parents[1] / "SQL" / "clickhouse" / "00_create_tables.sql"

GOLD_TABLES = [
    "dim_station",
    "dim_vehicle_type",
    "dim_date",
    "fact_station_status",
    "station_hourly_occupancy",
]

BATCH_SIZE = int(os.getenv("CLICKHOUSE_BATCH_SIZE", "50000"))


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


# ClickHouse is used through its HTTP interface: the Spark image ships Python 3.8,
# which the official Python driver no longer supports.
def clickhouse_request(data, query=None, database=None):
    params = {}
    if query is not None:
        params["query"] = query
    if database is not None:
        params["database"] = database

    response = requests.post(
        CLICKHOUSE_URL,
        params=params,
        data=data,
        auth=(CLICKHOUSE_USER, CLICKHOUSE_PASSWORD),
        timeout=120,
    )
    if response.status_code != 200:
        raise RuntimeError(f"ClickHouse error {response.status_code}: {response.text.strip()}")


def clickhouse_command(sql, database=CLICKHOUSE_DB):
    clickhouse_request(sql.encode("utf-8"), database=database)


def to_json_value(value):
    if isinstance(value, datetime):
        return value.strftime("%Y-%m-%d %H:%M:%S")
    if isinstance(value, date):
        return value.isoformat()
    if isinstance(value, Decimal):
        return float(value)
    if isinstance(value, float) and value != value:
        return None
    return value


def insert_rows(table, columns, rows):
    body = "\n".join(
        json.dumps({column: to_json_value(value) for column, value in zip(columns, row)})
        for row in rows
    )
    clickhouse_request(
        body.encode("utf-8"),
        query=f"INSERT INTO {table} FORMAT JSONEachRow",
        database=CLICKHOUSE_DB,
    )


def create_tables():
    clickhouse_command(f"CREATE DATABASE IF NOT EXISTS {CLICKHOUSE_DB}", database=None)

    for statement in DDL_FILE.read_text(encoding="utf-8").split(";"):
        if statement.strip():
            clickhouse_command(statement)


def load_table(spark, table):
    df = spark.read.format("delta").load(f"{GOLD_PATH}/{table}")
    columns = df.columns

    # Gold is rebuilt from silver on every run, so the warehouse table is replaced as a whole.
    clickhouse_command(f"TRUNCATE TABLE {table}")

    total = 0
    batch = []
    for row in df.toLocalIterator():
        batch.append(tuple(row))
        if len(batch) >= BATCH_SIZE:
            insert_rows(table, columns, batch)
            total += len(batch)
            batch = []

    if batch:
        insert_rows(table, columns, batch)
        total += len(batch)

    print(f"{table}: loaded {total} rows into ClickHouse")

    # The ClickHouse write goes through HTTP, so Spark cannot report it; send it explicitly.
    emit_run_event(
        "COMPLETE",
        job_name=f"load_clickhouse.{table}",
        inputs=[gold_dataset(table)],
        outputs=[clickhouse_dataset(CLICKHOUSE_HOST, CLICKHOUSE_PORT, CLICKHOUSE_DB, table)],
    )


if __name__ == "__main__":
    spark = None

    try:
        spark = create_spark()

        create_tables()

        for table in GOLD_TABLES:
            load_table(spark, table)

    except Exception as e:
        print(f"Error during loading: {e}")
        raise

    finally:
        if spark is not None:
            spark.stop()
