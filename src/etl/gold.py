import os
from pathlib import Path

from pyspark.sql import SparkSession

from common.lineage import with_lineage


MINIO_PASSWORD = os.getenv("MINIO_ROOT_PASSWORD", "minio1234")
MINIO_USER = os.getenv("MINIO_ROOT_USER", "minio12345")
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT_S3", "http://minio:9000")

SILVER_PATH = os.getenv("SILVER_PATH", "s3a://lakehouse/silver/mevo")
GOLD_PATH = os.getenv("GOLD_PATH", "s3a://lakehouse/gold/mevo")
SQL_DIR = Path(__file__).resolve().parents[1] / "SQL" / "gold"

SILVER_TABLES = [
    "station_information",
    "station_status",
    "vehicle_types",
    "system_pricing_plans",
]

# Built in order: later queries read earlier gold tables as gold_<name> views.
# (table name, SQL file relative to SQL_DIR, partition column)
GOLD_TABLES = [
    ("dim_station", "dimensions/dim_station.sql", None),
    ("dim_vehicle_type", "dimensions/dim_vehicle_type.sql", None),
    ("dim_date", "dimensions/dim_date.sql", None),
    ("fact_station_status", "facts/fact_station_status.sql", "date_key"),
    ("station_hourly_occupancy", "aggregates/station_hourly_occupancy.sql", "date_key"),
]


def create_spark():
    builder = (
        SparkSession.builder
        .appName("gold-build")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.hadoop.fs.s3a.endpoint", MINIO_ENDPOINT)
        .config("spark.hadoop.fs.s3a.access.key", MINIO_USER)
        .config("spark.hadoop.fs.s3a.secret.key", MINIO_PASSWORD)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    )
    return with_lineage(builder).getOrCreate()


def register_silver_views(spark):
    for table in SILVER_TABLES:
        (
            spark.read.format("delta")
            .load(f"{SILVER_PATH}/{table}")
            .createOrReplaceTempView(f"silver_{table}")
        )


def build_gold_table(spark, table, sql_file, partition_by):
    query = (SQL_DIR / sql_file).read_text(encoding="utf-8")
    path = f"{GOLD_PATH}/{table}"

    writer = (
        spark.sql(query)
        .coalesce(1)
        .write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
    )
    if partition_by is not None:
        writer = writer.partitionBy(partition_by)
    writer.save(path)

    spark.read.format("delta").load(path).createOrReplaceTempView(f"gold_{table}")
    print(f"{table}: gold rebuilt")


if __name__ == "__main__":
    spark = create_spark()

    try:
        register_silver_views(spark)

        for table, sql_file, partition_by in GOLD_TABLES:
            build_gold_table(spark, table, sql_file, partition_by)

    except Exception as e:
        print(f"Error during gold build: {e}")
        raise

    finally:
        spark.stop()
