import os

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.utils import AnalysisException

from lineage import with_lineage


MINIO_PASSWORD = os.getenv("MINIO_ROOT_PASSWORD", "minio1234")
MINIO_USER = os.getenv("MINIO_ROOT_USER", "minio12345")
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT_S3", "http://minio:9000")

BRONZE_PATH = "s3a://lakehouse/bronze/mevo"
SILVER_PATH = "s3a://lakehouse/silver/mevo"

# Rough bounding box of the Mevo service area (Tricity and surrounding municipalities).
SERVICE_AREA = {
    "lat_min": 53.9,
    "lat_max": 54.9,
    "lon_min": 17.8,
    "lon_max": 19.2,
}


def create_spark():
    builder = (
        SparkSession.builder
        .appName("silver-transformation")
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


def read_delta(spark, path):
    try:
        return spark.read.format("delta").load(path)
    except AnalysisException:
        return None


def read_new_bronze(spark, table, silver_df):
    df = spark.read.format("delta").load(f"{BRONZE_PATH}/{table}")

    if silver_df is not None:
        last_processed = silver_df.agg(F.max("last_updated")).first()[0]
        if last_processed is not None:
            df = df.filter(F.col("last_updated") > F.lit(last_processed))

    return df


def transform_free_bike_status(df):
    in_service_area = (
        F.col("lat").between(SERVICE_AREA["lat_min"], SERVICE_AREA["lat_max"])
        & F.col("lon").between(SERVICE_AREA["lon_min"], SERVICE_AREA["lon_max"])
    )

    return (
        df
        .drop("rental_uris", "year", "month")
        .withColumn("last_reported", F.timestamp_seconds("last_reported"))
        # Consecutive fetches return the same bike state until the bike reports again.
        .dropDuplicates(["bike_id", "last_reported"])
        .withColumn("has_invalid_coordinates", ~in_service_area)
        .withColumn("year", F.year("last_reported"))
        .withColumn("month", F.month("last_reported"))
    )


def count_vehicle_type(vehicle_type_id):
    return F.expr(
        "aggregate("
        f"filter(vehicle_types_available, x -> x.vehicle_type_id = '{vehicle_type_id}'), "
        "0L, (acc, x) -> acc + x.`count`)"
    )


def transform_station_status(df):
    has_negative_counts = (
        (F.col("num_bikes_available") < 0)
        | (F.col("num_docks_available") < 0)
        | (F.col("num_vehicles_available") < 0)
    )

    return (
        df
        .drop("year", "month")
        .withColumn("last_reported", F.timestamp_seconds("last_reported"))
        .withColumn("bikes_available", count_vehicle_type("bike"))
        .withColumn("ebikes_available", count_vehicle_type("ebike"))
        .drop("vehicle_types_available")
        # Every snapshot is kept so station occupancy can be analysed over time.
        .dropDuplicates(["station_id", "last_updated"])
        .withColumn("has_negative_counts", has_negative_counts)
        .withColumn("year", F.year("last_updated"))
        .withColumn("month", F.month("last_updated"))
    )


# Attributes whose changes create a new version of a station (SCD2).
STATION_TRACKED_COLUMNS = [
    "name",
    "address",
    "cross_street",
    "lat",
    "lon",
    "capacity",
    "is_virtual_station",
    "station_area",
]

SILVER_STATION_COLUMNS = [
    "station_key",
    "station_id",
    *STATION_TRACKED_COLUMNS,
    "attr_hash",
    "valid_from",
    "valid_to",
    "is_current",
]

# The first load has no earlier snapshots, so its versions are valid "from the beginning"
# and every historical station_status snapshot can be joined to a station version.
SCD2_INITIAL_VALID_FROM = "2000-01-01 00:00:00"


def transform_station_information(df):
    return (
        df
        .select("station_id", *STATION_TRACKED_COLUMNS, "last_updated")
        .dropDuplicates(["station_id"])
        .withColumn("attr_hash", F.sha2(F.to_json(F.struct(*STATION_TRACKED_COLUMNS)), 256))
    )


def overwrite_silver(df, table):
    (
        df.coalesce(1)
        .write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .save(f"{SILVER_PATH}/{table}")
    )


def merge_into_silver(spark, df, table, keys):
    path = f"{SILVER_PATH}/{table}"

    if read_delta(spark, path) is None:
        (
            df.coalesce(1)
            .write
            .format("delta")
            .partitionBy("year", "month")
            .save(path)
        )
        return

    df.createOrReplaceTempView("updates")
    condition = " AND ".join(f"target.{key} = source.{key}" for key in keys)

    spark.sql(f"""
        MERGE INTO delta.`{path}` AS target
        USING updates AS source
        ON {condition}
        WHEN NOT MATCHED THEN INSERT *
    """)


def build_free_bike_status(spark):
    table = "free_bike_status"
    silver_df = read_delta(spark, f"{SILVER_PATH}/{table}")

    df = read_new_bronze(spark, table, silver_df)
    df = transform_free_bike_status(df)

    merge_into_silver(spark, df, table, keys=["bike_id", "last_reported"])
    print(f"{table}: silver updated")


def build_station_status(spark):
    table = "station_status"
    silver_df = read_delta(spark, f"{SILVER_PATH}/{table}")

    df = read_new_bronze(spark, table, silver_df)
    df = transform_station_status(df)

    merge_into_silver(spark, df, table, keys=["station_id", "last_updated"])
    print(f"{table}: silver updated")


def build_station_information(spark):
    """Station dimension as SCD Type 2: one row per station version."""
    table = "station_information"
    path = f"{SILVER_PATH}/{table}"

    source = transform_station_information(
        spark.read.format("delta").load(f"{BRONZE_PATH}/{table}")
    )
    snapshot_at = source.agg(F.max("last_updated")).first()[0]

    # An empty feed would otherwise close every current station version.
    if snapshot_at is None:
        raise ValueError(f"{table}: bronze snapshot is empty, refusing to close all versions")

    silver_df = read_delta(spark, path)

    if silver_df is None:
        (
            source
            .withColumn("valid_from", F.lit(SCD2_INITIAL_VALID_FROM).cast("timestamp"))
            .withColumn("valid_to", F.lit(None).cast("timestamp"))
            .withColumn("is_current", F.lit(True))
            .withColumn("station_key", F.xxhash64("station_id", "valid_from"))
            .select(*SILVER_STATION_COLUMNS)
            .coalesce(1)
            .write
            .format("delta")
            .save(path)
        )
        print(f"{table}: silver created")
        return

    current = silver_df.filter("is_current").select("station_id", "attr_hash")

    staged_columns = ["merge_key", "station_id", *STATION_TRACKED_COLUMNS, "attr_hash", "last_updated"]

    compared = source.join(current.withColumnRenamed("attr_hash", "current_hash"), "station_id", "left")
    changed = compared.filter(
        F.col("current_hash").isNull() | (F.col("current_hash") != F.col("attr_hash"))
    )

    # New and changed stations: merge_key is NULL, so they never match and get inserted.
    new_versions = (
        changed
        .withColumn("merge_key", F.lit(None).cast("string"))
        .select(*staged_columns)
    )

    # Changed stations: match the current version and close it.
    closed_changed = (
        changed
        .filter(F.col("current_hash").isNotNull())
        .withColumn("merge_key", F.col("station_id"))
        .select(*staged_columns)
    )

    # Stations missing from the feed: close their current version, no new one is opened.
    removed = (
        silver_df
        .filter("is_current")
        .join(source.select("station_id"), "station_id", "left_anti")
        .withColumn("merge_key", F.col("station_id"))
        .withColumn("last_updated", F.lit(snapshot_at))
        .select(*staged_columns)
    )

    new_versions.unionByName(closed_changed).unionByName(removed).createOrReplaceTempView("staged")

    insert_columns = ", ".join(SILVER_STATION_COLUMNS)
    tracked = ", ".join(f"source.{column}" for column in STATION_TRACKED_COLUMNS)

    spark.sql(f"""
        MERGE INTO delta.`{path}` AS target
        USING staged AS source
        ON target.station_id = source.merge_key AND target.is_current = true
        WHEN MATCHED THEN UPDATE SET
            target.is_current = false,
            target.valid_to = source.last_updated
        WHEN NOT MATCHED AND source.merge_key IS NULL THEN INSERT ({insert_columns})
        VALUES (
            xxhash64(source.station_id, source.last_updated),
            source.station_id,
            {tracked},
            source.attr_hash,
            source.last_updated,
            NULL,
            true
        )
    """)
    print(f"{table}: silver SCD2 updated")


def build_static_table(spark, table, transform=None):
    df = spark.read.format("delta").load(f"{BRONZE_PATH}/{table}")

    if transform is not None:
        df = transform(df)

    overwrite_silver(df, table)
    print(f"{table}: silver overwritten")


if __name__ == "__main__":
    spark = create_spark()

    try:
        build_static_table(spark, "vehicle_types")
        build_static_table(spark, "system_pricing_plans")
        build_station_information(spark)
        build_station_status(spark)
        build_free_bike_status(spark)

    except Exception as e:
        print(f"Error during transformation: {e}")
        raise

    finally:
        spark.stop()
