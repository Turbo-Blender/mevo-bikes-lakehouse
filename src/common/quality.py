"""Data quality rules guarding the bronze layer.

Every rule is a Spark SQL expression that a valid record must satisfy. The same
expressions are used to split incoming records and as Delta CHECK constraints,
so a write that bypasses the validation is still rejected by the table itself.
"""
from pyspark.sql import functions as F


VALID_LAT = "BETWEEN -90 AND 90"
VALID_LON = "BETWEEN -180 AND 180"

# Negative counters are not rejected here: silver keeps them and flags them (has_negative_counts).
BRONZE_RULES = {
    "vehicle_types": {
        "vehicle_type_id_not_null": "vehicle_type_id IS NOT NULL",
        "rider_capacity_positive": "rider_capacity IS NULL OR rider_capacity > 0",
    },
    "system_pricing_plans": {
        "plan_id_not_null": "plan_id IS NOT NULL",
        "currency_not_null": "currency IS NOT NULL",
        "price_non_negative": "price IS NOT NULL AND price >= 0",
    },
    "station_information": {
        "station_id_not_null": "station_id IS NOT NULL",
        "station_lat_valid": f"lat IS NOT NULL AND lat {VALID_LAT}",
        "station_lon_valid": f"lon IS NOT NULL AND lon {VALID_LON}",
        "capacity_non_negative": "capacity IS NULL OR capacity >= 0",
    },
    "station_status": {
        "station_id_not_null": "station_id IS NOT NULL",
        "last_reported_not_null": "last_reported IS NOT NULL",
        "num_bikes_not_null": "num_bikes_available IS NOT NULL",
        "num_docks_not_null": "num_docks_available IS NOT NULL",
    },
    "free_bike_status": {
        "bike_id_not_null": "bike_id IS NOT NULL",
        "last_reported_not_null": "last_reported IS NOT NULL",
        "bike_lat_valid": f"lat IS NOT NULL AND lat {VALID_LAT}",
        "bike_lon_valid": f"lon IS NOT NULL AND lon {VALID_LON}",
    },
}

# Columns the rules and the silver layer depend on; a feed without them breaks the contract.
REQUIRED_COLUMNS = {
    "vehicle_types": ["vehicle_type_id", "rider_capacity"],
    "system_pricing_plans": ["plan_id", "currency", "price"],
    "station_information": ["station_id", "lat", "lon", "capacity"],
    "station_status": [
        "station_id",
        "last_reported",
        "num_bikes_available",
        "num_docks_available",
        "vehicle_types_available",
    ],
    "free_bike_status": ["bike_id", "last_reported", "lat", "lon"],
}


def validate_payload(payload, feed_name, records_key):
    """Reject a response that does not look like a GBFS feed before Spark touches it."""
    if not isinstance(payload, dict):
        raise ValueError(f"{feed_name}: payload is not a JSON object")

    last_updated = payload.get("last_updated")
    if not isinstance(last_updated, int) or last_updated <= 0:
        raise ValueError(f"{feed_name}: invalid last_updated {last_updated!r}")

    data = payload.get("data")
    if not isinstance(data, dict) or not isinstance(data.get(records_key), list):
        raise ValueError(f"{feed_name}: missing list data.{records_key}")


def check_required_columns(df, feed_name):
    missing = [column for column in REQUIRED_COLUMNS[feed_name] if column not in df.columns]
    if missing:
        raise ValueError(f"{feed_name}: missing required columns {missing}")


def split_by_rules(df, feed_name):
    """Return (valid, rejected); rejected rows carry the names of the rules they broke."""
    check_required_columns(df, feed_name)

    failed = F.array(*[
        F.when(~F.coalesce(F.expr(expression), F.lit(False)), F.lit(name))
        for name, expression in BRONZE_RULES[feed_name].items()
    ])
    checked = df.withColumn("failed_rules", F.filter(failed, lambda rule: rule.isNotNull())).cache()

    valid = checked.filter(F.size("failed_rules") == 0).drop("failed_rules")
    rejected = checked.filter(F.size("failed_rules") > 0)
    return valid, rejected


def ensure_constraints(spark, path, feed_name):
    """Add missing Delta CHECK constraints; existing ones are left untouched."""
    existing = {
        row.key
        for row in spark.sql(f"SHOW TBLPROPERTIES delta.`{path}`").collect()
    }
    for name, expression in BRONZE_RULES[feed_name].items():
        if f"delta.constraints.{name}" not in existing:
            spark.sql(f"ALTER TABLE delta.`{path}` ADD CONSTRAINT {name} CHECK ({expression})")
