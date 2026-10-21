CREATE TABLE IF NOT EXISTS dim_station
(
    station_key Int64,
    station_id String,
    station_name Nullable(String),
    address Nullable(String),
    cross_street Nullable(String),
    lat Nullable(Float64),
    lon Nullable(Float64),
    capacity Nullable(Int64),
    is_virtual_station Nullable(Bool),
    valid_from DateTime,
    valid_to Nullable(DateTime),
    is_current Bool
)
ENGINE = MergeTree
ORDER BY (station_id, valid_from);

CREATE TABLE IF NOT EXISTS dim_vehicle_type
(
    vehicle_type_id String,
    vehicle_type_name Nullable(String),
    form_factor Nullable(String),
    propulsion_type Nullable(String),
    rider_capacity Nullable(Int64),
    max_range_meters Nullable(Int64),
    pricing_plan_id Nullable(String),
    pricing_plan_name Nullable(String),
    price Nullable(Int64),
    currency Nullable(String)
)
ENGINE = MergeTree
ORDER BY vehicle_type_id;

CREATE TABLE IF NOT EXISTS dim_date
(
    date_key Int32,
    full_date Date,
    year Int32,
    quarter Int32,
    month Int32,
    month_name String,
    day Int32,
    day_of_week Int32,
    day_name String,
    is_weekend Bool
)
ENGINE = MergeTree
ORDER BY date_key;

CREATE TABLE IF NOT EXISTS fact_station_status
(
    station_key Nullable(Int64),
    station_id String,
    date_key Int32,
    hour Int32,
    snapshot_at DateTime,
    snapshot_at_utc DateTime,
    num_bikes_available Nullable(Int64),
    bikes_available Nullable(Int64),
    ebikes_available Nullable(Int64),
    num_docks_available Nullable(Int64),
    occupancy_ratio Nullable(Float64),
    is_empty Nullable(Bool),
    is_full Nullable(Bool),
    is_installed Nullable(Bool),
    is_renting Nullable(Bool),
    is_returning Nullable(Bool)
)
ENGINE = MergeTree
PARTITION BY intDiv(date_key, 100)
ORDER BY (station_id, snapshot_at_utc);

CREATE TABLE IF NOT EXISTS station_hourly_occupancy
(
    station_id String,
    date_key Int32,
    hour Int32,
    snapshot_count Int64,
    avg_bikes_available Nullable(Float64),
    min_bikes_available Nullable(Int64),
    max_bikes_available Nullable(Int64),
    avg_ebikes_available Nullable(Float64),
    avg_docks_available Nullable(Float64),
    avg_occupancy_ratio Nullable(Float64),
    empty_ratio Nullable(Float64),
    full_ratio Nullable(Float64)
)
ENGINE = MergeTree
PARTITION BY intDiv(date_key, 100)
ORDER BY (station_id, date_key, hour);
