WITH snapshots AS (
    SELECT
        *,
        from_utc_timestamp(last_updated, 'Europe/Warsaw') AS snapshot_at
    FROM silver_station_status
    WHERE NOT has_negative_counts
)

SELECT
    i.station_key,
    s.station_id,
    CAST(date_format(s.snapshot_at, 'yyyyMMdd') AS INT) AS date_key,
    hour(s.snapshot_at) AS hour,
    s.snapshot_at,
    s.last_updated AS snapshot_at_utc,
    s.num_bikes_available,
    s.bikes_available,
    s.ebikes_available,
    s.num_docks_available,
    s.num_bikes_available / NULLIF(i.capacity, 0) AS occupancy_ratio,
    s.num_bikes_available = 0 AS is_empty,
    s.num_docks_available = 0 AS is_full,
    s.is_installed,
    s.is_renting,
    s.is_returning
FROM snapshots AS s
-- SCD2: pick the station version that was valid when the snapshot was taken.
LEFT JOIN silver_station_information AS i
    ON s.station_id = i.station_id
    AND s.last_updated >= i.valid_from
    AND (i.valid_to IS NULL OR s.last_updated < i.valid_to)
