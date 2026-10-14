SELECT
    station_id,
    date_key,
    hour,
    COUNT(*) AS snapshot_count,
    ROUND(AVG(num_bikes_available), 2) AS avg_bikes_available,
    MIN(num_bikes_available) AS min_bikes_available,
    MAX(num_bikes_available) AS max_bikes_available,
    ROUND(AVG(ebikes_available), 2) AS avg_ebikes_available,
    ROUND(AVG(num_docks_available), 2) AS avg_docks_available,
    ROUND(AVG(occupancy_ratio), 3) AS avg_occupancy_ratio,
    ROUND(AVG(CAST(is_empty AS INT)), 3) AS empty_ratio,
    ROUND(AVG(CAST(is_full AS INT)), 3) AS full_ratio
FROM gold_fact_station_status
WHERE is_installed
GROUP BY
    station_id,
    date_key,
    hour
