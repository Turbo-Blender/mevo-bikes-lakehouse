SELECT
    station_id,
    name AS station_name,
    address,
    cross_street,
    lat,
    lon,
    capacity,
    is_virtual_station,
    last_updated AS source_updated_at
FROM silver_station_information
