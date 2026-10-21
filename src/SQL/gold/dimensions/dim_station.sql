SELECT
    station_key,
    station_id,
    name AS station_name,
    address,
    cross_street,
    lat,
    lon,
    capacity,
    is_virtual_station,
    valid_from,
    valid_to,
    is_current
FROM silver_station_information
