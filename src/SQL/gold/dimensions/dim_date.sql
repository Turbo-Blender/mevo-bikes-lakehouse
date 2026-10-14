WITH bounds AS (
    SELECT
        to_date(from_utc_timestamp(MIN(last_updated), 'Europe/Warsaw')) AS first_date,
        to_date(from_utc_timestamp(MAX(last_updated), 'Europe/Warsaw')) AS last_date
    FROM silver_station_status
),

dates AS (
    SELECT explode(sequence(first_date, last_date, INTERVAL 1 DAY)) AS full_date
    FROM bounds
)

SELECT
    CAST(date_format(full_date, 'yyyyMMdd') AS INT) AS date_key,
    full_date,
    year(full_date) AS year,
    quarter(full_date) AS quarter,
    month(full_date) AS month,
    date_format(full_date, 'MMMM') AS month_name,
    day(full_date) AS day,
    weekday(full_date) + 1 AS day_of_week,
    date_format(full_date, 'EEEE') AS day_name,
    weekday(full_date) >= 5 AS is_weekend
FROM dates
