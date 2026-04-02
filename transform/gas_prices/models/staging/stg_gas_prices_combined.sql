SELECT
    DATE(period)                     AS price_date,
    ROUND(CAST(value AS FLOAT64), 3) AS price_usd_per_gallon,
    LOWER(unit)                      AS unit,
    product_name                     AS product,
    series_id,
    ingested_at
FROM {{ source('raw', 'raw_eia_regular') }}
WHERE value IS NOT NULL

UNION ALL

SELECT
    DATE(period)                     AS price_date,
    ROUND(CAST(value AS FLOAT64), 3) AS price_usd_per_gallon,
    LOWER(unit)                      AS unit,
    product_name                     AS product,
    series_id,
    ingested_at
FROM {{ source('raw', 'raw_eia_midgrade') }}
WHERE value IS NOT NULL

UNION ALL

SELECT
    DATE(period)                     AS price_date,
    ROUND(CAST(value AS FLOAT64), 3) AS price_usd_per_gallon,
    LOWER(unit)                      AS unit,
    product_name                     AS product,
    series_id,
    ingested_at
FROM {{ source('raw', 'raw_eia_premium') }}
WHERE value IS NOT NULL
