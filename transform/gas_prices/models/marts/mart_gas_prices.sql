WITH base AS (
    SELECT * FROM {{ ref('stg_gas_prices_combined') }}
),

with_changes AS (
    SELECT
        price_date,
        product,
        price_usd_per_gallon,
        unit,
        LAG(price_usd_per_gallon, 1)
            OVER (PARTITION BY product ORDER BY price_date)
            AS prev_week_price,
        LAG(price_usd_per_gallon, 52)
            OVER (PARTITION BY product ORDER BY price_date)
            AS prev_year_price
    FROM base
)

SELECT
    price_date,
    product,
    price_usd_per_gallon,
    unit,
    prev_week_price,
    prev_year_price,
    ROUND(price_usd_per_gallon - prev_week_price, 3)        AS wow_change_usd,
    ROUND(
        SAFE_DIVIDE(price_usd_per_gallon - prev_week_price, prev_week_price) * 100, 2
    )                                                        AS wow_change_pct,
    ROUND(
        SAFE_DIVIDE(price_usd_per_gallon - prev_year_price, prev_year_price) * 100, 2
    )                                                        AS yoy_change_pct,
    CASE
        WHEN price_usd_per_gallon < 3.00 THEN 'low'
        WHEN price_usd_per_gallon < 4.00 THEN 'medium'
        ELSE 'high'
    END AS price_tier
FROM with_changes
ORDER BY price_date DESC, product
