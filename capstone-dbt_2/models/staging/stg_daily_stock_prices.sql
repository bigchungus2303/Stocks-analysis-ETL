{{ config(
    materialized = 'table'
) }}

WITH source AS (
    SELECT
        ticker,
        date,
        open,
        high,
        low,
        close,
        volume,
        rn,
        upload_d AS upload_date
    FROM {{ source('bigchungus0148', 'stock_prices_version5') }}
),

cleaned AS (
    SELECT
        ticker,
        CAST(date AS DATE) AS trading_date,
        CAST(NULLIF(open, 0) AS FLOAT) AS open_price,
        CAST(NULLIF(high, 0) AS FLOAT) AS high_price,
        CAST(NULLIF(low, 0) AS FLOAT) AS low_price,
        CAST(NULLIF(close, 0) AS FLOAT) AS close_price,
        CAST(volume AS INTEGER) AS trading_volume,
        rn,
        upload_date,
        -- Add date parts for easier filtering and aggregation
        EXTRACT(YEAR FROM CAST(date AS DATE)) AS year,
        EXTRACT(MONTH FROM CAST(date AS DATE)) AS month,
        EXTRACT(DAY FROM CAST(date AS DATE)) AS day,
        -- Add row hash for deduplication
        {{ dbt_utils.generate_surrogate_key(['ticker', 'date']) }} AS row_id
    FROM source
    WHERE
        -- Basic data quality filters
        ticker IS NOT NULL
        AND date IS NOT NULL
        AND close > 0
)

SELECT
    row_id,
    ticker,
    trading_date,
    open_price,
    high_price,
    low_price,
    close_price,
    trading_volume,
    year,
    month,
    day,
    rn,
    upload_date
FROM cleaned