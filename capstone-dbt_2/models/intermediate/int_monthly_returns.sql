WITH monthly_prices AS (
    SELECT
        ticker,
        DATE_TRUNC('month', trading_date) AS month_date,
        MIN(trading_date) AS first_day,
        MAX(trading_date) AS last_day,
        COUNT(*) AS trading_days,
        SUM(trading_volume) AS monthly_volume
    FROM {{ ref('stg_daily_stock_prices') }}
    GROUP BY ticker, DATE_TRUNC('month', trading_date)
),

month_end_prices AS (
    SELECT
        m.ticker,
        m.month_date,
        m.trading_days,
        m.monthly_volume,
        first_day.open_price AS month_open,
        last_day.close_price AS month_close
    FROM 
        monthly_prices m
        JOIN {{ ref('stg_daily_stock_prices') }} first_day
            ON m.ticker = first_day.ticker 
            AND m.first_day = first_day.trading_date
        JOIN {{ ref('stg_daily_stock_prices') }} last_day
            ON m.ticker = last_day.ticker 
            AND m.last_day = last_day.trading_date
)

SELECT
    {{ dbt_utils.generate_surrogate_key(['ticker', 'month_date']) }} AS monthly_return_id,
    ticker,
    month_date,
    EXTRACT(YEAR FROM month_date) AS year,
    EXTRACT(MONTH FROM month_date) AS month_number,
    -- Return calculations
    (month_close - month_open) / NULLIF(month_open, 0) AS monthly_return,
    month_close / NULLIF(month_open, 0) - 1 AS monthly_return_alt,
    -- Price levels
    month_open,
    month_close,
    -- Volume
    monthly_volume,
    monthly_volume / NULLIF(trading_days, 0) AS avg_daily_volume,
    trading_days
FROM month_end_prices
WHERE trading_days >= 15  -- Require at least 15 trading days