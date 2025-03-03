WITH daily_window_calcs AS (
    SELECT
        ticker,
        trading_date,
        DATE_TRUNC('month', trading_date) AS month_date,
        -- Window functions at daily level
        FIRST_VALUE(open_price) OVER (
            PARTITION BY ticker, DATE_TRUNC('month', trading_date)
            ORDER BY trading_date
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
        ) AS month_open,
        LAST_VALUE(close_price) OVER (
            PARTITION BY ticker, DATE_TRUNC('month', trading_date)
            ORDER BY trading_date
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
        ) AS month_close,
        high_price,
        low_price,
        trading_volume
    FROM {{ ref('stg_daily_stock_prices') }}
),
monthly_data AS (
    SELECT
        ticker,
        month_date,
        -- Take any row's month_open/close (they're all the same within month)
        MAX(month_open) AS month_open,
        MAX(month_close) AS month_close,
        MAX(high_price) AS month_high,
        MIN(low_price) AS month_low,
        SUM(trading_volume) AS month_volume,
        COUNT(*) AS trading_days
    FROM daily_window_calcs
    GROUP BY ticker, month_date
)

SELECT
    {{ dbt_utils.generate_surrogate_key(['ticker', 'month_date']) }} AS monthly_price_id,
    ticker,
    month_date,
    month_open,
    month_high,
    month_low,
    month_close,
    month_volume,
    -- Calculate monthly price change
    (month_close - month_open) / NULLIF(month_open, 0) AS monthly_return,
    -- Calculate true range
    (month_high - month_low) / NULLIF(month_open, 0) AS monthly_price_range,
    trading_days
FROM monthly_data
WHERE trading_days >= 15 -- Require at least 15 trading days