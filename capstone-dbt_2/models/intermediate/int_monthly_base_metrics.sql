WITH daily_data AS (
    SELECT
        ticker,
        DATE_TRUNC('month', trading_date) AS month_date,
        trading_date,
        trading_volume,
        open_price,
        close_price,
        high_price,
        low_price,
        COUNT(*) OVER (PARTITION BY ticker, DATE_TRUNC('month', trading_date)) AS trading_days,
        -- Price metrics
        FIRST_VALUE(open_price) OVER (
            PARTITION BY ticker, DATE_TRUNC('month', trading_date) 
            ORDER BY trading_date) AS month_open,
        FIRST_VALUE(close_price) OVER (
            PARTITION BY ticker, DATE_TRUNC('month', trading_date) 
            ORDER BY trading_date DESC) AS month_close
    FROM stg_daily_stock_prices
),
monthly_aggregates AS (
    SELECT
        ticker,
        month_date,
        -- Volume metrics
        SUM(trading_volume) AS monthly_volume,
        AVG(trading_volume) AS avg_daily_volume,
        MAX(trading_days) AS trading_days,
        -- Price metrics
        MAX(month_open) AS month_open,
        MAX(month_close) AS month_close,
        MAX(high_price) AS month_high,
        MIN(low_price) AS month_low
    FROM daily_data
    GROUP BY ticker, month_date
)

SELECT
    {{ dbt_utils.generate_surrogate_key(['ticker', 'month_date']) }} AS monthly_metric_id,
    ticker,
    month_date,
    monthly_volume,
    avg_daily_volume,
    trading_days,
    month_open,
    month_close,
    month_high,
    month_low,
    (month_close - month_open) / NULLIF(month_open, 0) AS monthly_return,
    (month_high - month_low) / NULLIF(month_open, 0) AS monthly_price_range
FROM monthly_aggregates
WHERE trading_days >= 15 -- Require at least 15 trading days