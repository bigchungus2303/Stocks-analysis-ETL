WITH daily_prices AS (
    SELECT
        ticker,
        trading_date,
        close_price,
        LAG(close_price) OVER (PARTITION BY ticker ORDER BY trading_date) AS prev_close_price
    FROM {{ ref('stg_daily_stock_prices') }}
),

returns AS (
    SELECT
        ticker,
        trading_date,
        close_price,
        prev_close_price,
        -- Daily return calculation
        CASE
            WHEN prev_close_price IS NOT NULL AND prev_close_price > 0 THEN (close_price - prev_close_price) / prev_close_price
            ELSE NULL
        END AS daily_return,
        EXTRACT(YEAR FROM trading_date) AS year,
        EXTRACT(MONTH FROM trading_date) AS month,
        DATE_TRUNC('month', trading_date) AS month_date
    FROM daily_prices
    WHERE close_price > 0 AND prev_close_price > 0
)

SELECT
    {{ dbt_utils.generate_surrogate_key(['ticker', 'trading_date']) }} AS return_id,
    ticker,
    trading_date,
    close_price,
    prev_close_price,
    daily_return,
    year,
    month,
    month_date
FROM returns