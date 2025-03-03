WITH volatility_with_prices AS (
    SELECT
        v.volatility_id,
        v.ticker,
        v.month_date,
        v.trading_days,
        v.avg_daily_return,
        v.daily_return_stddev,
        v.annualized_volatility,
        v.sharpe_ratio,
        v.max_daily_loss,
        v.max_daily_gain,
        v.daily_return_range,
        p.month_open,
        p.month_high,
        p.month_low,
        p.month_close,
        p.month_volume,
        p.monthly_return,
        p.monthly_price_range
    FROM {{ ref('int_monthly_volatility') }} v
    JOIN {{ ref('int_monthly_stock_prices') }} p
        ON v.ticker = p.ticker AND v.month_date = p.month_date
),

ranked_volatility AS (
    SELECT
        *,
        -- Rank by volatility within each month
        RANK() OVER (PARTITION BY month_date ORDER BY daily_return_stddev DESC) AS volatility_rank,
        -- Percentile rank by volatility
        PERCENT_RANK() OVER (PARTITION BY month_date ORDER BY daily_return_stddev) AS volatility_percentile
    FROM volatility_with_prices
)

SELECT
    volatility_id,
    ticker,
    month_date,
    trading_days,
    avg_daily_return,
    daily_return_stddev,
    annualized_volatility,
    sharpe_ratio,
    max_daily_loss,
    max_daily_gain,
    daily_return_range,
    month_open,
    month_high,
    month_low,
    month_close,
    month_volume,
    monthly_return,
    monthly_price_range,
    volatility_rank,
    volatility_percentile,
    -- Volatility categories
    CASE
        WHEN volatility_percentile >= 0.9 THEN 'Very High'
        WHEN volatility_percentile >= 0.7 THEN 'High'
        WHEN volatility_percentile >= 0.3 THEN 'Medium'
        WHEN volatility_percentile >= 0.1 THEN 'Low'
        ELSE 'Very Low'
    END AS volatility_category
FROM ranked_volatility