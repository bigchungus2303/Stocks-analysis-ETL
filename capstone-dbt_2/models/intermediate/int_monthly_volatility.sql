WITH monthly_stats AS (
    SELECT
        ticker,
        month_date,
        COUNT(*) AS trading_days,
        AVG(daily_return) AS avg_daily_return,
        STDDEV(daily_return) AS daily_return_stddev,
        -- Annualized volatility (approximation)
        STDDEV(daily_return) * SQRT(252) AS annualized_volatility,
        -- Additional derived metrics
        AVG(daily_return) / NULLIF(STDDEV(daily_return), 0) AS sharpe_ratio,
        MIN(daily_return) AS max_daily_loss,
        MAX(daily_return) AS max_daily_gain,
        MAX(daily_return) - MIN(daily_return) AS daily_return_range
    FROM {{ ref('int_daily_returns') }}
    WHERE daily_return IS NOT NULL
    GROUP BY ticker, month_date
    HAVING COUNT(*) >= 15 -- Require at least 15 trading days in the month
)

SELECT
    {{ dbt_utils.generate_surrogate_key(['ticker', 'month_date']) }} AS volatility_id,
    ticker,
    month_date,
    trading_days,
    avg_daily_return,
    daily_return_stddev,
    annualized_volatility,
    sharpe_ratio,
    max_daily_loss,
    max_daily_gain,
    daily_return_range
FROM monthly_stats