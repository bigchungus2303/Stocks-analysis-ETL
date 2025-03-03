{{ config(
    materialized = 'table'
) }}

WITH ranked_volatility AS (
    SELECT
        ticker,
        month_date,
        daily_return_stddev,
        monthly_return,
        month_volume,
        monthly_price_range,
        sharpe_ratio,
        volatility_percentile
    FROM {{ ref('int_volatility_ranks') }}
),
latest_volatility AS (
    SELECT 
        ticker,
        daily_return_stddev AS latest_volatility,
        month_date AS latest_month
    FROM ranked_volatility rv1
    WHERE month_date = (
        SELECT MAX(month_date)
        FROM ranked_volatility rv2
        WHERE rv2.ticker = rv1.ticker
    )
),
consistent_volatility AS (
    SELECT
        ticker,
        COUNT(*) AS months_analyzed,
        COUNT(CASE WHEN volatility_percentile >= 0.9 THEN 1 END) AS high_vol_months,
        AVG(daily_return_stddev) AS avg_volatility,
        STDDEV(daily_return_stddev) AS volatility_of_volatility,
        COUNT(CASE WHEN volatility_percentile >= 0.9 THEN 1 END) / COUNT(*)::FLOAT AS high_vol_frequency,
        AVG(monthly_return) AS avg_monthly_return,
        AVG(month_volume) AS avg_monthly_volume,
        AVG(monthly_price_range) AS avg_monthly_price_range,
        AVG(sharpe_ratio) AS avg_sharpe_ratio
    FROM ranked_volatility
    GROUP BY ticker
    HAVING COUNT(*) >= 6 -- Require at least 6 months of data
)

SELECT
    cv.ticker,
    td.sector,
    td.industry,
    cv.months_analyzed,
    cv.high_vol_months,
    cv.avg_volatility,
    cv.volatility_of_volatility,
    cv.high_vol_frequency,
    cv.avg_monthly_return,
    cv.avg_monthly_volume,
    cv.avg_monthly_price_range,
    cv.avg_sharpe_ratio,
    lv.latest_volatility,
    lv.latest_month,
    -- Classify volatility consistency
    CASE 
        WHEN cv.high_vol_frequency >= 0.8 THEN 'Consistently High'
        WHEN cv.high_vol_frequency >= 0.5 THEN 'Frequently High'
        WHEN cv.high_vol_frequency >= 0.3 THEN 'Occasionally High'
        ELSE 'Rarely High'
    END AS volatility_consistency_category,
    -- Classify risk-adjusted performance
    CASE
        WHEN cv.avg_sharpe_ratio > 0.5 THEN 'Good'
        WHEN cv.avg_sharpe_ratio > 0 THEN 'Average'
        ELSE 'Poor'
    END AS risk_adjusted_performance
FROM consistent_volatility cv
LEFT JOIN latest_volatility lv ON cv.ticker = lv.ticker
LEFT JOIN {{ ref('stg_ticker_details') }} td ON cv.ticker = td.ticker
ORDER BY cv.high_vol_frequency DESC, cv.avg_volatility DESC