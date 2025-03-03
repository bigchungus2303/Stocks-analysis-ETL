WITH monthly_stats AS (
    SELECT
        mr.ticker,
        mr.year,
        mr.month_number,
        mr.monthly_return,
        td.sector,
        td.industry,
        mr.month_date,
        -- Calculate relative volume
        mr.monthly_volume / NULLIF(
            AVG(mr.monthly_volume) OVER (
                PARTITION BY mr.ticker
                ORDER BY mr.month_date
                ROWS BETWEEN 12 PRECEDING AND 1 PRECEDING
            ), 0
        ) AS volume_vs_12m_avg
    FROM {{ ref('int_monthly_returns') }} mr
    LEFT JOIN {{ ref('stg_ticker_details') }} td
        ON mr.ticker = td.ticker
),

ticker_data_completeness AS (
    SELECT
        ticker,
        COUNT(DISTINCT (year * 100 + month_number)) AS total_months
    FROM monthly_stats
    GROUP BY ticker
),

seasonal_metrics AS (
    SELECT
        ms.ticker,
        ms.sector,
        ms.industry,
        ms.year,
        ms.month_number,
        ms.monthly_return,
        ms.volume_vs_12m_avg,
        -- Calculate metrics for seasonality analysis
        AVG(ms.monthly_return) OVER (
            PARTITION BY ms.ticker, ms.month_number
            ORDER BY ms.year
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
        ) AS avg_month_return_all_years,
        STDDEV(ms.monthly_return) OVER (
            PARTITION BY ms.ticker, ms.month_number
            ORDER BY ms.year
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
        ) AS std_month_return_all_years,
        COUNT(*) OVER (
            PARTITION BY ms.ticker, ms.month_number
        ) AS month_observations,
        tdc.total_months
    FROM monthly_stats ms
    JOIN ticker_data_completeness tdc ON ms.ticker = tdc.ticker
)

SELECT
    {{ dbt_utils.generate_surrogate_key(['ticker', 'sector', 'industry', 'year', 'month_number']) }} AS seasonal_pattern_id,
    sm.ticker,
    sm.sector,
    sm.industry,
    sm.year,
    sm.month_number,
    sm.monthly_return,
    sm.volume_vs_12m_avg,
    sm.avg_month_return_all_years,
    sm.std_month_return_all_years,
    sm.month_observations,
    sm.total_months, 
    -- Calculate z-score for the month's return
    (sm.monthly_return - sm.avg_month_return_all_years) / 
        NULLIF(sm.std_month_return_all_years, 0) AS month_return_zscore,
    -- Flag if return is positive
    CASE WHEN sm.monthly_return > 0 THEN 1 ELSE 0 END AS is_positive_return
FROM seasonal_metrics sm 
WHERE sm.total_months >= 24  -- Require at least 24 months of data total