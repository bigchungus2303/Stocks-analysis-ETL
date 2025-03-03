{{ config(
    materialized = 'table'
) }}

WITH monthly_volatility_series AS (
    -- Get volatility data at monthly grain
    SELECT
        vr.ticker,
        vr.month_date,
        vr.daily_return_stddev AS monthly_volatility,
        vr.annualized_volatility,
        vr.sharpe_ratio,
        vr.monthly_return,
        vr.month_volume,
        vr.monthly_price_range,
        vr.volatility_rank,
        vr.volatility_percentile,
        vr.volatility_category,
        -- Extract date parts for easier filtering using Snowflake functions
        DATE_PART('year', vr.month_date) AS year,
        DATE_PART('month', vr.month_date) AS month_number,
        -- Calculate relative metrics (compared to previous periods)
        LAG(vr.daily_return_stddev) OVER (
            PARTITION BY vr.ticker 
            ORDER BY vr.month_date
        ) AS prev_month_volatility,
        LAG(vr.daily_return_stddev, 3) OVER (
            PARTITION BY vr.ticker 
            ORDER BY vr.month_date
        ) AS volatility_3m_prior,
        LAG(vr.daily_return_stddev, 12) OVER (
            PARTITION BY vr.ticker 
            ORDER BY vr.month_date
        ) AS volatility_12m_prior
    FROM {{ ref('int_volatility_ranks') }} vr
),
month_to_month_changes AS (
    -- Calculate month-to-month changes and relative values
    SELECT
        ticker,
        month_date,
        year,
        month_number,
        monthly_volatility,
        annualized_volatility,
        sharpe_ratio,
        monthly_return,
        month_volume,
        monthly_price_range,
        volatility_rank,
        volatility_percentile,
        volatility_category,
        prev_month_volatility,
        volatility_3m_prior,
        volatility_12m_prior,
        -- Calculate month-over-month changes
        CASE 
            WHEN prev_month_volatility IS NOT NULL AND prev_month_volatility > 0
            THEN (monthly_volatility - prev_month_volatility) / prev_month_volatility
            ELSE NULL
        END AS volatility_mom_change,
        -- Calculate 3-month changes
        CASE 
            WHEN volatility_3m_prior IS NOT NULL AND volatility_3m_prior > 0
            THEN (monthly_volatility - volatility_3m_prior) / volatility_3m_prior
            ELSE NULL
        END AS volatility_3m_change,
        -- Calculate year-over-year changes
        CASE 
            WHEN volatility_12m_prior IS NOT NULL AND volatility_12m_prior > 0
            THEN (monthly_volatility - volatility_12m_prior) / volatility_12m_prior
            ELSE NULL
        END AS volatility_yoy_change
    FROM monthly_volatility_series
)

SELECT
    {{ dbt_utils.generate_surrogate_key(['mtm.ticker', 'mtm.month_date']) }} AS volatility_timeseries_id,
    mtm.ticker,
    td.company_name,
    td.sector,
    td.industry,
    mtm.month_date,
    mtm.year,
    mtm.month_number,
    mtm.monthly_volatility,
    mtm.annualized_volatility,
    mtm.sharpe_ratio,
    mtm.monthly_return,
    mtm.month_volume,
    mtm.monthly_price_range,
    mtm.volatility_rank,
    mtm.volatility_percentile,
    mtm.volatility_category,
    -- Relative change metrics
    mtm.volatility_mom_change,
    mtm.volatility_3m_change,
    mtm.volatility_yoy_change,
    -- Relative volatility compared to sector average
    mtm.monthly_volatility / NULLIF(
        AVG(mtm.monthly_volatility) OVER (
            PARTITION BY td.sector, mtm.month_date
        ), 0
    ) AS relative_sector_volatility,
    -- Volatility trend indicators
    CASE
        WHEN mtm.volatility_mom_change > 0.1 THEN 'Rising'
        WHEN mtm.volatility_mom_change < -0.1 THEN 'Falling'
        ELSE 'Stable'
    END AS volatility_trend,
    -- Flag for months where stock is in top 10% volatility
    CASE WHEN mtm.volatility_percentile >= 0.9 THEN 1 ELSE 0 END AS is_high_volatility_month,
    -- Consecutive high volatility flag
    CASE WHEN mtm.volatility_percentile >= 0.9 AND 
              LAG(CASE WHEN mtm.volatility_percentile >= 0.9 THEN 1 ELSE 0 END) OVER (
                  PARTITION BY mtm.ticker 
                  ORDER BY mtm.month_date
              ) = 1
         THEN 1 ELSE 0 
    END AS is_consecutive_high_volatility
FROM month_to_month_changes mtm
LEFT JOIN {{ ref('stg_ticker_details') }} td
    ON mtm.ticker = td.ticker
ORDER BY mtm.month_date, mtm.volatility_rank