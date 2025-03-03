WITH january_analysis AS (
    SELECT
        ticker,
        sector,
        industry,
        year,
        monthly_return AS january_return,
        volume_vs_12m_avg AS january_volume_ratio,
        -- Get previous December's return
        LAG(monthly_return) OVER (
            PARTITION BY ticker
            ORDER BY year
        ) AS prev_december_return,
        -- Get following month's return
        LEAD(monthly_return) OVER (
            PARTITION BY ticker
            ORDER BY year
        ) AS next_february_return
    FROM {{ ref('int_seasonal_patterns') }}
    WHERE month_number = 1  -- January only
),

other_months_stats AS (
    SELECT
        ticker,
        year,
        AVG(CASE WHEN month_number != 1 THEN monthly_return END) AS avg_non_january_return,
        STDDEV(CASE WHEN month_number != 1 THEN monthly_return END) AS std_non_january_return
    FROM {{ ref('int_seasonal_patterns') }}
    GROUP BY ticker, year
)

SELECT
    {{ dbt_utils.generate_surrogate_key([
        'ja.ticker', 'ja.sector', 'ja.industry', 'ja.year'
    ]) }} AS january_effect_id,
    ja.ticker,
    ja.sector,
    ja.industry,
    ja.year,
    ja.january_return,
    ja.january_volume_ratio,
    ja.prev_december_return,
    ja.next_february_return,
    oms.avg_non_january_return,
    oms.std_non_january_return,
    -- Calculate January effect metrics
    ja.january_return - oms.avg_non_january_return AS january_excess_return,
    (ja.january_return - oms.avg_non_january_return) / 
        NULLIF(oms.std_non_january_return, 0) AS january_effect_zscore,
    -- Analyze the persistence of the effect
    CASE
        WHEN ja.january_return > oms.avg_non_january_return THEN 1
        ELSE 0
    END AS january_outperformed,
    -- Small vs large effect analysis
    CASE
        WHEN ABS(ja.january_return - oms.avg_non_january_return) > 
             2 * oms.std_non_january_return THEN 'Strong'
        WHEN ABS(ja.january_return - oms.avg_non_january_return) > 
             oms.std_non_january_return THEN 'Moderate'
        ELSE 'Weak'
    END AS january_effect_strength
FROM january_analysis ja
LEFT JOIN other_months_stats oms
    ON ja.ticker = oms.ticker 
    AND ja.year = oms.year