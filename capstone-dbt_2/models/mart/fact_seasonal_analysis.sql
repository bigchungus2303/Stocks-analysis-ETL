{{ config(
    materialized = 'table'
) }}

WITH sector_monthly_data AS (
    SELECT
        sector,
        month_number,
        year,
        COUNT(DISTINCT ticker) AS stocks_count,
        AVG(monthly_return) AS avg_sector_return,
        -- Handle std_sector_return to never be NULL
        CASE 
            WHEN COUNT(*) <= 1 THEN 0
            ELSE STDDEV(monthly_return)
        END AS std_sector_return,
        -- Handle avg_volume_ratio to never be NULL
        AVG(COALESCE(volume_vs_12m_avg, 0)) AS avg_volume_ratio,
        SUM(CASE WHEN is_positive_return = 1 THEN 1 ELSE 0 END) / 
            NULLIF(COUNT(*), 0)::FLOAT AS positive_return_ratio
    FROM {{ ref('int_seasonal_patterns') }}
    WHERE sector IS NOT NULL -- Filter out NULL sectors
    GROUP BY sector, month_number, year
    HAVING COUNT(*) > 0 -- Ensure we have at least one stock in each group
),

january_analysis AS (
    SELECT
        jem.sector,
        MAX(jem.industry) AS industry, -- Handle multiple industries per sector
        jem.year,
        COUNT(DISTINCT jem.ticker) AS stocks_analyzed,
        AVG(jem.january_return) AS avg_january_return,
        AVG(jem.january_excess_return) AS avg_january_excess_return,
        AVG(COALESCE(jem.january_volume_ratio, 0)) AS avg_january_volume_ratio,
        SUM(CASE WHEN jem.january_outperformed = 1 THEN 1 ELSE 0 END) / 
            NULLIF(COUNT(*), 0)::FLOAT AS january_outperformance_ratio,
        SUM(CASE WHEN jem.january_effect_strength = 'Strong' THEN 1 ELSE 0 END) / 
            NULLIF(COUNT(*), 0)::FLOAT AS strong_effect_ratio,
        AVG(jem.january_effect_zscore) AS avg_effect_zscore,
        SUM(CASE WHEN jem.january_effect_strength IN ('Strong', 'Moderate') THEN 1 ELSE 0 END) / 
            NULLIF(COUNT(*), 0)::FLOAT AS significant_effect_ratio
    FROM {{ ref('int_january_effect_metrics') }} jem
    WHERE jem.sector IS NOT NULL
    GROUP BY jem.sector, jem.year
),

-- Get historical year-over-year changes 
yoy_changes AS (
    SELECT
        sector,
        year,
        avg_january_excess_return,
        LAG(avg_january_excess_return) OVER (
            PARTITION BY sector 
            ORDER BY year
        ) AS prev_year_excess_return
    FROM january_analysis
)

-- Final fact table with guaranteed uniqueness and no NULLs in critical columns
SELECT DISTINCT -- Ensure uniqueness
    {{ dbt_utils.generate_surrogate_key([
        'md.sector', 'md.month_number', 'md.year'
    ]) }} AS seasonal_analysis_id,
    md.sector,
    md.month_number,
    md.year,
    md.stocks_count,
    md.avg_sector_return,
    md.std_sector_return,
    md.avg_volume_ratio,   
    md.positive_return_ratio,
    
    -- Include January metrics only for January months
    CASE WHEN md.month_number = 1 THEN ja.industry ELSE NULL END AS industry,
    CASE WHEN md.month_number = 1 THEN ja.stocks_analyzed ELSE NULL END AS stocks_analyzed,
    CASE WHEN md.month_number = 1 THEN ja.avg_january_return ELSE NULL END AS avg_january_return,
    CASE WHEN md.month_number = 1 THEN ja.avg_january_excess_return ELSE NULL END AS avg_january_excess_return,
    CASE WHEN md.month_number = 1 THEN ja.avg_january_volume_ratio ELSE NULL END AS avg_january_volume_ratio,
    CASE WHEN md.month_number = 1 THEN ja.january_outperformance_ratio ELSE NULL END AS january_outperformance_ratio,
    CASE WHEN md.month_number = 1 THEN ja.strong_effect_ratio ELSE NULL END AS strong_effect_ratio,
    
    -- Historical metrics
    CASE WHEN md.month_number = 1 THEN ja.avg_effect_zscore ELSE NULL END AS historical_effect_zscore,
    CASE WHEN md.month_number = 1 THEN ja.significant_effect_ratio ELSE NULL END AS historical_significance_ratio,
    
    -- Year-over-year change
    CASE WHEN md.month_number = 1 THEN
        yc.avg_january_excess_return - yc.prev_year_excess_return
    ELSE NULL END AS yoy_effect_change,
    
    -- Seasonal strength category
    CASE
        WHEN md.avg_sector_return > 2 * NULLIF(md.std_sector_return, 0) THEN 'Very Strong'
        WHEN md.avg_sector_return > NULLIF(md.std_sector_return, 0) THEN 'Strong'
        WHEN md.avg_sector_return < -2 * NULLIF(md.std_sector_return, 0) THEN 'Very Weak'
        WHEN md.avg_sector_return < -NULLIF(md.std_sector_return, 0) THEN 'Weak'
        ELSE 'Neutral'
    END AS seasonal_strength
FROM sector_monthly_data md
LEFT JOIN january_analysis ja
    ON md.sector = ja.sector 
    AND md.year = ja.year
LEFT JOIN yoy_changes yc
    ON md.sector = yc.sector 
    AND md.year = yc.year