{{ config(
    materialized = 'table'
) }}

WITH pattern_analysis AS (
    SELECT
        COALESCE(fr.volume_spike_category, 'Unknown') AS volume_spike_category,
        COALESCE(fr.volume_price_pattern, 'Unknown') AS volume_price_pattern,
        COALESCE(td.sector, 'Unknown') AS sector,
        COALESCE(td.industry, 'Unknown') AS industry,
        -- Event counts
        COUNT(*) AS pattern_occurrences,
        COUNT(DISTINCT fr.ticker) AS unique_tickers,
        
        -- Return metrics
        AVG(fr.next_1m_return) AS avg_next_1m_return,
        STDDEV(fr.next_1m_return) AS std_next_1m_return,
        AVG(fr.next_3m_cumulative_return) AS avg_next_3m_return,
        STDDEV(fr.next_3m_cumulative_return) AS std_next_3m_return,
        
        -- Success rates with safety checks
        LEAST(
            COALESCE(
                SUM(CASE WHEN fr.next_1m_return > 0 THEN 1 ELSE 0 END)::FLOAT / 
                NULLIF(COUNT(*), 0),
                0
            ),
            1.0
        ) AS next_1m_success_rate,
        
        LEAST(
            COALESCE(
                SUM(CASE WHEN fr.next_3m_cumulative_return > 0 THEN 1 ELSE 0 END)::FLOAT / 
                NULLIF(COUNT(*), 0),
                0
            ),
            1.0
        ) AS next_3m_success_rate,
        
        -- Distribution statistics
        PERCENTILE_CONT(0.05) WITHIN GROUP (ORDER BY fr.next_1m_return) AS next_1m_return_5th_percentile,
        PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY fr.next_1m_return) AS next_1m_return_25th_percentile,
        PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY fr.next_1m_return) AS next_1m_return_75th_percentile,
        PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY fr.next_1m_return) AS next_1m_return_95th_percentile,
        
        -- Volume characteristics
        AVG(fr.volume_ratio_to_6m_avg) AS avg_volume_ratio,
        MAX(fr.volume_ratio_to_6m_avg) AS max_volume_ratio,
        
        -- Ensure high_volume_spike_frequency is never NULL by using COALESCE with zero
        COALESCE(
            LEAST(
                COUNT(CASE WHEN fr.volume_spike_category IN ('Extreme Spike (3x+)', 'Major Spike (2x-3x)')
                    THEN 1 END)::FLOAT / NULLIF(COUNT(*), 0),
                1.0
            ), 
            0
        ) AS high_volume_spike_frequency
    FROM {{ ref('int_forward_returns') }} fr
    JOIN {{ ref('stg_ticker_details') }} td
        ON fr.ticker = td.ticker
    WHERE fr.volume_spike_category IS NOT NULL
    GROUP BY 
        COALESCE(fr.volume_spike_category, 'Unknown'),
        COALESCE(fr.volume_price_pattern, 'Unknown'),
        COALESCE(td.sector, 'Unknown'),
        COALESCE(td.industry, 'Unknown')
)

-- Final SELECT with a clean pattern_analysis_id
SELECT
    MD5(
        COALESCE(volume_spike_category, '') || '|' ||
        COALESCE(volume_price_pattern, '') || '|' ||
        COALESCE(sector, '') || '|' ||
        COALESCE(industry, '')
    ) AS pattern_analysis_id,
    volume_spike_category,
    volume_price_pattern,
    sector,
    industry,
    pattern_occurrences,
    unique_tickers,
    avg_next_1m_return,
    std_next_1m_return,
    avg_next_3m_return,
    std_next_3m_return,
    next_1m_success_rate,
    next_3m_success_rate,
    next_1m_return_5th_percentile,
    next_1m_return_25th_percentile,
    next_1m_return_75th_percentile,
    next_1m_return_95th_percentile,
    avg_volume_ratio,
    max_volume_ratio,
    high_volume_spike_frequency
FROM pattern_analysis