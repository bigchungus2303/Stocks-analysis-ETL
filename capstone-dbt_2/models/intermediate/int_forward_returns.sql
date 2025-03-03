WITH forward_price_data AS (
    SELECT
        vt.monthly_metric_id,
        vt.ticker,
        vt.month_date,
        vt.volume_spike_category,
        vt.volume_price_pattern,
        vt.volume_ratio_to_6m_avg,
        vt.monthly_return AS current_month_return,
        -- Get returns for next months
        LEAD(mb.monthly_return, 1) OVER (
            PARTITION BY vt.ticker ORDER BY vt.month_date
        ) AS next_1m_return,
        LEAD(mb.monthly_return, 2) OVER (
            PARTITION BY vt.ticker ORDER BY vt.month_date
        ) AS next_2m_return,
        LEAD(mb.monthly_return, 3) OVER (
            PARTITION BY vt.ticker ORDER BY vt.month_date
        ) AS next_3m_return
    FROM {{ ref('int_volume_trend_analysis') }} vt
    LEFT JOIN {{ ref('int_monthly_base_metrics') }} mb
        ON vt.ticker = mb.ticker 
        AND DATEADD('month', 1, vt.month_date) = mb.month_date
)

SELECT
    monthly_metric_id,
    ticker,
    month_date,
    volume_spike_category,
    volume_price_pattern,
    volume_ratio_to_6m_avg,
    current_month_return,
    next_1m_return,
    next_2m_return,
    next_3m_return,
    -- Calculate cumulative forward returns
    (1 + COALESCE(next_1m_return, 0)) * 
    (1 + COALESCE(next_2m_return, 0)) * 
    (1 + COALESCE(next_3m_return, 0)) - 1 AS next_3m_cumulative_return,
    -- Flag positive returns
    CASE WHEN next_1m_return > 0 THEN 1 ELSE 0 END AS next_1m_positive,
    CASE WHEN next_3m_cumulative_return > 0 THEN 1 ELSE 0 END AS next_3m_positive
FROM forward_price_data
WHERE volume_spike_category != 'Insufficient History'