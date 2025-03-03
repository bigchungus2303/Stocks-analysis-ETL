WITH volume_trends AS (
    SELECT
        monthly_metric_id,
        ticker,
        month_date,
        monthly_volume,
        avg_daily_volume,
        monthly_return,
        -- Calculate 6-month moving average of volume (excluding current month)
        AVG(monthly_volume) OVER (
            PARTITION BY ticker 
            ORDER BY month_date 
            ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING
        ) AS prev_6m_avg_volume,
        -- Calculate volume ratio compared to moving average
        CASE 
            WHEN AVG(monthly_volume) OVER (
                PARTITION BY ticker 
                ORDER BY month_date 
                ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING
            ) > 0 
            THEN monthly_volume / NULLIF(
                AVG(monthly_volume) OVER (
                    PARTITION BY ticker 
                    ORDER BY month_date 
                    ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING
                ), 0)
            ELSE NULL
        END AS volume_ratio_to_6m_avg
    FROM {{ ref('int_monthly_base_metrics') }}
)

SELECT
    monthly_metric_id,
    ticker,
    month_date,
    monthly_volume,
    avg_daily_volume,
    monthly_return,
    prev_6m_avg_volume,
    volume_ratio_to_6m_avg,
    -- Categorize volume spikes
    CASE
        WHEN volume_ratio_to_6m_avg >= 3 THEN 'Extreme Spike (3x+)'
        WHEN volume_ratio_to_6m_avg >= 2 THEN 'Major Spike (2x-3x)'
        WHEN volume_ratio_to_6m_avg >= 1.5 THEN 'Moderate Spike (1.5x-2x)'
        WHEN volume_ratio_to_6m_avg >= 0.8 THEN 'Normal Volume (0.8x-1.5x)'
        WHEN volume_ratio_to_6m_avg IS NOT NULL THEN 'Volume Decline (<0.8x)'
        ELSE 'Insufficient History'
    END AS volume_spike_category,
    -- Categorize volume-price relationship
    CASE
        WHEN volume_ratio_to_6m_avg >= 2 AND monthly_return > 0.05 THEN 'Strong Rally on High Volume'
        WHEN volume_ratio_to_6m_avg >= 2 AND monthly_return < -0.05 THEN 'Strong Decline on High Volume'
        WHEN volume_ratio_to_6m_avg >= 1.5 AND monthly_return > 0 THEN 'Rally on Moderate Volume'
        WHEN volume_ratio_to_6m_avg >= 1.5 AND monthly_return < 0 THEN 'Decline on Moderate Volume'
        WHEN volume_ratio_to_6m_avg < 0.8 THEN 'Low Volume Trading'
        ELSE 'Normal Trading Pattern'
    END AS volume_price_pattern
FROM volume_trends