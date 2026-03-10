-- Campaign Performance Analytics (Presto/Athena SQL)
WITH campaign_metrics AS (
    SELECT
        campaign_id,
        campaign_name,
        channel,
        DATE_TRUNC('week', impression_date) AS week_start,
        SUM(impressions) AS total_impressions,
        SUM(clicks) AS total_clicks,
        SUM(CAST(spend AS DOUBLE)) AS total_spend,
        SUM(conversions) AS total_conversions,
        SUM(CAST(revenue AS DOUBLE)) AS attributed_revenue,
        APPROX_DISTINCT(user_id) AS unique_reach
    FROM analytics.campaign_events
    WHERE impression_date >= DATE_ADD('month', -3, CURRENT_DATE)
    GROUP BY campaign_id, campaign_name, channel, DATE_TRUNC('week', impression_date)
)
SELECT
    campaign_name,
    channel,
    week_start,
    total_impressions,
    total_clicks,
    ROUND(CAST(total_clicks AS DOUBLE) / NULLIF(total_impressions, 0) * 100, 2) AS ctr_pct,
    total_spend,
    total_conversions,
    ROUND(total_spend / NULLIF(CAST(total_conversions AS DOUBLE), 0), 2) AS cost_per_conversion,
    attributed_revenue,
    ROUND(attributed_revenue / NULLIF(total_spend, 0), 2) AS roas,
    unique_reach
FROM campaign_metrics
WHERE total_impressions > 1000
ORDER BY attributed_revenue DESC
