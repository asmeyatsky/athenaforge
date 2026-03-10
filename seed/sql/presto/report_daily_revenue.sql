-- Daily Revenue Report Query (Presto/Athena SQL)
SELECT
    DATE_TRUNC('day', order_date) AS report_date,
    region,
    product_category,
    COUNT(DISTINCT order_id) AS total_orders,
    APPROX_DISTINCT(customer_id) AS unique_customers,
    SUM(CAST(amount AS DOUBLE)) AS total_revenue,
    AVG(CAST(amount AS DOUBLE)) AS avg_order_value,
    ARRAY_AGG(DISTINCT payment_method) AS payment_methods,
    CARDINALITY(ARRAY_AGG(DISTINCT payment_method)) AS payment_method_count,
    DATE_FORMAT(NOW(), '%Y-%m-%d %H:%i:%s') AS generated_at
FROM analytics.daily_revenue
WHERE order_date >= DATE_ADD('day', -30, CURRENT_DATE)
  AND order_status = 'completed'
  AND amount > 0
GROUP BY 1, 2, 3
ORDER BY total_revenue DESC
