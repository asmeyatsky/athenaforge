-- Inventory Snapshot Query (Presto/Athena SQL)
SELECT
    warehouse_id,
    sku,
    product_name,
    COALESCE(TRY_CAST(quantity AS BIGINT), 0) AS quantity_on_hand,
    CAST(unit_cost AS DOUBLE) AS unit_cost,
    CAST(quantity AS DOUBLE) * CAST(unit_cost AS DOUBLE) AS inventory_value,
    CASE
        WHEN quantity < reorder_point THEN 'REORDER'
        WHEN quantity < reorder_point * 2 THEN 'LOW'
        ELSE 'OK'
    END AS stock_status,
    DATE_FORMAT(last_updated, '%Y-%m-%d') AS last_updated_date,
    CONTAINS(SPLIT(tags, ','), 'perishable') AS is_perishable,
    ARRAY_JOIN(SPLIT(tags, ','), ' | ') AS tag_list,
    IF(quantity = 0, 'OUT_OF_STOCK', 'IN_STOCK') AS availability,
    DATE_DIFF('day', last_received, CURRENT_DATE) AS days_since_receipt
FROM analytics.inventory
WHERE warehouse_id IN ('WH-001', 'WH-002', 'WH-003')
  AND is_active = true
ORDER BY inventory_value DESC
LIMIT 1000
