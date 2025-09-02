SELECT
    stock.sku_id,
    stock.inventory_sku_name,
    stock.city_name,
    CAST(stock.date AS DATE) AS date,
    stock.current_soh,
    stock.sku_name,
    stock.platform,
    COALESCE(sales.total_sales, 0) AS gmv_30d
FROM {{ ref('myntra_soh_report') }} AS stock
LEFT JOIN (
    SELECT 
        style_id, 
        SUM(total_sales) AS total_sales
    FROM {{ ref('myntra_sku_category_sales_summary') }}
    WHERE order_date >= current_date - INTERVAL '30' DAY
    GROUP BY style_id
) sales
    ON stock.sku_id = sales.style_id
ORDER BY stock.sku_id, date;