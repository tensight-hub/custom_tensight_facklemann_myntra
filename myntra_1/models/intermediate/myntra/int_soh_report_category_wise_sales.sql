WITH stock AS (
    SELECT
        sku_id,
        inventory_sku_name,
        city_name,
        CAST(date AS DATE) AS stock_date,
        current_soh,
        sku_name,
        platform
    FROM {{ ref('myntra_soh_report') }}
),

sales AS (
    SELECT
        style_id AS sku_id,
        CAST(order_date AS DATE) AS order_date,
        total_sales
    FROM {{ ref('myntra_sku_category_sales_summary') }}
)

SELECT
    s.sku_id,
    s.inventory_sku_name,
    s.city_name,
    s.stock_date,
    s.current_soh,
    s.sku_name,
    s.platform,
    sales.order_date,
    sales.total_sales
FROM stock s
LEFT JOIN sales
    ON s.sku_id = sales.sku_id;