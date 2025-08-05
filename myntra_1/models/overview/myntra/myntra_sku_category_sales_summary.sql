SELECT 
    sales.order_date,
    product.category,
    product.subcategory,
    sales.sku_code,
    SUM(sales.qty * sales.item_mrp) AS total_sales,
    SUM(sales.qty) AS total_units_sold,
    COUNT(*) AS number_of_orders
FROM {{ ref('stg_sales_myntra') }} AS sales

LEFT JOIN {{ ref('stg_lookups__product_master') }} AS product
    ON CAST(sales.style_id AS VARCHAR) = product.item_code

GROUP BY 
    sales.order_date,
    product.category,
    product.subcategory,
    sales.sku_code

ORDER BY order_date
