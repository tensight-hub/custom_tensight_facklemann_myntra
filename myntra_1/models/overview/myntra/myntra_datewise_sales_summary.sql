SELECT
    sales.order_date,
    SUM(sales.qty * sales.item_mrp) AS total_sales,
    SUM(sales.qty) AS total_units_sold,
    COUNT(*) AS number_of_orders  
FROM {{ ref('stg_sales_myntra') }} AS sales
GROUP BY sales.order_date
ORDER BY order_date