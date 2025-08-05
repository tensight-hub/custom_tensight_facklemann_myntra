SELECT  
    sales.order_date,
    sales.style_id,
    sales.sku_code,
    sales.qty,
    sales.item_mrp,
    product.item_code,
    product.category,
    product.subcategory,
    product.title,
    product.brand
FROM {{ ref('stg_sales_myntra') }} AS sales

LEFT JOIN {{ ref('stg_lookups__product_master') }} AS product 
    ON CAST(sales.style_id AS VARCHAR) = product.item_code