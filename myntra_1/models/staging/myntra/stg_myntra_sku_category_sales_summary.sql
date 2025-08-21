with source as (
  select *
  from {{ source('myntra_overview', 'myntra_sku_category_sales_summary') }}
),
renamed as (
  select
    style_id,
    CAST(order_date AS DATE) AS date,
    category,
    subcategory,
    sku_code,
    sku_name,
    total_sales,
    total_units_sold AS units_sold,
    number_of_orders,
    platform
  from source
)
select * from renamed;
