with source as (
  select *
  from {{ source('myntra_overview', 'myntra_soh_report') }}
),
renamed as (
  select
    sku_id,
    inventory_sku_name,
    city_name,
    current_soh,
    ean,
    sku_name,
    variant_product_name,
    title,
    grammage,
    subcategory,
    category,
    brand,
    platform,
    date
  from source
)
select * from renamed;
