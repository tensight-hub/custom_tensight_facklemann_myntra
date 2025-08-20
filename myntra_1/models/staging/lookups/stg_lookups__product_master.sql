with

source as (

    select * from {{ source('lookups', 'product_master') }}

),

renamed as (

    select
         account,
        "fm sku" as sku_id,
        "item code" as item_code,
        "front end id" as frontend_id,
        ean,
        "master product name" as sku_name,
        "varient product name" as variant_product_name,
        title,
        grammage,
        subcategory,
        category,
        brand

    from source)

select * from renamed


