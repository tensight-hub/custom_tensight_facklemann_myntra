

with source as (

    select * 
    from {{ source('myntra', 'sales_myntra') }}

),

renamed as (

    select
        -- Parse the date if it's in yyyymmdd format
        DATE_PARSE(CAST(ord_month AS VARCHAR), '%Y%m%d') AS order_date,

        -- Keep original column names or rename to snake_case if needed
        style_id,
        vendor_name,
        business_unit,
        sku_code,
        sub_bu,
        brand_type,
        sub_brand_type,
        po_type,
        article_type,
        brand,
        master_brand,
        master_category,
        gender,
        qty,
        item_mrp

    from source

)

select * from renamed
