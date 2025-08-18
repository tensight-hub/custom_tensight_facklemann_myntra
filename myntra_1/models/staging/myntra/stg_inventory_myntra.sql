with source as (

    select * 
    from {{ source('myntra', 'inventory_myntra') }}

),

renamed as (

    SELECT
platform,
inward_age_bucket,
business_unit,
style_id as sku_id,
std_vendor_name,
master_category,
brand,
article_type,
gender,
brand_type,
gtin,
style_name as inventory_sku_name,
vendor_article_name,
article_mrp,
item_status,
style_status,
po_type,
warehouse_name as city_name,
season_code,
inv_units_q1 as current_soh,
inv_value_q1,
inv_units_q1_stored,
inv_value_q1_stored,
extracted_date as date

    from source

)

select * from renamed