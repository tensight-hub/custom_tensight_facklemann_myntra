-- models/staging/stg_myntra_sov.sql
with source as (
  select *
  from {{ source('myntra', 'sov_myntra') }}
),
renamed as (
  select
    "sr.no." as sr_no,
    platform,
    date,
    keyword,
    city as city_name,
    location,
    tier,
    pincode,
    search_rank,
    product_id,
    product_url,
    "product image link" as product_image_link,
    brand,
    "brand visibility"  as brand_visibility,
    product_name,
    grammage,
    mrp,
    selling_price,
    type as listing_type,
    "organic placement"  as organic_placement,
    "inorganic placement" as inorganic_placement,
    week,
    month,
    year,
    datetime,
    discount_percent,
    "keyword type" as keyword_type
  from source
)
select * from renamed;