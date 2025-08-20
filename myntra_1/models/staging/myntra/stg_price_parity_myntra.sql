with source as (
  select *
  from {{ source('myntra', 'price_parity_myntra') }}
),
renamed as (        

  select
    "sr.no" as sr_no,
  	"product id" as product_id,
    platform,
    DATE(date) AS date,
    type,
    city,
    pincode,
    "darstore id" as darstore_id,
    product_url,
    brand,
    product_name,
    grammage,
    mrp,
    TRY_CAST(selling_price AS DOUBLE) AS sp,
    week,
    month,
    year,
    datetime,
    discount_percent,
    tier,
    REPLACE(SPLIT_PART("product image link", '|', 1), ' ', '') AS product_image_url,
    availability

  from source
)
select * from renamed;