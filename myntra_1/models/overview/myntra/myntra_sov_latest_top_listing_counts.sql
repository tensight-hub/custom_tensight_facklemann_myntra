{{ config(
    materialized='table'
) }}

with max_dates as (
    select
        platform,
        max(cast(date_trunc('day', datetime) as date)) as max_date
    from {{ ref('stg_sov_myntra') }}
    group by platform
),

filtered_data as (
    select
        r.keyword,
        concat(r.location, ' - ', cast(r.pincode as varchar)) as pincode,
        r.brand as brand_name,
        cast(r.search_rank as int) as best_rank,
        r.city_name,
        r.tier,
        r.product_name,
        r.grammage,
        r.mrp,
        r.selling_price,
        r.listing_type,
        r.keyword_type,
        r.discount_percent,
        r.platform,
        cast(date_trunc('day', r.datetime) as date) as date
    from {{ ref('stg_sov_myntra') }} r
    join max_dates md
      on r.platform = md.platform
     and cast(date_trunc('day', r.datetime) as date) = md.max_date
),

ranked_data as (
    select
        *,
        row_number() over (
            partition by platform, keyword, pincode, listing_type, brand_name
            order by best_rank asc
        ) as row_num
    from filtered_data
)

select
    keyword,
    pincode,
    brand_name,
    best_rank,
    city_name,
    tier,
    product_name,
    grammage,
    mrp,
    selling_price,
    listing_type,
    keyword_type,
    discount_percent,
    platform,
    date
from ranked_data
where row_num = 1
