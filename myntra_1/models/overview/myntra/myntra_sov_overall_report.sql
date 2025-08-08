{{ config(
    materialized='table',
    partitioned_by=['brand', 'platform', 'date']
) }}

WITH

{% if is_incremental() %}

latest_dates AS (
    SELECT
        platform,
        brand,
        MAX(DATE(datetime)) AS max_date
    FROM {{ ref('stg_sov_myntra') }}
    GROUP BY platform, brand
),

sov_filtered AS (
    SELECT x.*
    FROM {{ ref('stg_sov_myntra') }} x
    JOIN latest_dates l
      ON x.platform = l.platform
     AND x.brand = l.brand
     AND DATE(x.datetime) = l.max_date
    WHERE x.search_rank < 31
)

{% else %}

sov_filtered AS (
    SELECT *
    FROM {{ ref('stg_sov_myntra') }}
    WHERE search_rank < 31
)

{% endif %}

SELECT
    COALESCE(x.location, 'NA') || ' - ' || CAST(x.pincode AS VARCHAR) AS pincode,
    x.city_name,
    x.tier,
    x.keyword,
    x.keyword_type,
    x.listing_type,

    SUM(CASE WHEN x.brand_visibility = 1 THEN 1 ELSE 0 END) AS listing_count_brand,
    COUNT(x.search_rank) AS overall_listing_count,

    x.brand,
    x.platform,
    DATE(x.datetime) AS date

FROM sov_filtered x

GROUP BY
    COALESCE(x.location, 'NA') || ' - ' || CAST(x.pincode AS VARCHAR),
    x.city_name,
    x.tier,
    x.keyword,
    x.keyword_type,
    x.listing_type,
    x.brand,
    x.platform,
    DATE(x.datetime)
