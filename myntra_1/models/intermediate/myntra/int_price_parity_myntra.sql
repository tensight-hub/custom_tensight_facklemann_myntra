WITH latest_ppm AS (
    SELECT *
    FROM {{ ref('stg_price_parity_myntra') }}
)

SELECT
    r.*,
    p.*
FROM latest_ppm r
LEFT JOIN {{ ref('stg_lookups__product_master') }} p
  ON CAST(r."product id" AS varchar) = CAST(p."item_code" AS varchar)
 AND LOWER(r.platform) = LOWER(p."fm sku")



 