SELECT
    x.*
FROM {{ ref('stg_inventory_myntra') }} AS x

LEFT JOIN {{ ref('stg_lookups__product_master') }} pm
ON CAST(x.style_id AS varchar) = CAST(pm."item_code" AS varchar)