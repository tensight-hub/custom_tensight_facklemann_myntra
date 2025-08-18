WITH myntra_stock AS (
    SELECT a.*
    FROM (
        SELECT * FROM {{ ref('stg_inventory_myntra') }}
    ) a
    INNER JOIN (
        SELECT style_id, MAX(extracted_date) AS latest_date
        FROM {{ ref('stg_inventory_myntra') }}
        GROUP BY style_id
    ) b
      ON a.style_id = b.style_id
     AND a.extracted_date = b.latest_date
)

SELECT
    m.*
FROM myntra_stock m

LEFT JOIN {{ ref('stg_lookups__product_master') }} pm
ON CAST(m.style_id AS varchar) = CAST(pm."item_code" AS varchar)