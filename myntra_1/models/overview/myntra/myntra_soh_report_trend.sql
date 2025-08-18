{{ config(
    materialized='table',
    partitioned_by=['platform', 'date'],
    insert_overwrite=True
) }}

WITH myntra_stock AS (
    SELECT
        sku_id,
        inventory_sku_name,
        city_name,
        date,
        platform,
        current_soh
    FROM {{ ref('stg_inventory_myntra') }}
  
)

SELECT 
    m.sku_id,
    m.inventory_sku_name,
    m.city_name,
    m.current_soh,
    REGEXP_REPLACE(CAST(pm.ean AS varchar), '\.0$', '') AS ean,
    pm.sku_name,
    pm.variant_product_name,
    pm.title,
    pm.grammage,
    pm.subcategory,
    pm.category,
    pm.brand,
    m.platform,
    m.date
FROM myntra_stock m
LEFT JOIN {{ ref('stg_lookups__product_master') }} pm
  ON CAST(m.sku_id AS varchar) = CAST(pm.item_code AS varchar)