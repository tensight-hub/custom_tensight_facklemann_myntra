{{ config(
    materialized='table',
    partitioned_by=['platform'],
    insert_overwrite=True
) }}

WITH latest_ppm_data AS (
    -- Step 1: Find the most recent record for each product in the 'oos_report' table.
    SELECT
        product_id,  -- Using product_id (no quotes)
        product_name,
        product_image_url,
        grammage,
        tier,
        city,
        sp,
        date,
        platform,
        -- Assign a rank to each record based on date for each product/location
        ROW_NUMBER() OVER (
            PARTITION BY product_id, platform  -- Fixed: Use product_id (no quotes)
            ORDER BY DATE(date) DESC
        ) as record_rank
    FROM
        {{ ref('stg_price_parity_myntra') }}
    WHERE
        type = 'Brand'
        AND CAST(availability AS BOOLEAN) = TRUE
        AND LOWER(platform) = 'myntra'
)

-- Step 2: Join the latest data with the product lookup table.
SELECT
    ppm.product_id,
    ppm.product_name,
    products.sku_name,
    ean,
    -- Simple cleaning of the image URL from the latest record
    CASE
        WHEN TRIM(ppm.product_image_url) = '' OR TRIM(ppm.product_image_url) = '0' THEN NULL
        ELSE ppm.product_image_url
    END AS product_image_url,
    ppm.grammage,
    -- Columns from oos_report instead of the location lookup table
    ppm.tier,
    city, 
    ppm.sp,
    ppm.date,
    ppm.platform
FROM
    latest_ppm_data AS ppm
LEFT JOIN
    {{ ref('stg_lookups__product_master') }} AS products
    ON CAST(ppm.product_id AS VARCHAR) = CAST(products.item_code AS VARCHAR)  -- Fixed: Use product_id (no quotes)
    
WHERE
    ppm.record_rank = 1