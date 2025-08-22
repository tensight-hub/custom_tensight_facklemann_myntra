with 
-- Step 1: Get the daily stock level for each product
daily_stock as (
    select 
        cast(date as date) as event_date,
        lower(platform) as platform,
        sku_id,
        sku_name,
        sum(current_soh) as soh
    from {{ ref('stg_myntra_soh_report_trend') }}
    group by 1,2,3,4
),

-- Step 2: Get the total units sold for each product per day
daily_sales as (
    select 
        cast(order_date as date) as event_date,
        style_id,
        lower(platform) as platform,
        sku_code,
        sum(total_units_sold) as units_sold
    from {{ ref('stg_myntra_sku_category_sales_summary') }}
    group by 1,2,3,4
),

-- Step 3: Get the product master details to link everything together
product_master as (
    select 
        "fm sku" as fm_sku,
        "master product name" as master_product_name,
        "front end id" as front_end_id,
        "item code" as item_code
    from {{ ref('stg_lookups__product_master') }}
)

-- Step 4: Combine the stock, sales, and product master information
select
    stock.event_date,
    stock.platform,
    stock.sku_name,
    master.front_end_id,
    stock.soh,
    sales.style_id,
    sales.units_sold
from daily_stock as stock
left join product_master as master
    on lower(stock.platform) = lower(master.fm_sku)
   and cast(stock.sku_id as varchar) = cast(master.item_code as varchar)
left join daily_sales as sales
    on stock.event_date = sales.event_date
   and lower(stock.platform) = lower(sales.platform)
   and cast(master.item_code as varchar) = cast(sales.style_id as varchar)
;
