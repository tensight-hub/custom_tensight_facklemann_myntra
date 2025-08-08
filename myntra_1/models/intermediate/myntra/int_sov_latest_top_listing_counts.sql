SELECT
    x.*
FROM {{ ref('stg_sov_myntra') }} AS x

JOIN (
    SELECT
        platform,
        brand,
        MAX(CAST(DATE_TRUNC('day', datetime) AS DATE)) AS max_date
    FROM {{ ref('stg_sov_myntra') }}
    GROUP BY platform, brand
) AS l
ON x.platform = l.platform
AND x.brand = l.brand
AND CAST(DATE_TRUNC('day', x.datetime) AS DATE) = l.max_date

