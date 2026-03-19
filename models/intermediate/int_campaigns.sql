SELECT
    date_date,
    paid_source,
    campaign_key,
    campaign_name,
    CAST(ads_cost AS FLOAT64) AS ads_cost,
    CAST(impression AS INT64) AS impression,
    CAST(click AS INT64) AS click
FROM {{ ref('stg_raw__adwords') }}

UNION ALL

SELECT
    date_date,
    paid_source,
    campaign_key,
    campaign_name,
    CAST(ads_cost AS FLOAT64) AS ads_cost,
    CAST(impression AS INT64) AS impression,
    CAST(click AS INT64) AS click
FROM {{ ref('stg_raw__bing') }}

UNION ALL

SELECT
    date_date,
    paid_source,
    campaign_key,
    campaign_name,
    CAST(ads_cost AS FLOAT64) AS ads_cost,
    CAST(impression AS INT64) AS impression,
    CAST(click AS INT64) AS click
FROM {{ ref('stg_raw__criteo') }}

UNION ALL

SELECT
    date_date,
    paid_source,
    campaign_key,
    campaign_name,
    CAST(ads_cost AS FLOAT64) AS ads_cost,
    CAST(impression AS INT64) AS impression,
    CAST(click AS INT64) AS click
FROM {{ ref('stg_raw__facebook') }}