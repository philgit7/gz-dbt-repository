{{ config( materialized = 'table') }}
SELECT
    orders_id
,   MAX(date_date) AS date_date
,   ROUND(SUM(revenue), 2) AS revenue
,   ROUND(SUM(quantity), 2) AS quantity
,   ROUND(SUM(purchase_cost), 2) AS purchase_cost
,   ROUND(SUM(margin), 2) AS margin
FROM {{ ref("int_orders_marginn") }} AS s
GROUP BY orders_id
ORDER BY orders_id DESC
