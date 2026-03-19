{{ config(materialized = 'table') }}

WITH orders AS (
    SELECT
        orders_id
    ,   date_date
    ,   revenue,
        quantity
    ,   purchase_cost
    ,   margin
FROM {{ ref('int_sales_margin') }}
),

shipping AS (
    SELECT
        orders_id
    ,   shipping_fee
    ,   logcost AS log_cost
    ,   CAST(ship_cost AS FLOAT64) AS ship_cost
FROM {{ ref('stg_raw__ship') }}
)

SELECT
    o.orders_id
,   o.date_date
,   o.revenue
,   o.quantity
,   o.purchase_cost
,   o.margin
,   s.shipping_fee
,   s.log_cost
,   s.ship_cost
,   ROUND(o.margin + s.shipping_fee - s.log_cost - s.ship_cost, 2) AS operational_margin
FROM orders o
LEFT JOIN shipping s
ON o.orders_id = s.orders_id