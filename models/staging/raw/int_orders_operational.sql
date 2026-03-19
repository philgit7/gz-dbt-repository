{{ config(materialized='table') }}

WITH orders AS (
    SELECT
        orders_id,
        date_date,
        revenue,
        quantity,
        purchase_cost,
        margin
    FROM {{ ref('int_sales_margin') }}
),

shipping AS (
    SELECT
        orders_id,
        shipping_fee,
        logcost AS log_cost,
        CAST(ship_cost AS FLOAT64) AS ship_cost
    FROM {{ ref('stg_raw__ship') }}
)

SELECT
    o.orders_id,
    o.date_date,
    o.revenue,
    o.quantity,
    o.purchase_cost,
    o.margin,
    s.shipping_fee,
    s.log_cost,
    s.ship_cost,
    ROUND(
        o.margin
        + COALESCE(s.shipping_fee, 0)
        - COALESCE(s.log_cost, 0)
        - COALESCE(s.ship_cost, 0),
        2
    ) AS operational_margin
FROM orders AS o
LEFT JOIN shipping AS s
    ON o.orders_id = s.orders_id