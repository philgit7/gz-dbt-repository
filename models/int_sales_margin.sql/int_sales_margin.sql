WITH sub AS (
    SELECT 
        s.orders_id
    ,   s.date_date
    ,   s.revenue 
    ,   s.quantity
    ,   ROUND(s.quantity * p.purchase_price, 2) AS purchase_cost
    ,   ROUND(s.revenue - s.quantity * p.purchase_price, 2) AS margin
    FROM {{ ref('stg_raw__sales') }} AS s
    LEFT JOIN {{ ref('stg_raw__product') }} AS p
    USING (products_id)
)

SELECT 
    orders_id
,   date_date 
,   revenue
,   quantity 
,   purchase_cost
,   margin 
FROM sub 
