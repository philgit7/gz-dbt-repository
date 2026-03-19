WITH sub AS (
    SELECT 
        ROUND(s.quantity * p.purchase_price, 2) AS purchase_cost
    ,   ROUND(s.revenue - s.quantity * p.purchase_price, 2) AS margin
    FROM {{ ref('stg_raw__sales') }} AS s
    LEFT JOIN {{ ref('stg_raw__product') }} AS p
    USING (products_id)
)

SELECT *
,   margin 
FROM sub 




