WITH purchase_cost AS (
    SELECT 
      date_date
    , orders_id
    , SUM(quantity) AS quantity
    , SUM(revenue) AS revenue
    , ROUND(SUM(s.quantity * p.purchase_price), 0) AS purchase_cost
    FROM {{ ref('stg_raw__sales') }} AS s
    INNER JOIN {{ ref('stg_raw__product') }} AS p
    ON s.products_id = p.products_id
    GROUP BY date_date, orders_id 
),

margin AS (
    SELECT 
      date_date
    , orders_id
    , ROUND(SUM(s.revenue) - SUM(p.purchase_price), 0) AS margin 
    FROM {{ ref('stg_raw__sales') }} AS s
    INNER JOIN {{ ref('stg_raw__product') }} AS p
    ON s.products_id = p.products_id
    GROUP BY date_date, orders_id 
)

SELECT 
  pc.orders_id
, pc.date_date
, pc.quantity
, pc.revenue
, pc.purchase_cost
, m.margin 
FROM purchase_cost AS pc 
LEFT JOIN margin AS m
ON  pc.orders_id = m.orders_id
AND pc.date_date = m.date_date



