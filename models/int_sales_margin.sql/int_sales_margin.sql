WITH purchase_cost AS (
    SELECT 
      date_date
    , orders_id
    , ROUND(SUM(s.quantity)*SUM(p.purchase_price), 0)
    FROM {{ ref('stg_raw__sales') }} AS s
    INNER JOIN {{ ref('stg_raw__product') }} AS p
    ON s.products_id = p.products_id
    GROUP BY date_date, orders_id 
)

SELECT 
*
FROM purchase_cost;
