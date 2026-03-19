WITH finance AS (

    SELECT
        date_date AS date,

        COUNT(DISTINCT orders_id) AS total_nb_transactions,

        ROUND(SUM(revenue), 2) AS total_revenue,

        ROUND(SUM(revenue) / COUNT(DISTINCT orders_id), 2) AS average_basket,

        ROUND(
            SUM(
                margin
                + COALESCE(shipping_fee, 0)
                - COALESCE(log_cost, 0)
                - COALESCE(ship_cost, 0)
            ),
            2
        ) AS operational_margin,

        ROUND(SUM(purchase_cost), 2) AS total_purchase_cost,

        ROUND(SUM(shipping_fee), 2) AS total_shipping_fee,

        ROUND(SUM(log_cost), 2) AS total_log_cost,

        SUM(quantity) AS total_products_sold

    FROM {{ ref('int_orders_operational') }}

    GROUP BY date_date

)

SELECT *
FROM finance