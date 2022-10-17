WITH order_items as (
    SELECT * FROM {{ref('stg_order_items')}}
)

SELECT order_id
    , COUNT(distinct order_id) as num_products
    , SUM(quantity) as total_quantity
FROM order_items
GROUP BY 1