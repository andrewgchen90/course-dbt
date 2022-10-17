WITH order_items as (
    SELECT * FROM {{ref('stg_order_items')}}
)

, products as (
    SELECT * FROM {{ref('stg_products')}}
)

SELECT i.product_id
    , p.product_name
    , p.product_price
    , COUNT(distinct i.order_id) as num_orders
    , SUM(i.quantity) as num_quantity_purchased
    , p.product_price * SUM(i.quantity) as revenue
FROM order_items i
LEFT JOIN products p on i.product_id = p.product_id 
GROUP BY 1,2,3