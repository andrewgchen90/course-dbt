{{ config(materialized = 'view')}}

WITH products as (
    SELECT * FROM {{ref('stg_products')}}
)

, product_orders AS (
    SELECT * FROM {{ref('int_product_orders')}}
)

, product_events AS (
    SELECT * FROM {{ref('int_product_events')}}
)

SELECT  p.product_id
    , p.product_name
    , p.product_price
    , p.product_inventory
    , o.num_orders
    , o.num_quantity_purchased
    , o.revenue
    , e.lifetime_page_views
    , e.lifetime_add_to_carts
    , e.daily_page_views
    , e.daily_add_to_carts
FROM products p
LEFT JOIN product_events e ON p.product_id = e.product_id
LEFT JOIN product_orders o ON p.product_id = o.product_id


