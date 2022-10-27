
WITH products as (
    SELECT * FROM {{ref('stg_products')}}
)

, product_orders AS (
    SELECT * FROM {{ref('int_product_orders')}}
)

, product_events AS (
    SELECT * FROM {{ref('int_product_events')}}
)

SELECT 
    p.product_id
    , p.product_name
    , p.product_price
    , p.product_inventory
    , o.num_orders
    , o.num_quantity_purchased
    , o.revenue
    , o.product_first_purchase_date
    , o.product_most_recent_purchase_date
    , e.lifetime_page_view
    , e.lifetime_add_to_cart
    , e.lifetime_checkout
    , e.lifetime_package_shipped
    , e.lifetime_sessions
    , e.lifetime_days_with_purchase
    , e.lifetime_distinct_buyers
    , ROUND(div0(e.lifetime_checkout, e.lifetime_sessions),3) as conversion_rate
FROM products p
LEFT JOIN product_events e ON p.product_id = e.product_id
LEFT JOIN product_orders o ON p.product_id = o.product_id

