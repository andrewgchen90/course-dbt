{{ config(materialized = 'view')}}

with order_items as (
    select * from {{ref('stg_order_items')}}
)

, products as (
    select * from {{ref('stg_products')}}
)

select
    i.product_id
    , p.product_name
    , p.product_price
    , count(distinct i.order_id) as num_orders
    , sum(i.quantity) as num_quantity_purchased
    , p.product_price * sum(i.quantity) as revenue
FROM order_items i
LEFT JOIN products p on i.product_id = p.product_id 
group by 1,2,3