{{ config(materialized = 'view')}}

with order_items as (
    select * from {{ref('stg_order_items')}}
)

select
    order_id
    , count(distinct order_id) as num_products
    , sum(quantity) as total_quantity
from order_items
group by 1