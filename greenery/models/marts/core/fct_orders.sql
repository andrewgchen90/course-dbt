{{config(materialized = 'table')}}

with orders as (
    select * from {{ref('stg_orders')}}
)
, addresses as (
    select * from {{ref('stg_addresses')}}
)
, promos as (
    select * from {{ref('stg_promos')}}
)
, users as (
    select * from {{ref('stg_users')}}
)
select
    o.order_id,
    u.user_id,
    a.address_id,
    p.promo_id,
    o.created_at_utc,
    o.order_cost,
    o.shipping_cost,
    o.order_total,
    coalesce(p.promo_discount, 0) as order_discount,
    o.tracking_id,
    o.shipping_service,
    a.zipcode as order_delivery_zipcode,
    a.state as order_delivery_state,
    a.country as order_delivery_country,
    o.est_delivered_at_utc,
    o.delivered_at_utc,
    o.order_status,
    case when o.order_status = 'delivered' 
        then datediff(hour,o.created_at_utc,o.delivered_at_utc)
        end as delivery_time_hours
from orders o
left join addresses a on o.address_id = a.address_id
left join promos p on o.promo_id = p.promo_id
left join users u on o.user_id = u.user_id