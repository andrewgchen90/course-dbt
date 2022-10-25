
with orders as (
    SELECT * FROM {{ref('stg_orders')}}
)
, addresses as (
    SELECT * FROM {{ref('stg_addresses')}}
)
, promos as (
    SELECT * FROM {{ref('stg_promos')}}
)
, users as (
    SELECT * FROM {{ref('stg_users')}}
)
, order_qty as (
    SELECT * FROM {{ref('int_order_items_agg')}}
)

SELECT o.order_id
    , o.user_id
    , o.address_id
    , o.promo_id
    , o.created_at_utc
    , o.order_cost
    , o.shipping_cost
    , o.order_total
    , coalesce(p.promo_discount, 0) as order_discount
    , o.tracking_id
    , o.shipping_service
    , a.address as order_delivery_address
    , a.zipcode as order_delivery_zipcode
    , a.state as order_delivery_state
    , a.country as order_delivery_country
    , o.est_delivered_at_utc
    , o.delivered_at_utc
    , o.order_status
    , oq.num_products
    , oq.total_quantity
    , case when o.order_status = 'delivered' 
            then datediff(hour,o.created_at_utc,o.delivered_at_utc)
            end as delivery_time_hours
    , RANK() OVER(PARTITION BY u.user_id ORDER BY o.created_at_utc) as user_order_created_sequence_number
    , RANK() OVER(PARTITION BY u.user_id ORDER BY o.delivered_at_utc) as user_order_delivered_sequence_number
    , date(lead(o.created_at_utc,1) OVER (Partition by u.user_id ORDER BY o.created_at_utc)) as next_shipment_date
    , date(lead(o.created_at_utc,1) OVER (Partition by u.user_id ORDER BY o.created_at_utc)) - date(o.created_at_utc) days_till_next_shipment
FROM orders o
LEFT JOIN addresses a on o.address_id = a.address_id
LEFT JOIN promos p on o.promo_id = p.promo_id
LEFT JOIN users u on o.user_id = u.user_id
LEFT JOIN order_qty oq on o.order_id = oq.order_id