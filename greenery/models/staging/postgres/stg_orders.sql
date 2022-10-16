with orders as (
    select * from {{ source('postgres', 'orders') }}
)

SELECT order_id
    , user_id
    , promo_id
    , address_id
    , created_at AS created_at_utc
    , order_cost
    , shipping_cost
    , order_total
    , tracking_id
    , shipping_service
    , est_delivered_at_utc AS est_delivered_at_utc
    , delivered_at_utc AS delivered_at_utc
    , status AS order_status
FROM orders
