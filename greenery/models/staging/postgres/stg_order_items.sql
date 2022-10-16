WITH order_items as (
    SELECT * FROM {{ source('postgres', 'order_items') }}
)

SELECT order_id
    , product_id
    , quantity
FROM order_items