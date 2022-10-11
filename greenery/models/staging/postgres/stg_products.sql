
with products as (
    select * from {{ source('postgres', 'products') }}
)

SELECT product_id
    , name as product_name
    , price AS product_price
    , inventory AS product_quantity
FROM products
