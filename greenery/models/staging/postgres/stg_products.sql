
with products as (
    select * from {{ source('postgres', 'products') }}
)

SELECT product_id
    , name as product_name
    , price
    , inventory
FROM products
