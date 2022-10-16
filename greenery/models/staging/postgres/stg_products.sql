
WITH products as (
    SELECT * FROM {{ source('postgres', 'products') }}
)

SELECT product_id
    , name as product_name
    , price AS product_price
    , inventory AS product_inventory
FROM products
