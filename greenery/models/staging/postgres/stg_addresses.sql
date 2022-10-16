WITH addresses as (
    SELECT * FROM {{ source('postgres', 'addresses') }}
)

SELECT address_id
    , address
    , zipcode
    , state
    , country
FROM addresses