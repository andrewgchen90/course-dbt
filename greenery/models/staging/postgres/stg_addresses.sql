with addresses as (
    select * from {{ source('postgres', 'addresses') }}
)

select address_id
    , address
    , zipcode
    , state
    , country
from addresses