
with promos as (
    select * from {{ source('postgres', 'promos') }}
)

SELECT promo_id
    , discount
    , status as promo_status
FROM promos