
with promos as (
    select * from {{ source('postgres', 'promos') }}
)

SELECT promo_id
    , discount AS promo_discount
    , status AS promo_status
FROM promos
