with events as (
    select * from {{ source('postgres', 'events') }}
)

SELECT event_id
    , session_id
    , user_id
    , page_url
    , created_at AS created_at_utc
    , event_type
    , order_id
    , product_id
FROM events