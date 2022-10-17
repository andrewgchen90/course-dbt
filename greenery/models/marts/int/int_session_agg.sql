{{config(materialized = 'view')}}

with events as (
    select * from {{ref('stg_events')}}
)

select session_id
    , user_id
    , sum(case when event_type = 'page_view' then 1 else 0 end) as num_page_views
    , sum(case when event_type = 'add_to_cart' then 1 else 0 end) as num_added_to_cart
    , sum(case when event_type = 'checkout' then 1 else 0 end) as num_checkouts
    , sum(case when event_type = 'package_shipped' then 1 else 0 end) as num_package_shipped
    , count(distinct event_id) as num_events
    , count(distinct order_id) as num_orders
    , count(distinct page_url) as num_urls
    , min(created_at_utc) as session_start_time
    , max(created_at_utc) as session_end_time
    , datediff(minutes,session_start_time,session_end_time) as total_session_time_minutes
from events
group by 1,2