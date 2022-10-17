WITH events as (
    SELECT * FROM {{ref('stg_events')}}
)

SELECT session_id
    , user_id
    , SUM(case when event_type = 'page_view' then 1 else 0 end) as num_page_views
    , SUM(case when event_type = 'add_to_cart' then 1 else 0 end) as num_added_to_cart
    , SUM(case when event_type = 'checkout' then 1 else 0 end) as num_checkouts
    , SUM(case when event_type = 'package_shipped' then 1 else 0 end) as num_package_shipped
    , COUNT(distinct event_id) as num_events
    , COUNT(distinct order_id) as num_orders
    , COUNT(distinct page_url) as num_urls
    , MIN(created_at_utc) as session_start_time
    , MAX(created_at_utc) as session_end_time
    , DATEDIFF(minutes,session_start_time,session_end_time) as total_session_time_minutes
FROM events
GROUP BY 1,2