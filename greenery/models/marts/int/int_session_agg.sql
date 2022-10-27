{%-
    set event_types = dbt_utils.get_column_values(
        table = ref('int_order_item_events')
        , column = 'event_type'
        , order_by = 'event_type asc'
    )
-%}

WITH events as (
    SELECT * FROM {{ref('stg_events')}}
)

, sessions as (
    SELECT 
        session_id
        , user_id
        {% for event_type in event_types %}
        , SUM(CASE WHEN event_type = '{{ event_type }}' THEN 1 ELSE 0 END) as num_{{ event_type }}
        {% endfor %}
        , COUNT(distinct event_id) as num_events
        , COUNT(distinct order_id) as num_orders
        , COUNT(distinct page_url) as num_urls
        , MIN(created_at_utc) as session_start_time
        , MAX(CASE WHEN event_type != 'package_shipped' THEN created_at_utc ELSE NULL end) session_end_time --package shipping is recorded as an event, leading to unusually
        , DATEDIFF(minutes,session_start_time,session_end_time) as total_session_duration_minutes
        , CASE
                WHEN total_session_duration_minutes< 5 then '<5 minutes'
                WHEN total_session_duration_minutes <10 then '5-10 Minutes'
                WHEN total_session_duration_minutes <20 then '10-20 Minutes'
                WHEN total_session_duration_minutes <30 then '20-30 Minutes'
                WHEN total_session_duration_minutes >= 30 then '30 minutes or more'
                else null
            end as Session_Duration_Minutes_Tier
        , listagg(distinct product_id, ', ') products_viewed
    FROM events
    GROUP BY 1,2
)

SELECT 
    s.*
    , o.first_order_created_at 
    , o.last_order_created_at
    , CASE 
        WHEN s.session_end_time < o.first_order_created_at THEN 'session pre purchase'
        WHEN s.session_end_time >= o.first_order_created_at AND s.session_start_time < o.first_order_created_at THEN 'first purchase session'
        WHEN s.session_end_time >= o.last_order_created_at AND s.session_start_time < o.last_order_created_at THEN 'most recent purchase session'
        WHEN s.session_start_time > o.first_order_created_at THEN 'returning customer'
        WHEN o.first_order_created_at IS NULL THEN 'prospective customer'
    END as user_type
FROM sessions s
LEFT JOIN {{ref('fct_user_orders')}} o ON s.user_id = o.user_id