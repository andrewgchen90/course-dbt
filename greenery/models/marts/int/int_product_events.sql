{%-
    set event_types = dbt_utils.get_column_values(
        table = ref('int_order_item_events')
        , column = 'event_type'
        , order_by = 'event_type asc'
    )
-%}

with order_item_events as (
    select * from {{ ref('int_order_item_events') }}
)

SELECT product_guid_coalesce as product_id
    {% for event_type in event_types %}
    , SUM(CASE WHEN event_type = '{{ event_type }}' THEN 1 ELSE 0 END) as lifetime_{{ event_type }}
    {% endfor %}
    , COUNT(DISTINCT session_id) as lifetime_sessions
    , COUNT(DISTINCT DATE(created_at_utc)) as lifetime_days_with_purchase
    , COUNT(DISTINCT user_id) as lifetime_distinct_buyers
FROM order_item_events
WHERE product_id IS NOT NULL
GROUP BY 1
ORDER BY 1