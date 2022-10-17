with events as (
    SELECT * FROM {{ ref('stg_events') }}
)
, product_date_events as (
    SELECT product_id
        , DATE_TRUNC(day, created_at_utc) AS date
        , SUM(CASE WHEN event_type = 'page_view' THEN 1 ELSE 0 END) AS page_views
        , SUM(CASE WHEN event_type = 'add_to_cart' THEN 1 ELSE 0 END) AS add_to_carts
    FROM events
    WHERE product_id IS NOT NULL
    GROUP BY 1,2
    ORDER BY 1
)
SELECT product_id
    , SUM(page_views) AS lifetime_page_views
    , SUM(add_to_carts) AS lifetime_add_to_carts
    , ROUND(AVG(page_views),1) AS daily_page_views
    , ROUND(AVG(add_to_carts),1) AS daily_add_to_carts
FROM product_date_events
GROUP BY 1