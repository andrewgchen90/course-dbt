
WITH user_sessions as (
    SELECT * FROM {{ref('fct_user_sessions')}}
)

, user_orders AS (
    SELECT * FROM {{ref('fct_user_orders')}}
)

SELECT 
    s.user_id
    , s.first_name
    , s.last_name
    , s.email
    , s.phone_number
    , s.created_at_utc
    , s.lifetime_session_count
    , s.avg_session_duration_minutes
    , s.lifetime_page_views
    , s.lifetime_added_to_cart
    , s.lifetime_checkouts
    , s.lifetime_packages_shipped
    , s.lifetime_sessions_with_add_to_cart
    , s.lifetime_sessions_with_check_out
    , s.pct_of_sessions_with_item_added_with_checkout --to know conversion of sessions with with added items to cart
    , s.pct_of_total_sessions_with_checkout --to know conversion rate of typical sessions,
    , s.lifetime_days_with_session
    , o.lifetime_order_count
    , o.lifetime_completed_order_count
    , o.lifetime_avg_days_between_shipments
    , o.lifetime_spend_exc_shipping
    , o.lifetime_shipping_cost
    , o.lifetime_total_spend
    , o.lifetime_items_purchased
    , o.lifetime_average_order_value
    , o.lifetime_average_items_per_order
    , o.lifetime_promo_count
    , o.lifetime_promo_discounts
    , o.first_order_created_at  
    , o.second_order_created_at
    , o.third_order_created_at
    , o.fourth_order_created_at
    , o.fifth_order_created_at
    , first_order_delivered_at
    , second_order_delivered_at
    , third_order_delivered_at
    , fourth_order_delivered_at
    , fifth_order_delivered_at
    , o.last_order_created_at
    , o.days_since_last_order
    , o.lifetime_total_spend_ranking
    , o.lifetime_orders_ranking
    , o.lifetime_total_quantity_ranking
    , o.lifetime_average_order_value_ranking
    , o.lifetime_days_as_customer
    , o.lifetime_days_active
    , o.lifetime_weeks_active
    , o.lifetime_months_active
FROM user_sessions s
LEFT JOIN user_orders o on s.user_id = o.user_id