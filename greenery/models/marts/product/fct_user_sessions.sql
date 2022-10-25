
WITH sessions AS (
  SELECT * 
  FROM {{ ref('int_session_agg') }}
)
, users AS (
  SELECT *
  FROM {{ ref('stg_users') }} 
)
SELECT s.user_id
    , u.first_name
    , u.last_name
    , u.email
    , u.phone_number
    , u.created_at_utc
    , COUNT(distinct s.session_id) as lifetime_session_count
    , AVG(s.total_session_time_minutes) as avg_session_time_minutes
    , SUM(s.num_page_view) as lifetime_page_views
    , SUM(s.num_add_to_cart) as lifetime_added_to_cart
    , SUM(s.num_checkout) as lifetime_checkouts
    , SUM(s.num_package_shipped) as lifetime_packages_shipped
    , COUNT(distinct case when s.num_add_to_cart > 0 then s.session_id else null end) as lifetime_sessions_with_add_to_cart
    , COUNT(distinct case when s.num_checkout > 0 then s.session_id else null end) as lifetime_sessions_with_check_out
    , lifetime_sessions_with_check_out / lifetime_sessions_with_add_to_cart as pct_of_sessions_with_item_added_with_checkout --to know conversion of sessions with with added items to cart
    , lifetime_sessions_with_check_out / lifetime_session_count as pct_of_total_sessions_with_checkout --to know conversion rate of typical sessions,
    , COUNT(distinct date_trunc('day',created_at_utc)) as lifetime_days_with_session
FROM sessions s 
LEFT JOIN users u ON u.user_id = s.user_id
GROUP BY 1,2,3,4,5,6