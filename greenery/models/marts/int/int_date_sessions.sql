WITH date_spine as (
    SELECT calendar_date
        , calendar_year
        , calendar_month
        , calendar_day
         FROM {{ref('int_date_spine')}}
)
, session_date as (
    SELECT date(session_start_time) as session_start_date
        , COUNT(DISTINCT session_id) as session_count
        , COUNT(DISTINCT CASE WHEN num_page_view>0 THEN session_id ELSE NULL END) as sessions_with_pv_count
        , COUNT(DISTINCT CASE WHEN num_add_to_cart>0 THEN session_id ELSE NULL END) as sessions_with_add_to_cart_count
        , COUNT(DISTINCT CASE WHEN num_checkout>0 THEN session_id ELSE NULL END) as sessions_with_checkout_count
         FROM {{ref('int_session_agg')}}
    GROUP BY 1
)
, min_max_session_dates as (
    SELECT MIN(date(session_start_time)) as min_date
        , MAX(date(session_start_time)) as max_date
    FROM {{ref('int_session_agg')}}
)
SELECT * 
FROM date_spine d
LEFT JOIN session_date s ON d.calendar_date = s.session_start_date
WHERE d.calendar_date >= (SELECT min_date from min_max_session_dates)
    AND d.calendar_date <= (SELECT max_date from min_max_session_dates)

