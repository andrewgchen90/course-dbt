WITH date_spine as (
    SELECT calendar_date
        , calendar_year
        , calendar_month
        , calendar_day
         FROM {{ref('stg_date_spine')}}
)
SELECT * 
FROM date_spine