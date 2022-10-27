
/*
DIM_DATE using date_spine from dbt_utils:
Generic Date dimension '2021-01-01' that goes through 1 year after today.
*/


with date_spine as (
    {{ dbt_utils.date_spine 
        (
            datepart = "day",
            start_date = "cast('2021-01-01' as date)",
            end_date = "cast(dateadd('year', 1, current_date()) as date)"
        )
    }}
),
final as (
    select
        to_varchar(date_day, 'yyyymmdd') as calendar_id,
        date_day as calendar_date,
        date_part('year', date_day) as calendar_year,
        date_part('month', date_day) as calender_month,
        date_part('day', date_day) as calender_day
    from
        date_spine
)
select * from final