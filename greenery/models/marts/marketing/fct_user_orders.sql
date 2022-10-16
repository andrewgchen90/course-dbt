
WITH fct_orders as (
    select * from {{ ref('fct_orders') }}
)

SELECT user_id
    , count(order_id) as lifetime_order_count
    , sum(case when order_status = 'delivered' then 1 else 0 END) as lifetime_completed_order_count
    , ROUND(avg(DAYS_TILL_NEXT_SHIPMENT),1) as lifetime_avg_days_between_shipments
    , sum(order_cost) as lifetime_spend_exc_shipping
    , sum(shipping_cost) as lifetime_shipping_cost
    , sum(order_total) as lifetime_total_spend
    , sum(total_quantity) as lifetime_items_purchased
    , round(lifetime_total_spend / lifetime_order_count,2) as lifetime_average_order_value
    , round(lifetime_items_purchased / lifetime_order_count,1) as lifetime_average_items_per_order
    , count(promo_id) as lifetime_promo_count
    , sum(order_discount) as lifetime_promo_discounts
    , min(created_at_utc) as first_order_at  
    , max(CASE WHEN user_order_sequence_number = 2 THEN created_at_utc ELSE NULL END) as second_order_at
    , max(CASE WHEN user_order_sequence_number = 3 THEN created_at_utc ELSE NULL END) as third_order_at
    , max(CASE WHEN user_order_sequence_number = 4 THEN created_at_utc ELSE NULL END) as fourth_order_at
    , max(CASE WHEN user_order_sequence_number = 5 THEN created_at_utc ELSE NULL END) as fifth_order_at
    , max(created_at_utc) as last_order_at
    , current_date() - date(last_order_at) as days_since_last_order
    , rank() over (order by lifetime_total_spend desc) as lifetime_total_spend_ranking
    , rank() over (order by lifetime_order_count desc) as lifetime_orders_ranking
    , rank() over (order by lifetime_items_purchased desc) as lifetime_total_quantity_ranking
    , rank() over (order by lifetime_average_order_value desc) as lifetime_average_order_value_ranking
    , date(last_order_at) - date(first_order_at) as lifetime_days_as_customer
    , count(DISTINCT date_trunc('day',created_at_utc)) as lifetime_days_active
    , count(DISTINCT date_trunc('week',created_at_utc)) as lifetime_weeks_active
    , count(DISTINCT date_trunc('month',created_at_utc)) as lifetime_months_active
from fct_orders f 
group by 1