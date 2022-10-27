
WITH fct_orders as (
    SELECT * FROM {{ ref('fct_orders') }}
)

SELECT 
    user_id
    , COUNT(order_id) as lifetime_order_count
    , SUM(case when order_status = 'delivered' then 1 else 0 END) as lifetime_completed_order_count
    , ROUND(AVG(DAYS_TILL_NEXT_SHIPMENT),1) as lifetime_avg_days_between_shipments
    , SUM(order_cost) as lifetime_spend_exc_shipping
    , SUM(shipping_cost) as lifetime_shipping_cost
    , SUM(order_total) as lifetime_total_spend
    , SUM(total_quantity) as lifetime_items_purchased
    , ROUND(lifetime_total_spend / lifetime_order_count,2) as lifetime_average_order_value
    , ROUND(lifetime_items_purchased / lifetime_order_count,1) as lifetime_average_items_per_order
    , COUNT(promo_id) as lifetime_promo_count
    , SUM(order_discount) as lifetime_promo_discounts
    , MIN(created_at_utc) as first_order_created_at  
    , MAX(CASE WHEN user_order_created_sequence_number = 2 THEN created_at_utc ELSE NULL END) as second_order_created_at
    , MAX(CASE WHEN user_order_created_sequence_number = 3 THEN created_at_utc ELSE NULL END) as third_order_created_at
    , MAX(CASE WHEN user_order_created_sequence_number = 4 THEN created_at_utc ELSE NULL END) as fourth_order_created_at
    , MAX(CASE WHEN user_order_created_sequence_number = 5 THEN created_at_utc ELSE NULL END) as fifth_order_created_at
    , MIN(delivered_at_utc) as first_order_delivered_at
    , MAX(CASE WHEN user_order_delivered_sequence_number = 2 THEN created_at_utc ELSE NULL END) as second_order_delivered_at
    , MAX(CASE WHEN user_order_delivered_sequence_number = 3 THEN created_at_utc ELSE NULL END) as third_order_delivered_at
    , MAX(CASE WHEN user_order_delivered_sequence_number = 4 THEN created_at_utc ELSE NULL END) as fourth_order_delivered_at
    , MAX(CASE WHEN user_order_delivered_sequence_number = 5 THEN created_at_utc ELSE NULL END) as fifth_order_delivered_at
    , MAX(created_at_utc) as last_order_created_at
    , current_date() - date(last_order_created_at) as days_since_last_order
    , RANK() over (order by lifetime_total_spend desc) as lifetime_total_spend_ranking
    , RANK() over (order by lifetime_order_count desc) as lifetime_orders_ranking
    , RANK() over (order by lifetime_items_purchased desc) as lifetime_total_quantity_ranking
    , RANK() over (order by lifetime_average_order_value desc) as lifetime_average_order_value_ranking
    , DATE(last_order_created_at) - date(first_order_created_at) as lifetime_days_as_customer
    , COUNT(DISTINCT date_trunc('day',created_at_utc)) as lifetime_days_active
    , COUNT(DISTINCT date_trunc('week',created_at_utc)) as lifetime_weeks_active
    , COUNT(DISTINCT date_trunc('month',created_at_utc)) as lifetime_months_active
FROM fct_orders f 
GROUP BY 1