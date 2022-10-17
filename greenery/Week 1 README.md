
[Link to Query in Snowflake](https://app.snowflake.com/us-east-1/ryb00700/w5C7uCxDb8Ng#query)
## Andrew Chen - Co-Rise Week 1

#### How many users do we have?
- 130
```
SELECT COUNT(DISTINCT user_id) as distinct_users 
FROM stg_users;
```


#### On average, how many orders do we receive per hour?
- On average, there are 7.5 orders per hour
- My assumption was since there were two days we had to date_trunc hour to treat each day's hour separately so we could average at the right hourly level

```
WITH order_count_per_hour as(
SELECT date_trunc('hour',created_at) -- date_trunc since there are two distinct days
    , COUNT(order_id) num_orders
FROM STG_ORDERS
GROUP BY 1
)
SELECT AVG(num_orders) as orders_per_hour
FROM order_count_per_hour;
```

#### On average, how long does an order take from being placed to being delivered?
- On average, the time it takes to deliver an order is 3.9 days (or 93.4 hours)

```
WITH days as (
    SELECT order_id
    , CREATED_AT
    , DELIVERED_AT
    , DATEDIFF('hour', CREATED_AT, DELIVERED_AT) hours_to_ship
    FROM STG_ORDERS
)
SELECT ROUND(avg(hours_to_ship),1) as avg_hours_to_ship
    , ROUND(avg(hours_to_ship)/24.0,1) as avg_days_to_ship
FROM days;
```

#### How many users have only made one purchase? Two purchases? Three+ purchases?
- 1 purchase: 25
- 2 purchases: 28
- 3+ purchases: 71

```
WITH aggregated_order_count as (
    SELECT user_id
        , COUNT(*) order_count
    FROM stg_orders
    GROUP BY 1
)
SELECT SUM(CASE WHEN order_count = 1 THEN 1 ELSE NULL END) as user_one_order
    , SUM(CASE WHEN order_count = 2 THEN 1 ELSE NULL END) as user_two_orders
    , SUM(CASE WHEN order_count >= 3 THEN 1 ELSE NULL END) as user_three_plus_orders
FROM aggregated_order_count;
```

#### On average, how many unique sessions do we have per hour?
- On average, we are getting roughly 16.3 unique sessions per hour.

```
WITH session_count_per_hour as(
SELECT date_trunc('hour',created_at) -- date_trunc since there are two distinct days
    , COUNT(DISTINCT session_id) num_session
FROM STG_EVENTS
GROUP BY 1
)
SELECT ROUND(AVG(num_session),1) as sessions_per_hour
FROM session_count_per_hour;
```


