
[Link to Query in Snowflake](link url)
## Andrew Chen - Co-Rise Week 2

#### What is our user repeat rate?
_Repeat Rate = Users who purchased 2 or more times / users who purchased_
- Repeat rate is approximately 80%
```sql
SELECT SUM(CASE WHEN LIFETIME_ORDER_COUNT >= 2 THEN 1 ELSE 0 END) / 
        SUM(CASE WHEN LIFETIME_ORDER_COUNT >= 1 THEN 1 ELSE 0 END) AS REPEAT_RATE
FROM dim_users
;
```

#### What are good indicators of a user who will likely purchase again? What about indicators of users who are likely NOT to purchase again? If you had more data, what features would you want to look into to answer this question?
_NOTE: This is a hypothetical question vs. something we can analyze in our Greenery data set. Think about what exploratory analysis you would do to approach this question.

_I would would to explore the following indicators and their correlation to purchases
- Number of sessions
- Session Depth prior to adding to cart and purchase (page views per session)
- Adding to Cart Events
- Sessions Length 

_I would would to explore the following indicators and their correlation to customer churn
- Duration since last purchase 
- Cart Abandonment 
- Lengthier delivery than the estimated delivery date
- The length of the user spending on each stage (from add to cart -> checkout -> package shipped)
- The # of time they apply promo on their purchase
- Session Depth - Page Views in a session without a purchase or add to cart event (perhaps indicating they were not able to find an item they wanted)

_Additional Data I would want to look at
- Customer survey result after each order
- NPS survey score or user
- Whether they had to return the order after their purchase
- Whether their purchase failed or cancelled on them

#### Explain the marts models you added. Why did you organize the models in the way you did?
- I opted to enrich the order facts intermediate model with as much as I could given that this would be the model that would be used for many other aggregations later on. My goal was to roll up to some type of dimension table for users and products. I also wanted a fact table for user orders and user sessions which would be useful for understanding ordering and session behavior. Knowing that was my end goal I started to work backwards to make the intermediate models that would aggregate things at the appropriate level. 

### Tests
#### What assumptions are you making about each model? (i.e. why are you adding each test?)
- I added tests for primary key uniqueness and made sure some id's were not null. I also wanted to make sure there were only positive values for some values where I expected it to be positive and also where we would expect certain events to happen in a sequence like order placed, shipped, and delivered. 

#### Did you find any "bad" data as you added and ran tests on your models? How did you go about either cleaning the data in the dbt model or adjusting your assumptions/tests?
- 

#### Your stakeholders at Greenery want to understand the state of the data each day. Explain how you would ensure these tests are passing regularly and how you would alert stakeholders about bad data getting through.
- The job runs  `dbt build` each day in the morning that would be able to run the models and tests. If tests fail, the build will fail and trigger an alert in slack / email. This would alert the analytics engineering team to look into bugs or errors in the data that should be followed up on. 

### dbt Snapshots
#### Which orders changed from week 1 to week 2?
- The following `ORDER_ID`'s went from _Preparing_ to _Shipped_ status:
  - 914b8929-e04a-40f8-86ee-357f2be3a2a2
  - 05202733-0e17-4726-97c2-0520c024ab85
  - 939767ac-357a-4bec-91f8-a7b25edd46c9







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


