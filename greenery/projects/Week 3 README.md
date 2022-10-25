# Part 1 Conversion Rate
## 1. What is our overall conversion rate?

- Overall Conversion rate is approximately 62.5%

_NOTE: conversion rate is defined as the # of unique sessions with a purchase event / total number of unique sessions._

```sql
with purchase_sessions as (
    SELECT SUM(Num_orders) sessions_with_orders
        , COUNT(SESSION_ID) total_sessions
        , ROUND(sessions_with_orders/total_sessions,3) as conversion_rate
FROM DEV_DB.DBT_ACHEN.INT_SESSION_AGG
)
```

## 2. What is our conversion rate by product?

- I have a model for this - check table `DEV_DB.DBT_ACHEN.DIM_PRODUCTS`.

_NOTE: Conversion rate by product is defined as the # of unique sessions with a purchase event of that product / total number of unique sessions that viewed that product_


# Part 2: Macro
Create a macro
- Create a macro to grant permissions and test positive values

# Part 3: Post Hook
- Add a post hook to project to apply grants to the role “reporting”
- Added a post hook in the dbt_project.yml

# Part 4: Apply Macro or Tests
- Used a dbt_utils macro in the models `int_product_events` and `int_session_agg` to pivot the data by event type
- Used a dbt_utils macro to generate a surrogate key in `int_order_item_events`
- Adding tests for positive_values, not_null, unique on columns

# Part 5 Snapshot
## Which orders changed from week 2 to week 3 after running dbt snapshot
- 8385cfcd-2b3f-443a-a676-9756f7eb5404
- e24985f3-2fb3-456e-a1aa-aaf88f490d70
- 5741e351-3124-4de7-9dff-01a448e7dfd4