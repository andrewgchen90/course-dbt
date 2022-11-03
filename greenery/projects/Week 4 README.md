## Part 5: Snapshot
### Which orders changed from week 3 to week 4 after running dbt snapshot
- 38c516e8-b23a-493a-8a5c-bf7b2b9ea995
- d1020671-7cdf-493c-b008-c48535415611
- aafb9fbd-56e1-4dcc-b6b2-a3fd91381bb6

## Part 2: Funnel
### How are our users moving through the product funnel?
 - Overall the conversion rates per step of the funnel are pretty good, 77-80% per step. I'm sure there could be improvements but right now there isn't too much data to work with.

### Which steps in the funnel have largest drop off points?
 - The Add to Cart to Checkout Step has a lower conversion rate of 77% compared to 80% of adding to cart from the initial PV.

```sql
SELECT SUM(SESSIONS_WITH_PV_COUNT) SESSIONS_WITH_PV
    , SUM(sessions_with_add_to_cart_count) as sessions_with_add_to_cart
    , SUM(SESSIONS_WITH_CHECKOUT_COUNT) as SESSIONS_WITH_CHECKOUT
    , ROUND(sessions_with_add_to_cart/SESSIONS_WITH_PV,3) as pv_to_add_to_cart_conv_rate
    , ROUND(SESSIONS_WITH_CHECKOUT/sessions_with_add_to_cart,3) as add_to_cart_to_check_out_conv_rate
    FROM int_date_sessions
```

_Please create any additional dbt models needed to help answer these questions from our product team, and put your answers in a README in your repo._
- I have a model for this - check table `DEV_DB.DBT_ACHEN.INT_DATE_SESSIONS`.

_Use an exposure on your product analytics model to represent that this is being used in downstream BI tools. Please reference the course content if you have questions._


## Part 3a: dbt next steps
- 
- 
