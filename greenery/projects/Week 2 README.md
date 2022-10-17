
## Andrew Chen - Co-Rise Week 2

#### What is our user repeat rate?
_Repeat Rate = Users who purchased 2 or more times / users who purchased_
- Repeat rate is approximately 80%
```sql
SELECT SUM(CASE WHEN LIFETIME_ORDER_COUNT >= 2 THEN 1 ELSE 0 END) / 
        SUM(CASE WHEN LIFETIME_ORDER_COUNT >= 1 THEN 1 ELSE 0 END) AS REPEAT_RATE
FROM dim_users;
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
- No bad data so far

#### Your stakeholders at Greenery want to understand the state of the data each day. Explain how you would ensure these tests are passing regularly and how you would alert stakeholders about bad data getting through.
- The job runs  `dbt build` each day in the morning that would be able to run the models and tests. If tests fail, the build will fail and trigger an alert in slack / email. This would alert the analytics engineering team to look into bugs or errors in the data that should be followed up on. 

### dbt Snapshots
#### Which orders changed from week 1 to week 2?
- The following `ORDER_ID`'s went from _Preparing_ to _Shipped_ status:
  - 914b8929-e04a-40f8-86ee-357f2be3a2a2
  - 05202733-0e17-4726-97c2-0520c024ab85
  - 939767ac-357a-4bec-91f8-a7b25edd46c9
