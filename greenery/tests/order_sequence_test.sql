--To check whether the sequences of dates is not broken. 
SELECT order_id
FROM {{ ref ('fct_orders') }}
    WHERE created_at_utc > est_delivered_at_utc
       OR created_at_utc > delivered_at_utc


