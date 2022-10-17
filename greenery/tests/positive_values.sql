--To check whether the sequences of dates is not broken. 
SELECT order_id
FROM {{ ref ('fct_orders') }}
WHERE order_total < 0
OR shipping_cost < 0 
OR order_cost < 0
OR num_products < 0
OR total_quantity < 0