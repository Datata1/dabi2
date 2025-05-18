{{
  config(
    materialized='table'
  )
}}

WITH user_orders_sequenced AS (
  SELECT
    user_id,
    order_id,
    order_timestamp,        
    order_date_sk,         
    CAST(tip_given AS INTEGER) AS tip_given,
    ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY order_timestamp ASC, order_id ASC) AS user_order_sequence
  FROM
    {{ ref('f_orders') }}    

),

lagged_tipping_info AS (
  SELECT
    *,
    LAG(tip_given, 1, NULL) OVER (PARTITION BY user_id ORDER BY user_order_sequence ASC) AS tip_given_lag1,
    LAG(tip_given, 2, NULL) OVER (PARTITION BY user_id ORDER BY user_order_sequence ASC) AS tip_given_lag2, 
    LAG(tip_given, 3, NULL) OVER (PARTITION BY user_id ORDER BY user_order_sequence ASC) AS tip_given_lag3, 
    LAG(tip_given, 4, NULL) OVER (PARTITION BY user_id ORDER BY user_order_sequence ASC) AS tip_given_lag4, 
    LAG(tip_given, 5, NULL) OVER (PARTITION BY user_id ORDER BY user_order_sequence ASC) AS tip_given_lag5
  FROM
    user_orders_sequenced
)

SELECT
  lts.user_id,
  lts.order_id,
  lts.order_timestamp,
  lts.order_date_sk,
  lts.user_order_sequence,
  lts.tip_given,
  lts.tip_given_lag1,
  lts.tip_given_lag2,
  lts.tip_given_lag3,
  lts.tip_given_lag4,
  lts.tip_given_lag5,
  dd.full_date,             
  dd.year,           
  dd.month,          
  dd.day,            
  dd.day_of_week, 
  dd.day_of_year,
  dd.week_of_year,
  dd.quarter,   
  dd.is_weekend

FROM
  lagged_tipping_info lts
LEFT JOIN
  {{ ref('dim_date') }} dd ON lts.order_date_sk = dd.date_sk 
ORDER BY
  lts.user_id,
  lts.user_order_sequence
