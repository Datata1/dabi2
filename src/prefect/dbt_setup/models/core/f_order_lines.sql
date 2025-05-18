{{
    config(
        materialized='incremental',
        unique_key=['order_id', 'product_id'], 
        tags=['marts_model'],
    )
}}

WITH new_order_lines AS (
    SELECT *
    FROM {{ ref('stg_order_products') }}
    WHERE op_type = 'c' 
),

orders_info AS (
    SELECT
        order_id,
        user_id,
        order_timestamp,
        to_timestamp(order_timestamp / 1000000.0) AS order_timestamp_ts
    FROM {{ ref('stg_orders') }}
),

snapshot_products AS (
    SELECT 
        product_id, 
        product_name, 
        aisle_name, 
        department_name, 
        effective_last_updated_ts, 
        dbt_scd_id, 
        dbt_valid_from,
        dbt_valid_to,
    FROM {{ ref('dim_products') }} 
),


snapshot_users AS (
    SELECT 
        user_id, 
        effective_last_updated_ts, 
        dbt_valid_from,
        dbt_scd_id, 
        dbt_valid_to,
    FROM {{ ref('dim_users') }} 
),

dim_date AS (
    SELECT date_sk, full_date FROM {{ ref('dim_date') }}
)

SELECT
    nol.order_id,
    nol.product_id, 

    sp.dbt_scd_id AS product_version_sk, 
    su.dbt_scd_id AS user_version_sk,   
    dd.date_sk  AS order_date_sk, 

    nol.add_to_cart_order,

    oi.order_timestamp,
    nol.staging_load_timestamp

FROM new_order_lines nol
INNER JOIN orders_info oi ON nol.order_id = oi.order_id

LEFT JOIN snapshot_products sp
    ON nol.product_id = sp.product_id
    AND oi.order_timestamp_ts >= sp.dbt_valid_from  
    AND oi.order_timestamp_ts < COALESCE(
                                    sp.dbt_valid_to, 
                                    to_timestamp(4113387935) 
                                )

LEFT JOIN snapshot_users su
    ON oi.user_id = su.user_id
    AND oi.order_timestamp_ts >= su.dbt_valid_from 
    AND oi.order_timestamp_ts < COALESCE(
                                    su.dbt_valid_to, 
                                    to_timestamp(4113387935)
                                )

LEFT JOIN dim_date dd ON CAST(strftime(to_timestamp(oi.order_timestamp / 1000000), '%Y%m%d') AS INTEGER) = dd.date_sk
