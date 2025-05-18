{{
    config(
        materialized='incremental',
        unique_key='order_id',
        tags=['marts_model'],
    )
}}

WITH latest_orders AS (
    SELECT
        order_id,
        user_id,
        order_timestamp, 
        to_timestamp(order_timestamp / 1000000.0) AS order_timestamp_ts, 
        tip_given,
        op_type,
        source_timestamp_ms,
        staging_load_timestamp,
        ROW_NUMBER() OVER (
            PARTITION BY order_id
            ORDER BY source_timestamp_ms DESC, staging_load_timestamp DESC
        ) as rn
    FROM {{ ref('stg_orders') }} 

    {% if is_incremental() %}
      WHERE staging_load_timestamp > (SELECT max(staging_load_timestamp) FROM {{ this }})
    {% endif %}
),

snapshot_users AS (
    SELECT
        dbt_scd_id, 
        user_id,   
        dbt_valid_from, 
        dbt_valid_to,  
    FROM {{ ref('dim_users') }} 
),

dim_date AS (
    SELECT date_sk, full_date FROM {{ ref('dim_date') }}
)

SELECT
    lo.order_id,

    lo.user_id AS user_id,
    su.dbt_scd_id AS user_version_sk,
    COALESCE(dd.date_sk, -1) AS order_date_sk,

    lo.tip_given,

    CASE WHEN lo.op_type = 'd' THEN FALSE ELSE TRUE END AS is_active_order,

    lo.order_timestamp, 
    lo.source_timestamp_ms,
    lo.staging_load_timestamp

FROM latest_orders lo

LEFT JOIN snapshot_users su
    ON lo.user_id = su.user_id
    AND lo.order_timestamp_ts >= su.dbt_valid_from 
    AND lo.order_timestamp_ts < COALESCE(
                                    su.dbt_valid_to, 
                                    to_timestamp(4113387935) 
                                )

LEFT JOIN dim_date dd ON CAST(strftime(to_timestamp(lo.order_timestamp / 1000000.0), '%Y%m%d') AS INTEGER) = dd.date_sk

WHERE lo.rn = 1 