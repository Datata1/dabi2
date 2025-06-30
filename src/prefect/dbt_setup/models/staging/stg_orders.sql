{{
    config(
        materialized='view'
    )
}}

WITH source AS (

    SELECT * FROM {{ source('cdc_raw_data', 'stg_raw_orders') }}

),

ranked AS (

    SELECT
        *,
        ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY _ts_ms DESC) as rn
    FROM source

)

SELECT
    order_id::BIGINT AS order_id,
    user_id::BIGINT AS user_id,

    order_date::BIGINT AS order_timestamp,
    tip_given,

    _op AS op_type,
    _ts_ms AS source_timestamp_ms,
    load_ts AS staging_load_timestamp

FROM ranked
WHERE rn = 1