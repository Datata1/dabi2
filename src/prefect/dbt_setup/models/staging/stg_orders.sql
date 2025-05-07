SELECT
    order_id::BIGINT AS order_id,
    user_id::BIGINT AS user_id,
    order_date AS order_timestamp,
    tip_given::BOOLEAN AS tip_given, 

    "_op" AS op_type, 
    "_ts_ms" AS source_timestamp_ms,
    load_ts AS staging_load_timestamp 

FROM {{ source('cdc_staging', 'orders') }} 