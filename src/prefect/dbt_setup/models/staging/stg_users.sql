SELECT
    user_id::BIGINT AS user_id,
    "_op" AS op_type,
    "_ts_ms" AS user_source_timestamp_ms,
    load_ts AS staging_load_timestamp
FROM {{ source('cdc_staging', 'users') }}