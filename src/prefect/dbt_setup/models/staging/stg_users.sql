SELECT
    p.user_id,
    p._op AS op_type,
    p._ts_ms AS source_timestamp_ms,
    p.load_ts AS staging_load_timestamp
FROM
    {{ source('cdc_raw_data', 'stg_raw_users') }} p
