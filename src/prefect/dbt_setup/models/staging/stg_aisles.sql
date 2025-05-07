SELECT
    aisle_id::INTEGER AS aisle_id,
    aisle,
    "_op" AS op_type,
    "_ts_ms" AS source_timestamp_ms,
    load_ts AS staging_load_timestamp
FROM {{ source('cdc_staging', 'aisles') }}