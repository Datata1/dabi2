SELECT
    aisle_id::INTEGER,
    aisle,
    "__op" AS op_type,
    "__source_ts_ms" AS source_timestamp_ms,
    load_ts AS staging_load_timestamp
FROM {{ source('cdc_staging', 'aisles') }}