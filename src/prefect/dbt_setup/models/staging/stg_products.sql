SELECT
    product_id::BIGINT,
    product_name,
    aisle_id::INTEGER,
    department_id::INTEGER,
    "__op" AS op_type,
    "__source_ts_ms" AS source_timestamp_ms,
    load_ts AS staging_load_timestamp
FROM {{ source('cdc_staging', 'products') }};