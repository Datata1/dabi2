SELECT
    product_id::BIGINT AS product_id,
    product_name AS product,
    aisle_id::INTEGER AS aisle_id,
    department_id::INTEGER AS department_id,
    "_op" AS op_type,
    "_ts_ms" AS product_source_timestamp_ms,
    load_ts AS staging_load_timestamp
FROM {{ source('cdc_staging', 'products') }}