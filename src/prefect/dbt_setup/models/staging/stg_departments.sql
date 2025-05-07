SELECT
    department_id::INTEGER AS department_id,
    department,
    "_op" AS op_type,
    "_ts_ms" AS source_timestamp_ms,
    load_ts AS staging_load_timestamp
FROM {{ source('cdc_staging', 'departments') }}