SELECT
    p.product_id,
    p.product_name,
    p._ts_ms AS product_source_timestamp_ms,
    p._op AS product_op_type,
    p.load_ts AS product_staging_load_timestamp,
    a.aisle_id AS aisle_id,
    a.aisle AS aisle_name,
    a._ts_ms AS aisle_source_timestamp_ms,
    a._op AS aisle_op_type,
    a.load_ts AS aisle_staging_load_timestamp,
    d.department_id AS department_id,
    d.department AS department_name,
    d._ts_ms AS department_source_timestamp_ms,
    d._op AS department_op_type,
    d.load_ts AS department_staging_load_timestamp
FROM
    {{ source('cdc_raw_data', 'stg_raw_products') }} p
LEFT JOIN {{ source('cdc_raw_data', 'stg_raw_aisles') }} a
    ON p.aisle_id = a.aisle_id
LEFT JOIN {{ source('cdc_raw_data', 'stg_raw_departments') }} d
    ON p.department_id = d.department_id