-- models/staging/stg_order_products.sql

{{
    config(
        materialized='view'
    )
}}

SELECT
    order_id::BIGINT AS order_id,
    product_id::BIGINT AS product_id,

    add_to_cart_order::INTEGER AS add_to_cart_order,

    _op AS op_type,
    _ts_ms AS source_timestamp_ms,
    load_ts AS staging_load_timestamp

FROM
    {{ source('cdc_raw_data', 'stg_raw_order_products') }}