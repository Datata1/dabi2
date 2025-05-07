{{
    config(
        materialized='incremental',
        unique_key='product_id',
        on_schema_change='sync_all_columns'
    )
}}

WITH source_delta AS (
    SELECT
        product_id,
        product AS product_name, 
        aisle_id,
        department_id,
        op_type,
        product_source_timestamp_ms
    FROM {{ ref('stg_products') }}
),

latest_delta_per_key AS (
    SELECT
        product_id,
        product_name,
        aisle_id,
        department_id,
        op_type,
        product_source_timestamp_ms
    FROM source_delta
)

SELECT
    product_id,
    product_name, 
    aisle_id,         
    department_id, 
    product_source_timestamp_ms AS last_changed_ts
FROM latest_delta_per_key
WHERE op_type IN ('c', 'u')

{% if is_incremental() %}

{% endif %}