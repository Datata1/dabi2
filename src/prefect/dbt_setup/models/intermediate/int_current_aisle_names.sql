{{
    config(
        materialized='incremental',
        unique_key='aisle_id',
        on_schema_change='sync_all_columns' 
    )
}}

WITH source_delta AS (
    SELECT
        aisle_id,
        aisle,
        op_type,
        source_timestamp_ms
    FROM {{ ref('stg_aisles') }}
),

latest_delta_per_key AS (
    SELECT
        aisle_id,
        aisle,
        op_type,
        source_timestamp_ms
    FROM source_delta
)

SELECT
    aisle_id,
    aisle AS aisle_name, 
    source_timestamp_ms AS last_changed_ts
FROM latest_delta_per_key
WHERE op_type IN ('c', 'u')

{% if is_incremental() %}

{% endif %}