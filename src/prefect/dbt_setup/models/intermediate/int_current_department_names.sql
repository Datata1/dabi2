{{
    config(
        materialized='incremental',
        unique_key='department_id',
        on_schema_change='sync_all_columns'
    )
}}

WITH source_delta AS (
    SELECT
        department_id,
        department,
        op_type,
        source_timestamp_ms
    FROM {{ ref('stg_departments') }}
),

latest_delta_per_key AS (
    SELECT
        department_id,
        department,
        op_type,
        source_timestamp_ms
    FROM source_delta
)

SELECT
    department_id,
    department AS department_name,
    source_timestamp_ms AS last_changed_ts
FROM latest_delta_per_key
WHERE op_type IN ('c', 'u')

{% if is_incremental() %}

{% endif %}