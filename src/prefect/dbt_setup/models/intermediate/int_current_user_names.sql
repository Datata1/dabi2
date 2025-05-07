{{
    config(
        materialized='incremental',
        unique_key='user_id',
        on_schema_change='sync_all_columns' 
    )
}}

WITH source_delta AS (
    SELECT
        user_id,
        op_type,
        user_source_timestamp_ms
    FROM {{ ref('stg_users') }}
),

latest_delta_per_key AS (
    SELECT
        user_id,
        op_type,
        user_source_timestamp_ms
    FROM source_delta
)

SELECT
    user_id,
    user_source_timestamp_ms AS last_changed_ts
FROM latest_delta_per_key
WHERE op_type IN ('c', 'u')

{% if is_incremental() %}

{% endif %}