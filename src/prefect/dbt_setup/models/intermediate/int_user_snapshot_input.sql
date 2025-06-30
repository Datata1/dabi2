{{
    config(
        materialized='table'
    )
}}

WITH source AS (
    SELECT
        *,
        ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY source_timestamp_ms DESC) as rn
    FROM
        {{ ref('stg_users') }}
),


active_users AS (
    SELECT
        user_id,
        toDateTime64(source_timestamp_ms / 1000, 6) AS effective_last_updated_ts
    FROM
        source
    WHERE
        rn = 1 AND op_type != 'd' 
)

SELECT * FROM active_users