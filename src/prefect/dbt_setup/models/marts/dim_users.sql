-- models/marts/dim_users.sql
{{
    config(
        materialized='table'
    )
}}

{% set initial_valid_from = '1900-01-01 00:00:00' %} 
{{ log("Using fixed initial valid_from for dim_users: " ~ initial_valid_from, info=True) }}

SELECT
    {{ dbt_utils.generate_surrogate_key(['user_id']) }} AS user_sk,
    user_id,
    -- SCD Typ 2 Spalten für den initialen Load
    CAST('{{ initial_valid_from }}' AS timestamp) AS valid_from,
    CAST(NULL AS timestamp) AS valid_to,
    TRUE AS is_current
FROM {{ ref('stg_orders') }}
GROUP BY user_id -- Nur eindeutige User