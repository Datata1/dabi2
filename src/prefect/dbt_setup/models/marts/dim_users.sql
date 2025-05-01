-- models/marts/dim_users.sql
{{
    config(
        materialized='table'
    )
}}

SELECT
    -- Surrogate Key generieren (eindeutiger künstlicher Schlüssel)
    {{ dbt_utils.generate_surrogate_key(['user_id']) }} AS user_sk,
    user_id -- Natural Key (aus den Quelldaten)
    -- Hier könnten später weitere User-Attribute hinzukommen
FROM {{ ref('stg_orders') }}
-- Nur eindeutige User behalten
GROUP BY user_id