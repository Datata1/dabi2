-- models/marts/f_orders.sql (Historisch korrekt)
{{
    config(
        materialized='table'
    )
}}

WITH orders AS (
    SELECT * FROM {{ ref('stg_orders') }}
),

tips AS (
    SELECT * FROM {{ ref('stg_tips') }}
),

dim_users_hist AS ( -- Umbenannt zur Klarheit, referenziert die volle History-Tabelle
    SELECT user_sk, user_id, valid_from, valid_to
    FROM {{ ref('dim_users') }}
),

dim_date AS (
    SELECT date_sk, full_date FROM {{ ref('dim_date') }}
)

SELECT
    o.order_id,
    -- Dieser User-SK gehört zur User-Version, die zum Zeitpunkt der Bestellung gültig war
    du.user_sk,
    dd.date_sk,
    t.tip_given

FROM orders o
LEFT JOIN tips t ON o.order_id = t.order_id
-- Join mit der historischen User-Dimension
LEFT JOIN dim_users_hist du
    ON o.user_id = du.user_id
    -- UND: Zeitstempel der Bestellung muss im Gültigkeitsbereich liegen
    AND o.order_timestamp >= du.valid_from
    AND o.order_timestamp < COALESCE(du.valid_to, '9999-12-31 23:59:59'::timestamp) -- COALESCE für aktuell gültige Einträge
-- Join mit Datum bleibt gleich
LEFT JOIN dim_date dd ON o.order_timestamp::date = dd.full_date