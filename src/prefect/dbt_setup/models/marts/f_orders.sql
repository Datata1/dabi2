-- models/marts/f_orders.sql
{{
    config(
        materialized='table'
    )
}}

WITH orders AS (
    SELECT * FROM {{ ref('stg_orders') }}
),

tips AS (
    -- Stellt sicher, dass stg_tips nur einen Eintrag pro order_id hat (sollte der Fall sein)
    SELECT * FROM {{ ref('stg_tips') }}
),

dim_users AS (
    SELECT user_sk, user_id FROM {{ ref('dim_users') }}
),

dim_date AS (
    SELECT date_sk, full_date FROM {{ ref('dim_date') }}
)

SELECT
    -- Primärschlüssel oder degenerierte Dimension der Bestellung
    o.order_id,

    -- Foreign Keys zu den Dimensionen
    du.user_sk,
    dd.date_sk,

    -- Measures / Fakten auf Bestell-Ebene
    t.tip_given -- Trinkgeld gehört eindeutig zur Bestellung

    -- Hier könnten später weitere Measures auf Bestell-Ebene hinzukommen,
    -- z.B. SUM(preis), COUNT(artikel) etc., wenn man sie aus den Order Lines aggregiert.

FROM orders o
-- Join mit Tips, um die Trinkgeld-Info zu bekommen (sollte 1:1 sein)
LEFT JOIN tips t ON o.order_id = t.order_id
-- Join mit Dimensionen, um die Surrogate Keys (SKs) zu erhalten
LEFT JOIN dim_users du ON o.user_id = du.user_id
LEFT JOIN dim_date dd ON o.order_timestamp::date = dd.full_date
-- Kein GROUP BY nötig, wenn stg_orders und stg_tips eindeutig pro order_id sind.