-- models/marts/f_order_lines.sql (Angepasste Version)
{{
    config(
        materialized='table'
    )
}}

WITH order_products AS (
    SELECT * FROM {{ ref('stg_order_products') }}
),

orders AS (
    SELECT order_id, user_id, order_timestamp FROM {{ ref('stg_orders') }}
),


dim_users AS (
    SELECT user_sk, user_id FROM {{ ref('dim_users') }}
),

dim_products AS (
    SELECT product_sk, product_id FROM {{ ref('dim_products') }}
),

dim_date AS (
    SELECT date_sk, full_date FROM {{ ref('dim_date') }}
)

SELECT
    -- Foreign Keys
    op.order_id,        -- Beibehalten zum Verbinden mit f_orders oder Gruppieren
    dp.product_sk,      -- Zur Produktdimension
    du.user_sk,         -- Zur Userdimension (redundant, aber oft nützlich hier)
    dd.date_sk,         -- Zur Datumdimension (redundant, aber oft nützlich hier)

    -- Measures / Fakten auf Positions-Ebene
    op.add_to_cart_order

    -- tip_given wurde entfernt

FROM order_products op
-- Join mit Orders, um user_id und timestamp für weitere Joins zu bekommen
LEFT JOIN orders o ON op.order_id = o.order_id
-- Join zu tips wurde entfernt
-- LEFT JOIN tips t ON op.order_id = t.order_id
-- Join mit Dimensionen
LEFT JOIN dim_products dp ON op.product_id = dp.product_id
LEFT JOIN dim_users du ON o.user_id = du.user_id -- Join user via orders
LEFT JOIN dim_date dd ON o.order_timestamp::date = dd.full_date -- Join date via orders