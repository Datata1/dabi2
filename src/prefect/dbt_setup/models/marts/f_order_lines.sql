-- models/marts/f_order_lines.sql (Historisch korrekt)
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

dim_products_hist AS ( -- Umbenannt, referenziert volle History
    SELECT product_sk, product_id, valid_from, valid_to
    FROM {{ ref('dim_products') }}
),

dim_users_hist AS ( -- Umbenannt, referenziert volle History
    SELECT user_sk, user_id, valid_from, valid_to
    FROM {{ ref('dim_users') }}
),

dim_date AS (
    SELECT date_sk, full_date FROM {{ ref('dim_date') }}
)

SELECT
    op.order_id,
    dp.product_sk, -- Der historisch korrekte Produkt-SK
    du.user_sk,    -- Der historisch korrekte User-SK
    dd.date_sk,
    o.order_timestamp, -- Zeitstempel ggf. für Analysen behalten
    op.add_to_cart_order

FROM order_products op
LEFT JOIN orders o ON op.order_id = o.order_id
-- Join mit historischer Produkt-Dimension
LEFT JOIN dim_products_hist dp
    ON op.product_id = dp.product_id
    AND o.order_timestamp >= dp.valid_from
    AND o.order_timestamp < COALESCE(dp.valid_to, '9999-12-31 23:59:59'::timestamp)
-- Join mit historischer User-Dimension
LEFT JOIN dim_users_hist du
    ON o.user_id = du.user_id
    AND o.order_timestamp >= du.valid_from
    AND o.order_timestamp < COALESCE(du.valid_to, '9999-12-31 23:59:59'::timestamp)
-- Join mit Datum bleibt gleich
LEFT JOIN dim_date dd ON o.order_timestamp::date = dd.full_date