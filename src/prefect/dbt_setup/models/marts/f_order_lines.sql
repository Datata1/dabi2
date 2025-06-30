{{
    config(
        materialized='incremental',
        engine='ReplacingMergeTree()',
        order_by=('order_id', 'product_id'),
        unique_key=['order_id', 'product_id'],
    )
}}

WITH order_lines AS (
    SELECT * FROM {{ ref('stg_order_products') }}
),
orders AS (
    SELECT * FROM {{ ref('stg_orders') }}
)

SELECT
    -- Nur noch die Surrogate Keys, die wir wirklich brauchen
    dp.dbt_scd_id AS product_sk,
    du.dbt_scd_id AS user_sk,
    -- ... andere Keys wie order_date_sk

    -- Fakten und Metriken
    ol.order_id AS order_id,
    ol.product_id AS product_id,
    ol.add_to_cart_order AS add_to_cart_order,

    -- Timestamps
    o.order_timestamp AS order_timestamp,
    o.staging_load_timestamp AS staging_load_timestamp

FROM order_lines ol
LEFT JOIN orders o ON ol.order_id = o.order_id

-- Join auf den denormalisierten Produkt-Snapshot
LEFT JOIN {{ ref('dim_products') }} dp
    ON ol.product_id = dp.product_id
    AND o.source_timestamp_ms >= dp.dbt_valid_from
    AND o.source_timestamp_ms < coalesce(dp.dbt_valid_to, toDateTime64('9999-12-31 23:59:59.999999', 6))



-- Join auf den User-Snapshot (unverändert)
LEFT JOIN {{ ref('dim_users') }} du
    ON o.user_id = du.user_id
    AND o.source_timestamp_ms >= du.dbt_valid_from
    AND o.source_timestamp_ms < coalesce(du.dbt_valid_to, toDateTime64('9999-12-31 23:59:59.999999', 6))

