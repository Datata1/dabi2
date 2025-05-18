-- models/marts/f_order_lines.sql (Angepasst für CDC und Snapshots)
{{
    config(
        materialized='incremental',
        unique_key=['order_id', 'product_id'], 
        tags=['marts_model'],
    )
}}

WITH new_order_lines AS (
    -- Wähle nur neue Einträge aus dem Staging-Modell
    SELECT *
    FROM {{ ref('stg_order_products') }}
    WHERE op_type = 'c' 
),

orders_info AS (
    SELECT
        order_id,
        user_id,
        order_timestamp,
        to_timestamp(order_timestamp / 1000000.0) AS order_timestamp_ts
    FROM {{ ref('stg_orders') }}
),

snapshot_products AS (
    SELECT 
        product_id, 
        product_name, 
        aisle_name, 
        department_name, 
        effective_last_updated_ts, 
        dbt_scd_id, 
        dbt_valid_from,
        dbt_valid_to,
    FROM {{ ref('dim_products') }} 
),


snapshot_users AS (
    SELECT 
        user_id, 
        effective_last_updated_ts, 
        dbt_valid_from,
        dbt_scd_id, 
        dbt_valid_to,
    FROM {{ ref('dim_users') }} 
),

dim_date AS (
    SELECT date_sk, full_date FROM {{ ref('dim_date') }}
)

SELECT
    nol.order_id,
    nol.product_id, 

    -- === WICHTIG: Surrogate Keys aus den Snapshots ===
    sp.dbt_scd_id AS product_version_sk, -- Oder einen Hash von -1 wenn es ein Hash ist
    su.dbt_scd_id AS user_version_sk,   -- Oder einen Hash von -1 wenn es ein Hash ist
    dd.date_sk  AS order_date_sk, -- Das war schon korrekt

    -- Kennzahlen
    nol.add_to_cart_order,

    -- Metadaten (optional, aber nützlich)
    oi.order_timestamp,
    nol.staging_load_timestamp

FROM new_order_lines nol
INNER JOIN orders_info oi ON nol.order_id = oi.order_id

-- Join mit Produkt-Snapshot zum Zeitpunkt der Bestellung
LEFT JOIN snapshot_products sp
    ON nol.product_id = sp.product_id
    -- Vergleiche TIMESTAMP mit TIMESTAMP
    AND oi.order_timestamp_ts >= sp.dbt_valid_from  -- sp.dbt_valid_from ist TIMESTAMP
    AND oi.order_timestamp_ts < COALESCE(
                                    sp.dbt_valid_to, -- ist TIMESTAMP
                                    to_timestamp(4113387935) -- Fallback ist jetzt auch TIMESTAMP (entspricht 4113387935000000 / 1000000)
                                )

-- Join mit User-Snapshot zum Zeitpunkt der Bestellung
LEFT JOIN snapshot_users su
    ON oi.user_id = su.user_id
    -- Vergleiche TIMESTAMP mit TIMESTAMP
    AND oi.order_timestamp_ts >= su.dbt_valid_from -- su.dbt_valid_from ist TIMESTAMP
    AND oi.order_timestamp_ts < COALESCE(
                                    su.dbt_valid_to, -- ist TIMESTAMP
                                    to_timestamp(4113387935) -- Fallback ist jetzt auch TIMESTAMP
                                )

-- Join mit Datum
LEFT JOIN dim_date dd ON CAST(strftime(to_timestamp(oi.order_timestamp / 1000000), '%Y%m%d') AS INTEGER) = dd.date_sk
