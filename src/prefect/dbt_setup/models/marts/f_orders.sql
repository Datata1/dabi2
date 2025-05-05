-- models/marts/f_orders.sql (Angepasst für CDC und Snapshots)
{{
    config(
        materialized='incremental',
        unique_key='order_id'
    )
}}

WITH latest_orders AS (
    -- Wähle den letzten Stand jeder Bestellung aus dem aktuellen Batch
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY order_id
            ORDER BY source_timestamp_ms DESC, staging_load_timestamp DESC
        ) as rn
    FROM {{ ref('stg_orders') }} -- Quelle ist das neue Staging-Modell

    {% if is_incremental() %}
      WHERE staging_load_timestamp > (SELECT max(staging_load_timestamp) FROM {{ this }})
    {% endif %}
),

-- Referenziere die Snapshot-Tabellen!
snapshot_users AS (
    SELECT user_id, source_timestamp_ms, dbt_valid_from, dbt_valid_to
    FROM {{ ref('scd_dim_users') }} -- Annahme: Snapshot existiert
),

dim_date AS (
    SELECT date_sk, full_date FROM {{ ref('dim_date') }}
)

SELECT
    -- Schlüssel
    lo.order_id,

    -- Fremdschlüssel (Natural Key oder Surrogate Key aus Snapshot)
    lo.user_id AS dim_user_id, -- Natural Key
    -- COALESCE(su.user_sk, -1) AS user_sk, -- Surrogate Key aus Snapshot
    COALESCE(dd.date_sk, -1) AS order_date_sk,

    -- Kennzahlen/Attribute der Bestellung
    lo.tip_given, -- Direkt aus den Order-Daten

    -- Status basierend auf letzter Operation
    CASE WHEN lo.op_type = 'd' THEN FALSE ELSE TRUE END AS is_active_order,

    -- Metadaten
    lo.order_timestamp,
    lo.source_timestamp_ms,
    lo.staging_load_timestamp

FROM latest_orders lo

-- Join mit User-Snapshot zum Zeitpunkt der Bestellung
LEFT JOIN snapshot_users su
    ON lo.user_id = su.user_id
    AND lo.order_timestamp >= su.dbt_valid_from
    AND lo.order_timestamp < COALESCE(su.dbt_valid_to, '9999-12-31 23:59:59'::timestamp)

-- Join mit Datum
LEFT JOIN dim_date dd ON lo.order_timestamp::date = dd.full_date

WHERE lo.rn = 1 -- Nur den aktuellsten Stand aus dem Batch verarbeiten

-- Die MERGE-Logik von dbt (basierend auf unique_key='order_id') wird:
-- 1. Neue Orders einfügen.
-- 2. Bestehende Orders aktualisieren (z.B. wenn tip_given sich ändert oder is_active).