-- models/marts/f_orders.sql (Angepasst für CDC und Snapshots mit Surrogate Keys)
{{
    config(
        materialized='incremental',
        unique_key='order_id',
        tags=['marts_model'],
    )
}}

WITH latest_orders AS (
    -- Wähle den letzten Stand jeder Bestellung aus dem aktuellen Batch
    SELECT
        order_id,
        user_id,
        order_timestamp, -- Der originale BIGINT (Mikrosekunden)
        to_timestamp(order_timestamp / 1000000.0) AS order_timestamp_ts, -- Konvertiert zu TIMESTAMP
        tip_given,
        op_type,
        source_timestamp_ms,
        staging_load_timestamp,
        ROW_NUMBER() OVER (
            PARTITION BY order_id
            ORDER BY source_timestamp_ms DESC, staging_load_timestamp DESC
        ) as rn
    FROM {{ ref('stg_orders') }} -- Quelle ist das neue Staging-Modell

    {% if is_incremental() %}
      WHERE staging_load_timestamp > (SELECT max(staging_load_timestamp) FROM {{ this }})
    {% endif %}
),

-- Referenziere die Snapshot-Tabellen und wähle den Surrogate Key (dbt_scd_id) aus!
snapshot_users AS (
    SELECT
        dbt_scd_id, -- Surrogate Key für die User-Version
        user_id,    -- Natural Key für den Join
        dbt_valid_from, -- Ist TIMESTAMP WITH TIME ZONE
        dbt_valid_to,   -- Ist TIMESTAMP WITH TIME ZONE
        -- source_timestamp_ms (kann hier auch selektiert werden, falls benötigt)
    FROM {{ ref('dim_users') }} -- Annahme: Snapshot existiert
),

dim_date AS (
    SELECT date_sk, full_date FROM {{ ref('dim_date') }}
)

SELECT
    -- Schlüssel
    lo.order_id,

    -- Fremdschlüssel
    lo.user_id AS user_id,
    su.dbt_scd_id AS user_version_sk,
    COALESCE(dd.date_sk, -1) AS order_date_sk,

    lo.tip_given,

    CASE WHEN lo.op_type = 'd' THEN FALSE ELSE TRUE END AS is_active_order,

    lo.order_timestamp, -- Der originale BIGINT Wert
    lo.source_timestamp_ms,
    lo.staging_load_timestamp

FROM latest_orders lo

-- Join mit User-Snapshot zum Zeitpunkt der Bestellung
LEFT JOIN snapshot_users su
    ON lo.user_id = su.user_id
    AND lo.order_timestamp_ts >= su.dbt_valid_from 
    AND lo.order_timestamp_ts < COALESCE(
                                    su.dbt_valid_to, -- ist TIMESTAMP
                                    to_timestamp(4113387935) -- Fallback ist jetzt auch TIMESTAMP (entspricht 4113387935000000 Mikrosekunden / 1000000)
                                )

-- Join mit Datum (dieser Teil bleibt unverändert, da er den BIGINT lo.order_timestamp für die date_sk Berechnung benötigt)
LEFT JOIN dim_date dd ON CAST(strftime(to_timestamp(lo.order_timestamp / 1000000.0), '%Y%m%d') AS INTEGER) = dd.date_sk

WHERE lo.rn = 1 -- Nur den aktuellsten Stand aus dem Batch verarbeiten