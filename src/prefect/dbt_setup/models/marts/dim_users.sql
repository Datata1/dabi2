-- models/marts/dim_users.sql (Angepasst für CDC und Inkrementalität)
{{
    config(
        materialized='incremental',
        unique_key='user_id'
    )
}}

-- Optional: CTE, um den neuesten Stand pro User im Batch zu bekommen
-- (falls kein separates Intermediate-Modell verwendet wird)
WITH latest_user_updates AS (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY user_id
            ORDER BY source_timestamp_ms DESC, staging_load_timestamp DESC
        ) as rn
    FROM {{ ref('stg_users') }} -- Quelle ist das neue Staging-Modell

    {% if is_incremental() %}
      -- Verarbeite nur Staging-Daten, die neuer sind als die letzte Ladung in DIESE Tabelle
      -- Wichtig: Benötigt staging_load_timestamp im SELECT unten
      WHERE staging_load_timestamp > (SELECT max(staging_load_timestamp) FROM {{ this }})
    {% endif %}
)

SELECT
    -- user_id ist der unique_key
    lu.user_id,
    -- Surrogate Key kann hier oder später generiert werden, falls benötigt
    -- {{ dbt_utils.generate_surrogate_key(['lu.user_id']) }} AS user_sk,

    -- Logisches Delete-Flag basierend auf dem letzten op_type im Batch
    CASE WHEN lu.op_type = 'd' THEN FALSE ELSE TRUE END AS is_active,

    -- Metadaten für Nachverfolgung und Snapshots
    lu.source_timestamp_ms,
    lu.staging_load_timestamp

FROM latest_user_updates lu
WHERE lu.rn = 1 -- Nur den aktuellsten Stand aus dem Batch verarbeiten

-- Die MERGE-Logik von dbt (basierend auf unique_key) wird:
-- 1. Neue user_id's einfügen (mit is_active=true).
-- 2. Bestehende user_id's aktualisieren (setzt is_active auf true/false, aktualisiert Zeitstempel).