-- models/intermediate/int_aisles__latest_active.sql
{{
    config(
        materialized='ephemeral',
    )
}}

-- Wählt den aktuellsten, aktiven Namen für jede aisle_id
-- basierend auf den neuesten CDC-Events im Staging-Layer.
WITH ranked_stg_aisles AS (
    SELECT
        aisle_id,
        aisle, -- Der Name
        op_type,
        ROW_NUMBER() OVER (
            PARTITION BY aisle_id
            ORDER BY source_timestamp_ms DESC, staging_load_timestamp DESC
        ) as rn
    FROM {{ ref('stg_aisles') }} -- Annahme: Dieses Staging-Modell existiert
)
SELECT
    aisle_id,
    aisle
FROM ranked_stg_aisles
WHERE rn = 1 AND op_type != 'd' -- Nur der letzte Stand und nicht gelöscht