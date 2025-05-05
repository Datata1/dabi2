-- models/intermediate/int_departments__latest_active.sql
{{
    config(
        materialized='ephemeral',
    )
}}

-- Wählt den aktuellsten, aktiven Namen für jede department_id
-- basierend auf den neuesten CDC-Events im Staging-Layer.
WITH ranked_stg_departments AS (
    SELECT
        department_id,
        department, -- Der Name
        op_type,
        ROW_NUMBER() OVER (
            PARTITION BY department_id
            ORDER BY source_timestamp_ms DESC, staging_load_timestamp DESC
        ) as rn
    FROM {{ ref('stg_departments') }} -- Annahme: Dieses Staging-Modell existiert
)
SELECT
    department_id,
    department
FROM ranked_stg_departments
WHERE rn = 1 AND op_type != 'd' -- Nur der letzte Stand und nicht gelöscht