-- models/intermediate/int_products__latest_batch.sql
{{
    config(
        materialized='ephemeral',
    )
}}

WITH ranked_stg_products AS (
    -- Wähle nur die neuesten Änderungen pro Produkt innerhalb des Staging-Batches
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY product_id -- Pro Produkt
            ORDER BY
                source_timestamp_ms DESC, -- Neueste Event-Zeit zuerst
                staging_load_timestamp DESC -- Bei gleicher Zeit, spätere Ladung zuerst
        ) as rn
    FROM {{ ref('stg_products') }} -- Nutzt das bereinigte Staging-Modell

    {% if is_incremental() %}
        -- Verarbeite nur Staging-Zeilen, die neuer sind als die letzte Verarbeitung dieses Modells
        -- Diese Bedingung ist nur sinnvoll, wenn dieses Modell selbst 'incremental' ist
        WHERE staging_load_timestamp > (SELECT max(staging_load_timestamp) FROM {{ this }})
    {% endif %}
)
-- Wähle alle Spalten des neuesten Eintrags (rn=1) pro Produkt aus
SELECT
    product_id,
    product_name,
    aisle_id,
    department_id,
    op_type,                 -- Wichtig für die Mart-Logik (war es ein Delete?)
    source_timestamp_ms,     -- Zeitstempel des Events
    staging_load_timestamp   -- Zeitstempel des Ladevorgangs
FROM ranked_stg_products
WHERE rn = 1