-- models/marts/dim_products.sql (Korrigiert für Star Schema Denormalisierung)
{{
    config(
        materialized='incremental',
        unique_key='product_id'
    )
}}

-- Optional: CTE für den letzten Stand der Produkte im Batch (falls kein int_products__latest_batch existiert)
WITH products_latest_batch AS (
     SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY product_id
            ORDER BY source_timestamp_ms DESC, staging_load_timestamp DESC
        ) as rn
    FROM {{ ref('stg_products') }} -- Quelle ist das Staging-Modell der Produkte

    {% if is_incremental() %}
      -- Verarbeite nur Staging-Daten, die neuer sind als die letzte Ladung in DIESE Zieltabelle
      WHERE staging_load_timestamp > (SELECT max(staging_load_timestamp) FROM {{ this }})
    {% endif %}
)

SELECT
    -- Schlüssel
    p.product_id,

    -- Attribute vom Produkt selbst
    p.product_name,

    -- Fremdschlüssel (können bleiben, falls nützlich)
    p.aisle_id,
    p.department_id,

    -- === DENORMALISIERTE SPALTEN ===
    -- Hole den Aisle-Namen aus dem Intermediate Model
    COALESCE(a.aisle, 'Unknown') AS aisle,
    -- Hole den Department-Namen aus dem Intermediate Model
    COALESCE(d.department, 'Unknown') AS department,

    -- Metadaten / SCD-Steuerung
    p.source_timestamp_ms,
    p.staging_load_timestamp,
    CASE WHEN p.op_type = 'd' THEN FALSE ELSE TRUE END AS is_active

FROM products_latest_batch p

-- Joine mit den neuesten aktiven Aisle-Infos (ephemeral)
LEFT JOIN {{ ref('int_aisles__latest_active') }} a ON p.aisle_id = a.aisle_id
-- Joine mit den neuesten aktiven Department-Infos (ephemeral)
LEFT JOIN {{ ref('int_departments__latest_active') }} d ON p.department_id = d.department_id

WHERE p.rn = 1 -- Verarbeite nur den letzten Stand jedes Produkts aus dem Batch

-- Die WHERE-Klausel im is_incremental()-Block oben sorgt dafür, dass nur
-- Daten aus neuen Batches betrachtet werden. Die Standard-MERGE-Logik von DBT
-- (basierend auf unique_key) kümmert sich dann um das Einfügen oder Aktualisieren
-- in der Zieltabelle `dim_products`.