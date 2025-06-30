{{
    config(
        materialized='table'
    )
}}

WITH source_data AS (
    -- 1. Lade alle Events aus deinem Staging-Modell
    SELECT
        product_id,
        product_name,
        aisle_id,
        aisle_name,
        department_id,
        department_name,
        product_op_type,
        -- Der Zeitstempel der letzten Änderung für dieses Event
        GREATEST(
            COALESCE(product_source_timestamp_ms, 0),
            COALESCE(aisle_source_timestamp_ms, 0),
            COALESCE(department_source_timestamp_ms, 0)
        ) AS effective_source_timestamp_ms
    FROM
        {{ ref('stg_products') }}
),

latest_events AS (
    -- 2. Finde für jede product_id den absolut letzten Event (egal ob 'c', 'u' oder 'd')
    SELECT
        *,
        ROW_NUMBER() OVER (PARTITION BY product_id ORDER BY effective_source_timestamp_ms DESC) as rn
    FROM
        source_data
)

-- 3. Finale, saubere Liste der AKTIVEN Produkte
SELECT
    product_id,
    product_name,
    aisle_id,
    aisle_name,
    department_id,
    department_name,
    -- Diese Spalte ist für die 'timestamp'-Strategie im Snapshot essenziell
    fromUnixTimestamp64Milli(effective_source_timestamp_ms * 1000) AS effective_last_updated_ts
    
FROM
    latest_events
WHERE
    rn = 1  -- Nimm nur den allerletzten Zustand