-- models/staging/stg_order_tips.sql (NEU oder Umbenennung von stg_tips.sql)
-- Extrahiert Tip-Informationen direkt aus den Order-Events

SELECT
    -- Benötigte Schlüssel und Daten
    order_id::BIGINT,
    tip_given::BOOLEAN, -- Oder ::INTEGER für 1/0 wie vorher

    -- CDC Metadaten (wichtig, um den richtigen Stand der Info zu haben)
    "__op" AS op_type,
    "__source_ts_ms" AS source_timestamp_ms,
    load_ts AS staging_load_timestamp

FROM {{ source('cdc_staging', 'orders') }} -- Quelle sind die Order-Events!

-- Optional: Nur Zeilen berücksichtigen, wo sich tip_given geändert hat oder die neu sind?
-- WHERE op_type = 'c' OR (op_type = 'u' AND ...) -- Wird komplex, einfacher in Marts zu handhaben