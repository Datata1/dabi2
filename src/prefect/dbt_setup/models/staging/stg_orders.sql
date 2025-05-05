-- models/staging/stg_orders.sql
-- Staging-Modell für Order-CDC-Events

SELECT
    -- Spalten aus dem CDC-Payload (Namen wie in der DB / Parquet-Datei)
    order_id::BIGINT,
    user_id::BIGINT,
    CAST(order_date AS TIMESTAMP) AS order_timestamp, -- order_date ist der ursprüngliche Spaltenname
    tip_given::BOOLEAN AS tip_given, -- Tip-Info ist hier

    -- CDC Metadaten übernehmen und ggf. umbenennen
    "__op" AS op_type, -- Zugriff auf Spalten mit Sonderzeichen in ""
    "__source_ts_ms" AS source_timestamp_ms,
    load_ts AS staging_load_timestamp -- Vom Prefect Task hinzugefügt

FROM {{ source('cdc_staging', 'orders') }} 