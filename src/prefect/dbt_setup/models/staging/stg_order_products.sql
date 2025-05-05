-- models/staging/stg_order_products.sql
-- Staging-Modell für Order-Products-CDC-Events

SELECT
    -- Spalten aus dem CDC-Payload
    order_id::BIGINT,
    product_id::BIGINT,
    add_to_cart_order::INTEGER,
    -- Produkt-, Aisle-, Department-Namen sind hier NICHT MEHR drin!
    -- Diese kommen aus den jeweiligen Dimensions-Tabellen und werden später gejoined.

    -- CDC Metadaten
    "__op" AS op_type,
    "__source_ts_ms" AS source_timestamp_ms,
    load_ts AS staging_load_timestamp

FROM {{ source('cdc_staging', 'order_products') }}