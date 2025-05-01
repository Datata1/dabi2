SELECT
    user_id::BIGINT AS user_id, -- Beispiel Typkonvertierung
    order_id::BIGINT AS order_id,
    CAST(order_date AS TIMESTAMP) AS order_timestamp -- Sicherstellen, dass es ein Zeitstempel ist
    -- Füge weitere Spalten hinzu, falls vorhanden/nötig
FROM {{ source('raw_data', 'raw_orders') }}