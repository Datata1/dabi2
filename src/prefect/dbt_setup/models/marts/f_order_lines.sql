-- models/marts/f_order_lines.sql (Angepasst für CDC und Snapshots)
{{
    config(
        materialized='incremental',
        unique_key=['order_id', 'product_id'], 
    )
}}

WITH new_order_lines AS (
    -- Wähle nur neue Einträge aus dem Staging-Modell
    SELECT *
    FROM {{ ref('stg_order_products') }}
    WHERE op_type = 'c' -- Nur Inserts für Order Lines relevant? Annahme hier.

    {% if is_incremental() %}
      -- Verarbeite nur Staging-Daten, die neuer sind als die letzte Ladung in DIESE Tabelle
      AND staging_load_timestamp > (SELECT max(staging_load_timestamp) FROM {{ this }})
    {% endif %}
),

orders_info AS (
    -- Holen Zeitstempel und User ID von der Bestellung
    SELECT order_id, user_id, order_timestamp
    FROM {{ ref('stg_orders') }} -- Annahme: order_timestamp ist hier aktuell
    -- Ggf. auch hier nur neueste Version pro order_id aus dem Batch holen?
    QUALIFY ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY source_timestamp_ms DESC, staging_load_timestamp DESC) = 1
),

-- Referenziere die Snapshot-Tabellen!
snapshot_products AS (
    SELECT product_id, product_name, aisle, department, source_timestamp_ms, dbt_valid_from, dbt_valid_to
    FROM {{ ref('scd_products') }} -- Oder source('snapshots', 'scd_products')
),


snapshot_users AS (
    SELECT user_id, source_timestamp_ms, dbt_valid_from, dbt_valid_to
    FROM {{ ref('scd_dim_users') }} -- Annahme: Snapshot existiert
),

dim_date AS (
    SELECT date_sk, full_date FROM {{ ref('dim_date') }}
)

SELECT
    -- Schlüssel für die Faktentabelle (könnte auch generiert werden)
    nol.order_id,
    nol.product_id,

    -- Fremdschlüssel zu den Dimensionen (hier Natural Keys, SKs wären ähnlich)
    -- Join mit Snapshots zum Zeitpunkt der Bestellung!
    nol.product_id AS dim_product_id, -- Natural Key
    oi.user_id AS dim_user_id,       -- Natural Key
    COALESCE(dd.date_sk, -1) AS order_date_sk, -- Surrogate Key Datum

    -- Kennzahlen
    nol.add_to_cart_order,

    -- Metadaten (optional, aber nützlich)
    oi.order_timestamp,
    nol.staging_load_timestamp -- Wichtig für inkrementelle Logik oben

    -- Surrogate Keys aus Snapshots holen (optional)
    -- (Benötigt SKs im Snapshot SELECT)
    -- COALESCE(sp.product_sk, -1) AS product_sk,
    -- COALESCE(su.user_sk, -1) AS user_sk

FROM new_order_lines nol
INNER JOIN orders_info oi ON nol.order_id = oi.order_id

-- Join mit Produkt-Snapshot zum Zeitpunkt der Bestellung
LEFT JOIN snapshot_products sp
    ON nol.product_id = sp.product_id
    AND oi.order_timestamp >= sp.dbt_valid_from
    AND oi.order_timestamp < COALESCE(sp.dbt_valid_to, '9999-12-31 23:59:59'::timestamp)

-- Join mit User-Snapshot zum Zeitpunkt der Bestellung
LEFT JOIN snapshot_users su
    ON oi.user_id = su.user_id
    AND oi.order_timestamp >= su.dbt_valid_from
    AND oi.order_timestamp < COALESCE(su.dbt_valid_to, '9999-12-31 23:59:59'::timestamp)

-- Join mit Datum
LEFT JOIN dim_date dd ON oi.order_timestamp::date = dd.full_date

-- Die WHERE-Klausel im 'is_incremental()' Block oben filtert bereits nur neue Zeilen.
-- Die Standard-Strategie 'merge' (oder 'append' wenn kein unique_key definiert) fügt diese hinzu.