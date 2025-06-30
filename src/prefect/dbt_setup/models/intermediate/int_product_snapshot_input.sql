{{
    config(
        materialized='table'
    )
}}

WITH stg_products AS (
    -- Simuliere stg_products, da wir es nicht direkt haben
    SELECT * FROM {{ ref('stg_products') }}
),

latest_product_details AS (
    SELECT
        product_id,
        product_name,
        fromUnixTimestamp64Milli(product_source_timestamp_ms * 1000) AS latest_product_ts,         
        ROW_NUMBER() OVER (PARTITION BY product_id ORDER BY product_source_timestamp_ms DESC) as rn
    FROM stg_products
    WHERE product_source_timestamp_ms IS NOT NULL -- Stelle sicher, dass ein Timestamp existiert
),

latest_aisle_details AS (
    SELECT
        product_id, -- product_id als Join-Schlüssel
        aisle_id,
        aisle_name,
        fromUnixTimestamp64Milli(aisle_source_timestamp_ms * 1000) AS latest_aisle_ts,
        ROW_NUMBER() OVER (PARTITION BY product_id ORDER BY aisle_source_timestamp_ms DESC) as rn
    FROM stg_products
    WHERE aisle_source_timestamp_ms IS NOT NULL
),

latest_department_details AS (
    SELECT
        product_id, -- product_id als Join-Schlüssel
        department_id,
        department_name,
        fromUnixTimestamp64Milli(department_source_timestamp_ms * 1000) AS latest_department_ts,
        ROW_NUMBER() OVER (PARTITION BY product_id ORDER BY department_source_timestamp_ms DESC) as rn
    FROM stg_products
    WHERE department_source_timestamp_ms IS NOT NULL
)

SELECT
    lp.product_id AS product_id,
    lp.product_name AS product_name,
    la.aisle_id AS aisle_id, 
    la.aisle_name AS aisle_name,
    ld.department_id AS department_id, 
    ld.department_name AS department_name,
    -- Der effektive letzte Update-Zeitstempel ist hier das Maximum der individuellen aktuellsten Zeitstempel
    GREATEST(
        COALESCE(lp.latest_product_ts, 0),
        COALESCE(la.latest_aisle_ts, 0),
        COALESCE(ld.latest_department_ts, 0)
    ) AS effective_last_updated_ts
FROM
    (SELECT * FROM latest_product_details WHERE rn = 1) AS lp
LEFT JOIN
    (SELECT * FROM latest_aisle_details WHERE rn = 1) AS la ON lp.product_id = la.product_id
LEFT JOIN
    (SELECT * FROM latest_department_details WHERE rn = 1) AS ld ON lp.product_id = ld.product_id

-- OPTIONAL: Filtere auch hier nach product_op_type wenn du z.B. gelöschte Produkte ausschließen willst
-- WHERE lp.product_op_type <> 'd' -- Hängt davon ab, wo product_op_type am besten zu finden ist in diesem Modell