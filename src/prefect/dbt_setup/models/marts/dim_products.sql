-- models/marts/dim_products.sql (Liest aus Snapshot)
{{
    config(
        materialized='table'
    )
}}

SELECT
    -- Surrogate Key generieren (basiert auf dem Natural Key)
    {{ dbt_utils.generate_surrogate_key(['product_id']) }} AS product_sk,

    -- Spalten aus dem Snapshot übernehmen
    product_id,
    product_name,
    aisle_id,
    aisle,
    department_id,
    department,

    -- Gültigkeitsspalten aus dem Snapshot übernehmen und umbenennen
    dbt_valid_from AS valid_from,
    dbt_valid_to AS valid_to,

    -- Aktuell-Flag ableiten
    (dbt_valid_to IS NULL) AS is_current

FROM {{ ref('scd_products') }} 

WHERE dbt_valid_to IS NULL