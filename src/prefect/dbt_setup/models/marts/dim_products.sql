-- models/marts/dim_products.sql
{{
    config(
        materialized='table'
    )
}}

SELECT DISTINCT -- Stellt sicher, dass jede Produkt-Kombination nur einmal vorkommt
    product_id,
    product_name,
    aisle_id,
    aisle,
    department_id,
    department,
    -- Surrogate Key generieren
    {{ dbt_utils.generate_surrogate_key(['product_id']) }} AS product_sk
FROM {{ ref('stg_order_products') }}
-- Wichtig: Sicherstellen, dass die Quelle hier wirklich eindeutige Kombis pro product_id liefert
-- Ggf. GROUP BY product_id und Aggregation für Namen etc. verwenden, falls nötig.
-- Für diese Daten scheint DISTINCT zu reichen.