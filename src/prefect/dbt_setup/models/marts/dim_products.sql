-- models/marts/dim_products.sql (Liest aus Snapshot)
{{
    config(
        materialized='table'
    )
}}

SELECT
    dbt_scd_id AS product_sk,
    product_id,
    product_name,
    aisle_id,
    aisle,
    department_id,
    department,
    dbt_valid_from AS valid_from,
    dbt_valid_to AS valid_to,
    (dbt_valid_to IS NULL) AS is_current

FROM {{ ref('scd_products') }} 
