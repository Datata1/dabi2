-- models/intermediate/vw_product_snapshot_input.sql
SELECT
    p_attr.product_id,
    p_attr.product_name,
    p_attr.aisle_id,
    aisle.aisle_name, 
    p_attr.department_id,
    dept.department_name, 
    to_timestamp(
        GREATEST(
            p_attr.last_changed_ts,
            aisle.last_changed_ts,
            dept.last_changed_ts
        ) / 1000
    ) AS effective_last_updated_ts
FROM {{ ref('int_current_product_attributes') }} p_attr
LEFT JOIN {{ ref('int_current_aisle_names') }} aisle ON p_attr.aisle_id = aisle.aisle_id
LEFT JOIN {{ ref('int_current_department_names') }} dept ON p_attr.department_id = dept.department_id
