SELECT
    p.product_id,
    p.product_name,
    p.aisle_id,
    a.aisle AS aisle_name,
    p.department_id,
    d.department AS department_name,
    '1900-01-01 00:00:00'::TIMESTAMP AS historical_effective_from
FROM
    products p  
LEFT JOIN
    aisles a ON p.aisle_id = a.aisle_id
LEFT JOIN
    departments d ON p.department_id = d.department_id
ORDER BY
    p.product_id
;

SELECT
    u.user_id,
    '1900-01-01 00:00:00'::TIMESTAMP AS historical_effective_from

FROM
    users u 
ORDER BY
    u.user_id
;