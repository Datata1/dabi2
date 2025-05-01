SELECT
    order_id::BIGINT AS order_id,
    product_id::BIGINT AS product_id,
    product_name,
    add_to_cart_order::INTEGER AS add_to_cart_order,
    aisle_id::INTEGER AS aisle_id,
    aisle,
    department_id::INTEGER AS department_id,
    department
FROM {{ source('raw_data', 'raw_order_products') }}