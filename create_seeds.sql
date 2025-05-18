-- RAW AISLES
SELECT
    aisle_id,
    aisle,
    'c' AS _op,
    0::BIGINT AS _ts_ms, 
    '1970-01-01 00:00:00.000 +0000'::timestamptz AS load_ts 
FROM
    aisles;

-- RAW DEPARTMENTS
SELECT
    department_id,
    department,
    'c' AS _op,
    0::BIGINT AS _ts_ms,
    '1970-01-01 00:00:00.000 +0000'::timestamptz AS load_ts
FROM
    departments;

-- RAW PRODUCTS
SELECT
    product_id,
    product_name,
    aisles_id,
    department_id,
    'c' AS _op,
    0::BIGINT AS _ts_ms,
    '1970-01-01 00:00:00.000 +0000'::timestamptz AS load_ts
FROM
    products;

-- RAW USERS
SELECT
    user_id,
    'c' AS _op,
    0::BIGINT AS _ts_ms,
    '1970-01-01 00:00:00.000 +0000'::timestamptz AS load_ts
FROM
    users;