CREATE DATABASE prefect;

CREATE TABLE IF NOT EXISTS users (
    user_id BIGINT PRIMARY KEY NOT NULL
);

CREATE TABLE IF NOT EXISTS departments (
    department_id INTEGER PRIMARY KEY NOT NULL,
    department TEXT 
);

CREATE TABLE IF NOT EXISTS aisles (
    aisle_id INTEGER PRIMARY KEY NOT NULL,
    aisle TEXT
);

CREATE TABLE IF NOT EXISTS products (
    product_id BIGINT PRIMARY KEY NOT NULL,
    product_name TEXT,
    aisle_id INTEGER,
    department_id INTEGER,

    CONSTRAINT fk_products_aisle FOREIGN KEY (aisle_id) REFERENCES aisles(aisle_id),
    CONSTRAINT fk_products_department FOREIGN KEY (department_id) REFERENCES departments(department_id)
);

CREATE INDEX IF NOT EXISTS ix_products_aisle ON products (aisle_id);
CREATE INDEX IF NOT EXISTS ix_products_department ON products (department_id);


CREATE TABLE IF NOT EXISTS orders (
    order_id BIGINT PRIMARY KEY NOT NULL,
    user_id BIGINT NOT NULL,
    order_date TIMESTAMP NOT NULL,
    tip_given BOOLEAN NULL, 
    CONSTRAINT fk_orders_user FOREIGN KEY (user_id) REFERENCES users(user_id)
);

CREATE INDEX IF NOT EXISTS ix_orders_user_timestamp ON orders (user_id, order_date);

CREATE TABLE IF NOT EXISTS order_products (
    order_id BIGINT NOT NULL,
    product_id BIGINT NOT NULL,
    add_to_cart_order INTEGER,
    CONSTRAINT pk_order_products PRIMARY KEY (order_id, product_id),
    CONSTRAINT fk_order_products_order FOREIGN KEY (order_id) REFERENCES orders(order_id),
    CONSTRAINT fk_order_products_product FOREIGN KEY (product_id) REFERENCES products(product_id)
);

CREATE INDEX IF NOT EXISTS ix_order_products_order ON order_products (order_id);
CREATE INDEX IF NOT EXISTS ix_order_products_product ON order_products (product_id);

