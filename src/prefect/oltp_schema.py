from sqlalchemy import (
    MetaData, Table, Column, Integer, BigInteger, String, DateTime,
    Boolean, ForeignKey, UniqueConstraint, Index, TEXT
)

oltp_metadata = MetaData(schema="public") 

# Tabellen-Definitionen (wie im vorherigen Post gezeigt)
users_table = Table(
    "users", oltp_metadata,
    Column("user_id", BigInteger, primary_key=True, nullable=False),
)

departments_table = Table(
    "departments", oltp_metadata,
    Column("department_id", Integer, primary_key=True, nullable=False),
    Column("department", TEXT),
)

aisles_table = Table(
    "aisles", oltp_metadata,
    Column("aisle_id", Integer, primary_key=True, nullable=False),
    Column("aisle", TEXT),
)

products_table = Table(
    "products", oltp_metadata,
    Column("product_id", BigInteger, primary_key=True, nullable=False),
    Column("product_name", TEXT),
    Column("aisle_id", Integer, ForeignKey("aisles.aisle_id")),
    Column("department_id", Integer, ForeignKey("departments.department_id")),
    Index("ix_products_aisle", "aisle_id"),
    Index("ix_products_department", "department_id"),
)

orders_table = Table(
    "orders", oltp_metadata,
    Column("order_id", BigInteger, primary_key=True, nullable=False),
    Column("user_id", BigInteger, ForeignKey("users.user_id"), nullable=False),
    Column("order_date", DateTime, nullable=False),
    Column("tip_given", Boolean, nullable=True),
    Index("ix_orders_user_timestamp", "user_id", "order_date"),
)

order_products_table = Table(
    "order_products", oltp_metadata,
    Column("order_id", BigInteger, ForeignKey("orders.order_id"), primary_key=True),
    Column("product_id", BigInteger, ForeignKey("products.product_id"), primary_key=True),
    Column("add_to_cart_order", Integer),
)
