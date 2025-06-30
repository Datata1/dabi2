def get_staging_table_schema(table_name: str) -> str:
    """Gibt das CREATE TABLE DDL für eine spezifische Staging-Tabelle zurück."""
    base_columns = [
        "_op String",
        "_ts_ms DateTime64(3)",  
        "load_ts DateTime",
    ]
    
    specific_columns = []
    order_by_clause = "ORDER BY tuple()" # Standard-Fallback

    # Füge spezifische Spalten für jede Tabelle hinzu und definiere den ORDER BY Key
    if table_name == "aisles":
        specific_columns = [
            "aisle_id Int64",
            "aisle String",
        ]
        order_by_clause = "ORDER BY aisle_id"
    elif table_name == "products":
        specific_columns = [
            "product_id Int64",
            "product_name String",
            "aisle_id Nullable(Int64)", 
            "department_id Nullable(Int64)", 
        ]
        order_by_clause = "ORDER BY product_id"
    elif table_name == "departments":
        specific_columns = [
            "department_id Int64",
            "department String",
        ]
        order_by_clause = "ORDER BY department_id"
    elif table_name == "users":
        specific_columns = [
            "user_id Int64",
        ]
        order_by_clause = "ORDER BY user_id"
    elif table_name == "orders":
        specific_columns = [
            "order_id Int64",
            "user_id Int64",
            "order_date Int64", # Behalte Int64 bei, da es ein Timestamp sein könnte
            "tip_given Nullable(Boolean)",
        ]
        order_by_clause = "ORDER BY order_id"
    elif table_name == "order_products":
        specific_columns = [
            "order_id Int64",
            "product_id Int64",
            "add_to_cart_order Int64",
        ]
        order_by_clause = "ORDER BY (order_id, product_id)" # Komposit-Schlüssel
    else:
        raise ValueError(f"Unbekannter Tabellenname '{table_name}' für Schema-Definition.")

    all_columns = specific_columns + base_columns
    columns_ddl = ",\n".join(all_columns)

    return f"""
    CREATE TABLE IF NOT EXISTS default_raw_seeds.stg_raw_{table_name} (
        {columns_ddl}
    )
    ENGINE = MergeTree()
    {order_by_clause};
    """