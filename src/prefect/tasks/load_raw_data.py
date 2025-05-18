import pandas as pd
import duckdb 
import os
from prefect import task, get_run_logger
from pathlib import Path
import time

APP_DIR = Path("/app")
DATA_DIR = APP_DIR / "data"
DBT_SETUP_DIR = APP_DIR / "dbt_setup"
ORDERS_PATH = DATA_DIR / "orders.parquet"
TIPS_PATH = DATA_DIR / "tips_public.csv"
ORDER_PRODUCTS_PATH = DATA_DIR / "order_products_denormalized.csv"
DUCKDB_PATH = DBT_SETUP_DIR / "dev.duckdb"

TARGET_SCHEMA = "main" 
RAW_TABLES_TO_CHECK = ["raw_orders", "raw_tips", "raw_order_products"]

@task(name="Load Raw Data to DuckDB (Conditional)")
def load_raw_data(
    orders_path: Path = ORDERS_PATH,
    tips_path: Path = TIPS_PATH,
    order_products_path: Path = ORDER_PRODUCTS_PATH,
    db_path: Path = DUCKDB_PATH,
):
    """
    Prüft, ob Raw-Tabellen existieren. Lädt Rohdaten nur, wenn eine oder mehrere fehlen.
    Verwendet DuckDB Python Client.
    """
    logger = get_run_logger()
    logger.info(f"Checking if raw data load to DuckDB is needed...")

    con = None
    skip_load = False
    try:
        logger.info(f"Connecting to DuckDB at {db_path} for table check...")
        # Sicherstellen, dass das Verzeichnis existiert
        db_path.parent.mkdir(parents=True, exist_ok=True)
        con = duckdb.connect(database=str(db_path.resolve()), read_only=False)
        logger.info("Connection successful. Checking tables...")

        query = f"""
        SELECT table_name
        FROM information_schema.tables
        WHERE table_schema = '{TARGET_SCHEMA}'
        AND table_name IN ({str(RAW_TABLES_TO_CHECK)[1:-1]})
        """
        existing_tables = set(con.sql(query).df()['table_name'])
        logger.info(f"Found existing tables in schema '{TARGET_SCHEMA}': {existing_tables}")

        if set(RAW_TABLES_TO_CHECK).issubset(existing_tables):
             logger.info(f"All raw tables already exist. Skipping raw data load process.")
             skip_load = True
        else:
            missing_tables = set(RAW_TABLES_TO_CHECK) - existing_tables
            logger.info(f"Raw tables missing: {missing_tables}. Proceeding with full raw data load.")

    except Exception as e:
        logger.error(f"Error during database connection or table inspection: {e}", exc_info=True)
        raise e 
    finally:
        if con:
            con.close()
            logger.info("Check connection closed.")

    if skip_load:
        logger.info("Raw data load task finished (skipped).")
        return True

    logger.info(f"Starting full raw data load process to DuckDB: {db_path}")
    t_start = time.time()
    con = None
    try:
        con = duckdb.connect(database=str(db_path.resolve()), read_only=False)
        logger.info("DuckDB connection for loading established.")

        logger.info(f"Loading orders from: {orders_path}")
        if not orders_path.is_file(): raise FileNotFoundError(f"File not found: {orders_path}")
        orders_df = pd.read_parquet(orders_path)
        logger.info(f"Read {len(orders_df)} orders. Writing to 'raw_orders' (replace)...")
        con.sql(f"DROP TABLE IF EXISTS {TARGET_SCHEMA}.raw_orders;")
        con.sql(f"CREATE TABLE {TARGET_SCHEMA}.raw_orders AS SELECT * FROM orders_df;")
        count = con.sql(f'SELECT COUNT(*) FROM {TARGET_SCHEMA}.raw_orders').fetchone()[0]
        logger.info(f"Loaded {count} rows into raw_orders")

        logger.info(f"Loading tips from: {tips_path}")
        if not tips_path.is_file(): raise FileNotFoundError(f"File not found: {tips_path}")
        tips_df = pd.read_csv(tips_path)
        tips_df.columns = [col.strip().lower().replace(' ', '_') for col in tips_df.columns]
        if 'unnamed:_0' in tips_df.columns: tips_df = tips_df.rename(columns={'unnamed:_0': 'csv_index'})
        logger.info(f"Read {len(tips_df)} tips. Writing to 'raw_tips' (replace)...")
        con.sql(f"DROP TABLE IF EXISTS {TARGET_SCHEMA}.raw_tips;")
        con.sql(f"CREATE TABLE {TARGET_SCHEMA}.raw_tips AS SELECT * FROM tips_df;")
        count = con.sql(f'SELECT COUNT(*) FROM {TARGET_SCHEMA}.raw_tips').fetchone()[0]
        logger.info(f"Loaded {count} rows into raw_tips")

        logger.info(f"Loading order products from: {order_products_path}")
        if not order_products_path.is_file(): raise FileNotFoundError(f"File not found: {order_products_path}")
        logger.info(f"Reading and writing order products (replace)...")
        con.sql(f"DROP TABLE IF EXISTS {TARGET_SCHEMA}.raw_order_products;")
        con.sql(f"""
            CREATE TABLE {TARGET_SCHEMA}.raw_order_products AS
            SELECT * FROM read_csv_auto('{str(order_products_path.resolve())}', header=True, normalize_names=True);
        """)
        count = con.sql(f'SELECT COUNT(*) FROM {TARGET_SCHEMA}.raw_order_products').fetchone()[0]
        logger.info(f"Loaded {count} rows into raw_order_products using read_csv_auto.")

        logger.info("Successfully loaded all raw tables to DuckDB.")

    except Exception as e:
        logger.error(f"Error during raw data loading execution to DuckDB: {e}", exc_info=True)
        raise e
    finally:
        if con:
            con.close()
            logger.info("Load connection closed.")

    t_end = time.time()
    logger.info(f"Raw data loading process to DuckDB finished in {t_end - t_start:.2f} seconds.")
    return True