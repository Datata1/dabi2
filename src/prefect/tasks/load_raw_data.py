# src/prefect/tasks/load_raw_data.py (Neue Version)
import pandas as pd
# import duckdb # Entfernen
from sqlalchemy import create_engine, inspect # Importieren
import os # Für Umgebungsvariablen
from prefect import task, get_run_logger
from pathlib import Path
import time

# --- Paths --- (Bleiben gleich)
APP_DIR = Path("/app")
DATA_DIR = APP_DIR / "data"
ORDERS_PATH = DATA_DIR / "orders.parquet"
TIPS_PATH = DATA_DIR / "tips_public.csv"
ORDER_PRODUCTS_PATH = DATA_DIR / "order_products_denormalized.csv"

# --- DB Connection Details from Environment Variables ---
PG_HOST = os.getenv("PG_DWH_HOST", "dwh-db")
PG_PORT = os.getenv("PG_DWH_PORT", "5432")
PG_DBNAME = os.getenv("PG_DWH_DBNAME", "dwh_dabi")
PG_USER = os.getenv("PG_DWH_USER", "dwh_user")
PG_PASSWORD = os.getenv("PG_DWH_PASSWORD") # Sollte gesetzt sein!

# Zielschema in PostgreSQL (z.B. 'public')
TARGET_SCHEMA = "public"

@task(name="Load Raw Data to PostgreSQL")
def load_raw_data(
    orders_path: Path = ORDERS_PATH,
    tips_path: Path = TIPS_PATH,
    order_products_path: Path = ORDER_PRODUCTS_PATH,
):
    """
    Loads raw data from parquet/csv files into PostgreSQL tables using SQLAlchemy.
    """
    logger = get_run_logger()
    logger.info(f"Starting raw data load to PostgreSQL...")

    if not PG_PASSWORD:
        logger.error("PostgreSQL Password (PG_DWH_PASSWORD) not set in environment!")
        raise ValueError("PostgreSQL Password (PG_DWH_PASSWORD) not set")

    # --- Create SQLAlchemy Engine ---
    # Verwende psycopg2 als Treiber (stelle sicher, dass psycopg2-binary installiert ist)
    db_url = f"postgresql+psycopg2://{PG_USER}:{PG_PASSWORD}@{PG_HOST}:{PG_PORT}/{PG_DBNAME}"
    engine = None
    t_start = time.time()
    skip_load = False

    TARGET_SCHEMA = "public" 
    RAW_TABLES_TO_CHECK = ["raw_orders", "raw_tips", "raw_order_products"] 

    try:
        logger.info(f"Connecting to {PG_HOST}:{PG_PORT}/{PG_DBNAME} for table check...")
        engine = create_engine(db_url)
        with engine.connect() as connection: 
            logger.info("Connection successful. Inspecting schema...")
            inspector = inspect(engine) 
            tables_exist_flags = [inspector.has_table(tbl, schema=TARGET_SCHEMA) for tbl in RAW_TABLES_TO_CHECK]
            logger.info(f"Existence check for tables {RAW_TABLES_TO_CHECK} in schema '{TARGET_SCHEMA}': {list(zip(RAW_TABLES_TO_CHECK, tables_exist_flags))}")

            if all(tables_exist_flags):
                logger.info(f"All raw tables already exist. Skipping raw data load process.")
                skip_load = True
            else:
                missing_tables = [tbl for tbl, exists in zip(RAW_TABLES_TO_CHECK, tables_exist_flags) if not exists]
                logger.info(f"Raw tables missing: {missing_tables}. Proceeding with full raw data load.")

    except Exception as e:
        logger.error(f"Error during database connection or table inspection: {e}", exc_info=True)
        raise e
    finally:
        if engine:
            engine.dispose()
            logger.info("Check connection engine disposed.")

    # --- Schritt 2: Laden überspringen oder durchführen ---
    if skip_load:
        logger.info("Raw data load task finished (skipped).")
        return True 

    logger.info(f"Starting full raw data load process...")
    t_start = time.time()
    engine = None

    try:
        logger.info(f"Attempting to connect to PostgreSQL: {PG_HOST}:{PG_PORT}/{PG_DBNAME}")
        engine = create_engine(db_url)
        # Test connection
        with engine.connect() as connection:
            logger.info("PostgreSQL connection established.")

        logger.info(f"Loading orders from: {orders_path}")
        if not orders_path.is_file():
             logger.error(f"Orders file not found at {orders_path}!")
             raise FileNotFoundError(f"Orders file not found at {orders_path}!")
        orders_df = pd.read_parquet(orders_path)
        logger.info(f"Read {len(orders_df)} orders.")
        logger.info(f"Writing {len(orders_df)} rows to raw_orders in schema {TARGET_SCHEMA}...")
        
        orders_df.to_sql(
            name="raw_orders",
            con=engine,
            schema=TARGET_SCHEMA,
            if_exists="replace", # Entspricht DROP + CREATE/INSERT
            index=False,        # Keinen DataFrame-Index schreiben
            chunksize=10000     # Optional: In Chunks schreiben
        )
        logger.info(f"Loaded raw_orders")

        # --- Load Tips ---
        logger.info(f"Loading tips from: {tips_path}")
        if not tips_path.is_file():
             logger.error(f"Tips file not found at {tips_path}!")
             raise FileNotFoundError(f"Tips file not found at {tips_path}!")
        tips_df = pd.read_csv(tips_path)
        logger.info(f"Read {len(tips_df)} tips records.")
        tips_df.columns = [col.strip().lower().replace(' ', '_') for col in tips_df.columns]
        # Umbenennen der problematischen Spalte 'unnamed:_0' (falls sie existiert)
        if 'unnamed:_0' in tips_df.columns:
             tips_df = tips_df.rename(columns={'unnamed:_0': 'csv_index'}) # Beispiel
        logger.info(f"Writing {len(tips_df)} rows to raw_tips in schema {TARGET_SCHEMA}...")
        tips_df.to_sql(
            name="raw_tips",
            con=engine,
            schema=TARGET_SCHEMA,
            if_exists="replace",
            index=False,
            chunksize=10000
        )
        logger.info(f"Loaded raw_tips")

        # --- Load Order Products ---
        logger.info(f"Loading order products from: {order_products_path}")
        if not order_products_path.is_file():
             logger.error(f"Order products file not found at {order_products_path}!")
             raise FileNotFoundError(f"Order products file not found at {order_products_path}!")
        # Lese CSV in Chunks, um Speicher zu sparen (optional aber empfohlen)
        chunk_iter = pd.read_csv(order_products_path, chunksize=200000)
        logger.info(f"Reading and writing order products in chunks...")
        first_chunk = True
        total_rows = 0
        for chunk_df in chunk_iter:
             logger.info(f"Read {len(chunk_df)} order products chunk.")
             total_rows += len(chunk_df)
             chunk_df.columns = [col.strip().lower().replace(' ', '_') for col in chunk_df.columns]
             if 'unnamed:_0' in chunk_df.columns:
                  chunk_df = chunk_df.rename(columns={'unnamed:_0': 'csv_index'})

             write_mode = "replace" if first_chunk else "append"
             chunk_df.to_sql(
                 name="raw_order_products",
                 con=engine,
                 schema=TARGET_SCHEMA,
                 if_exists=write_mode,
                 index=False
             )
             first_chunk = False
             logger.info(f"Written chunk to raw_order_products (Mode: {write_mode}). Total rows so far: {total_rows}")

        logger.info(f"Loaded raw_order_products ({total_rows} total rows)")

        logger.info("Successfully loaded all raw tables to PostgreSQL.")

    except Exception as e:
        logger.error(f"Error during raw data loading to PostgreSQL: {e}", exc_info=True)
        raise e
    finally:
        if engine:
            engine.dispose() # Wichtig: Engine schließen/Ressourcen freigeben
            logger.info("SQLAlchemy engine disposed.")

    t_end = time.time()
    logger.info(f"Raw data loading to PostgreSQL finished in {t_end - t_start:.2f} seconds.")
    return True