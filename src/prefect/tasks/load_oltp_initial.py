# src/prefect/tasks/oltp_load_tasks.py

import pandas as pd
from sqlalchemy import create_engine, Table
import os
from prefect import task, get_run_logger
from pathlib import Path
import time
from typing import Tuple, Dict, Any

# --- Konstanten ---
APP_DIR = Path("/app")
DATA_DIR = APP_DIR / "data"
ORDERS_PATH = DATA_DIR / "orders.parquet"
TIPS_PATH = DATA_DIR / "tips_public.csv"
ORDER_PRODUCTS_PATH = DATA_DIR / "order_products_denormalized.csv"

OLTP_HOST = os.getenv("OLTP_DB_HOST", "db") 
OLTP_PORT = os.getenv("OLTP_DB_PORT", "5432")
OLTP_DBNAME = os.getenv("OLTP_DB_NAME", "oltp")
OLTP_USER = os.getenv("OLTP_DB_USER", "datata1")
OLTP_PASSWORD = "devpassword"
TARGET_SCHEMA = "public"

from oltp_schema import (
    departments_table, aisles_table, products_table, users_table,
    orders_table, order_products_table # Importiere auch die für load_fact_tables
)

# --- Hilfsfunktion (optional, aber nützlich) ---
def get_dtype_map(table: Table) -> Dict[str, Any]:
    """Erstellt ein Dictionary für das dtype-Argument von pandas.to_sql
       aus einem SQLAlchemy Table-Objekt."""
    return {col.name: col.type for col in table.c}

# --- Hilfsfunktion für DB Engine (optional, vermeidet Code-Wiederholung) ---
def get_oltp_engine():
    if not OLTP_PASSWORD:
        raise ValueError("OLTP Database Password not set in environment!")
    db_url = f"postgresql+psycopg2://{OLTP_USER}:{OLTP_PASSWORD}@{OLTP_HOST}:{OLTP_PORT}/{OLTP_DBNAME}"
    return create_engine(db_url)

# --- Task 1: Dateien einlesen ---
@task(name="Read OLTP Source Files")
def read_source_files(
    orders_path: Path = ORDERS_PATH,
    tips_path: Path = TIPS_PATH,
    order_products_path: Path = ORDER_PRODUCTS_PATH,
) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]: # Gibt Tuple mit DataFrames zurück
    """Liest Rohdaten-Dateien in Pandas DataFrames."""
    logger = get_run_logger()
    logger.info("Reading source files for OLTP load...")

    if not all([orders_path.is_file(), tips_path.is_file(), order_products_path.is_file()]):
        missing = [p for p in [orders_path, tips_path, order_products_path] if not p.is_file()]
        logger.error(f"Source file(s) not found: {missing}")
        raise FileNotFoundError(f"Source file(s) not found: {missing}")

    try:
        orders_df_raw = pd.read_parquet(orders_path)
        tips_df_raw = pd.read_csv(tips_path, index_col=False)
        order_products_df_raw = pd.read_csv(order_products_path, index_col=False)

        # Spaltennamen bereinigen
        tips_df_raw.columns = [col.strip().lower().replace(' ', '_') for col in tips_df_raw.columns]
        if 'unnamed:_0' in tips_df_raw.columns: # Falls doch ein Index gelesen wurde
             tips_df_raw = tips_df_raw.rename(columns={'unnamed:_0': 'csv_index_tips'}).drop(columns=['csv_index_tips'])
        if 'unnamed:_0' in order_products_df_raw.columns:
             order_products_df_raw = order_products_df_raw.rename(columns={'unnamed:_0': 'csv_index_op'}).drop(columns=['csv_index_op'])

        logger.info(f"Read {len(orders_df_raw)} orders, {len(tips_df_raw)} tips, {len(order_products_df_raw)} order products.")
        return orders_df_raw, tips_df_raw, order_products_df_raw
    except Exception as e:
        logger.error(f"Error reading source files: {e}", exc_info=True)
        raise e

# --- Task 2: Dimensionstabellen laden ---
@task(name="Load OLTP Dimension Tables")
def load_dimension_tables(
    orders_df: pd.DataFrame,
    order_products_df: pd.DataFrame
) -> bool: 
    """Extrahiert und lädt dimensionale Tabellen (users, products, aisles, departments)."""
    logger = get_run_logger()
    logger.info("Loading OLTP dimension tables...")
    engine = get_oltp_engine()
    try:
        with engine.connect() as connection: 
            logger.info("Extracting and loading departments...")
            departments_df = order_products_df[['department_id', 'department']].drop_duplicates().dropna(subset=['department_id'])
            dtype_map_dept = get_dtype_map(departments_table) 
            departments_df.to_sql(
                'departments', engine, schema=TARGET_SCHEMA, if_exists='append', index=False,
                dtype=dtype_map_dept 
            )
            logger.info(f"Loaded {len(departments_df)} departments.")

            # Aisles (analog)
            logger.info("Extracting and loading aisles...")
            aisles_df = order_products_df[['aisle_id', 'aisle']].drop_duplicates().dropna(subset=['aisle_id'])
            dtype_map_aisle = get_dtype_map(aisles_table) 
            aisles_df.to_sql(
                'aisles', engine, schema=TARGET_SCHEMA, if_exists='append', index=False,
                dtype=dtype_map_aisle 
            )
            logger.info(f"Loaded {len(aisles_df)} aisles.")

            # Products (analog)
            logger.info("Extracting and loading products...")
            products_df = order_products_df[['product_id', 'product_name', 'aisle_id', 'department_id']].drop_duplicates().dropna(subset=['product_id'])
            dtype_map_prod = get_dtype_map(products_table) 
            products_df.to_sql(
                'products', engine, schema=TARGET_SCHEMA, if_exists='append', index=False, chunksize=10000,
                dtype=dtype_map_prod 
            )
            logger.info(f"Loaded {len(products_df)} products.")

            # Users (analog)
            logger.info("Extracting and loading users...")
            users_df = pd.DataFrame(orders_df['user_id'].unique(), columns=['user_id']).dropna()
            dtype_map_user = get_dtype_map(users_table) 
            users_df.to_sql(
                'users', engine, schema=TARGET_SCHEMA, if_exists='append', index=False,
                dtype=dtype_map_user 
            )
            logger.info(f"Loaded {len(users_df)} users.")
            return True

    except Exception as e:
        logger.error(f"Error loading dimension tables: {e}", exc_info=True)
        raise e
    finally:
        if engine:
            engine.dispose()
            logger.info("Dimension load connection engine disposed.")

# --- Task 3: Faktentabellen laden ---
@task(name="Load OLTP Fact Tables")
def load_fact_tables(
    orders_df_raw: pd.DataFrame,
    tips_df_raw: pd.DataFrame,
    order_products_df_raw: pd.DataFrame
) -> bool: # Gibt True bei Erfolg zurück
    """Bereitet vor und lädt Faktentabellen (orders, order_products) mit expliziten Dtypes."""
    logger = get_run_logger()
    logger.info("Loading OLTP fact tables with explicit dtypes...")
    engine = get_oltp_engine() # Hole die Engine
    try:
        with engine.connect() as connection: # Nutze die Verbindung
            logger.info("Connected to OLTP DB for fact loading.")

            # --- Orders (mit Tip Info) ---
            logger.info("Preparing and loading orders...")
            # Sichere Typkonvertierung für den Merge-Key
            try:
                orders_df_raw['order_id'] = orders_df_raw['order_id'].astype(str)
                # Prüfe ob Spalte in tips_df existiert
                if 'order_id' in tips_df_raw.columns:
                    tips_df_raw['order_id'] = tips_df_raw['order_id'].astype(str)
                    orders_df_merged = pd.merge(orders_df_raw, tips_df_raw[['order_id', 'tip']], on='order_id', how='left')
                else:
                    logger.warning("Spalte 'order_id' nicht in tips_df_raw gefunden. Füge 'tip' als None hinzu.")
                    orders_df_merged = orders_df_raw.copy()
                    orders_df_merged['tip'] = None
            except KeyError as e:
                 logger.error(f"Fehlende 'order_id' Spalte für Merge: {e}")
                 raise e

            # Selektiere und bereinige Spalten für die Zieltabelle
            orders_to_load = orders_df_merged[['order_id', 'user_id', 'order_date', 'tip']].copy()
            # Konvertiere 'tip' sicher zu Boolean (berücksichtigt Strings und Booleans)
            orders_to_load['tip'] = orders_to_load['tip'].map(
                {'True': True, 'False': False, True: True, False: False}
            ).astype('boolean') # Pandas Boolean type (unterstützt None/NA)
            orders_to_load = orders_to_load.rename(columns={'tip': 'tip_given'})

            # Hole das dtype Mapping vom SQLAlchemy Modell
            dtype_map_orders = get_dtype_map(orders_table)
            logger.info(f"Using dtype map for orders: {dtype_map_orders}")

            # Schreibe in DB mit expliziten Typen
            orders_to_load.to_sql(
                'orders',
                engine,
                schema=TARGET_SCHEMA,
                if_exists='append',
                index=False,
                chunksize=10000,
                dtype=dtype_map_orders # <-- dtype Mapping übergeben
            )
            logger.info(f"Loaded {len(orders_to_load)} orders.")

            # --- Order Products (Zwischentabelle) ---
            logger.info("Preparing and loading order_products...")
            order_products_to_load = order_products_df_raw[['order_id', 'product_id', 'add_to_cart_order']].copy()

            # Hole das dtype Mapping vom SQLAlchemy Modell
            dtype_map_op = get_dtype_map(order_products_table)
            logger.info(f"Using dtype map for order_products: {dtype_map_op}")

            # Schreibe in DB mit expliziten Typen
            order_products_to_load.to_sql(
                'order_products',
                engine,
                schema=TARGET_SCHEMA,
                if_exists='append',
                index=False,
                chunksize=50000,
                dtype=dtype_map_op # <-- dtype Mapping übergeben
            )
            logger.info(f"Loaded {len(order_products_to_load)} order_products line items.")

            logger.info("Fact tables loaded successfully.")
            return True

    except Exception as e:
        logger.error(f"Error loading fact tables: {e}", exc_info=True)
        raise e
    finally:
        if engine:
            engine.dispose()
            logger.info("Fact load connection engine disposed.")