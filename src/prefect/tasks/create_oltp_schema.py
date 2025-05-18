from sqlalchemy import create_engine
import os
from prefect import task, get_run_logger

from oltp_schema import oltp_metadata 

OLTP_HOST = os.getenv("OLTP_DB_HOST", "db")
OLTP_PORT = os.getenv("OLTP_DB_PORT", "5432")
OLTP_DBNAME = os.getenv("OLTP_DB_NAME", "oltp")
OLTP_USER = os.getenv("OLTP_DB_USER", "datata1")
OLTP_PASSWORD = "devpassword"
TARGET_SCHEMA = "public" 

@task(name="Create OLTP Schema (if not exists)")
def create_oltp_schema():
    """Stellt sicher, dass alle im MetaData-Objekt definierten Tabellen existieren."""
    logger = get_run_logger()
    logger.info(f"Attempting to create OLTP schema '{TARGET_SCHEMA}' if needed...")

    if not OLTP_PASSWORD:
        logger.error("OLTP Database Password not set!")
        raise ValueError("OLTP Database Password not set")

    db_url = f"postgresql+psycopg2://{OLTP_USER}:{OLTP_PASSWORD}@{OLTP_HOST}:{OLTP_PORT}/{OLTP_DBNAME}"
    engine = None
    try:
        engine = create_engine(db_url)
        with engine.connect() as connection:
            logger.info(f"Connected to OLTP DB. Checking/Creating tables...")
            oltp_metadata.create_all(bind=engine, checkfirst=True)
            logger.info(f"Schema check/creation complete for schema '{TARGET_SCHEMA}'.")
            return True
    except Exception as e:
        logger.error(f"Error during schema creation/check: {e}", exc_info=True)
        raise e
    finally:
        if engine:
            engine.dispose()
            logger.info("Schema creation connection engine disposed.")